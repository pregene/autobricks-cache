use crate::error::{CacheError, ErrorCode, Result};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub(crate) const DEFAULT_SIZE: usize = 50;

struct PromiseValue<T> {
    result: Option<T>,
    waiting: bool,
}

struct PromiseHandler<T> {
    index: AtomicU64,
    assigned: AtomicBool,
    value: Mutex<PromiseValue<T>>,
    completed: Condvar,
}

struct PromiseManagerState<T> {
    handlers: Vec<Arc<PromiseHandler<T>>>,
    next_generation: u64,
}

struct PromiseManagerInner<T> {
    state: Mutex<PromiseManagerState<T>>,
    shutting_down: AtomicBool,
}

pub(crate) struct PromiseManager<T> {
    inner: Arc<PromiseManagerInner<T>>,
}

impl<T> Clone for PromiseManager<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> PromiseManager<T> {
    pub(crate) fn new(size: usize) -> Result<Self> {
        if size == 0 {
            return Err(CacheError::new(
                ErrorCode::InvalidArgument,
                "Promise Manager size must be greater than zero",
            ));
        }
        let handlers = (0..size)
            .map(|_| {
                Arc::new(PromiseHandler {
                    index: AtomicU64::new(0),
                    assigned: AtomicBool::new(false),
                    value: Mutex::new(PromiseValue {
                        result: None,
                        waiting: false,
                    }),
                    completed: Condvar::new(),
                })
            })
            .collect();
        Ok(Self {
            inner: Arc::new(PromiseManagerInner {
                state: Mutex::new(PromiseManagerState {
                    handlers,
                    next_generation: 1,
                }),
                shutting_down: AtomicBool::new(false),
            }),
        })
    }

    pub(crate) fn create(&self) -> Result<u64> {
        if self.inner.shutting_down.load(Ordering::Acquire) {
            return shutting_down();
        }
        let mut state = self.lock_state()?;
        let position = state
            .handlers
            .iter()
            .position(|handler| !handler.assigned.load(Ordering::Acquire))
            .ok_or_else(|| {
                CacheError::new(
                    ErrorCode::WorkQueueFull,
                    "Promise Manager has no free Handler",
                )
            })?;
        let size = u64::try_from(state.handlers.len()).map_err(|_| exhausted())?;
        let slot = u64::try_from(position).map_err(|_| exhausted())?;
        let index = state
            .next_generation
            .checked_mul(size)
            .and_then(|generation| generation.checked_add(slot))
            .ok_or_else(exhausted)?;
        state.next_generation = state.next_generation.checked_add(1).ok_or_else(exhausted)?;
        let handler = Arc::clone(state.handlers.get(position).ok_or_else(exhausted)?);
        let mut value = lock_value(&handler)?;
        value.result = None;
        value.waiting = false;
        handler.index.store(index, Ordering::Release);
        handler.assigned.store(true, Ordering::Release);
        Ok(index)
    }

    pub(crate) fn wait(&self, index: u64) -> Result<T> {
        let handler = self.handler(index)?;
        let mut value = lock_value(&handler)?;
        if value.waiting {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "Promise Index already has a waiting Thread",
            ));
        }
        value.waiting = true;
        while value.result.is_none() {
            if self.inner.shutting_down.load(Ordering::Acquire) {
                value.waiting = false;
                return shutting_down();
            }
            value = handler.completed.wait(value).map_err(|_| {
                CacheError::new(ErrorCode::LockPoisoned, "Promise Handler wait failed")
            })?;
            self.ensure_current(&handler, index)?;
        }
        value.waiting = false;
        value.result.take().ok_or_else(|| {
            CacheError::new(
                ErrorCode::InternalError,
                "Promise completed without a result",
            )
        })
    }

    pub(crate) fn set(&self, index: u64, result: T) -> Result<()> {
        let handler = self.handler(index)?;
        let mut value = lock_value(&handler)?;
        self.ensure_current(&handler, index)?;
        if value.result.is_some() {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "Promise result is already set",
            ));
        }
        value.result = Some(result);
        handler.completed.notify_one();
        Ok(())
    }

    pub(crate) fn release(&self, index: u64) -> Result<()> {
        let handler = self.handler(index)?;
        let mut value = lock_value(&handler)?;
        self.ensure_current(&handler, index)?;
        if value.waiting {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "Promise cannot be released while waiting",
            ));
        }
        value.result = None;
        handler.assigned.store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn shutdown(&self) -> Result<()> {
        self.inner.shutting_down.store(true, Ordering::Release);
        let handlers = self.lock_state()?.handlers.clone();
        for handler in handlers {
            handler.completed.notify_all();
        }
        Ok(())
    }

    fn handler(&self, index: u64) -> Result<Arc<PromiseHandler<T>>> {
        let state = self.lock_state()?;
        let size = u64::try_from(state.handlers.len()).map_err(|_| exhausted())?;
        let position = usize::try_from(index.checked_rem(size).ok_or_else(exhausted)?)
            .map_err(|_| exhausted())?;
        let handler = Arc::clone(state.handlers.get(position).ok_or_else(exhausted)?);
        self.ensure_current(&handler, index)?;
        Ok(handler)
    }

    fn ensure_current(&self, handler: &PromiseHandler<T>, index: u64) -> Result<()> {
        if !handler.assigned.load(Ordering::Acquire)
            || handler.index.load(Ordering::Acquire) != index
        {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "Promise Index is not assigned",
            ));
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, PromiseManagerState<T>>> {
        self.inner
            .state
            .lock()
            .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Promise Manager is unavailable"))
    }
}

fn lock_value<T>(
    handler: &PromiseHandler<T>,
) -> Result<std::sync::MutexGuard<'_, PromiseValue<T>>> {
    handler
        .value
        .lock()
        .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Promise Handler is unavailable"))
}

fn exhausted() -> CacheError {
    CacheError::new(ErrorCode::InternalError, "Promise Index is exhausted")
}

fn shutting_down<T>() -> Result<T> {
    Err(CacheError::new(
        ErrorCode::OperationRejectedShuttingDown,
        "Promise Manager is shutting down",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_slot_with_a_new_index() {
        let promises = PromiseManager::new(1).unwrap();
        let first = promises.create().unwrap();
        promises.set(first, 7).unwrap();
        assert_eq!(promises.wait(first).unwrap(), 7);
        promises.release(first).unwrap();
        let second = promises.create().unwrap();
        assert_ne!(first, second);
        assert!(promises.wait(first).is_err());
        promises.release(second).unwrap();
    }

    #[test]
    fn set_wakes_waiter() {
        let promises = PromiseManager::new(1).unwrap();
        let index = promises.create().unwrap();
        let waiter = promises.clone();
        let thread = std::thread::spawn(move || waiter.wait(index));
        promises.set(index, 9).unwrap();
        assert_eq!(thread.join().unwrap().unwrap(), 9);
        promises.release(index).unwrap();
    }
}
