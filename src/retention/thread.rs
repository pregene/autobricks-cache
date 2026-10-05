use crate::cache::Cache;
use crate::error::{CacheError, ErrorCode, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const DRAIN_INTERVAL: Duration = Duration::from_secs(60);

/// One instance is shared by the entire Cache system.
pub struct RetentionDrainThread {
    caches: Arc<Mutex<Vec<Weak<Cache>>>>,
    running: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl RetentionDrainThread {
    pub fn new() -> Self {
        Self {
            caches: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            wake: Arc::new((Mutex::new(()), Condvar::new())),
            thread: None,
        }
    }

    pub fn register(&self, cache: &Arc<Cache>) -> Result<()> {
        let mut caches = self.caches.lock().map_err(|_| {
            CacheError::new(
                ErrorCode::LockPoisoned,
                "Retention Cache registry is unavailable",
            )
        })?;
        caches.push(Arc::downgrade(cache));
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        if self.thread.is_some() {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "Retention Drain Thread is already running",
            ));
        }
        self.running.store(true, Ordering::Release);
        let caches = Arc::clone(&self.caches);
        let running = Arc::clone(&self.running);
        let wake = Arc::clone(&self.wake);
        let handle = thread::Builder::new()
            .name("autobricks-cache-retention".to_owned())
            .spawn(move || run(caches, running, wake))
            .map_err(|error| {
                CacheError::new(
                    ErrorCode::ThreadSpawnFailed,
                    format!("failed to start Retention Drain Thread: {error}"),
                )
            })?;
        self.thread = Some(handle);
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        self.running.store(false, Ordering::Release);
        self.wake.1.notify_all();
        thread.join().map_err(|_| {
            CacheError::new(
                ErrorCode::InternalError,
                "Retention Drain Thread stopped unexpectedly",
            )
        })?
    }
}

impl Default for RetentionDrainThread {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RetentionDrainThread {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn run(
    caches: Arc<Mutex<Vec<Weak<Cache>>>>,
    running: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
) -> Result<()> {
    while running.load(Ordering::Acquire) {
        let state = wake
            .0
            .lock()
            .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Retention wait lock failed"))?;
        let (_state, _) = wake
            .1
            .wait_timeout(state, DRAIN_INTERVAL)
            .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Retention wait failed"))?;
        if !running.load(Ordering::Acquire) {
            break;
        }
        let active = {
            let mut registry = caches.lock().map_err(|_| {
                CacheError::new(
                    ErrorCode::LockPoisoned,
                    "Retention Cache registry is unavailable",
                )
            })?;
            let active = registry
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            registry.retain(|cache| cache.strong_count() > 0);
            active
        };
        for cache in active {
            cache.drain(DRAIN_INTERVAL.as_secs())?;
        }
    }
    Ok(())
}
