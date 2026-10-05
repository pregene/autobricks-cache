//! Connection-owned database worker thread.

use super::thread::BaseThread;
use crate::error::{CacheError, ErrorCode, Result};
use crate::operation::Queue;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// Database-specific execution is injected into the generic worker.
pub trait DatabaseWriter: Send + 'static {
    /// The call must return `Ok` only after the database operation has
    /// completed successfully. A failed item remains at the Queue head.
    fn execute(&mut self, object: &[u8]) -> Result<()>;
}

/// Owns one Connection's Queue consumer lifecycle.
pub struct DBWorkerThread {
    connection_id: String,
    base: Arc<BaseThread>,
    queue: Arc<Queue>,
    writer: Option<Box<dyn DatabaseWriter>>,
    thread: Option<JoinHandle<Result<()>>>,
    last_error: Arc<Mutex<Option<CacheError>>>,
}

impl DBWorkerThread {
    pub fn new(
        connection_id: impl Into<String>,
        queue: Arc<Queue>,
        writer: impl DatabaseWriter,
    ) -> Self {
        Self {
            connection_id: connection_id.into(),
            base: Arc::new(BaseThread::new()),
            queue,
            writer: Some(Box::new(writer)),
            thread: None,
            last_error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn start(&mut self) -> Result<()> {
        if self.thread.is_some() {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "DB Worker Thread is already running",
            ));
        }
        let writer = self.writer.take().ok_or_else(|| {
            CacheError::new(
                ErrorCode::InvalidStateTransition,
                "DB Connection is unavailable",
            )
        })?;
        self.base.running().store(true, Ordering::Release);
        let base = Arc::clone(&self.base);
        let queue = Arc::clone(&self.queue);
        let last_error = Arc::clone(&self.last_error);
        let thread_name = format!("autobricks-cache-{}-db", self.connection_id);

        match thread::Builder::new()
            .name(thread_name)
            .spawn(move || run(base, queue, writer, last_error))
        {
            Ok(thread) => {
                self.thread = Some(thread);
                Ok(())
            }
            Err(error) => {
                self.base.running().store(false, Ordering::Release);
                Err(CacheError::new(
                    ErrorCode::ThreadSpawnFailed,
                    format!("failed to start DB Worker Thread: {error}"),
                ))
            }
        }
    }

    pub fn stop(&mut self) -> Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        {
            let _state = self.base.mutex().lock().map_err(|_| {
                CacheError::new(
                    ErrorCode::LockPoisoned,
                    "DB Worker Thread state is unavailable",
                )
            })?;
            self.base.running().store(false, Ordering::Release);
            self.base.notify_all();
        }
        thread.join().map_err(|_| {
            CacheError::new(
                ErrorCode::InternalError,
                "DB Worker Thread stopped unexpectedly",
            )
        })?
    }

    pub fn signal(&self) -> Result<()> {
        self.base.notify_one();
        Ok(())
    }

    pub fn take_last_error(&self) -> Result<Option<CacheError>> {
        self.last_error
            .lock()
            .map(|mut error| error.take())
            .map_err(|_| {
                CacheError::new(
                    ErrorCode::LockPoisoned,
                    "DB Worker error state is unavailable",
                )
            })
    }
}

impl Drop for DBWorkerThread {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn run(
    base: Arc<BaseThread>,
    queue: Arc<Queue>,
    mut writer: Box<dyn DatabaseWriter>,
    last_error: Arc<Mutex<Option<CacheError>>>,
) -> Result<()> {
    loop {
        if !base.running().load(Ordering::Acquire) {
            break;
        }
        let observed_wake = base.wake_sequence();

        let Some(object) = queue.peek()? else {
            wait(&base, observed_wake)?;
            continue;
        };

        let outcome = writer.execute(&object);
        match outcome {
            Ok(()) => queue.pop()?,
            Err(error) => {
                let mut state = last_error.lock().map_err(|_| {
                    CacheError::new(
                        ErrorCode::LockPoisoned,
                        "DB Worker error state is unavailable",
                    )
                })?;
                *state = Some(error);
                drop(state);
                // Keep the failed item at the Queue head. A later signal can
                // retry it after the Connection has recovered.
                wait(&base, observed_wake)?;
            }
        }
    }
    Ok(())
}

fn wait(base: &BaseThread, observed_wake: u64) -> Result<()> {
    let mut state = base.mutex().lock().map_err(|_| {
        CacheError::new(
            ErrorCode::LockPoisoned,
            "DB Worker Thread state is unavailable",
        )
    })?;
    while base.running().load(Ordering::Acquire) && base.wake_sequence() == observed_wake {
        state = base.condition().wait(state).map_err(|_| {
            CacheError::new(ErrorCode::LockPoisoned, "DB Worker Thread wait failed")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct CountingExecutor {
        count: Arc<AtomicUsize>,
        fail: bool,
    }

    impl DatabaseWriter for CountingExecutor {
        fn execute(&mut self, _object: &[u8]) -> Result<()> {
            self.count.fetch_add(1, AtomicOrdering::SeqCst);
            if self.fail {
                Err(CacheError::new(ErrorCode::DbNativeError, "test DB failure"))
            } else {
                Ok(())
            }
        }
    }

    fn test_queue(name: &str) -> (std::path::PathBuf, Arc<Queue>) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "autobricks-worker-{name}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let queue = Arc::new(Queue::open(&directory, "database-write").unwrap());
        (directory, queue)
    }

    fn wait_for_count(count: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if count.load(AtomicOrdering::SeqCst) >= expected {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("worker did not process expected item");
    }

    #[test]
    fn removes_item_only_after_success() {
        let (directory, queue) = test_queue("success");
        queue.push(b"insert").unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let mut worker = DBWorkerThread::new(
            "connection-a",
            Arc::clone(&queue),
            CountingExecutor {
                count: Arc::clone(&count),
                fail: false,
            },
        );
        worker.start().unwrap();
        worker.signal().unwrap();
        wait_for_count(&count, 1);
        worker.stop().unwrap();
        assert_eq!(queue.count().unwrap(), 0);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn leaves_failed_item_at_queue_head() {
        let (directory, queue) = test_queue("failure");
        queue.push(b"update").unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let mut worker = DBWorkerThread::new(
            "connection-b",
            Arc::clone(&queue),
            CountingExecutor {
                count: Arc::clone(&count),
                fail: true,
            },
        );
        worker.start().unwrap();
        worker.signal().unwrap();
        wait_for_count(&count, 1);
        worker.stop().unwrap();
        assert_eq!(queue.count().unwrap(), 1);
        assert!(worker.take_last_error().unwrap().is_some());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
