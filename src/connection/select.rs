use crate::database::{DatabaseRecord, DatabaseValue};
use crate::error::{CacheError, ErrorCode, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

pub trait DatabaseReader: Send + 'static {
    fn select(&mut self, query: &str, values: &[DatabaseValue]) -> Result<Vec<DatabaseRecord>>;

    fn count(&mut self, query: &str, values: &[DatabaseValue]) -> Result<usize> {
        Ok(self.select(query, values)?.len())
    }

    fn select_each(
        &mut self,
        query: &str,
        values: &[DatabaseValue],
        consumer: &mut dyn FnMut(DatabaseRecord) -> Result<()>,
    ) -> Result<()> {
        for record in self.select(query, values)? {
            consumer(record)?;
        }
        Ok(())
    }
}

struct SelectConnection {
    assigned: AtomicBool,
    reader: Mutex<Box<dyn DatabaseReader>>,
}

struct PoolState {
    connections: VecDeque<Arc<SelectConnection>>,
}

struct PoolInner {
    state: Mutex<PoolState>,
    available: Condvar,
}

/// Select Connection Pool. The current runtime inserts exactly one physical
/// Connection; acquire/release stays unchanged when the Pool grows later.
#[derive(Clone)]
pub struct SelectConnectionPool {
    inner: Arc<PoolInner>,
}

pub struct SelectConnectionLease {
    connection: Arc<SelectConnection>,
    pool: Arc<PoolInner>,
    released: bool,
}

impl SelectConnectionPool {
    pub fn new<R: DatabaseReader>(readers: Vec<R>) -> Result<Self> {
        if readers.is_empty() {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "Select Connection Pool requires at least one Connection",
            ));
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                state: Mutex::new(PoolState {
                    connections: readers
                        .into_iter()
                        .map(|reader| {
                            Arc::new(SelectConnection {
                                assigned: AtomicBool::new(false),
                                reader: Mutex::new(Box::new(reader) as Box<dyn DatabaseReader>),
                            })
                        })
                        .collect(),
                }),
                available: Condvar::new(),
            }),
        })
    }

    pub fn size(&self) -> Result<usize> {
        self.inner
            .state
            .lock()
            .map(|state| state.connections.len())
            .map_err(|_| {
                CacheError::new(
                    ErrorCode::LockPoisoned,
                    "Select Connection Pool is unavailable",
                )
            })
    }

    pub fn acquire(&self) -> Result<SelectConnectionLease> {
        let mut state = self.inner.state.lock().map_err(|_| {
            CacheError::new(
                ErrorCode::LockPoisoned,
                "Select Connection Pool is unavailable",
            )
        })?;
        let position = loop {
            if let Some(position) = state.connections.iter().position(|connection| {
                connection
                    .assigned
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            }) {
                break position;
            }
            state = self.inner.available.wait(state).map_err(|_| {
                CacheError::new(
                    ErrorCode::LockPoisoned,
                    "Select Connection Pool wait failed",
                )
            })?;
        };
        let connection = state
            .connections
            .remove(position)
            .ok_or_else(|| unavailable("Select Connection is unavailable"))?;
        state.connections.push_back(Arc::clone(&connection));
        Ok(SelectConnectionLease {
            connection,
            pool: Arc::clone(&self.inner),
            released: false,
        })
    }

    pub fn release(&self, mut lease: SelectConnectionLease) -> Result<()> {
        if !Arc::ptr_eq(&self.inner, &lease.pool) {
            return Err(unavailable("Select Connection belongs to another Pool"));
        }
        if !lease.connection.assigned.swap(false, Ordering::AcqRel) {
            return Err(unavailable("Select Connection is not assigned"));
        }
        lease.released = true;
        self.inner.available.notify_one();
        Ok(())
    }
}

impl SelectConnectionLease {
    pub fn connection(&self) -> Result<MutexGuard<'_, Box<dyn DatabaseReader>>> {
        self.connection
            .reader
            .lock()
            .map_err(|_| unavailable("Select Connection is unavailable"))
    }
}

impl Drop for SelectConnectionLease {
    fn drop(&mut self) {
        if !self.released && self.connection.assigned.swap(false, Ordering::AcqRel) {
            self.pool.available.notify_one();
        }
    }
}

fn unavailable(message: &str) -> CacheError {
    CacheError::new(ErrorCode::InvalidStateTransition, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Reader;

    impl DatabaseReader for Reader {
        fn select(
            &mut self,
            _query: &str,
            _values: &[DatabaseValue],
        ) -> Result<Vec<DatabaseRecord>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn borrows_and_returns_the_single_select_connection() {
        let pool = SelectConnectionPool::new(vec![Reader]).unwrap();
        let lease = pool.acquire().unwrap();
        pool.release(lease).unwrap();
        let lease = pool.acquire().unwrap();
        pool.release(lease).unwrap();
    }

    #[test]
    fn rejects_a_lease_from_another_pool() {
        let first = SelectConnectionPool::new(vec![Reader]).unwrap();
        let second = SelectConnectionPool::new(vec![Reader]).unwrap();
        let lease = first.acquire().unwrap();
        assert!(second.release(lease).is_err());
    }

    #[test]
    fn lends_every_prepared_read_connection() {
        let pool = SelectConnectionPool::new((0..9).map(|_| Reader).collect()).unwrap();
        let leases = (0..9).map(|_| pool.acquire().unwrap()).collect::<Vec<_>>();
        for lease in leases {
            pool.release(lease).unwrap();
        }
        assert_eq!(pool.size().unwrap(), 9);
    }

    #[test]
    fn returns_connections_safely_under_competition() {
        let pool = SelectConnectionPool::new((0..9).map(|_| Reader).collect()).unwrap();
        let start = Arc::new(std::sync::Barrier::new(20));
        let threads = (0..20)
            .map(|_| {
                let pool = pool.clone();
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..100 {
                        let lease = pool.acquire().unwrap();
                        pool.release(lease).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(pool.size().unwrap(), 9);
    }
}
