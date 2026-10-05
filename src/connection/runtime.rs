//! Connection-level owner of the shared Queue and DB worker.

use super::{DBWorkerThread, DatabaseReader, DatabaseWriter, SelectConnectionPool};
use crate::database::{DatabaseRecord, DatabaseValue};
use crate::error::Result;
use crate::operation::Queue;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// One runtime is created per Connection and shared by all Cache definitions
/// that reference that Connection.
pub struct ConnectionRuntime {
    connection_id: String,
    select_pool: SelectConnectionPool,
    queue: Arc<Queue>,
    worker: Mutex<DBWorkerThread>,
}

impl ConnectionRuntime {
    pub(crate) fn open(
        connection_id: impl Into<String>,
        queue_directory: &Path,
        reader: impl DatabaseReader,
        writer: impl DatabaseWriter,
    ) -> Result<Self> {
        Self::open_pool(connection_id, queue_directory, vec![reader], writer)
    }

    pub(crate) fn open_pool<R: DatabaseReader>(
        connection_id: impl Into<String>,
        queue_directory: &Path,
        readers: Vec<R>,
        writer: impl DatabaseWriter,
    ) -> Result<Self> {
        let pool_size = readers.len().checked_add(1).ok_or_else(|| {
            crate::error::CacheError::new(
                crate::error::ErrorCode::InvalidConnectionRuntimeConfig,
                "Connection Pool size overflow",
            )
        })?;
        if pool_size < 2 {
            return Err(crate::error::CacheError::new(
                crate::error::ErrorCode::InvalidConnectionRuntimeConfig,
                "pool_size must be at least 2",
            ));
        }
        let connection_id = connection_id.into();
        let queue = Arc::new(Queue::open(queue_directory, &connection_id)?);
        let select_pool = SelectConnectionPool::new(readers)?;
        let worker = DBWorkerThread::new(&connection_id, Arc::clone(&queue), writer);
        Ok(Self {
            connection_id,
            select_pool,
            queue,
            worker: Mutex::new(worker),
        })
    }

    pub(crate) fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub(crate) fn pool_size(&self) -> Result<usize> {
        self.select_pool.size()?.checked_add(1).ok_or_else(|| {
            crate::error::CacheError::new(
                crate::error::ErrorCode::InvalidConnectionRuntimeConfig,
                "Connection Pool size overflow",
            )
        })
    }

    pub(crate) fn select_pool_size(&self) -> Result<usize> {
        self.select_pool.size()
    }

    pub(crate) fn start(&self) -> Result<()> {
        self.worker.lock().map_err(|_| worker_lock_error())?.start()
    }

    pub(crate) fn enqueue(&self, object: &[u8]) -> Result<()> {
        self.queue.push(object)?;
        self.worker
            .lock()
            .map_err(|_| worker_lock_error())?
            .signal()
    }

    pub(crate) fn pending_count(&self) -> Result<u64> {
        self.queue.count()
    }

    /// Borrows a Select Connection from the Pool, executes, and returns it.
    pub(crate) fn select(
        &self,
        query: &str,
        values: &[DatabaseValue],
    ) -> Result<Vec<DatabaseRecord>> {
        let lease = self.select_pool.acquire()?;
        let selected = {
            let mut connection = lease.connection()?;
            connection.select(query, values)
        };
        let released = self.select_pool.release(lease);
        match (selected, released) {
            (Ok(records), Ok(())) => Ok(records),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    pub(crate) fn select_count(&self, query: &str, values: &[DatabaseValue]) -> Result<usize> {
        let lease = self.select_pool.acquire()?;
        let counted = {
            let mut connection = lease.connection()?;
            connection.count(query, values)
        };
        let released = self.select_pool.release(lease);
        match (counted, released) {
            (Ok(count), Ok(())) => Ok(count),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    pub(crate) fn select_each(
        &self,
        query: &str,
        values: &[DatabaseValue],
        consumer: &mut dyn FnMut(DatabaseRecord) -> Result<()>,
    ) -> Result<()> {
        let lease = self.select_pool.acquire()?;
        let selected = {
            let mut connection = lease.connection()?;
            connection.select_each(query, values, consumer)
        };
        let released = self.select_pool.release(lease);
        match (selected, released) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }

    pub(crate) fn stop(&self) -> Result<()> {
        self.worker.lock().map_err(|_| worker_lock_error())?.stop()
    }
}

fn worker_lock_error() -> crate::error::CacheError {
    crate::error::CacheError::new(
        crate::error::ErrorCode::LockPoisoned,
        "DB Worker Thread is unavailable",
    )
}
