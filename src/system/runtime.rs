use crate::cache::Cache;
use crate::connection::ConnectionRuntime;
use crate::definition::CacheDefinition;
use crate::error::{CacheError, ErrorCode, Result};
use crate::retention::RetentionDrainThread;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Owns every Cache and exactly one Retention Drain Thread.
pub struct CacheSystem {
    caches: RwLock<HashMap<String, Arc<Cache>>>,
    retention: RetentionDrainThread,
}

impl CacheSystem {
    pub fn start() -> Result<Self> {
        let mut retention = RetentionDrainThread::new();
        retention.start()?;
        Ok(Self {
            caches: RwLock::new(HashMap::new()),
            retention,
        })
    }

    pub fn register(
        &self,
        definition: CacheDefinition,
        connection: Arc<ConnectionRuntime>,
    ) -> Result<Arc<Cache>> {
        let cache_id = definition.cache_id.clone();
        let cache = Arc::new(Cache::connected(definition, connection)?);
        let mut caches = self.caches.write().map_err(|_| {
            CacheError::new(ErrorCode::LockPoisoned, "Cache registry is unavailable")
        })?;
        if caches.contains_key(&cache_id) {
            return Err(CacheError::new(
                ErrorCode::InvalidCacheDefinition,
                "cache_id is already registered",
            ));
        }
        self.retention.register(&cache)?;
        caches.insert(cache_id, Arc::clone(&cache));
        Ok(cache)
    }

    pub fn cache(&self, cache_id: &str) -> Result<Option<Arc<Cache>>> {
        self.caches
            .read()
            .map(|caches| caches.get(cache_id).cloned())
            .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Cache registry is unavailable"))
    }

    pub fn cache_count(&self) -> Result<usize> {
        self.caches
            .read()
            .map(|caches| caches.len())
            .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Cache registry is unavailable"))
    }
}
