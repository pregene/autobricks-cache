mod mariadb;
mod postgresql;
mod record;
mod sqlite;

use crate::connection::{ConnectionDefinition, ConnectionRuntime};
use crate::error::{CacheError, ErrorCode, Result};
pub use record::{DatabaseRecord, DatabaseValue};
use std::path::Path;

pub(crate) struct Database;

impl Database {
    pub(crate) fn open(
        definition: &ConnectionDefinition,
        queue_directory: &Path,
    ) -> Result<ConnectionRuntime> {
        match definition.driver.as_str() {
            "postgresql" => postgresql::PostgresDatabase::open(definition, queue_directory),
            "mariadb" => mariadb::MariaDbDatabase::open(definition, queue_directory),
            "sqlite" => sqlite::SqliteDatabase::open(definition, queue_directory),
            driver => Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                format!("Database Driver is not implemented: {driver}"),
            )),
        }
    }
}
