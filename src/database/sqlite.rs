use super::{DatabaseRecord, DatabaseValue};
use crate::connection::{
    ConnectionDefinition, ConnectionRuntime, DatabaseReader, DatabaseWriter, SqliteCacheMode,
    SqliteJournalMode, SqliteMutexMode, SqliteOpenMode, SqliteOptions, SqliteSynchronous,
};
use crate::error::{CacheError, ErrorCode, Result};
use crate::operation;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection, OpenFlags};
use std::path::Path;
use std::time::Duration;

pub struct SqliteDatabase;

pub struct SqliteReader {
    connection: Connection,
}

pub struct SqliteWriter {
    connection: Connection,
}

impl SqliteDatabase {
    pub fn open(
        definition: &ConnectionDefinition,
        queue_directory: &Path,
    ) -> Result<ConnectionRuntime> {
        definition.validate()?;
        let mut readers = Vec::with_capacity(definition.select_connections()?);
        for _ in 0..definition.select_connections()? {
            readers.push(SqliteReader {
                connection: connect(definition)?,
            });
        }
        let writer = SqliteWriter {
            connection: connect(definition)?,
        };
        ConnectionRuntime::open_pool(&definition.connection_id, queue_directory, readers, writer)
    }
}

impl DatabaseReader for SqliteReader {
    fn select(&mut self, query: &str, values: &[DatabaseValue]) -> Result<Vec<DatabaseRecord>> {
        let parameters = values
            .iter()
            .map(sqlite_value)
            .collect::<Result<Vec<_>>>()?;
        let mut statement = self
            .connection
            .prepare(query)
            .map_err(|error| db_error(ErrorCode::SelectFailed, "prepare SELECT", error))?;
        let names = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let rows = statement
            .query_map(params_from_iter(parameters.iter()), |row| {
                decode_row(row, &names)
            })
            .map_err(|error| db_error(ErrorCode::SelectFailed, "SELECT", error))?;
        rows.map(|record| {
            record.map_err(|error| db_error(ErrorCode::RecordDecodingFailed, "decode row", error))
        })
        .collect()
    }

    fn count(&mut self, query: &str, values: &[DatabaseValue]) -> Result<usize> {
        let query = query.trim().trim_end_matches(';');
        let count_query =
            format!("SELECT COUNT(*) AS record_count FROM ({query}) AS autobricks_cache_count");
        let parameters = values
            .iter()
            .map(sqlite_value)
            .collect::<Result<Vec<_>>>()?;
        let count: i64 = self
            .connection
            .query_row(&count_query, params_from_iter(parameters.iter()), |row| {
                row.get(0)
            })
            .map_err(|error| db_error(ErrorCode::SelectFailed, "COUNT", error))?;
        usize::try_from(count).map_err(|_| {
            CacheError::new(
                ErrorCode::RecordDecodingFailed,
                "SQLite SELECT COUNT is outside usize range",
            )
        })
    }
}

impl DatabaseWriter for SqliteWriter {
    fn execute(&mut self, object: &[u8]) -> Result<()> {
        let operation = operation::decode(object)?;
        let parameters = operation
            .values
            .iter()
            .map(sqlite_value)
            .collect::<Result<Vec<_>>>()?;
        self.connection
            .execute(&operation.query, params_from_iter(parameters.iter()))
            .map(|_| ())
            .map_err(|error| db_error(ErrorCode::DbNativeError, "WRITE", error))
    }
}

fn connect(definition: &ConnectionDefinition) -> Result<Connection> {
    let options = definition.opt.as_ref().ok_or_else(|| {
        CacheError::new(
            ErrorCode::InvalidConnectionRuntimeConfig,
            "opt is required for SQLite Connections",
        )
    })?;
    let connection = Connection::open_with_flags(&definition.database, open_flags(options))
        .map_err(|error| db_error(ErrorCode::DbNativeError, "CONNECT", error))?;
    connection
        .pragma_update(None, "cipher_log_level", "NONE")
        .map_err(|error| db_error(ErrorCode::DbNativeError, "disable cipher logging", error))?;
    if let Some(key) = &options.key {
        connection
            .pragma_update(None, "key", key)
            .map_err(|error| db_error(ErrorCode::DbNativeError, "apply key", error))?;
    }
    let cipher_version: String = connection
        .query_row("PRAGMA cipher_version", [], |row| row.get(0))
        .map_err(|error| db_error(ErrorCode::DbNativeError, "verify SQLCipher", error))?;
    if cipher_version.trim().is_empty() {
        return Err(CacheError::new(
            ErrorCode::InvalidConnectionRuntimeConfig,
            "SQLCipher support is unavailable",
        ));
    }
    connection
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| db_error(ErrorCode::DbNativeError, "verify database key", error))?;
    connection
        .busy_timeout(Duration::from_millis(definition.query_timeout_ms))
        .map_err(|error| db_error(ErrorCode::DbNativeError, "configure busy timeout", error))?;
    connection
        .execute_batch(&format!(
            "PRAGMA foreign_keys = {}; PRAGMA journal_mode = {}; PRAGMA synchronous = {};",
            if options.foreign_keys { "ON" } else { "OFF" },
            journal_mode(options.journal_mode),
            synchronous(options.synchronous),
        ))
        .map_err(|error| db_error(ErrorCode::DbNativeError, "configure Connection", error))?;
    Ok(connection)
}

fn open_flags(options: &SqliteOptions) -> OpenFlags {
    let mut flags = match options.open_mode {
        SqliteOpenMode::Only => OpenFlags::SQLITE_OPEN_READ_ONLY,
        SqliteOpenMode::Write => OpenFlags::SQLITE_OPEN_READ_WRITE,
        SqliteOpenMode::WriteCreate => {
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
        }
    };
    flags |= match options.mutex {
        SqliteMutexMode::Full => OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        SqliteMutexMode::No => OpenFlags::SQLITE_OPEN_NO_MUTEX,
    };
    if options.uri {
        flags |= OpenFlags::SQLITE_OPEN_URI;
    }
    flags |= match options.cache {
        SqliteCacheMode::Default => OpenFlags::empty(),
        SqliteCacheMode::Shared => OpenFlags::SQLITE_OPEN_SHARED_CACHE,
        SqliteCacheMode::Private => OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
    };
    flags
}

fn journal_mode(mode: SqliteJournalMode) -> &'static str {
    match mode {
        SqliteJournalMode::Delete => "DELETE",
        SqliteJournalMode::Truncate => "TRUNCATE",
        SqliteJournalMode::Persist => "PERSIST",
        SqliteJournalMode::Memory => "MEMORY",
        SqliteJournalMode::Wal => "WAL",
        SqliteJournalMode::Off => "OFF",
    }
}

fn synchronous(mode: SqliteSynchronous) -> &'static str {
    match mode {
        SqliteSynchronous::Off => "OFF",
        SqliteSynchronous::Normal => "NORMAL",
        SqliteSynchronous::Full => "FULL",
        SqliteSynchronous::Extra => "EXTRA",
    }
}

fn sqlite_value(value: &DatabaseValue) -> Result<Value> {
    match value {
        DatabaseValue::Null => Ok(Value::Null),
        DatabaseValue::Boolean(value) => Ok(Value::Integer(i64::from(*value))),
        DatabaseValue::Signed(value) => Ok(Value::Integer(*value)),
        DatabaseValue::Unsigned(value) => i64::try_from(*value).map(Value::Integer).map_err(|_| {
            CacheError::new(
                ErrorCode::RecordDecodingFailed,
                "SQLite integer exceeds i64",
            )
        }),
        DatabaseValue::Float(value) => Ok(Value::Real(*value)),
        DatabaseValue::Text(value) => Ok(Value::Text(value.clone())),
        DatabaseValue::Bytes(value) => Ok(Value::Blob(value.clone())),
    }
}

fn decode_row(row: &rusqlite::Row<'_>, names: &[String]) -> rusqlite::Result<DatabaseRecord> {
    names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let value = match row.get_ref(index)? {
                ValueRef::Null => DatabaseValue::Null,
                ValueRef::Integer(value) => DatabaseValue::Signed(value),
                ValueRef::Real(value) => DatabaseValue::Float(value),
                ValueRef::Text(value) => {
                    DatabaseValue::Text(String::from_utf8_lossy(value).into_owned())
                }
                ValueRef::Blob(value) => DatabaseValue::Bytes(value.to_vec()),
            };
            Ok((name.clone(), value))
        })
        .collect()
}

fn db_error(code: ErrorCode, action: &str, error: rusqlite::Error) -> CacheError {
    CacheError::new(code, format!("SQLite {action} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionDefinition;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sqlcipher_encrypts_and_requires_the_configured_key() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "autobricks-cache-sqlcipher-{}-{nonce}.sqlite3",
            std::process::id()
        ));
        let keyed_definition = definition(&path, Some("test-only-sqlcipher-key"));
        let connection = connect(&keyed_definition).expect("create encrypted database");
        connection
            .execute("CREATE TABLE encrypted_test (id INTEGER PRIMARY KEY)", [])
            .expect("create encrypted table");
        drop(connection);

        let header = fs::read(&path).expect("read encrypted database");
        assert!(!header.starts_with(b"SQLite format 3\0"));
        assert!(connect(&definition(&path, None)).is_err());
        let reopened = connect(&keyed_definition).expect("reopen encrypted database");
        let count: i64 = reopened
            .query_row("SELECT COUNT(*) FROM encrypted_test", [], |row| row.get(0))
            .expect("read encrypted table");
        assert_eq!(count, 0);

        for suffix in ["", "-wal", "-shm"] {
            let _ = fs::remove_file(format!("{}{}", path.display(), suffix));
        }
    }

    fn definition(path: &Path, key: Option<&str>) -> ConnectionDefinition {
        let key = key.map_or_else(|| "null".to_owned(), |value| format!("\"{value}\""));
        ConnectionDefinition::from_json(&format!(
            r#"{{
                "connection_id":"sqlcipher_test", "name":"sqlcipher test",
                "kind":"DATABASE", "driver":"sqlite", "host":"", "port":0,
                "database":"{}", "queue_directory":"test/data/queue",
                "authentication":{{"username":"","password":""}},
                "tls_used":false, "connect_timeout_ms":2000, "query_timeout_ms":5000,
                "pool_used":true, "pool_size":2,
                "opt":{{"key":{key}, "open_mode":"READ_WRITE_CREATE", "mutex":"FULL",
                "uri":false, "cache":"DEFAULT", "journal_mode":"WAL",
                "synchronous":"NORMAL", "foreign_keys":true}}
            }}"#,
            path.display()
        ))
        .expect("parse SQLCipher test definition")
    }
}
