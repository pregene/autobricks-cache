use crate::error::{CacheError, ErrorCode, Result};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionDefinition {
    pub connection_id: String,
    pub name: String,
    pub kind: String,
    pub driver: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub queue_directory: String,
    pub authentication: AuthenticationDefinition,
    pub tls_used: bool,
    pub tls: Option<TlsDefinition>,
    pub connect_timeout_ms: u64,
    pub query_timeout_ms: u64,
    pub pool_used: bool,
    pub pool_size: usize,
    pub opt: Option<SqliteOptions>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationDefinition {
    pub username: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TlsDefinition {
    pub ca_file: String,
    pub cert: Option<String>,
    pub key: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SqliteOptions {
    pub key: Option<String>,
    pub open_mode: SqliteOpenMode,
    pub mutex: SqliteMutexMode,
    pub uri: bool,
    pub cache: SqliteCacheMode,
    pub journal_mode: SqliteJournalMode,
    pub synchronous: SqliteSynchronous,
    pub foreign_keys: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqliteOpenMode {
    #[serde(rename = "READ_ONLY")]
    Only,
    #[serde(rename = "READ_WRITE")]
    Write,
    #[serde(rename = "READ_WRITE_CREATE")]
    WriteCreate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SqliteMutexMode {
    Full,
    No,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SqliteCacheMode {
    Default,
    Shared,
    Private,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SqliteJournalMode {
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SqliteSynchronous {
    Off,
    Normal,
    Full,
    Extra,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct ConnectionPoolDefinition {
    pub pool_used: bool,
    pub pool_size: usize,
}

impl ConnectionPoolDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.pool_used && self.pool_size < 2 {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "pool_size must be at least 2 when pool_used is true",
            ));
        }
        Ok(())
    }

    pub fn select_connections(&self) -> Result<usize> {
        self.validate()?;
        if !self.pool_used {
            return Ok(0);
        }
        self.pool_size.checked_sub(1).ok_or_else(|| {
            CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "pool_size cannot provide a SELECT Connection",
            )
        })
    }

    pub fn write_connections(&self) -> Result<usize> {
        self.validate()?;
        Ok(usize::from(self.pool_used))
    }
}

impl ConnectionDefinition {
    pub fn from_json(json: &str) -> Result<Self> {
        let definition: Self = serde_json::from_str(json).map_err(|error| {
            CacheError::new(
                ErrorCode::InvalidJson,
                format!("failed to parse Connection Definition: {error}"),
            )
        })?;
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> Result<()> {
        if self.connection_id.trim().is_empty()
            || self.database.trim().is_empty()
            || self.queue_directory.trim().is_empty()
        {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "Connection fields must not be empty",
            ));
        }
        if self.kind != "DATABASE"
            || !matches!(self.driver.as_str(), "postgresql" | "mariadb" | "sqlite")
        {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "only DATABASE/postgresql, DATABASE/mariadb, and DATABASE/sqlite are supported",
            ));
        }
        if self.driver != "sqlite"
            && (self.host.trim().is_empty() || self.authentication.username.trim().is_empty())
        {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "network Database Connection fields must not be empty",
            ));
        }
        if self.driver == "sqlite" && (self.tls_used || self.tls.is_some()) {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "SQLite does not support TLS Connection settings",
            ));
        }
        match (self.driver.as_str(), &self.opt) {
            ("sqlite", None) => {
                return Err(CacheError::new(
                    ErrorCode::InvalidConnectionRuntimeConfig,
                    "opt is required for SQLite Connections",
                ))
            }
            ("sqlite", Some(_)) | (_, None) => {}
            (_, Some(_)) => {
                return Err(CacheError::new(
                    ErrorCode::InvalidConnectionRuntimeConfig,
                    "opt is only supported for SQLite Connections",
                ))
            }
        }
        self.validate_transport()?;
        ConnectionPoolDefinition {
            pool_used: self.pool_used,
            pool_size: self.pool_size,
        }
        .validate()?;
        if !self.pool_used {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "pool_used must be true",
            ));
        }
        Ok(())
    }

    fn validate_transport(&self) -> Result<()> {
        let tls = match (self.tls_used, &self.tls) {
            (false, None) => return Ok(()),
            (true, Some(tls)) => tls,
            (false, Some(_)) => {
                return Err(CacheError::new(
                    ErrorCode::InvalidConnectionRuntimeConfig,
                    "tls must be omitted when tls_used is false",
                ))
            }
            (true, None) => {
                return Err(CacheError::new(
                    ErrorCode::InvalidConnectionRuntimeConfig,
                    "tls is required when tls_used is true",
                ))
            }
        };
        if tls.cert.is_some() != tls.key.is_some() {
            return Err(CacheError::new(
                ErrorCode::InvalidConnectionRuntimeConfig,
                "TLS cert and key must be configured together",
            ));
        }
        Ok(())
    }

    pub fn select_connections(&self) -> Result<usize> {
        ConnectionPoolDefinition {
            pool_used: self.pool_used,
            pool_size: self.pool_size,
        }
        .select_connections()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(driver: &str, tls: &str) -> String {
        format!(
            r#"{{
                "connection_id":"database",
                "name":"database",
                "kind":"DATABASE",
                "driver":"{driver}",
                "host":"database.example.test",
                "port":5432,
                "database":"application",
                "queue_directory":"test/data/queue",
                "authentication":{{"username":"cache","password":"secret"}},
                "tls_used":{},
                {tls}
                "connect_timeout_ms":2000,
                "query_timeout_ms":5000,
                "pool_used":true,
                "pool_size":2
            }}"#,
            !tls.is_empty()
        )
    }

    #[test]
    fn splits_ten_connections_into_nine_readers_and_one_writer() {
        let definition = ConnectionPoolDefinition {
            pool_used: true,
            pool_size: 10,
        };
        assert_eq!(definition.select_connections().unwrap(), 9);
        assert_eq!(definition.write_connections().unwrap(), 1);
    }

    #[test]
    fn rejects_pool_size_below_two() {
        for pool_size in [0, 1] {
            let definition = ConnectionPoolDefinition {
                pool_used: true,
                pool_size,
            };
            assert!(definition.validate().is_err());
        }
    }

    #[test]
    fn accepts_postgresql_mariadb_and_sqlite_transports() {
        let plain = "";
        let tls = r#""tls":{"ca_file":"ca.pem"},"#;
        let mtls = r#""tls":{"ca_file":"ca.pem","cert":"client.pem","key":"client.key"},"#;
        for driver in ["postgresql", "mariadb"] {
            for transport in [plain, tls, mtls] {
                assert!(ConnectionDefinition::from_json(&definition(driver, transport)).is_ok());
            }
        }
        let sqlite = definition("sqlite", plain).replace(
            r#""pool_size":2"#,
            r#""pool_size":2,
                "opt":{
                    "key":null,
                    "open_mode":"READ_WRITE_CREATE",
                    "mutex":"FULL",
                    "uri":false,
                    "cache":"DEFAULT",
                    "journal_mode":"WAL",
                    "synchronous":"NORMAL",
                    "foreign_keys":true
                }"#,
        );
        assert!(ConnectionDefinition::from_json(&sqlite).is_ok());
        assert!(ConnectionDefinition::from_json(&definition("sqlite", plain)).is_err());
    }

    #[test]
    fn rejects_non_sql_product_drivers() {
        let plain = "";
        for driver in ["couchbase", "mongodb"] {
            assert!(ConnectionDefinition::from_json(&definition(driver, plain)).is_err());
        }
    }
}
