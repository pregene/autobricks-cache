use super::{DatabaseRecord, DatabaseValue};
use crate::connection::{ConnectionDefinition, ConnectionRuntime, DatabaseReader, DatabaseWriter};
use crate::error::{CacheError, ErrorCode, Result};
use crate::operation;
use mysql::prelude::Queryable;
use mysql::{ClientIdentity, Conn, OptsBuilder, Params, Row, SslOpts, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct MariaDbDatabase;

pub struct MariaDbReader {
    connection: Conn,
}

pub struct MariaDbWriter {
    connection: Conn,
}

impl MariaDbDatabase {
    pub fn open(
        definition: &ConnectionDefinition,
        queue_directory: &Path,
    ) -> Result<ConnectionRuntime> {
        definition.validate()?;
        let mut readers = Vec::with_capacity(definition.select_connections()?);
        for _ in 0..definition.select_connections()? {
            readers.push(MariaDbReader {
                connection: connect(definition)?,
            });
        }
        let writer = MariaDbWriter {
            connection: connect(definition)?,
        };
        ConnectionRuntime::open_pool(&definition.connection_id, queue_directory, readers, writer)
    }
}

impl DatabaseReader for MariaDbReader {
    fn select(&mut self, query: &str, values: &[DatabaseValue]) -> Result<Vec<DatabaseRecord>> {
        let rows: Vec<Row> = self
            .connection
            .exec(query, parameters(values))
            .map_err(|error| db_error(ErrorCode::SelectFailed, "SELECT", error))?;
        rows.into_iter().map(decode_row).collect()
    }

    fn count(&mut self, query: &str, values: &[DatabaseValue]) -> Result<usize> {
        let query = query.trim().trim_end_matches(';');
        let count_query =
            format!("SELECT COUNT(*) AS record_count FROM ({query}) AS autobricks_cache_count");
        let count: Option<u64> = self
            .connection
            .exec_first(count_query, parameters(values))
            .map_err(|error| db_error(ErrorCode::SelectFailed, "COUNT", error))?;
        usize::try_from(count.unwrap_or_default()).map_err(|_| {
            CacheError::new(
                ErrorCode::RecordDecodingFailed,
                "MariaDB SELECT COUNT is outside usize range",
            )
        })
    }
}

impl DatabaseWriter for MariaDbWriter {
    fn execute(&mut self, object: &[u8]) -> Result<()> {
        let operation = operation::decode(object)?;
        self.connection
            .exec_drop(operation.query, parameters(&operation.values))
            .map_err(|error| db_error(ErrorCode::DbNativeError, "WRITE", error))
    }
}

fn connect(definition: &ConnectionDefinition) -> Result<Conn> {
    let timeout = Duration::from_millis(definition.query_timeout_ms);
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(Some(definition.host.clone()))
        .tcp_port(definition.port)
        .db_name(Some(definition.database.clone()))
        .user(Some(definition.authentication.username.clone()))
        .pass(Some(definition.authentication.password.clone()))
        .tcp_connect_timeout(Some(Duration::from_millis(definition.connect_timeout_ms)))
        .read_timeout(Some(timeout))
        .write_timeout(Some(timeout));
    if let Some(tls) = &definition.tls {
        let mut ssl = SslOpts::default().with_root_cert_path(Some(PathBuf::from(&tls.ca_file)));
        if let (Some(certificate), Some(private_key)) = (&tls.cert, &tls.key) {
            ssl = ssl.with_client_identity(Some(ClientIdentity::new(
                PathBuf::from(certificate),
                PathBuf::from(private_key),
            )));
        }
        builder = builder.ssl_opts(Some(ssl));
    }
    Conn::new(builder).map_err(|error| db_error(ErrorCode::DbNativeError, "CONNECT", error))
}

fn parameters(values: &[DatabaseValue]) -> Params {
    Params::Positional(values.iter().map(mysql_value).collect())
}

fn mysql_value(value: &DatabaseValue) -> Value {
    match value {
        DatabaseValue::Null => Value::NULL,
        DatabaseValue::Boolean(value) => Value::Int(i64::from(*value)),
        DatabaseValue::Signed(value) => Value::Int(*value),
        DatabaseValue::Unsigned(value) => Value::UInt(*value),
        DatabaseValue::Float(value) => Value::Double(*value),
        DatabaseValue::Text(value) => Value::Bytes(value.as_bytes().to_vec()),
        DatabaseValue::Bytes(value) => Value::Bytes(value.clone()),
    }
}

fn decode_row(row: Row) -> Result<DatabaseRecord> {
    let names: Vec<String> = row
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    names
        .into_iter()
        .zip(row.unwrap())
        .map(|(name, value)| Ok((name, decode_value(value)?)))
        .collect()
}

fn decode_value(value: Value) -> Result<DatabaseValue> {
    match value {
        Value::NULL => Ok(DatabaseValue::Null),
        Value::Bytes(value) => match String::from_utf8(value) {
            Ok(value) => Ok(DatabaseValue::Text(value)),
            Err(error) => Ok(DatabaseValue::Bytes(error.into_bytes())),
        },
        Value::Int(value) => Ok(DatabaseValue::Signed(value)),
        Value::UInt(value) => Ok(DatabaseValue::Unsigned(value)),
        Value::Float(value) => Ok(DatabaseValue::Float(f64::from(value))),
        Value::Double(value) => Ok(DatabaseValue::Float(value)),
        Value::Date(year, month, day, hour, minute, second, micros) => Ok(DatabaseValue::Text(
            format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"),
        )),
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if negative { "-" } else { "" };
            Ok(DatabaseValue::Text(format!(
                "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
            )))
        }
    }
}

fn db_error(code: ErrorCode, action: &str, error: mysql::Error) -> CacheError {
    CacheError::new(code, format!("MariaDB {action} failed: {error}"))
}
