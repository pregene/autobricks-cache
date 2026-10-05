use super::{DatabaseRecord, DatabaseValue};
use crate::connection::{ConnectionDefinition, ConnectionRuntime, DatabaseReader, DatabaseWriter};
use crate::error::{CacheError, ErrorCode, Result};
use crate::operation;
use bytes::BytesMut;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{to_sql_checked, FromSqlOwned, IsNull, ToSql, Type};
use postgres::{Client, Config, NoTls, Row};
use postgres_openssl::MakeTlsConnector;
use std::error::Error;
use std::path::Path;
use std::time::Duration;

pub struct PostgresDatabase;

pub struct PostgresReader {
    client: Client,
}

pub struct PostgresWriter {
    client: Client,
}

impl PostgresDatabase {
    pub fn open(
        definition: &ConnectionDefinition,
        queue_directory: &Path,
    ) -> Result<ConnectionRuntime> {
        definition.validate()?;
        let mut readers = Vec::with_capacity(definition.select_connections()?);
        for _ in 0..definition.select_connections()? {
            readers.push(PostgresReader {
                client: connect(definition)?,
            });
        }
        let writer = PostgresWriter {
            client: connect(definition)?,
        };
        ConnectionRuntime::open_pool(&definition.connection_id, queue_directory, readers, writer)
    }

    pub fn reader(definition: &ConnectionDefinition) -> Result<PostgresReader> {
        definition.validate()?;
        Ok(PostgresReader {
            client: connect(definition)?,
        })
    }
}

impl DatabaseReader for PostgresReader {
    fn select(&mut self, query: &str, values: &[DatabaseValue]) -> Result<Vec<DatabaseRecord>> {
        let parameters = parameters(values);
        self.client
            .query(query, &parameters)
            .map_err(|error| db_error(ErrorCode::SelectFailed, "SELECT", error))?
            .iter()
            .map(decode_row)
            .collect()
    }

    fn count(&mut self, query: &str, values: &[DatabaseValue]) -> Result<usize> {
        let query = query.trim().trim_end_matches(';');
        let count_query = format!(
            "SELECT COUNT(*)::BIGINT AS record_count FROM ({query}) AS autobricks_cache_count"
        );
        let parameters = parameters(values);
        let row = self
            .client
            .query_one(&count_query, &parameters)
            .map_err(|error| db_error(ErrorCode::SelectFailed, "COUNT", error))?;
        let count: i64 = row.try_get(0).map_err(row_error)?;
        usize::try_from(count).map_err(|_| {
            CacheError::new(
                ErrorCode::RecordDecodingFailed,
                "SELECT COUNT is outside usize range",
            )
        })
    }

    fn select_each(
        &mut self,
        query: &str,
        values: &[DatabaseValue],
        consumer: &mut dyn FnMut(DatabaseRecord) -> Result<()>,
    ) -> Result<()> {
        let parameters = parameters(values);
        let mut rows = self
            .client
            .query_raw(query, parameters)
            .map_err(|error| db_error(ErrorCode::SelectFailed, "SELECT", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| db_error(ErrorCode::SelectFailed, "SELECT row", error))?
        {
            consumer(decode_row(&row)?)?;
        }
        Ok(())
    }
}

impl DatabaseWriter for PostgresWriter {
    fn execute(&mut self, object: &[u8]) -> Result<()> {
        let operation = operation::decode(object)?;
        let parameters = parameters(&operation.values);
        self.client
            .execute(&operation.query, &parameters)
            .map(|_| ())
            .map_err(|error| db_error(ErrorCode::DbNativeError, "WRITE", error))
    }
}

fn connect(definition: &ConnectionDefinition) -> Result<Client> {
    let mut config = Config::new();
    config
        .host(&definition.host)
        .port(definition.port)
        .dbname(&definition.database)
        .user(&definition.authentication.username)
        .password(&definition.authentication.password)
        .connect_timeout(Duration::from_millis(definition.connect_timeout_ms));
    let Some(tls) = &definition.tls else {
        return config
            .connect(NoTls)
            .map_err(|error| db_error(ErrorCode::DbNativeError, "CONNECT", error));
    };
    let mut connector = SslConnector::builder(SslMethod::tls()).map_err(|error| {
        CacheError::new(
            ErrorCode::InvalidConnectionRuntimeConfig,
            format!("failed to configure PostgreSQL TLS: {error}"),
        )
    })?;
    connector
        .set_ca_file(&tls.ca_file)
        .map_err(tls_config_error)?;
    if let (Some(cert), Some(key)) = (&tls.cert, &tls.key) {
        connector
            .set_certificate_chain_file(cert)
            .map_err(tls_config_error)?;
        connector
            .set_private_key_file(key, SslFiletype::PEM)
            .map_err(tls_config_error)?;
        connector.check_private_key().map_err(tls_config_error)?;
    }
    config
        .connect(MakeTlsConnector::new(connector.build()))
        .map_err(|error| db_error(ErrorCode::DbNativeError, "CONNECT", error))
}

fn tls_config_error(error: openssl::error::ErrorStack) -> CacheError {
    CacheError::new(
        ErrorCode::InvalidConnectionRuntimeConfig,
        format!("failed to configure PostgreSQL TLS: {error}"),
    )
}

fn parameters(values: &[DatabaseValue]) -> Vec<&(dyn ToSql + Sync)> {
    values
        .iter()
        .map(|value| value as &(dyn ToSql + Sync))
        .collect()
}

fn decode_row(row: &Row) -> Result<DatabaseRecord> {
    row.columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = decode_value(row, index, column.type_())?;
            Ok((column.name().to_owned(), value))
        })
        .collect()
}

fn decode_value(row: &Row, index: usize, value_type: &Type) -> Result<DatabaseValue> {
    match *value_type {
        Type::BOOL => decode_nullable(row, index, DatabaseValue::Boolean),
        Type::INT2 => decode_nullable(row, index, |value: i16| {
            DatabaseValue::Signed(i64::from(value))
        }),
        Type::INT4 => decode_nullable(row, index, |value: i32| {
            DatabaseValue::Signed(i64::from(value))
        }),
        Type::INT8 => decode_nullable(row, index, DatabaseValue::Signed),
        Type::FLOAT4 => decode_nullable(row, index, |value: f32| {
            DatabaseValue::Float(f64::from(value))
        }),
        Type::FLOAT8 => decode_nullable(row, index, DatabaseValue::Float),
        Type::BYTEA => decode_nullable(row, index, DatabaseValue::Bytes),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            decode_nullable(row, index, DatabaseValue::Text)
        }
        _ => Err(CacheError::new(
            ErrorCode::RecordDecodingFailed,
            format!("PostgreSQL type {value_type} is not supported"),
        )),
    }
}

fn decode_nullable<T>(
    row: &Row,
    index: usize,
    present: impl FnOnce(T) -> DatabaseValue,
) -> Result<DatabaseValue>
where
    T: FromSqlOwned,
{
    Ok(nullable_value(
        row.try_get::<_, Option<T>>(index).map_err(row_error)?,
        present,
    ))
}

fn nullable_value<T>(value: Option<T>, present: impl FnOnce(T) -> DatabaseValue) -> DatabaseValue {
    match value {
        Some(value) => present(value),
        None => DatabaseValue::Null,
    }
}

impl ToSql for DatabaseValue {
    fn to_sql(
        &self,
        value_type: &Type,
        output: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Boolean(value) => value.to_sql(value_type, output),
            Self::Signed(value) => value.to_sql(value_type, output),
            Self::Unsigned(value) => i64::try_from(*value)?.to_sql(value_type, output),
            Self::Float(value) => value.to_sql(value_type, output),
            Self::Text(value) => value.to_sql(value_type, output),
            Self::Bytes(value) => value.to_sql(value_type, output),
        }
    }

    fn accepts(_value_type: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

fn row_error(error: postgres::Error) -> CacheError {
    db_error(
        ErrorCode::RecordDecodingFailed,
        "decode PostgreSQL row",
        error,
    )
}

fn db_error(code: ErrorCode, action: &str, error: postgres::Error) -> CacheError {
    CacheError::new(code, format!("PostgreSQL {action} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_null_for_every_supported_postgresql_value_type() {
        assert_eq!(
            nullable_value(None::<bool>, DatabaseValue::Boolean),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<i16>, |value| DatabaseValue::Signed(i64::from(value))),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<i32>, |value| DatabaseValue::Signed(i64::from(value))),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<i64>, DatabaseValue::Signed),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<f32>, |value| DatabaseValue::Float(f64::from(value))),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<f64>, DatabaseValue::Float),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<Vec<u8>>, DatabaseValue::Bytes),
            DatabaseValue::Null
        );
        assert_eq!(
            nullable_value(None::<String>, DatabaseValue::Text),
            DatabaseValue::Null
        );
    }
}
