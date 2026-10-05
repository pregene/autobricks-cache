use super::{DatabaseRecord, DatabaseValue};
use crate::connection::{ConnectionDefinition, ConnectionRuntime, DatabaseReader, DatabaseWriter};
use crate::error::{CacheError, ErrorCode, Result};
use crate::operation;
use bytes::BytesMut;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::{to_sql_checked, IsNull, ToSql, Type};
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
    if row
        .try_get::<_, Option<Vec<u8>>>(index)
        .is_ok_and(|value| value.is_none())
    {
        return Ok(DatabaseValue::Null);
    }
    match *value_type {
        Type::BOOL => Ok(DatabaseValue::Boolean(
            row.try_get(index).map_err(row_error)?,
        )),
        Type::INT2 => Ok(DatabaseValue::Signed(i64::from(
            row.try_get::<_, i16>(index).map_err(row_error)?,
        ))),
        Type::INT4 => Ok(DatabaseValue::Signed(i64::from(
            row.try_get::<_, i32>(index).map_err(row_error)?,
        ))),
        Type::INT8 => Ok(DatabaseValue::Signed(
            row.try_get(index).map_err(row_error)?,
        )),
        Type::FLOAT4 => Ok(DatabaseValue::Float(f64::from(
            row.try_get::<_, f32>(index).map_err(row_error)?,
        ))),
        Type::FLOAT8 => Ok(DatabaseValue::Float(row.try_get(index).map_err(row_error)?)),
        Type::BYTEA => Ok(DatabaseValue::Bytes(row.try_get(index).map_err(row_error)?)),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            Ok(DatabaseValue::Text(row.try_get(index).map_err(row_error)?))
        }
        _ => Err(CacheError::new(
            ErrorCode::RecordDecodingFailed,
            format!("PostgreSQL type {value_type} is not supported"),
        )),
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
