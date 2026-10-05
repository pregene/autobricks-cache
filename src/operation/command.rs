use crate::database::DatabaseValue;
use crate::error::{CacheError, ErrorCode, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum OperationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct DatabaseOperation {
    pub(crate) cache_id: String,
    pub(crate) kind: OperationKind,
    pub(crate) query: String,
    pub(crate) values: Vec<DatabaseValue>,
}

pub(crate) fn decode(object: &[u8]) -> Result<DatabaseOperation> {
    serde_json::from_slice(object).map_err(|error| {
        CacheError::new(
            ErrorCode::RecordDecodingFailed,
            format!("failed to decode database operation: {error}"),
        )
    })
}

pub(crate) fn insert(cache_id: &str, query: &str, values: Vec<DatabaseValue>) -> Result<Vec<u8>> {
    encode(cache_id, OperationKind::Insert, query, values)
}

pub(crate) fn update(cache_id: &str, query: &str, values: Vec<DatabaseValue>) -> Result<Vec<u8>> {
    encode(cache_id, OperationKind::Update, query, values)
}

pub(crate) fn delete(cache_id: &str, query: &str, values: Vec<DatabaseValue>) -> Result<Vec<u8>> {
    encode(cache_id, OperationKind::Delete, query, values)
}

fn encode(
    cache_id: &str,
    kind: OperationKind,
    query: &str,
    values: Vec<DatabaseValue>,
) -> Result<Vec<u8>> {
    serde_json::to_vec(&DatabaseOperation {
        cache_id: cache_id.to_owned(),
        kind,
        query: query.to_owned(),
        values,
    })
    .map_err(|error| {
        CacheError::new(
            ErrorCode::InternalError,
            format!("failed to encode database operation: {error}"),
        )
    })
}
