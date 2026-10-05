use crate::connection::ConnectionRuntime;
use crate::core::{Core, QueryResult};
use crate::database::{DatabaseRecord, DatabaseValue};
use crate::definition::{CacheDefinition, CacheType, RetentionType};
use crate::error::{CacheError, ErrorCode, Result};
use crate::operation;
use serde_json::json;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const PRODUCT_NAME: &str = "Autobricks Cache";
const PRODUCT_VERSION: &str = env!("AUTOBRICKS_CACHE_VERSION");
const PRODUCT_COPYRIGHT: &str = "(C) 2026 Autobricks, Co.";

struct State {
    schema: Option<RecordSchema>,
}

#[derive(Clone)]
struct RecordSchema {
    fields: Vec<String>,
}

pub struct Cache {
    definition: Arc<CacheDefinition>,
    connection: Option<Arc<ConnectionRuntime>>,
    core: Core,
    state: RwLock<State>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CacheStatus {
    pub record_count: usize,
    pub memory_bytes: usize,
}

pub(crate) struct CacheQueryResult<'a> {
    inner: Option<QueryResult<'a>>,
}

impl CacheQueryResult<'_> {
    pub(crate) fn len(&self) -> usize {
        self.inner.as_ref().map_or(0, QueryResult::len)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn record(&self, index: usize) -> Result<&[u8]> {
        let inner = self
            .inner
            .as_ref()
            .ok_or_else(|| CacheError::new(ErrorCode::RecordNotFound, "query result is empty"))?;
        Ok(inner.record(index)?.data)
    }
}

impl Cache {
    pub(crate) fn new(definition: CacheDefinition) -> Result<Self> {
        definition.validate()?;
        let core = Core::new(
            definition.maps.len(),
            0,
            0,
            retention_type(&definition),
            definition.retention.value,
        )?;
        Ok(Self {
            definition: Arc::new(definition),
            connection: None,
            core,
            state: RwLock::new(State { schema: None }),
        })
    }

    pub(crate) fn connected(
        definition: CacheDefinition,
        connection: Arc<ConnectionRuntime>,
    ) -> Result<Self> {
        if definition.connection_id != connection.connection_id() {
            return Err(CacheError::new(
                ErrorCode::InvalidArgument,
                "Cache and Connection IDs do not match",
            ));
        }
        let mut cache = Self::new(definition)?;
        cache.connection = Some(connection);
        Ok(cache)
    }

    pub(crate) fn preload(&self) -> Result<usize> {
        if self.definition.cache_type != CacheType::Preload {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "preload requires PRELOAD cache_type",
            ));
        }
        self.load_from_database(&[])
    }

    pub fn query(&self, input: &str) -> String {
        let result = parse_record(input).and_then(|record| {
            let result = self.query_record(&record)?;
            self.decode_query_result(&result)
        });
        match result {
            Ok(records) => json!({"code": 0, "message": "success", "records": records}).to_string(),
            Err(error) => error_json(&error),
        }
    }

    pub(crate) fn query_record(&self, input: &DatabaseRecord) -> Result<CacheQueryResult<'_>> {
        let fields = self.query_map(input)?;
        let values = query_values(input, fields)?;
        let records = self.lookup_at(fields, &values, unix_time())?;
        if !records.is_empty() || self.definition.cache_type == CacheType::Preload {
            return Ok(records);
        }
        let select_values = query_values(input, &self.definition.select.fields)?;
        self.load_from_database(&select_values)?;
        self.lookup_at(fields, &values, unix_time())
    }

    fn decode_query_result(&self, result: &CacheQueryResult<'_>) -> Result<Vec<DatabaseRecord>> {
        if result.is_empty() {
            return Ok(Vec::new());
        }
        let state = self.read_state()?;
        let schema = state.schema.as_ref().ok_or_else(missing_schema)?;
        (0..result.len())
            .map(|index| schema.decode(result.record(index)?))
            .collect()
    }

    pub(crate) fn record_count(&self) -> Result<usize> {
        Ok(self.core.status()?.record_count)
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn benchmark_load_records(&self, records: Vec<DatabaseRecord>) -> Result<usize> {
        let count = records.len();
        self.insert_records_at(records, unix_time())?;
        Ok(count)
    }

    pub fn status(&self) -> String {
        match self.status_data() {
            Ok(status) => json!({
                "code": 0,
                "message": "success",
                "product": PRODUCT_NAME,
                "version": PRODUCT_VERSION,
                "copyright": PRODUCT_COPYRIGHT,
                "record_count": status.record_count,
                "memory_bytes": status.memory_bytes
            })
            .to_string(),
            Err(error) => error_json(&error),
        }
    }

    pub(crate) fn status_data(&self) -> Result<CacheStatus> {
        let state = self.read_state()?;
        let core = self.core.status()?;
        let schema_bytes = state
            .schema
            .as_ref()
            .map(|schema| -> Result<usize> {
                let field_storage = schema
                    .fields
                    .capacity()
                    .checked_mul(std::mem::size_of::<String>())
                    .ok_or_else(memory_accounting_overflow)?;
                schema
                    .fields
                    .iter()
                    .try_fold(field_storage, |total, field| {
                        total
                            .checked_add(field.capacity())
                            .ok_or_else(memory_accounting_overflow)
                    })
            })
            .transpose()?
            .unwrap_or_default();
        let memory_bytes = core
            .memory_bytes
            .checked_add(schema_bytes)
            .ok_or_else(memory_accounting_overflow)?;
        Ok(CacheStatus {
            record_count: core.record_count,
            memory_bytes,
        })
    }

    pub fn insert(&self, input: &str) -> String {
        operation_json(parse_record(input).and_then(|record| self.insert_record(record)))
    }

    pub(crate) fn insert_record(&self, record: DatabaseRecord) -> Result<()> {
        self.change_record(record, Change::Insert)
    }

    pub fn update(&self, input: &str) -> String {
        operation_json(parse_record(input).and_then(|record| self.update_record(record)))
    }

    pub(crate) fn update_record(&self, record: DatabaseRecord) -> Result<()> {
        self.change_record(record, Change::Update)
    }

    pub fn delete(&self, input: &str) -> String {
        operation_json(parse_record(input).and_then(|key| self.delete_record(&key).map(|_| ())))
    }

    pub(crate) fn delete_record(&self, key: &DatabaseRecord) -> Result<usize> {
        let connection = self.connection()?;
        let fields = self.exact_map(key)?;
        let values = query_values(key, fields)?;
        let state = self.write_state()?;
        let ids = self
            .core
            .query(self.map_index(fields)?, &encode_values(&values)?)?;
        if ids.is_empty() {
            return Err(CacheError::new(
                ErrorCode::RecordNotFound,
                "DELETE target does not exist in Cache",
            ));
        }
        let mut deleted = 0usize;
        for record_id in ids {
            let record = state
                .schema
                .as_ref()
                .ok_or_else(missing_schema)?
                .decode(&self.core.read(record_id)?)?;
            let encoded = self.core.read(record_id)?;
            let keys = map_keys(&record, &self.definition.maps)?;
            let previous_retention = self.core.retention(record_id)?;
            let object = operation::delete(
                &self.definition.cache_id,
                &self.definition.delete.query,
                query_values(&record, &self.definition.delete.fields)?,
            )?;
            self.core.delete(record_id)?;
            if let Err(error) = connection.enqueue(&object) {
                let restored = self.core.insert(&encoded, &keys, unix_time())?;
                self.core.set_retention(restored, previous_retention)?;
                return Err(error);
            }
            deleted = deleted.checked_add(1).ok_or_else(|| {
                CacheError::new(ErrorCode::InternalError, "deleted record count overflow")
            })?;
        }
        Ok(deleted)
    }

    pub(crate) fn drain(&self, score_step: u64) -> Result<usize> {
        self.drain_at(unix_time(), score_step)
    }

    fn load_from_database(&self, values: &[DatabaseValue]) -> Result<usize> {
        let connection = self.connection()?;
        if values.len() != self.definition.select.fields.len() {
            return Err(CacheError::new(
                ErrorCode::QueryParameterMismatch,
                "SELECT values and fields do not match",
            ));
        }
        if self.definition.cache_type == CacheType::Preload {
            let expected = connection.select_count(&self.definition.select.query, values)?;
            let primary_map = self.map_index(&self.definition.primary_key)?;
            let now = unix_time();
            let mut loaded = 0usize;
            let mut state = self.write_state()?;
            connection.select_each(&self.definition.select.query, values, &mut |record| {
                insert_stream_record(
                    &mut state,
                    &self.core,
                    &self.definition,
                    record,
                    primary_map,
                    expected,
                    now,
                )?;
                loaded = loaded.checked_add(1).ok_or_else(|| {
                    CacheError::new(ErrorCode::InternalError, "loaded record count overflow")
                })?;
                Ok(())
            })?;
            Ok(loaded)
        } else {
            let records = connection.select(&self.definition.select.query, values)?;
            let count = records.len();
            self.insert_records_at(records, unix_time())?;
            Ok(count)
        }
    }

    fn change_record(&self, database: DatabaseRecord, change: Change) -> Result<()> {
        let connection = self.connection()?;
        let query = match change {
            Change::Insert => &self.definition.insert,
            Change::Update => &self.definition.update,
        };
        let values = query_values(&database, &query.fields)?;
        let object = match change {
            Change::Insert => operation::insert(&self.definition.cache_id, &query.query, values)?,
            Change::Update => operation::update(&self.definition.cache_id, &query.query, values)?,
        };
        let mut state = self.write_state()?;
        ensure_schema(&mut state, std::slice::from_ref(&database))?;
        let schema = state.schema.as_ref().ok_or_else(missing_schema)?.clone();
        let primary = query_values(&database, &self.definition.primary_key)?;
        let existing = self
            .core
            .query(
                self.map_index(&self.definition.primary_key)?,
                &encode_values(&primary)?,
            )?
            .first()
            .copied();
        match change {
            Change::Insert if existing.is_some() => {
                return Err(CacheError::new(
                    ErrorCode::CacheKeyAlreadyExists,
                    "INSERT primary key already exists in Cache",
                ))
            }
            Change::Update if existing.is_none() => {
                return Err(CacheError::new(
                    ErrorCode::RecordNotFound,
                    "UPDATE primary key does not exist in Cache",
                ))
            }
            _ => {}
        }
        let encoded = schema.encode(&database)?;
        let keys = map_keys(&database, &self.definition.maps)?;
        let previous = if let Some(id) = existing {
            let bytes = self.core.read(id)?;
            let record = schema.decode(&bytes)?;
            let keys = map_keys(&record, &self.definition.maps)?;
            let retention = self.core.retention(id)?;
            Some((bytes, keys, retention))
        } else {
            None
        };
        let now = unix_time();
        let record_id = if let Some(id) = existing {
            self.core.update(id, &encoded, &keys, now)?;
            id
        } else {
            self.core.insert(&encoded, &keys, now)?
        };
        if let Err(error) = connection.enqueue(&object) {
            if let Some((bytes, keys, retention)) = previous {
                self.core.update(record_id, &bytes, &keys, now)?;
                self.core.set_retention(record_id, retention)?;
            } else {
                self.core.delete(record_id)?;
            }
            return Err(error);
        }
        Ok(())
    }

    fn connection(&self) -> Result<&ConnectionRuntime> {
        self.connection.as_deref().ok_or_else(|| {
            CacheError::new(
                ErrorCode::InvalidStateTransition,
                "Cache is not connected to its Connection",
            )
        })
    }

    fn query_map<'a>(&'a self, input: &DatabaseRecord) -> Result<&'a [String]> {
        let candidates = self
            .definition
            .maps
            .iter()
            .filter(|fields| {
                fields.iter().all(|field| input.contains_key(field))
                    && input.keys().all(|field| {
                        fields.contains(field) || self.definition.select.fields.contains(field)
                    })
            })
            .collect::<Vec<_>>();
        match candidates.as_slice() {
            [fields] => Ok(fields),
            [] => Err(CacheError::new(
                ErrorCode::LookupMapNotFound,
                "query fields do not match a registered MAP",
            )),
            _ => Err(CacheError::new(
                ErrorCode::InvalidArgument,
                "query fields match more than one MAP",
            )),
        }
    }

    fn exact_map<'a>(&'a self, input: &DatabaseRecord) -> Result<&'a [String]> {
        self.definition
            .maps
            .iter()
            .find(|fields| {
                fields.len() == input.len() && fields.iter().all(|field| input.contains_key(field))
            })
            .map(Vec::as_slice)
            .ok_or_else(|| {
                CacheError::new(
                    ErrorCode::LookupMapNotFound,
                    "input fields do not match a registered MAP",
                )
            })
    }

    pub(crate) fn insert_records_at(&self, records: Vec<DatabaseRecord>, now: u64) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut state = self.write_state()?;
        ensure_schema(&mut state, &records)?;
        let schema = state.schema.as_ref().ok_or_else(missing_schema)?.clone();
        let primary_map = self.map_index(&self.definition.primary_key)?;
        for record in records {
            let primary = query_values(&record, &self.definition.primary_key)?;
            let existing = self
                .core
                .query(primary_map, &encode_values(&primary)?)?
                .first()
                .copied();
            let encoded = schema.encode(&record)?;
            let keys = map_keys(&record, &self.definition.maps)?;
            if let Some(id) = existing {
                self.core.update(id, &encoded, &keys, now)?;
            } else {
                self.core.insert(&encoded, &keys, now)?;
            }
        }
        Ok(())
    }

    pub(crate) fn lookup_at(
        &self,
        fields: &[String],
        values: &[DatabaseValue],
        now: u64,
    ) -> Result<CacheQueryResult<'_>> {
        if fields.len() != values.len() || fields.is_empty() {
            return Err(CacheError::new(
                ErrorCode::InvalidArgument,
                "lookup fields and values must have the same non-zero length",
            ));
        }
        let map_index = self.map_index(fields)?;
        let result = self
            .core
            .query_view(map_index, &encode_values(values)?, now)?;
        Ok(CacheQueryResult {
            inner: Some(result),
        })
    }

    pub(crate) fn drain_at(&self, now: u64, score_step: u64) -> Result<usize> {
        self.core.drain(now, score_step)
    }

    fn map_index(&self, fields: &[String]) -> Result<usize> {
        self.definition
            .maps
            .iter()
            .position(|candidate| candidate == fields)
            .ok_or_else(|| CacheError::new(ErrorCode::LookupMapNotFound, "MAP is not registered"))
    }

    fn read_state(&self) -> Result<std::sync::RwLockReadGuard<'_, State>> {
        self.state
            .read()
            .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Cache read lock is unavailable"))
    }
    fn write_state(&self) -> Result<std::sync::RwLockWriteGuard<'_, State>> {
        self.state.write().map_err(|_| {
            CacheError::new(ErrorCode::LockPoisoned, "Cache write lock is unavailable")
        })
    }
}

#[derive(Clone, Copy)]
enum Change {
    Insert,
    Update,
}

impl RecordSchema {
    fn from_record(record: &DatabaseRecord) -> Self {
        Self {
            fields: record.keys().cloned().collect(),
        }
    }
    fn validate(&self, record: &DatabaseRecord) -> Result<()> {
        if record.len() != self.fields.len()
            || !self.fields.iter().all(|field| record.contains_key(field))
        {
            return Err(decode_error(
                "database record fields do not match Cache schema",
            ));
        }
        Ok(())
    }
    fn encoded_len(&self, record: &DatabaseRecord) -> Result<usize> {
        self.validate(record)?;
        self.fields.iter().try_fold(0usize, |total, field| {
            let value = record
                .get(field)
                .ok_or_else(|| decode_error("validated record field is missing"))?;
            total
                .checked_add(value_encoded_len(value)?)
                .ok_or_else(|| decode_error("encoded record length overflow"))
        })
    }
    fn encode(&self, record: &DatabaseRecord) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(self.encoded_len(record)?);
        for field in &self.fields {
            let value = record
                .get(field)
                .ok_or_else(|| decode_error("validated record field is missing"))?;
            encode_value(value, &mut output)?;
        }
        Ok(output)
    }
    fn decode(&self, data: &[u8]) -> Result<DatabaseRecord> {
        let mut cursor = 0;
        let mut record = DatabaseRecord::new();
        for field in &self.fields {
            record.insert(field.clone(), decode_value(data, &mut cursor)?);
        }
        if cursor != data.len() {
            return Err(decode_error("record contains trailing bytes"));
        }
        Ok(record)
    }
}

fn ensure_schema(state: &mut State, records: &[DatabaseRecord]) -> Result<()> {
    if state.schema.is_some() {
        let schema = state.schema.as_ref().ok_or_else(missing_schema)?;
        for record in records {
            schema.validate(record)?;
        }
        return Ok(());
    }
    let first = records
        .first()
        .ok_or_else(|| decode_error("cannot create schema without a record"))?;
    let schema = RecordSchema::from_record(first);
    for record in records {
        schema.encoded_len(record)?;
    }
    state.schema = Some(schema);
    Ok(())
}

fn insert_stream_record(
    state: &mut State,
    core: &Core,
    definition: &CacheDefinition,
    record: DatabaseRecord,
    primary_map: usize,
    expected_records: usize,
    now: u64,
) -> Result<()> {
    if state.schema.is_none() {
        let schema = RecordSchema::from_record(&record);
        let expected_bytes = schema
            .encoded_len(&record)?
            .checked_mul(expected_records)
            .ok_or_else(|| decode_error("record size overflow"))?;
        core.reserve(expected_records, expected_bytes)?;
        state.schema = Some(schema);
    }
    let schema = state.schema.as_ref().ok_or_else(missing_schema)?.clone();
    let primary = query_values(&record, &definition.primary_key)?;
    let existing = core
        .query(primary_map, &encode_values(&primary)?)?
        .first()
        .copied();
    let encoded = schema.encode(&record)?;
    let keys = map_keys(&record, &definition.maps)?;
    if let Some(id) = existing {
        core.update(id, &encoded, &keys, now)?;
    } else {
        core.insert(&encoded, &keys, now)?;
    }
    Ok(())
}

fn retention_type(definition: &CacheDefinition) -> u32 {
    match definition.retention.retention_type {
        RetentionType::None => 0,
        RetentionType::Score => 1,
        RetentionType::Timestamp => 2,
    }
}

fn map_keys(record: &DatabaseRecord, maps: &[Vec<String>]) -> Result<Vec<Vec<u8>>> {
    maps.iter()
        .map(|fields| encode_values(&query_values(record, fields)?))
        .collect()
}

fn query_values(record: &DatabaseRecord, fields: &[String]) -> Result<Vec<DatabaseValue>> {
    fields
        .iter()
        .map(|field| {
            record.get(field).cloned().ok_or_else(|| {
                decode_error(&format!("database record does not contain field {field}"))
            })
        })
        .collect()
}

fn parse_record(input: &str) -> Result<DatabaseRecord> {
    serde_json::from_str(input).map_err(|error| {
        CacheError::new(
            ErrorCode::InvalidJson,
            format!("record JSON is invalid: {error}"),
        )
    })
}

fn operation_json(result: Result<()>) -> String {
    match result {
        Ok(()) => json!({"code": 0, "message": "success"}).to_string(),
        Err(error) => error_json(&error),
    }
}

fn error_json(error: &CacheError) -> String {
    json!({"code": error.code() as i32, "message": error.message()}).to_string()
}

fn encode_values(values: &[DatabaseValue]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for value in values {
        encode_value(value, &mut output)?;
    }
    Ok(output)
}

fn value_encoded_len(value: &DatabaseValue) -> Result<usize> {
    let length = match value {
        DatabaseValue::Null => 1,
        DatabaseValue::Boolean(_) => 2,
        DatabaseValue::Signed(_) | DatabaseValue::Unsigned(_) | DatabaseValue::Float(_) => 9,
        DatabaseValue::Text(v) => 5usize
            .checked_add(v.len())
            .ok_or_else(|| decode_error("text field length overflow"))?,
        DatabaseValue::Bytes(v) => 5usize
            .checked_add(v.len())
            .ok_or_else(|| decode_error("byte field length overflow"))?,
    };
    Ok(length)
}

fn encode_value(value: &DatabaseValue, output: &mut Vec<u8>) -> Result<()> {
    match value {
        DatabaseValue::Null => output.push(0),
        DatabaseValue::Boolean(v) => {
            output.push(1);
            output.push(u8::from(*v));
        }
        DatabaseValue::Signed(v) => {
            output.push(2);
            output.extend_from_slice(&v.to_le_bytes());
        }
        DatabaseValue::Unsigned(v) => {
            output.push(3);
            output.extend_from_slice(&v.to_le_bytes());
        }
        DatabaseValue::Float(v) => {
            output.push(4);
            output.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        DatabaseValue::Text(v) => {
            output.push(5);
            encode_blob(v.as_bytes(), output)?;
        }
        DatabaseValue::Bytes(v) => {
            output.push(6);
            encode_blob(v, output)?;
        }
    }
    Ok(())
}

fn encode_blob(value: &[u8], output: &mut Vec<u8>) -> Result<()> {
    let size = u32::try_from(value.len()).map_err(|_| decode_error("field exceeds u32 length"))?;
    output.extend_from_slice(&size.to_le_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn decode_value(data: &[u8], cursor: &mut usize) -> Result<DatabaseValue> {
    let tag = take(data, cursor, 1)?
        .first()
        .copied()
        .ok_or_else(|| decode_error("record value tag is missing"))?;
    Ok(match tag {
        0 => DatabaseValue::Null,
        1 => DatabaseValue::Boolean(
            take(data, cursor, 1)?
                .first()
                .copied()
                .ok_or_else(|| decode_error("boolean value is missing"))?
                != 0,
        ),
        2 => DatabaseValue::Signed(i64::from_le_bytes(take_array(data, cursor)?)),
        3 => DatabaseValue::Unsigned(u64::from_le_bytes(take_array(data, cursor)?)),
        4 => DatabaseValue::Float(f64::from_bits(u64::from_le_bytes(take_array(
            data, cursor,
        )?))),
        5 => DatabaseValue::Text(
            String::from_utf8(decode_blob(data, cursor)?.to_vec())
                .map_err(|_| decode_error("text is not UTF-8"))?,
        ),
        6 => DatabaseValue::Bytes(decode_blob(data, cursor)?.to_vec()),
        _ => return Err(decode_error("unknown value tag")),
    })
}
fn decode_blob<'a>(data: &'a [u8], cursor: &mut usize) -> Result<&'a [u8]> {
    let size = u32::from_le_bytes(take_array(data, cursor)?) as usize;
    take(data, cursor, size)
}
fn take_array<const N: usize>(data: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    take(data, cursor, N)?
        .try_into()
        .map_err(|_| decode_error("record is truncated"))
}
fn take<'a>(data: &'a [u8], cursor: &mut usize, size: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(size)
        .ok_or_else(|| decode_error("record offset overflow"))?;
    let value = data
        .get(*cursor..end)
        .ok_or_else(|| decode_error("record is truncated"))?;
    *cursor = end;
    Ok(value)
}
fn missing_schema() -> CacheError {
    decode_error("Cache schema is missing")
}
fn decode_error(message: &str) -> CacheError {
    CacheError::new(ErrorCode::RecordDecodingFailed, message)
}
fn memory_accounting_overflow() -> CacheError {
    CacheError::new(ErrorCode::InternalError, "Cache memory accounting overflow")
}
fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{DatabaseReader, DatabaseWriter};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Reader;
    impl DatabaseReader for Reader {
        fn select(&mut self, _: &str, _: &[DatabaseValue]) -> Result<Vec<DatabaseRecord>> {
            Ok(Vec::new())
        }
    }
    struct Writer(Arc<AtomicUsize>);
    impl DatabaseWriter for Writer {
        fn execute(&mut self, _: &[u8]) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn definition(retention: &str, value: u64) -> CacheDefinition {
        CacheDefinition::from_json(&format!(r#"{{"cache_id":"users","connection_id":"database","cache_type":"PRELOAD","retention":{{"type":"{retention}","value":{value}}},"primary_key":["tenant_id","id"],"select":{{"query":"SELECT * FROM users","fields":[]}},"insert":{{"query":"INSERT INTO users VALUES ($1)","fields":["id"]}},"update":{{"query":"UPDATE users SET name=$1 WHERE id=$2","fields":["name","id"]}},"delete":{{"query":"DELETE FROM users WHERE id=$1","fields":["id"]}},"maps":[["tenant_id","id"],["user_id"]]}}"#)).unwrap()
    }
    fn record(id: i64, user: &str) -> DatabaseRecord {
        DatabaseRecord::from([
            ("tenant_id".into(), DatabaseValue::Signed(10)),
            ("id".into(), DatabaseValue::Signed(id)),
            ("user_id".into(), DatabaseValue::from(user)),
            ("name".into(), DatabaseValue::from("Paul")),
        ])
    }

    #[test]
    fn cpp_core_builds_composite_and_group_maps() {
        let cache = Cache::new(definition("NONE", 0)).unwrap();
        cache
            .insert_records_at(vec![record(7, "shared"), record(8, "shared")], 100)
            .unwrap();
        let result = cache
            .lookup_at(&["user_id".into()], &[DatabaseValue::from("shared")], 101)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(!result.record(0).unwrap().is_empty());
        assert!(!result.record(1).unwrap().is_empty());
        assert_eq!(
            cache
                .lookup_at(
                    &["tenant_id".into(), "id".into()],
                    &[DatabaseValue::Signed(10), DatabaseValue::Signed(7)],
                    101
                )
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn cpp_core_group_query_can_return_more_records_than_the_lock_pool() {
        let cache = Cache::new(definition("NONE", 0)).unwrap();
        let records = (0..100).map(|id| record(id, "shared")).collect::<Vec<_>>();
        cache.insert_records_at(records, 100).unwrap();
        let result = cache
            .lookup_at(&["user_id".into()], &[DatabaseValue::from("shared")], 101)
            .unwrap();
        assert_eq!(result.len(), 100);
        for index in 0..result.len() {
            assert!(!result.record(index).unwrap().is_empty());
        }
    }

    #[test]
    fn cpp_core_retention_removes_records() {
        let cache = Cache::new(definition("TIMESTAMP", 3600)).unwrap();
        cache
            .insert_records_at(vec![record(7, "test_user")], 100)
            .unwrap();
        assert_eq!(
            cache
                .lookup_at(
                    &["user_id".into()],
                    &[DatabaseValue::from("test_user")],
                    3600
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(cache.drain_at(7199, 60).unwrap(), 0);
        assert_eq!(cache.drain_at(7200, 60).unwrap(), 1);
    }

    #[test]
    fn cpp_core_score_retention_reaches_zero_before_removal() {
        let cache = Cache::new(definition("SCORE", 120)).unwrap();
        cache
            .insert_records_at(vec![record(7, "test_user")], 100)
            .unwrap();
        assert_eq!(cache.drain_at(101, 60).unwrap(), 0);
        assert_eq!(cache.drain_at(102, 60).unwrap(), 1);
    }

    #[test]
    fn codec_round_trip_preserves_values() {
        let source = record(7, "test_user");
        let schema = RecordSchema::from_record(&source);
        assert_eq!(
            schema.decode(&schema.encode(&source).unwrap()).unwrap(),
            source
        );
    }

    #[test]
    fn cpp_core_insert_update_delete_use_database_queue() {
        let directory = std::env::temp_dir().join(format!(
            "autobricks-cache-cpp-{}-{}",
            std::process::id(),
            unix_time()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let connection = Arc::new(
            ConnectionRuntime::open("database", &directory, Reader, Writer(Arc::clone(&count)))
                .unwrap(),
        );
        connection.start().unwrap();
        let cache = Cache::connected(definition("NONE", 0), Arc::clone(&connection)).unwrap();

        let inserted: serde_json::Value = serde_json::from_str(
            &cache.insert(r#"{"tenant_id":10,"id":7,"user_id":"test_user","name":"Test User"}"#),
        )
        .unwrap();
        assert_eq!(inserted["code"], 0);

        let queried: serde_json::Value =
            serde_json::from_str(&cache.query(r#"{"user_id":"test_user"}"#)).unwrap();
        assert_eq!(queried["code"], 0);
        assert_eq!(queried["records"].as_array().unwrap().len(), 1);

        let updated: serde_json::Value = serde_json::from_str(
            &cache.update(r#"{"tenant_id":10,"id":7,"user_id":"changed","name":"Paul"}"#),
        )
        .unwrap();
        assert_eq!(updated["code"], 0);

        let deleted: serde_json::Value =
            serde_json::from_str(&cache.delete(r#"{"user_id":"changed"}"#)).unwrap();
        assert_eq!(deleted["code"], 0);
        assert!(deleted.get("records").is_none());

        let status: serde_json::Value = serde_json::from_str(&cache.status()).unwrap();
        assert_eq!(status["code"], 0);
        assert_eq!(status["product"], PRODUCT_NAME);
        assert_eq!(status["version"], "0.1.102");
        assert_eq!(status["copyright"], PRODUCT_COPYRIGHT);
        assert_eq!(status["record_count"], 0);
        for _ in 0..100 {
            if count.load(Ordering::SeqCst) == 3 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        connection.stop().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
