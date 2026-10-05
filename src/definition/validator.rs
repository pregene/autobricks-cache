use super::{CacheDefinition, CacheType, QueryDefinition, RetentionType};
use crate::error::{CacheError, ErrorCode, Result};
use std::collections::HashSet;

impl CacheDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.cache_id.is_empty() || self.connection_id.is_empty() {
            return invalid("cache_id and connection_id must not be empty");
        }
        validate_fields(&self.primary_key, ErrorCode::PrimaryKeyNotDefined)?;
        if self.maps.is_empty() {
            return invalid_map("at least one MAP is required");
        }

        let mut maps = HashSet::new();
        for fields in &self.maps {
            validate_fields(fields, ErrorCode::InvalidMapDefinition)?;
            if !maps.insert(fields.clone()) {
                return Err(CacheError::new(
                    ErrorCode::DuplicateMapDefinition,
                    "duplicate MAP field combination",
                ));
            }
        }
        if !maps.contains(&self.primary_key) {
            return invalid_map("primary_key must be present in maps");
        }

        match self.cache_type {
            CacheType::Preload if !self.select.fields.is_empty() => {
                return invalid("PRELOAD select fields must be empty")
            }
            CacheType::OnDemand if self.select.fields.is_empty() => {
                return invalid("ON_DEMAND select fields must not be empty")
            }
            _ => {}
        }

        validate_query(&self.select)?;
        validate_query(&self.insert)?;
        validate_query(&self.update)?;
        validate_query(&self.delete)?;

        match (self.retention.retention_type, self.retention.value) {
            (RetentionType::None, 0) => {}
            (RetentionType::Score | RetentionType::Timestamp, 1..) => {}
            _ => return invalid("retention must be NONE/0 or SCORE|TIMESTAMP/positive"),
        }
        Ok(())
    }
}

fn validate_query(query: &QueryDefinition) -> Result<()> {
    if query.query.trim().is_empty() {
        return invalid("query must not be empty");
    }
    let mut parameters = HashSet::new();
    let bytes = query.query.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes.get(index).copied() != Some(b'$') {
            index = index.checked_add(1).ok_or_else(parameter_overflow)?;
            continue;
        }
        index = index.checked_add(1).ok_or_else(parameter_overflow)?;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index = index.checked_add(1).ok_or_else(parameter_overflow)?;
        }
        if start != index {
            let parameter = query
                .query
                .get(start..index)
                .ok_or_else(parameter_overflow)?;
            let number = parameter
                .parse::<usize>()
                .map_err(|_| parameter_overflow())?;
            parameters.insert(number);
        }
    }
    let expected: HashSet<_> = (1..=query.fields.len()).collect();
    if parameters != expected {
        return Err(CacheError::new(
            ErrorCode::QueryParameterMismatch,
            "query parameters and fields do not match",
        ));
    }
    Ok(())
}

fn parameter_overflow() -> CacheError {
    CacheError::new(ErrorCode::QueryParameterMismatch, "invalid query parameter")
}

fn validate_fields(fields: &[String], code: ErrorCode) -> Result<()> {
    if fields.is_empty() || fields.iter().any(|field| field.trim().is_empty()) {
        return Err(CacheError::new(code, "field list must not be empty"));
    }
    let unique: HashSet<_> = fields.iter().collect();
    if unique.len() != fields.len() {
        return Err(CacheError::new(code, "field list contains duplicates"));
    }
    Ok(())
}

fn invalid(message: &str) -> Result<()> {
    Err(CacheError::new(ErrorCode::InvalidCacheDefinition, message))
}

fn invalid_map(message: &str) -> Result<()> {
    Err(CacheError::new(ErrorCode::InvalidMapDefinition, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_documented_examples() {
        let examples = [
            include_str!("../../example/cache.user.json"),
            include_str!("../../example/cache.user.ondemand.json"),
            include_str!("../../example/cache.user.score.json"),
            include_str!("../../example/cache.user.timestamp.json"),
            include_str!("../../example/cache.user.preload-retention.json"),
            include_str!("../../example/cache.position.json"),
        ];
        for example in examples {
            CacheDefinition::from_json(example).unwrap();
        }
    }

    #[test]
    fn rejects_primary_key_without_map() {
        let result = CacheDefinition::from_json(
            r#"{
                "cache_id":"users","connection_id":"database","cache_type":"PRELOAD",
                "retention":{"type":"NONE","value":0},"primary_key":["id"],
                "select":{"query":"SELECT * FROM users","fields":[]},
                "insert":{"query":"INSERT INTO users VALUES ($1)","fields":["id"]},
                "update":{"query":"UPDATE users SET name=$1","fields":["name"]},
                "delete":{"query":"DELETE FROM users WHERE id=$1","fields":["id"]},
                "maps":[["user_id"]]
            }"#,
        );
        assert_eq!(result.unwrap_err().code(), ErrorCode::InvalidMapDefinition);
    }
}
