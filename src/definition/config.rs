use crate::error::{CacheError, ErrorCode, Result};
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CacheDefinition {
    pub cache_id: String,
    pub connection_id: String,
    pub cache_type: CacheType,
    pub retention: RetentionDefinition,
    pub primary_key: Vec<String>,
    pub select: QueryDefinition,
    pub insert: QueryDefinition,
    pub update: QueryDefinition,
    pub delete: QueryDefinition,
    pub maps: Vec<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CacheType {
    Preload,
    OnDemand,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueryDefinition {
    pub query: String,
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetentionDefinition {
    #[serde(rename = "type")]
    pub retention_type: RetentionType,
    pub value: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetentionType {
    None,
    Score,
    Timestamp,
}

impl CacheDefinition {
    pub fn from_json(json: &str) -> Result<Self> {
        let definition: Self = serde_json::from_str(json).map_err(|error| {
            CacheError::new(
                ErrorCode::InvalidJson,
                format!("failed to parse Cache Definition: {error}"),
            )
        })?;
        definition.validate()?;
        Ok(definition)
    }

    pub fn list_from_json(json: &str) -> Result<Vec<Self>> {
        let definitions: Vec<Self> = serde_json::from_str(json).map_err(|error| {
            CacheError::new(
                ErrorCode::InvalidJson,
                format!("failed to parse Cache Definition array: {error}"),
            )
        })?;
        if definitions.is_empty() {
            return Err(CacheError::new(
                ErrorCode::InvalidCacheDefinition,
                "Cache Definition array must not be empty",
            ));
        }
        let mut cache_ids = HashSet::with_capacity(definitions.len());
        for definition in &definitions {
            definition.validate()?;
            if !cache_ids.insert(definition.cache_id.as_str()) {
                return Err(CacheError::new(
                    ErrorCode::InvalidCacheDefinition,
                    "cache_id must be unique in Cache Definition array",
                ));
            }
        }
        Ok(definitions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(cache_id: &str) -> String {
        format!(
            r#"{{
                "cache_id":"{cache_id}","connection_id":"database","cache_type":"PRELOAD",
                "retention":{{"type":"NONE","value":0}},"primary_key":["id"],
                "select":{{"query":"SELECT id FROM users","fields":[]}},
                "insert":{{"query":"INSERT INTO users (id) VALUES ($1)","fields":["id"]}},
                "update":{{"query":"UPDATE users SET id=$1 WHERE id=$2","fields":["id","id"]}},
                "delete":{{"query":"DELETE FROM users WHERE id=$1","fields":["id"]}},
                "maps":[["id"]]
            }}"#
        )
    }

    #[test]
    fn parses_cache_definition_array() {
        let json = format!("[{},{}]", definition("users"), definition("sessions"));
        let definitions = CacheDefinition::list_from_json(&json).unwrap();
        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].cache_id, "users");
        assert_eq!(definitions[1].cache_id, "sessions");
    }

    #[test]
    fn rejects_empty_cache_definition_array() {
        let error = CacheDefinition::list_from_json("[]").unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidCacheDefinition);
    }

    #[test]
    fn rejects_duplicate_cache_id_in_array() {
        let item = definition("users");
        let error = CacheDefinition::list_from_json(&format!("[{item},{item}]")).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidCacheDefinition);
    }
}
