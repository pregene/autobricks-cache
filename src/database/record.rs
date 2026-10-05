use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum DatabaseValue {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl PartialEq for DatabaseValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            (Self::Signed(left), Self::Signed(right)) => left == right,
            (Self::Unsigned(left), Self::Unsigned(right)) => left == right,
            (Self::Float(left), Self::Float(right)) => left.to_bits() == right.to_bits(),
            (Self::Text(left), Self::Text(right)) => left == right,
            (Self::Bytes(left), Self::Bytes(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for DatabaseValue {}

impl Hash for DatabaseValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::Signed(value) => value.hash(state),
            Self::Unsigned(value) => value.hash(state),
            Self::Float(value) => value.to_bits().hash(state),
            Self::Text(value) => value.hash(state),
            Self::Bytes(value) => value.hash(state),
        }
    }
}

impl From<&str> for DatabaseValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for DatabaseValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for DatabaseValue {
    fn from(value: i64) -> Self {
        Self::Signed(value)
    }
}

impl From<u64> for DatabaseValue {
    fn from(value: u64) -> Self {
        Self::Unsigned(value)
    }
}

pub type DatabaseRecord = BTreeMap<String, DatabaseValue>;

impl DatabaseValue {
    pub(crate) fn allocated_bytes(&self) -> usize {
        match self {
            Self::Text(value) => value.capacity(),
            Self::Bytes(value) => value.capacity(),
            _ => 0,
        }
    }
}
