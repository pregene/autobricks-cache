use super::ErrorCode;
use std::error::Error;
use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, CacheError>;

#[derive(Debug)]
pub struct CacheError {
    code: ErrorCode,
    message: String,
}

impl CacheError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for CacheError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code as i32, self.message)
    }
}

impl Error for CacheError {}
