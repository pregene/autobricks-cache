//! Autobricks database cache runtime.

#![allow(dead_code)]

include!(concat!(env!("OUT_DIR"), "/product_metadata.rs"));

mod cache;
mod connection;
mod core;
mod database;
mod definition;
mod error;
mod ffi;
mod operation;
mod retention;
mod system;

pub use cache::Cache;
pub use connection::ConnectionRuntime;
pub use database::{DatabaseRecord, DatabaseValue};
pub use error::{CacheError, ErrorCode, Result};
