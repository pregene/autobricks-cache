use crate::cache::Cache;
use crate::connection::{ConnectionDefinition, ConnectionRuntime};
use crate::database::Database;
use crate::definition::{CacheDefinition, CacheType};
use crate::error::{CacheError, ErrorCode, Result};
use crate::system::CacheSystem;
use serde_json::json;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock};

struct PublicRuntime {
    system: Mutex<CacheSystem>,
    connection: Arc<ConnectionRuntime>,
}

impl PublicRuntime {
    fn stop(&self) -> Result<()> {
        self.connection.stop()?;
        self.system.lock().map_err(|_| lock_error())?.stop()
    }
}

static RUNTIME: OnceLock<Mutex<Option<Arc<PublicRuntime>>>> = OnceLock::new();

fn runtime() -> &'static Mutex<Option<Arc<PublicRuntime>>> {
    RUNTIME.get_or_init(|| Mutex::new(None))
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_initialize(
    connection_config: *const c_char,
    cache_config: *const c_char,
) -> *mut c_char {
    ffi_json(|| {
        initialize(
            required_string(connection_config, "connection_config")?,
            required_string(cache_config, "cache_config")?,
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_query(
    cache_id: *const c_char,
    input: *const c_char,
) -> *mut c_char {
    cache_call(cache_id, |cache| {
        Ok(cache.query(required_string(input, "input")?))
    })
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_insert(
    cache_id: *const c_char,
    input: *const c_char,
) -> *mut c_char {
    cache_call(cache_id, |cache| {
        Ok(cache.insert(required_string(input, "input")?))
    })
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_update(
    cache_id: *const c_char,
    input: *const c_char,
) -> *mut c_char {
    cache_call(cache_id, |cache| {
        Ok(cache.update(required_string(input, "input")?))
    })
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_delete(
    cache_id: *const c_char,
    input: *const c_char,
) -> *mut c_char {
    cache_call(cache_id, |cache| {
        Ok(cache.delete(required_string(input, "input")?))
    })
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_status(cache_id: *const c_char) -> *mut c_char {
    cache_call(cache_id, |cache| Ok(cache.status()))
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_uninitialize() -> *mut c_char {
    ffi_json(|| {
        let current = runtime()
            .lock()
            .map_err(|_| lock_error())?
            .take()
            .ok_or_else(|| {
                CacheError::new(ErrorCode::CacheNotReady, "Cache runtime is not initialized")
            })?;
        current.stop()?;
        Ok(success_json())
    })
}

#[no_mangle]
pub unsafe extern "C" fn ab_cache_string_free(value: *mut c_char) {
    if !value.is_null() {
        drop(CString::from_raw(value));
    }
}

fn initialize(connection_json: &str, cache_json: &str) -> Result<String> {
    let mut current = runtime().lock().map_err(|_| lock_error())?;
    if current.is_some() {
        return Err(CacheError::new(
            ErrorCode::InvalidStateTransition,
            "Cache runtime is already initialized",
        ));
    }
    let connection_definition = ConnectionDefinition::from_json(connection_json)?;
    let queue_directory = Path::new(&connection_definition.queue_directory);
    std::fs::create_dir_all(queue_directory).map_err(|error| {
        CacheError::new(
            ErrorCode::QueueDirectoryInvalid,
            format!("failed to create Queue directory: {error}"),
        )
    })?;
    let definitions = CacheDefinition::list_from_json(cache_json)?;
    if definitions
        .iter()
        .any(|definition| definition.connection_id != connection_definition.connection_id)
    {
        return Err(CacheError::new(
            ErrorCode::InvalidCacheDefinition,
            "Cache and Connection IDs do not match",
        ));
    }
    let connection = Arc::new(Database::open(&connection_definition, queue_directory)?);
    connection.start()?;
    let system = CacheSystem::start()?;
    for definition in definitions {
        let preload = definition.cache_type == CacheType::Preload;
        let cache = system.register(definition, Arc::clone(&connection))?;
        if preload {
            cache.preload()?;
        }
    }
    let cache_count = system.cache_count()?;
    *current = Some(Arc::new(PublicRuntime {
        system: Mutex::new(system),
        connection,
    }));
    Ok(json!({"code": 0, "message": "success", "cache_count": cache_count}).to_string())
}

unsafe fn cache_call(
    cache_id: *const c_char,
    operation: impl FnOnce(Arc<Cache>) -> Result<String>,
) -> *mut c_char {
    ffi_json(|| {
        let cache_id = required_string(cache_id, "cache_id")?;
        let current = runtime()
            .lock()
            .map_err(|_| lock_error())?
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                CacheError::new(ErrorCode::CacheNotReady, "Cache runtime is not initialized")
            })?;
        let cache = current
            .system
            .lock()
            .map_err(|_| lock_error())?
            .cache(cache_id)?
            .ok_or_else(|| {
                CacheError::new(ErrorCode::CacheNotReady, "cache_id is not initialized")
            })?;
        operation(cache)
    })
}

unsafe fn required_string<'a>(value: *const c_char, name: &str) -> Result<&'a str> {
    if value.is_null() {
        return Err(CacheError::new(
            ErrorCode::InvalidArgument,
            format!("{name} is null"),
        ));
    }
    CStr::from_ptr(value).to_str().map_err(|error| {
        CacheError::new(
            ErrorCode::InvalidArgument,
            format!("{name} is not UTF-8: {error}"),
        )
    })
}

fn ffi_json(operation: impl FnOnce() -> Result<String>) -> *mut c_char {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
    let value = match outcome {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => error_json(&error),
        Err(_) => {
            json!({"code": ErrorCode::InternalError as i32, "message": "Cache interface panicked"})
                .to_string()
        }
    };
    CString::new(value)
        .map(CString::into_raw)
        .unwrap_or(ptr::null_mut())
}

fn success_json() -> String {
    json!({"code": 0, "message": "success"}).to_string()
}

fn error_json(error: &CacheError) -> String {
    json!({"code": error.code() as i32, "message": error.message()}).to_string()
}

fn lock_error() -> CacheError {
    CacheError::new(ErrorCode::LockPoisoned, "Cache runtime is unavailable")
}
