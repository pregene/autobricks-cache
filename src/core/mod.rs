use crate::error::{CacheError, ErrorCode, Result};
use std::ffi::c_int;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

const OK: c_int = 0;
const CAPACITY_TOO_SMALL: c_int = 3;

#[repr(C)]
struct CoreOpaque {
    _private: [u8; 0],
}

#[repr(C)]
struct QueryResultOpaque {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Default)]
struct RawRecordView {
    data: *const u8,
    size: usize,
    record_id: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Bytes {
    data: *const u8,
    size: usize,
}

#[repr(C)]
#[derive(Default)]
struct RawStatus {
    record_count: usize,
    record_bytes: usize,
    record_capacity_bytes: usize,
    slot_bytes: usize,
    map_count: usize,
    map_entry_count: usize,
    map_key_bytes: usize,
    map_reference_bytes: usize,
}

unsafe extern "C" {
    fn ab_cache_core_create(
        map_count: usize,
        expected_records: usize,
        expected_record_bytes: usize,
        retention_type: u32,
        retention_value: u64,
    ) -> *mut CoreOpaque;
    fn ab_cache_core_destroy(core: *mut CoreOpaque);
    fn ab_cache_core_reserve(
        core: *mut CoreOpaque,
        expected_records: usize,
        expected_record_bytes: usize,
    ) -> c_int;
    fn ab_cache_core_insert(
        core: *mut CoreOpaque,
        record: Bytes,
        map_keys: *const Bytes,
        map_key_count: usize,
        now: u64,
        record_id: *mut u32,
    ) -> c_int;
    fn ab_cache_core_update(
        core: *mut CoreOpaque,
        record_id: u32,
        record: Bytes,
        map_keys: *const Bytes,
        map_key_count: usize,
        now: u64,
    ) -> c_int;
    fn ab_cache_core_delete(core: *mut CoreOpaque, record_id: u32) -> c_int;
    fn ab_cache_core_query(
        core: *const CoreOpaque,
        map_index: usize,
        key: Bytes,
        record_ids: *mut u32,
        capacity: usize,
        result_count: *mut usize,
    ) -> c_int;
    fn ab_cache_core_read(
        core: *const CoreOpaque,
        record_id: u32,
        output: *mut u8,
        capacity: usize,
        record_size: *mut usize,
    ) -> c_int;
    fn ab_cache_core_status(core: *const CoreOpaque, status: *mut RawStatus) -> c_int;
    fn ab_cache_core_query_view(
        core: *mut CoreOpaque,
        map_index: usize,
        key: Bytes,
        now: u64,
        result_count: *mut usize,
    ) -> *mut QueryResultOpaque;
    fn ab_cache_query_result_count(result: *const QueryResultOpaque) -> usize;
    fn ab_cache_query_result_record(
        result: *const QueryResultOpaque,
        index: usize,
        record: *mut RawRecordView,
    ) -> c_int;
    fn ab_cache_query_result_destroy(result: *mut QueryResultOpaque);
    fn ab_cache_core_retention_get(
        core: *const CoreOpaque,
        record_id: u32,
        value: *mut u64,
    ) -> c_int;
    fn ab_cache_core_retention_set(core: *mut CoreOpaque, record_id: u32, value: u64) -> c_int;
    fn ab_cache_core_drain(
        core: *mut CoreOpaque,
        now: u64,
        score_step: u64,
        removed_count: *mut usize,
    ) -> c_int;
}

pub(crate) struct Core {
    handle: NonNull<CoreOpaque>,
}

unsafe impl Send for Core {}
unsafe impl Sync for Core {}

pub(crate) struct CoreStatus {
    pub record_count: usize,
    pub memory_bytes: usize,
}

pub(crate) struct QueryResult<'a> {
    result: NonNull<QueryResultOpaque>,
    count: usize,
    _core: PhantomData<&'a Core>,
    _not_send: PhantomData<Rc<()>>,
}

pub struct RecordView<'a> {
    pub data: &'a [u8],
    pub record_id: u32,
}

impl Core {
    pub(crate) fn new(
        map_count: usize,
        expected_records: usize,
        bytes: usize,
        retention_type: u32,
        retention_value: u64,
    ) -> Result<Self> {
        let handle = NonNull::new(unsafe {
            ab_cache_core_create(
                map_count,
                expected_records,
                bytes,
                retention_type,
                retention_value,
            )
        })
        .ok_or_else(|| {
            CacheError::new(ErrorCode::InternalError, "C++ Cache Core creation failed")
        })?;
        Ok(Self { handle })
    }

    pub(crate) fn reserve(&self, records: usize, bytes: usize) -> Result<()> {
        check(unsafe { ab_cache_core_reserve(self.handle.as_ptr(), records, bytes) })
    }

    pub(crate) fn insert(&self, record: &[u8], keys: &[Vec<u8>], now: u64) -> Result<u32> {
        let raw_keys = raw_keys(keys);
        let mut record_id = 0;
        check(unsafe {
            ab_cache_core_insert(
                self.handle.as_ptr(),
                bytes(record),
                raw_keys.as_ptr(),
                raw_keys.len(),
                now,
                &mut record_id,
            )
        })?;
        Ok(record_id)
    }

    pub(crate) fn update(&self, id: u32, record: &[u8], keys: &[Vec<u8>], now: u64) -> Result<()> {
        let raw_keys = raw_keys(keys);
        check(unsafe {
            ab_cache_core_update(
                self.handle.as_ptr(),
                id,
                bytes(record),
                raw_keys.as_ptr(),
                raw_keys.len(),
                now,
            )
        })
    }

    pub(crate) fn delete(&self, id: u32) -> Result<()> {
        check(unsafe { ab_cache_core_delete(self.handle.as_ptr(), id) })
    }

    pub(crate) fn query(&self, map_index: usize, key: &[u8]) -> Result<Vec<u32>> {
        let mut count = 0;
        let result = unsafe {
            ab_cache_core_query(
                self.handle.as_ptr(),
                map_index,
                bytes(key),
                std::ptr::null_mut(),
                0,
                &mut count,
            )
        };
        if result == OK && count == 0 {
            return Ok(Vec::new());
        }
        if result != CAPACITY_TOO_SMALL {
            check(result)?;
        }
        let mut ids = vec![0; count];
        check(unsafe {
            ab_cache_core_query(
                self.handle.as_ptr(),
                map_index,
                bytes(key),
                ids.as_mut_ptr(),
                ids.len(),
                &mut count,
            )
        })?;
        ids.truncate(count);
        Ok(ids)
    }

    pub(crate) fn read(&self, id: u32) -> Result<Vec<u8>> {
        let mut size = 0;
        let result = unsafe {
            ab_cache_core_read(self.handle.as_ptr(), id, std::ptr::null_mut(), 0, &mut size)
        };
        if result != CAPACITY_TOO_SMALL && !(result == OK && size == 0) {
            check(result)?;
        }
        let mut output = vec![0; size];
        check(unsafe {
            ab_cache_core_read(
                self.handle.as_ptr(),
                id,
                output.as_mut_ptr(),
                output.len(),
                &mut size,
            )
        })?;
        output.truncate(size);
        Ok(output)
    }

    pub(crate) fn status(&self) -> Result<CoreStatus> {
        let mut status = RawStatus::default();
        check(unsafe { ab_cache_core_status(self.handle.as_ptr(), &mut status) })?;
        let memory_bytes = status
            .record_capacity_bytes
            .checked_add(status.slot_bytes)
            .and_then(|value| value.checked_add(status.map_key_bytes))
            .and_then(|value| value.checked_add(status.map_reference_bytes))
            .ok_or_else(|| {
                CacheError::new(ErrorCode::InternalError, "Core memory accounting overflow")
            })?;
        Ok(CoreStatus {
            record_count: status.record_count,
            memory_bytes,
        })
    }

    pub(crate) fn query_view(
        &self,
        map_index: usize,
        key: &[u8],
        now: u64,
    ) -> Result<QueryResult<'_>> {
        let mut count = 0;
        let result = NonNull::new(unsafe {
            ab_cache_core_query_view(self.handle.as_ptr(), map_index, bytes(key), now, &mut count)
        })
        .ok_or_else(|| {
            CacheError::new(ErrorCode::InternalError, "C++ query view creation failed")
        })?;
        Ok(QueryResult {
            result,
            count,
            _core: PhantomData,
            _not_send: PhantomData,
        })
    }

    pub(crate) fn retention(&self, id: u32) -> Result<u64> {
        let mut value = 0;
        check(unsafe { ab_cache_core_retention_get(self.handle.as_ptr(), id, &mut value) })?;
        Ok(value)
    }

    pub(crate) fn set_retention(&self, id: u32, value: u64) -> Result<()> {
        check(unsafe { ab_cache_core_retention_set(self.handle.as_ptr(), id, value) })
    }

    pub(crate) fn drain(&self, now: u64, score_step: u64) -> Result<usize> {
        let mut removed = 0;
        check(unsafe { ab_cache_core_drain(self.handle.as_ptr(), now, score_step, &mut removed) })?;
        Ok(removed)
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        unsafe { ab_cache_core_destroy(self.handle.as_ptr()) };
    }
}

impl QueryResult<'_> {
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn record(&self, index: usize) -> Result<RecordView<'_>> {
        let mut view = RawRecordView::default();
        check(unsafe { ab_cache_query_result_record(self.result.as_ptr(), index, &mut view) })?;
        let data = if view.size == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(view.data, view.size) }
        };
        Ok(RecordView {
            data,
            record_id: view.record_id,
        })
    }
}

impl Drop for QueryResult<'_> {
    fn drop(&mut self) {
        unsafe { ab_cache_query_result_destroy(self.result.as_ptr()) };
    }
}

fn bytes(value: &[u8]) -> Bytes {
    Bytes {
        data: value.as_ptr(),
        size: value.len(),
    }
}

fn raw_keys(keys: &[Vec<u8>]) -> Vec<Bytes> {
    keys.iter().map(|key| bytes(key)).collect()
}

fn check(code: c_int) -> Result<()> {
    if code == OK {
        Ok(())
    } else {
        Err(CacheError::new(
            ErrorCode::InternalError,
            format!("C++ Cache Core failed with code {code}"),
        ))
    }
}
