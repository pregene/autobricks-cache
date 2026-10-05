#ifndef AUTOBRICKS_CACHE_CORE_H
#define AUTOBRICKS_CACHE_CORE_H

#include <cstddef>
#include <cstdint>

extern "C" {

struct AbCacheCore;
struct AbCacheQueryResult;

struct AbCacheBytes {
    const std::uint8_t* data;
    std::size_t size;
};

struct AbCacheCoreStatus {
    std::size_t record_count;
    std::size_t record_bytes;
    std::size_t record_capacity_bytes;
    std::size_t slot_bytes;
    std::size_t map_count;
    std::size_t map_entry_count;
    std::size_t map_key_bytes;
    std::size_t map_reference_bytes;
};

struct AbCacheRecordView {
    const std::uint8_t* data;
    std::size_t size;
    std::uint32_t record_id;
};

AbCacheCore* ab_cache_core_create(
    std::size_t map_count,
    std::size_t expected_records,
    std::size_t expected_record_bytes,
    std::uint32_t retention_type,
    std::uint64_t retention_value);

int ab_cache_core_reserve(
    AbCacheCore* core,
    std::size_t expected_records,
    std::size_t expected_record_bytes);

void ab_cache_core_destroy(AbCacheCore* core);

int ab_cache_core_insert(
    AbCacheCore* core,
    AbCacheBytes record,
    const AbCacheBytes* map_keys,
    std::size_t map_key_count,
    std::uint64_t now,
    std::uint32_t* record_id);

int ab_cache_core_update(
    AbCacheCore* core,
    std::uint32_t record_id,
    AbCacheBytes record,
    const AbCacheBytes* map_keys,
    std::size_t map_key_count,
    std::uint64_t now);

int ab_cache_core_delete(AbCacheCore* core, std::uint32_t record_id);

int ab_cache_core_query(
    const AbCacheCore* core,
    std::size_t map_index,
    AbCacheBytes key,
    std::uint32_t* record_ids,
    std::size_t capacity,
    std::size_t* result_count);

int ab_cache_core_read(
    const AbCacheCore* core,
    std::uint32_t record_id,
    std::uint8_t* output,
    std::size_t capacity,
    std::size_t* record_size);

AbCacheQueryResult* ab_cache_core_query_view(
    AbCacheCore* core,
    std::size_t map_index,
    AbCacheBytes key,
    std::uint64_t now,
    std::size_t* result_count);

std::size_t ab_cache_query_result_count(const AbCacheQueryResult* result);

int ab_cache_query_result_record(
    const AbCacheQueryResult* result,
    std::size_t index,
    AbCacheRecordView* record);

void ab_cache_query_result_destroy(AbCacheQueryResult* result);

int ab_cache_core_retention_get(
    const AbCacheCore* core,
    std::uint32_t record_id,
    std::uint64_t* value);

int ab_cache_core_retention_set(
    AbCacheCore* core,
    std::uint32_t record_id,
    std::uint64_t value);

int ab_cache_core_drain(
    AbCacheCore* core,
    std::uint64_t now,
    std::uint64_t score_step,
    std::size_t* removed_count);

int ab_cache_core_status(const AbCacheCore* core, AbCacheCoreStatus* status);

}

#endif
