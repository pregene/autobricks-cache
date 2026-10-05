#include "cache_core.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

constexpr int OK = 0;
constexpr int INVALID_ARGUMENT = 1;
constexpr int RECORD_NOT_FOUND = 2;
constexpr int CAPACITY_TOO_SMALL = 3;
constexpr int RECORD_ID_EXHAUSTED = 4;
constexpr std::uint32_t INVALID_RECORD_ID = std::numeric_limits<std::uint32_t>::max();
constexpr std::size_t LOCK_STRIPE_COUNT = 64;
constexpr std::size_t RECORD_LOCK_COUNT = 64;

class RecordLockManager {
public:
    class Lease {
    public:
        Lease() = default;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept { move_from(other); }
        Lease& operator=(Lease&& other) noexcept {
            if (this != &other) {
                release();
                move_from(other);
            }
            return *this;
        }
        ~Lease() { release(); }

    private:
        friend class RecordLockManager;
        Lease(RecordLockManager* owner, std::size_t index)
            : owner_(owner), index_(index), lock_(owner->slots_[index].mutex) {}

        void move_from(Lease& other) noexcept {
            owner_ = std::exchange(other.owner_, nullptr);
            index_ = other.index_;
            lock_ = std::move(other.lock_);
        }
        void release() {
            if (owner_ == nullptr) return;
            lock_.unlock();
            owner_->release(index_);
            owner_ = nullptr;
        }

        RecordLockManager* owner_ = nullptr;
        std::size_t index_ = 0;
        std::unique_lock<std::mutex> lock_;
    };

    Lease acquire(std::uint32_t record_id) {
        for (;;) {
            const auto index = try_acquire(record_id);
            if (index != RECORD_LOCK_COUNT) return Lease(this, index);
            std::unique_lock standby(standby_mutex_);
            standby_condition_.wait(standby, [&] { return can_acquire(record_id); });
        }
    }

private:
    struct Slot {
        std::mutex mutex;
        std::atomic<std::uint64_t> state{0};
    };

    static std::uint64_t assigned_state(std::uint32_t record_id, std::uint32_t users) {
        return (static_cast<std::uint64_t>(record_id) + 1U) << 32U | users;
    }
    static std::uint32_t assigned_record(std::uint64_t state) {
        return static_cast<std::uint32_t>((state >> 32U) - 1U);
    }
    static std::uint32_t assigned_users(std::uint64_t state) {
        return static_cast<std::uint32_t>(state);
    }
    std::size_t try_acquire(std::uint32_t record_id) {
        const auto start = static_cast<std::size_t>(record_id) % slots_.size();
        for (std::size_t offset = 0; offset < slots_.size(); ++offset) {
            const auto index = (start + offset) % slots_.size();
            auto state = slots_[index].state.load(std::memory_order_acquire);
            while (state != 0 && assigned_record(state) == record_id) {
                const auto users = assigned_users(state);
                if (users == std::numeric_limits<std::uint32_t>::max()) break;
                if (slots_[index].state.compare_exchange_weak(
                        state, assigned_state(record_id, users + 1),
                        std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return index;
                }
            }
        }
        for (std::size_t offset = 0; offset < slots_.size(); ++offset) {
            const auto index = (start + offset) % slots_.size();
            std::uint64_t empty = 0;
            if (slots_[index].state.compare_exchange_strong(
                    empty, assigned_state(record_id, 1),
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                return index;
            }
        }
        return RECORD_LOCK_COUNT;
    }
    bool can_acquire(std::uint32_t record_id) const {
        for (const auto& slot : slots_) {
            const auto state = slot.state.load(std::memory_order_acquire);
            if (state == 0 || assigned_record(state) == record_id) return true;
        }
        return false;
    }
    void release(std::size_t index) {
        auto state = slots_[index].state.load(std::memory_order_acquire);
        for (;;) {
            const auto users = assigned_users(state);
            const auto next = users == 1 ? 0 : state - 1;
            if (slots_[index].state.compare_exchange_weak(
                    state, next, std::memory_order_acq_rel, std::memory_order_acquire)) {
                if (next == 0) standby_condition_.notify_one();
                return;
            }
        }
    }

    std::array<Slot, RECORD_LOCK_COUNT> slots_;
    std::mutex standby_mutex_;
    std::condition_variable standby_condition_;
};

struct RecordSlot {
    std::shared_ptr<std::uint8_t[]> version;
    std::size_t size = 0;
    bool active = false;
    std::atomic<std::uint64_t> retention{0};

    RecordSlot() = default;
    RecordSlot(const RecordSlot&) = delete;
    RecordSlot& operator=(const RecordSlot&) = delete;
    RecordSlot(RecordSlot&& other) noexcept
        : version(std::move(other.version)),
          size(other.size),
          active(other.active),
          retention(other.retention.load(std::memory_order_relaxed)) {}
    RecordSlot& operator=(RecordSlot&& other) noexcept {
        version = std::move(other.version);
        size = other.size;
        active = other.active;
        retention.store(other.retention.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
        return *this;
    }
};

struct RecordReferences {
    std::uint32_t first = INVALID_RECORD_ID;
    std::vector<std::uint32_t> additional;

    void insert(std::uint32_t record_id) {
        if (first == INVALID_RECORD_ID) {
            first = record_id;
        } else if (first != record_id &&
                   std::find(additional.begin(), additional.end(), record_id) == additional.end()) {
            additional.push_back(record_id);
        }
    }

    void remove(std::uint32_t record_id) {
        if (first == record_id) {
            if (additional.empty()) {
                first = INVALID_RECORD_ID;
            } else {
                first = additional.back();
                additional.pop_back();
            }
        } else {
            additional.erase(
                std::remove(additional.begin(), additional.end(), record_id),
                additional.end());
        }
    }

    bool empty() const { return first == INVALID_RECORD_ID; }
    std::size_t size() const { return empty() ? 0 : 1 + additional.size(); }
    bool contains(std::uint32_t record_id) const {
        return first == record_id ||
               std::find(additional.begin(), additional.end(), record_id) != additional.end();
    }
};

struct StringHash {
    using is_transparent = void;

    std::size_t operator()(std::string_view value) const noexcept {
        return std::hash<std::string_view>{}(value);
    }
};

struct StringEqual {
    using is_transparent = void;

    bool operator()(std::string_view left, std::string_view right) const noexcept {
        return left == right;
    }
};

using Index = std::unordered_map<std::string, RecordReferences, StringHash, StringEqual>;

std::string_view key_view(AbCacheBytes key) {
    if (key.size == 0) return {};
    return std::string_view(reinterpret_cast<const char*>(key.data), key.size);
}

std::string key_string(AbCacheBytes key) {
    return std::string(key_view(key));
}

}  // namespace

struct AbCacheCore {
    mutable std::mutex writer_mutex;
    mutable std::array<std::shared_mutex, LOCK_STRIPE_COUNT> stripes;
    mutable RecordLockManager record_locks;
    std::vector<RecordSlot> slots;
    std::vector<std::uint32_t> free_record_ids;
    std::vector<Index> maps;
    std::size_t active_records = 0;
    std::size_t active_record_bytes = 0;
    std::uint32_t retention_type = 0;
    std::uint64_t retention_value = 0;
};

struct AbCacheQueryResult {
    std::vector<std::uint32_t> candidates;
    std::vector<std::shared_ptr<std::uint8_t[]>> versions;
    std::vector<AbCacheRecordView> views;
};

namespace {

struct QueryResultPool {
    std::vector<AbCacheQueryResult*> available;

    ~QueryResultPool() {
        for (auto* result : available) delete result;
    }
};

thread_local QueryResultPool query_result_pool;

bool valid_bytes(AbCacheBytes bytes) { return bytes.size == 0 || bytes.data != nullptr; }

std::size_t stripe_index(AbCacheBytes key) {
    return StringHash{}(key_view(key)) % LOCK_STRIPE_COUNT;
}

std::vector<std::unique_lock<std::shared_mutex>> lock_all_exclusive(AbCacheCore& core) {
    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(LOCK_STRIPE_COUNT);
    for (auto& stripe : core.stripes) locks.emplace_back(stripe);
    return locks;
}

void initialize_retention(AbCacheCore& core, RecordSlot& slot, std::uint64_t now) {
    const auto value = core.retention_type == 1 ? core.retention_value : now;
    slot.retention.store(value, std::memory_order_release);
}

void touch_retention(AbCacheCore& core, RecordSlot& slot, std::uint64_t now) {
    if (core.retention_type == 0) return;
    const auto value = core.retention_type == 1 ? core.retention_value : now;
    slot.retention.store(value, std::memory_order_release);
}

std::shared_ptr<std::uint8_t[]> make_version(AbCacheBytes record) {
    auto version = std::shared_ptr<std::uint8_t[]>(new std::uint8_t[record.size]);
    if (record.size != 0) std::memcpy(version.get(), record.data, record.size);
    return version;
}

void remove_references(AbCacheCore& core, std::uint32_t record_id) {
    for (auto& index : core.maps) {
        for (auto iterator = index.begin(); iterator != index.end();) {
            iterator->second.remove(record_id);
            if (iterator->second.empty()) {
                iterator = index.erase(iterator);
            } else {
                ++iterator;
            }
        }
    }
}

void add_references(
    AbCacheCore& core,
    std::uint32_t record_id,
    const AbCacheBytes* map_keys,
    std::size_t map_key_count) {
    for (std::size_t index = 0; index < map_key_count; ++index) {
        core.maps[index][key_string(map_keys[index])].insert(record_id);
    }
}

}  // namespace

extern "C" AbCacheCore* ab_cache_core_create(
    std::size_t map_count,
    std::size_t expected_records,
    std::size_t expected_record_bytes,
    std::uint32_t retention_type,
    std::uint64_t retention_value) {
    if (retention_type > 2 || (retention_type == 0 && retention_value != 0) ||
        (retention_type != 0 && retention_value == 0)) {
        return nullptr;
    }
    try {
        auto core = std::make_unique<AbCacheCore>();
        (void)expected_record_bytes;
        core->retention_type = retention_type;
        core->retention_value = retention_value;
        core->slots.reserve(expected_records);
        core->maps.resize(map_count);
        for (auto& map : core->maps) {
            map.reserve(expected_records);
        }
        return core.release();
    } catch (...) {
        return nullptr;
    }
}

extern "C" void ab_cache_core_destroy(AbCacheCore* core) { delete core; }

extern "C" int ab_cache_core_reserve(
    AbCacheCore* core,
    std::size_t expected_records,
    std::size_t expected_record_bytes) {
    if (core == nullptr) return INVALID_ARGUMENT;
    try {
        (void)expected_record_bytes;
        std::unique_lock writer(core->writer_mutex);
        auto locks = lock_all_exclusive(*core);
        core->slots.reserve(expected_records);
        for (auto& map : core->maps) map.reserve(expected_records);
        return OK;
    } catch (...) {
        return INVALID_ARGUMENT;
    }
}

extern "C" int ab_cache_core_insert(
    AbCacheCore* core,
    AbCacheBytes record,
    const AbCacheBytes* map_keys,
    std::size_t map_key_count,
    std::uint64_t now,
    std::uint32_t* record_id) {
    if (core == nullptr || record_id == nullptr || !valid_bytes(record) ||
        map_key_count != core->maps.size() || (map_key_count != 0 && map_keys == nullptr)) {
        return INVALID_ARGUMENT;
    }
    for (std::size_t index = 0; index < map_key_count; ++index) {
        if (!valid_bytes(map_keys[index])) return INVALID_ARGUMENT;
    }
    try {
        auto version = make_version(record);
        std::unique_lock writer(core->writer_mutex);
        auto locks = lock_all_exclusive(*core);
        std::uint32_t id;
        if (!core->free_record_ids.empty()) {
            id = core->free_record_ids.back();
            core->free_record_ids.pop_back();
        } else {
            if (core->slots.size() >= INVALID_RECORD_ID) return RECORD_ID_EXHAUSTED;
            id = static_cast<std::uint32_t>(core->slots.size());
            core->slots.emplace_back();
        }
        auto& slot = core->slots[id];
        slot.version = std::move(version);
        slot.size = record.size;
        slot.active = true;
        add_references(*core, id, map_keys, map_key_count);
        initialize_retention(*core, slot, now);
        ++core->active_records;
        core->active_record_bytes += record.size;
        *record_id = id;
        return OK;
    } catch (...) {
        return INVALID_ARGUMENT;
    }
}

extern "C" int ab_cache_core_update(
    AbCacheCore* core,
    std::uint32_t record_id,
    AbCacheBytes record,
    const AbCacheBytes* map_keys,
    std::size_t map_key_count,
    std::uint64_t now) {
    if (core == nullptr || !valid_bytes(record) || map_key_count != core->maps.size() ||
        (map_key_count != 0 && map_keys == nullptr)) {
        return INVALID_ARGUMENT;
    }
    try {
        auto version = make_version(record);
        std::unique_lock writer(core->writer_mutex);
        auto record_lock = core->record_locks.acquire(record_id);
        auto locks = lock_all_exclusive(*core);
        if (record_id >= core->slots.size() || !core->slots[record_id].active) {
            return RECORD_NOT_FOUND;
        }
        for (std::size_t index = 0; index < map_key_count; ++index) {
            if (!valid_bytes(map_keys[index])) return INVALID_ARGUMENT;
        }
        remove_references(*core, record_id);
        auto& slot = core->slots[record_id];
        core->active_record_bytes -= slot.size;
        slot.version = std::move(version);
        slot.size = record.size;
        core->active_record_bytes += record.size;
        add_references(*core, record_id, map_keys, map_key_count);
        initialize_retention(*core, slot, now);
        return OK;
    } catch (...) {
        return INVALID_ARGUMENT;
    }
}

extern "C" int ab_cache_core_delete(AbCacheCore* core, std::uint32_t record_id) {
    if (core == nullptr) return INVALID_ARGUMENT;
    std::unique_lock writer(core->writer_mutex);
    auto record_lock = core->record_locks.acquire(record_id);
    auto locks = lock_all_exclusive(*core);
    if (record_id >= core->slots.size() || !core->slots[record_id].active) {
        return RECORD_NOT_FOUND;
    }
    remove_references(*core, record_id);
    auto& slot = core->slots[record_id];
    core->active_record_bytes -= slot.size;
    slot.version.reset();
    slot.size = 0;
    slot.active = false;
    core->free_record_ids.push_back(record_id);
    --core->active_records;
    return OK;
}

extern "C" int ab_cache_core_query(
    const AbCacheCore* core,
    std::size_t map_index,
    AbCacheBytes key,
    std::uint32_t* record_ids,
    std::size_t capacity,
    std::size_t* result_count) {
    if (core == nullptr || result_count == nullptr || !valid_bytes(key) ||
        map_index >= core->maps.size()) {
        return INVALID_ARGUMENT;
    }
    std::shared_lock lock(core->stripes[stripe_index(key)]);
    const auto found = core->maps[map_index].find(key_view(key));
    if (found == core->maps[map_index].end()) {
        *result_count = 0;
        return OK;
    }
    *result_count = found->second.size();
    if (capacity < *result_count || (capacity != 0 && record_ids == nullptr)) {
        return CAPACITY_TOO_SMALL;
    }
    std::size_t output = 0;
    record_ids[output++] = found->second.first;
    for (const auto record_id : found->second.additional) record_ids[output++] = record_id;
    return OK;
}

extern "C" int ab_cache_core_read(
    const AbCacheCore* core,
    std::uint32_t record_id,
    std::uint8_t* output,
    std::size_t capacity,
    std::size_t* record_size) {
    if (core == nullptr || record_size == nullptr) return INVALID_ARGUMENT;
    auto record_lock = core->record_locks.acquire(record_id);
    std::shared_lock lock(core->stripes[0]);
    if (record_id >= core->slots.size() || !core->slots[record_id].active) {
        return RECORD_NOT_FOUND;
    }
    const auto& slot = core->slots[record_id];
    *record_size = slot.size;
    if (capacity < slot.size || (slot.size != 0 && output == nullptr)) {
        return CAPACITY_TOO_SMALL;
    }
    if (slot.size != 0) std::memcpy(output, slot.version.get(), slot.size);
    return OK;
}

extern "C" AbCacheQueryResult* ab_cache_core_query_view(
    AbCacheCore* core,
    std::size_t map_index,
    AbCacheBytes key,
    std::uint64_t now,
    std::size_t* result_count) {
    if (core == nullptr || result_count == nullptr || !valid_bytes(key) ||
        map_index >= core->maps.size()) {
        return nullptr;
    }
    AbCacheQueryResult* result = nullptr;
    try {
        if (query_result_pool.available.empty()) {
            result = new AbCacheQueryResult();
        } else {
            result = query_result_pool.available.back();
            query_result_pool.available.pop_back();
        }
        result->candidates.clear();
        result->versions.clear();
        result->views.clear();
        {
            std::shared_lock map_lock(core->stripes[stripe_index(key)]);
            const auto found = core->maps[map_index].find(key_view(key));
            if (found != core->maps[map_index].end()) {
                result->candidates.reserve(found->second.size());
                result->candidates.push_back(found->second.first);
                result->candidates.insert(result->candidates.end(),
                                          found->second.additional.begin(),
                                          found->second.additional.end());
            }
        }
        result->versions.reserve(result->candidates.size());
        result->views.reserve(result->candidates.size());
        for (const auto record_id : result->candidates) {
            auto record_lock = core->record_locks.acquire(record_id);
            std::shared_lock map_lock(core->stripes[stripe_index(key)]);
            const auto found = core->maps[map_index].find(key_view(key));
            if (found == core->maps[map_index].end() || !found->second.contains(record_id) ||
                record_id >= core->slots.size() || !core->slots[record_id].active) {
                continue;
            }
            auto& slot = core->slots[record_id];
            touch_retention(*core, slot, now);
            result->versions.push_back(slot.version);
            result->views.push_back(AbCacheRecordView{
                result->versions.back().get(),
                slot.size,
                record_id,
            });
        }
        *result_count = result->views.size();
        return result;
    } catch (...) {
        if (result != nullptr) {
            result->candidates.clear();
            result->versions.clear();
            result->views.clear();
            query_result_pool.available.push_back(result);
        }
        return nullptr;
    }
}

extern "C" std::size_t ab_cache_query_result_count(const AbCacheQueryResult* result) {
    return result == nullptr ? 0 : result->views.size();
}

extern "C" int ab_cache_query_result_record(
    const AbCacheQueryResult* result,
    std::size_t index,
    AbCacheRecordView* record) {
    if (result == nullptr || record == nullptr || index >= result->views.size()) {
        return INVALID_ARGUMENT;
    }
    *record = result->views[index];
    return OK;
}

extern "C" void ab_cache_query_result_destroy(AbCacheQueryResult* result) {
    if (result == nullptr) return;
    result->candidates.clear();
    result->versions.clear();
    result->views.clear();
    query_result_pool.available.push_back(result);
}

extern "C" int ab_cache_core_retention_get(
    const AbCacheCore* core,
    std::uint32_t record_id,
    std::uint64_t* value) {
    if (core == nullptr || value == nullptr) return INVALID_ARGUMENT;
    auto record_lock = core->record_locks.acquire(record_id);
    std::shared_lock lock(core->stripes[0]);
    if (record_id >= core->slots.size() || !core->slots[record_id].active) {
        return RECORD_NOT_FOUND;
    }
    *value = core->slots[record_id].retention.load(std::memory_order_acquire);
    return OK;
}

extern "C" int ab_cache_core_retention_set(
    AbCacheCore* core,
    std::uint32_t record_id,
    std::uint64_t value) {
    if (core == nullptr) return INVALID_ARGUMENT;
    auto record_lock = core->record_locks.acquire(record_id);
    std::shared_lock lock(core->stripes[0]);
    if (record_id >= core->slots.size() || !core->slots[record_id].active) {
        return RECORD_NOT_FOUND;
    }
    core->slots[record_id].retention.store(value, std::memory_order_release);
    return OK;
}

extern "C" int ab_cache_core_drain(
    AbCacheCore* core,
    std::uint64_t now,
    std::uint64_t score_step,
    std::size_t* removed_count) {
    if (core == nullptr || removed_count == nullptr) return INVALID_ARGUMENT;
    std::unique_lock writer(core->writer_mutex);
    *removed_count = 0;
    if (core->retention_type == 0) return OK;
    std::size_t slot_count;
    {
        std::shared_lock lock(core->stripes[0]);
        slot_count = core->slots.size();
    }
    for (std::size_t index = 0; index < slot_count; ++index) {
        auto record_lock = core->record_locks.acquire(static_cast<std::uint32_t>(index));
        auto locks = lock_all_exclusive(*core);
        auto& slot = core->slots[index];
        if (!slot.active) continue;
        const auto current = slot.retention.load(std::memory_order_acquire);
        bool remove = false;
        if (core->retention_type == 1) {
            const auto next = current > score_step ? current - score_step : 0;
            slot.retention.store(next, std::memory_order_release);
            remove = next == 0;
        } else {
            remove = now >= current && now - current >= core->retention_value;
        }
        if (remove) {
            const auto record_id = static_cast<std::uint32_t>(index);
            remove_references(*core, record_id);
            core->active_record_bytes -= slot.size;
            slot.version.reset();
            slot.size = 0;
            slot.active = false;
            core->free_record_ids.push_back(record_id);
            --core->active_records;
            ++*removed_count;
        }
    }
    return OK;
}

extern "C" int ab_cache_core_status(const AbCacheCore* core, AbCacheCoreStatus* status) {
    if (core == nullptr || status == nullptr) return INVALID_ARGUMENT;
    std::shared_lock lock(core->stripes[0]);
    *status = AbCacheCoreStatus{};
    status->record_count = core->active_records;
    status->record_bytes = core->active_record_bytes;
    status->record_capacity_bytes = core->active_record_bytes;
    status->slot_bytes = core->slots.capacity() * sizeof(RecordSlot) +
                         core->free_record_ids.capacity() * sizeof(std::uint32_t);
    status->map_count = core->maps.size();
    for (const auto& index : core->maps) {
        status->map_entry_count += index.size();
        for (const auto& [key, references] : index) {
            status->map_key_bytes += key.capacity();
            status->map_reference_bytes += sizeof(RecordReferences) +
                                           references.additional.capacity() * sizeof(std::uint32_t);
        }
    }
    return OK;
}
