# Cache Architecture

This document defines the in-memory record, MAP, initial loading, and change-processing architecture of Autobricks Cache.

## Public Interface

Cache users call the following five interfaces.

```text
cache.query()
cache.update()
cache.insert()
cache.delete()
cache.status()
```

- `query()` looks up a MAP. On an ON_DEMAND Cache Miss, it executes the single SELECT using SELECT inputs supplied separately from the MAP key, loads the returned records, and then queries the same MAP again.
- `insert()`, `update()`, and `delete()` change the Cache first and enqueue the DB operation in the Connection Queue.
- `status()` returns the current Cache record count and the estimated memory usage owned by the Cache.
- Initial loading, Retention Drain, Connection checkout and return, and Background WRITE are internal lifecycle functions and are not added to the Cache user interface.

## Baseline Structure

The basic memory structure of Autobricks Cache is as follows.

```text
Rust Cache Runtime
  └── C++ Cache Core
        ├── Immutable Record Versions
        ├── RecordId Slot
        ├── MAP 1 ──► RecordId
        ├── MAP 2 ──► RecordId
        ├── MAP N ──► RecordId List
        ├── Immutable Record Version
        ├── Retention
        ├── 64 MAP Lock Stripes
        └── 64 Record Locks + 1 Standby Lock
```

- Rust manages the public interface and the DB Record conversion boundary.
- The C++ Core stores the current Record Version in the Record Slot.
- MAPs store `uint32_t RecordId` values instead of copies of source records or Primary Keys.
- Do not create a record copy for each MAP.
- Retention lookup updates and Drain operations are handled inside the C++ Core.
- A Record Version returned by QUERY remains unchanged after UPDATE and DELETE.
- When updating an existing record, remove every old MAP reference before adding new MAP references.
- Distinguish an uninitialized Cache from an empty lookup result.

## Runtime Structure

The relationship between Connection Runtime and Cache Runtime is as follows.

```text
Cache Runtime
  ├── Connection Runtime
  │     ├── Persistent Queue
  │     └── DBWorkerThread
  │
  └── Cache Runtime
        ├── Active Store
        ├── Primary MAP
        └── MAP 1..N
```

| Component | Role |
| --- | --- |
| Database Record | Existing DB fields and values obtained from a SELECT result |
| Primary MAP | Single-record lookup by Primary Key |
| MAP | Lookup of the record-reference set matching a condition |
| Cache Runtime | Per-Cache Store, MAP, and concurrency management |
| Connection Runtime | Persistent Queue, DBWorkerThread, and DB connection management |
| C ABI | Provides Cache functions to external programs |
| Error Contract | Provides stable error numbers and asynchronous results |

## Record and MAP Ownership

The C++ Record Version owns the record, and every MAP references the same record's `RecordId`.

```text
Record Store
  └── RecordId ──► Record A
        ▲       ▲       ▲
        │       │       │
     PK MAP  MAP(id)  MAP(user_id)
```

The following structure is prohibited.

```text
PK MAP      ──► Record A copy 1
USER_ID MAP ──► Record A copy 2
```

Per-MAP copies can cause only one MAP to be updated or different Snapshots to be returned.

## Cache Initialization

Initial loading does not directly apply partial updates to the active Cache.

```text
DB lookup
  -> Record conversion and validation
  -> Construct all MAPs
  -> Validate duplicates, omissions, and types
  -> Publish the complete Store atomically under the Write Lock
```

If an error occurs during initialization, do not publish an incomplete Cache. If an active Cache already exists, retain it according to the Reload rules and expose the error state.

## Lookup

Every public lookup accesses a MAP defined in the Cache Definition directly.

```text
Lookup Condition
  -> Select MAP
  -> Normalize Key
  → Hash Lookup
  -> Read-only View of a Record or Record Set
```

- A MAP returns the set of record references belonging to the same condition for one Key.
- A set containing one record returns one record; a set containing multiple records returns multiple records.
- No lookup result is not an error; it is `NOT_FOUND` or an empty set.
- A condition without a MAP returns `9024 LOOKUP_MAP_NOT_FOUND`.
- No fallback lookup scans the entire Store to test a condition.

When a lookup condition contains two or more fields, define the complete condition as one composite MAP Key.

```text
(field_a, field_b) → MAP → Record Reference Set
```

In other words, do not sequentially scan records linked to a Key to evaluate a condition; construct the entire actual lookup condition as the MAP Key.

## INSERT

```text
Validate input
  -> Create Record
  -> Create all MAP Keys and check conflicts
  -> Ensure Queue capacity
  -> Publish to the Store and all MAPs as one logical change
  -> Enqueue DB INSERT in the Connection Queue
  -> Signal DBWorkerThread
```

If a PK conflict exists, do not change the Cache or Queue.

## UPDATE

UPDATE removes all MAP references for the existing record before adding the new record's MAP references.

```text
Look up the existing Record in the PK MAP
  -> Construct the new Record and all new MAP Keys
  -> Validate MAP Keys before and after the change
  -> Remove existing MAP references
  -> Add the new Record and MAP references
  -> Enqueue DB UPDATE in the Connection Queue
```

Even when MAP Keys do not change, every MAP must point to the same new Record Snapshot. Do not expose a state in which only some MAPs have been updated to external lookups.

## DELETE

```text
Look up the Record in the PK MAP
  -> Identify every MAP Key containing the Record
  -> Remove references from every MAP
  -> Remove the Record from the Store
  -> Enqueue DB DELETE in the Connection Queue
```

When the last record is removed from a MAP, remove the empty Key entry as well.

## Change Atomicity

A single record change processes the following items as one logical operation.

- Record Store
- Primary MAP
- All MAPs
- Connection Queue registration

Do not report success when the Cache change succeeds but Queue registration fails. The implementation must reserve Queue space and serialization first or be able to roll back the Cache change completely on failure.

Use a per-Cache Write Lock or an equivalent Snapshot replacement mechanism so readers cannot observe an intermediate state while multiple MAPs are changed.

## MAP and SEQ

```text
SEQ | CALLSIGN | LAT | LNG | ...
```

- `SEQ` is an opaque Primary Key.
- `CALLSIGN` is a MAP Key that may link to multiple position records.
- Do not assume that SEQ starts at 1, is contiguous, or indicates chronological order.

```text
SEQ MAP
arbitrary SEQ → Arc<PositionRecord>

CALLSIGN MAP
CALLSIGN → Collection<Arc<PositionRecord>>
```

If results containing multiple records require ordering, define an explicit sorting index other than SEQ in the Cache Definition. A MAP without defined ordering does not guarantee result order.

## Concurrency

Cache Runtime concurrency is managed by the C++ Core's MAP Lock Stripes and Record Lock Pool.

- Lookup: Find RecordId under a MAP Stripe Shared Lock and acquire the current Version using one of 64 assigned Record Locks.
- INSERT: Serial WRITE Lock and Exclusive Locks on all MAP Stripes
- UPDATE/DELETE: Serial WRITE Lock, the target Record Lock, and Exclusive Locks on all MAP Stripes
- Retention Drain: Per-record Record Lock and Exclusive Locks on all MAP Stripes
- DB operation consumption: The Connection's DBWorkerThread

Public lookup results are read-only Views referencing immutable C++ Record Versions. Ownership of the current Version is transferred to the Query Result under the Record Lock, after which the Lock is released immediately. UPDATE replaces it with a new Version, so data in existing Query Results does not change. Query Result storage is reused per Thread, avoiding `new/delete` on every lookup. Rust does not copy records, rebuild `BTreeMap`, or increment and decrement Core `Arc` values.

WRITE locks the target Record Lock and every MAP Stripe, so lookups cannot observe Record Version and MAP changes in progress. Because WRITEs are serialized by the per-Connection Queue and DBWorkerThread, lookup-lock distribution takes priority.

## Capacity Management

If capacity limits or Eviction are required, define them explicitly in the Cache Definition. Do not use a product-independent fixed capacity limit as the default. Do not scan the entire Cache on the public lookup path to find Eviction candidates. If ordering or Eviction criteria exist, maintain a separate management index when changes occur.

Each Cache must also specify whether the product contract permits an evicted record to remain in the DB while being invisible to Cache lookups.

## Normalization

Each MAP may require Key normalization.

Normalization must be declared in the Cache Definition, and the same function must be used for INSERT, UPDATE, DELETE, initial loading, and lookup.

Examples:

- ASCII case normalization
- Delimiter removal
- Leading Padding removal
- Fixed-length Binary conversion

Do not hide normalization rules only inside a specific lookup function in the code.

## Current Implementation Scope

The Rust code implements a Connection-owned persistent Queue and DBWorkerThread, Cache Runtime, Record Store, single and composite MAPs, DB loading, immediate changes, and Retention processing.
