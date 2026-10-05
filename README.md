# Autobricks Cache

Autobricks Cache is an **in-memory cache layer positioned between a database and a program** in the Autobricks product family.

The program passes database connection information and Cache Definitions, and Autobricks Cache retains records in memory according to those definitions. One Connection may be shared by multiple Caches, while each Cache is associated with and managed through one Connection. The final artifact is a shared library (`.so`) that programs can link and use.

Autobricks Cache is implemented in Rust and C++. Rust owns the public Interface, configuration, Connections, Queue, and DB Worker. The C++ Cache Core owns the Cache Record Store, MAP, Query, Insert, Update, Delete, Retention, and concurrency control.

## Primary Objective

The primary objective of Autobricks Cache is to provide applications with a
native shared library that owns Database Connections, Cache definitions,
persistent WRITE Queues, and MAP-based in-memory record access behind a small
JSON Interface.

As concurrent requests increase, direct database lookups can show greater per-request response-time variance because of Connection Pool waits, DB load, and I/O conditions. Autobricks Cache isolates this DB response variance and Connection contention from the user lookup path and provides predictable response performance under concurrent requests through MAP-based in-memory lookups.

The current source supports PostgreSQL, MariaDB, and SQLite. Records loaded into the Cache provide stable lookup performance isolated from Connection contention and response-time variance in the source DBMS. MySQL, Oracle, DB2, and Microsoft SQL Server will be added in a later version. Couchbase and MongoDB are outside the supported scope and are provided by the separate `jcache` product.

## Why Autobricks Cache

Autobricks Cache is designed for applications that need predictable read latency while keeping a database as the system of record.

- **Stable concurrent lookups:** MAP-based lookups remain independent of Database Connection Pool waits and Database I/O.
- **Isolation from database contention:** Cache lookups do not borrow a Database Connection. Pool waits, Database load, storage latency, and network variance remain outside the Cache lookup path.
- **Direct MAP access:** Every supported lookup uses a predefined MAP. The runtime does not fall back to scanning all cached records when a lookup definition is missing.
- **Immediate in-memory changes:** Insert, Update, and Delete operations update the Cache first, so subsequent Cache lookups observe the new state without waiting for the Database write.
- **Connection-owned write ordering:** Each Connection owns one persistent WRITE Queue and one DB Worker. Caches sharing a Connection preserve serialized Database write order without creating a Worker per Cache.
- **One record, multiple indexes:** Primary Key and secondary or group MAPs reference the same Cache record instead of storing independent record copies.
- **Server and embedded databases:** The same Cache contract supports PostgreSQL, MariaDB, plain SQLite, and encrypted SQLCipher databases.
- **Deployable native library:** Rust and C++ are packaged as a C ABI shared library for macOS and Ubuntu on arm64 and x86-64/amd64.

## Source Organization Principles

Source directories are separated by object or function.

```text
src/{object-or-feature}/{source}.rs
```

The `src` root contains only Crate entry points and top-level Module declarations; actual implementation is not concentrated in one file or the root directory.

The current structure is as follows.

```text
src/
├── lib.rs
├── connection/
│   ├── mod.rs
│   ├── config.rs
│   ├── runtime.rs
│   ├── select.rs
│   ├── thread.rs
│   └── worker.rs
├── definition/
│   ├── mod.rs
│   ├── config.rs
│   └── validator.rs
├── cache/
│   ├── mod.rs
│   └── runtime.rs
├── core/
│   ├── mod.rs
│   ├── cache_core.h
│   └── cache_core.cpp
├── database/
│   ├── mod.rs
│   └── record.rs
├── operation/
│   ├── mod.rs
│   ├── command.rs
│   ├── promise.rs
│   └── queue.rs
├── retention/
│   ├── mod.rs
│   └── thread.rs
├── system/
│   ├── mod.rs
│   └── runtime.rs
├── error/
│   ├── mod.rs
│   ├── code.rs
│   └── detail.rs
```

Directory responsibilities are separated as follows.

| Directory | Responsibility |
| --- | --- |
| `connection` | SELECT Connection Pool, serialized WRITE Connection, Background Thread, and lifecycle management |
| `definition` | Cache Definition configuration and validation |
| `cache` | Record storage, mutation, and lookup Snapshot |
| `database` | DB Adapter and Query and Transaction execution |
| `operation` | Asynchronous DB operations, Queue, and result tracking |
| `retention` | Global Retention Drain Thread |
| `system` | Cache Registry and global lifecycle management |
| `error` | `ERROR.md` codes, error information, and Driver error conversion |

Functionality is not collected in a generic `utils.rs` merely because it is shared across Modules. Shared code is placed in the object or functional Module that owns the actual responsibility. When one source begins to have multiple responsibilities, it is split into additional files within the relevant functional directory.

## Goals

- Keep average Cache lookup response time within a consistent range as the number of request Threads increases.
- Separate the lookup path so DB Connection contention and response-time variance do not directly affect user lookup performance.
- Convert repetitive database lookups into in-memory lookups.
- Immediately apply changes to the Cache when `INSERT`, `UPDATE`, or `DELETE` is requested.
- Send database persistence work to the Background Thread owned by the Connection, reducing wait time in the call path.
- Configure a MAP for every lookup condition in every Cache to provide fast, predictable lookup performance.
- Do not allow lookups that sequentially scan the entire Cache.

## Operational Overview

```text
Application
    │
    │ Connection information + Cache Definition
    ▼
Autobricks Cache (.so)
    ├── Connection A
    │   ├── Cache A-1
    │   ├── Cache A-2
    │   └── Background Worker ──► Database A
    └── Connection B
        ├── Cache B-1
        └── Background Worker ──► Database B
```

The relationship between Connections and Caches is as follows.

```text
Connection 1 ────── N Cache

Cache N ─────────── 1 Connection
```

- One or more Caches may be associated with one Connection.
- Each Cache specifies one Connection that it uses.
- The Connection owns the Background Thread and work queue.
- All Caches associated with the same Connection share that Connection's Background Thread and work queue.
- Records and MAPs for each lookup condition are managed independently per Cache.

### Lookup

1. The program requests a record using a lookup condition defined in the Cache Definition.
2. Autobricks Cache finds the record in the MAP for that lookup condition.
3. It immediately returns the cached record without repeatedly querying the database.

Every lookup condition must be associated with a MAP predefined in the Cache Definition. Lookups that sequentially scan all Cache records for a condition without a MAP are not provided.

### Mutation

`INSERT`, `UPDATE`, and `DELETE` requests are processed in the following order.

1. Validate the request.
2. Immediately apply the mutation to the in-memory record and related indexes.
3. Submit the database persistence operation to the work queue of the Connection associated with the Cache.
4. Return control to the caller without waiting for the database operation to complete.
5. The Connection's Background Thread applies the mutation to the database in operation order.

A separate Background Thread is not created for each Cache. When multiple Caches reference the same Connection, all their database operations are submitted to the Connection's shared work queue.

Mutation order for the same record must be preserved. Retry behavior, error notification, and handling of pending work at shutdown after an asynchronous database persistence failure are defined as explicit operational rules during implementation.

## Cache Types

Autobricks Cache provides two Cache structures based on record characteristics.

### 1. Single PK Structure

This structure identifies one record with one Primary Key.

It is suitable for data such as user information, where one key retrieves the current state.

```text
PK ──► PK MAP ──► Record
```

Example:

```text
user_id = 1001 ──► PK MAP ──► User Record
```

Primary operations:

- Look up one record by PK
- `INSERT` by PK
- `UPDATE` by PK
- `DELETE` by PK

### 2. SEQ PK + Group Lookup Structure

In a history table, `SEQ` itself is the database Primary Key that identifies each record. Fields such as `CALLSIGN`, `flight_id`, and `aircraft_id` are not Primary Keys; they are group lookup conditions used to find multiple history records at once.

Therefore, `(group key, SEQ)` is not treated as a composite Primary Key. The SEQ MAP accesses one record, while a group MAP provides multiple records belonging to the same group as one set.

Even when the column is named `SEQ`, its value is not interpreted as a sequence number. SEQ is an opaque Primary Key that cannot be assumed to start at 1, be contiguous, increase with each insertion, or represent creation order. SEQ values from other records may occur between records with the same `CALLSIGN`, deletion may create gaps, and arbitrary values generated outside the DB may be used.

The implementation therefore prohibits the following behavior.

- Using SEQ as an array index or an ordinal within a group
- Inferring the next key as `current maximum SEQ + 1`
- Inferring creation time or business ordering from relative SEQ values
- Assuming the first SEQ is 1
- Assuming there are no missing values between SEQ values
- Assuming a SEQ range lookup is equivalent to a time-range lookup without a separate sort criterion

```text
SEQ ──► SEQ MAP ──► Record

Lookup key ──► MAP ──► Record Reference Set
```

Assume, for example, that the following position information is stored.

```text
SEQ | CALLSIGN | LAT | LNG | ...
```

Position information for `FL042` is managed as follows.

```text
seq = 1002 ──► SEQ MAP ──► Position Record

CALLSIGN = FL042 ──► CALLSIGN MAP ──► Position Record Reference Set
```

The SEQ MAP and group MAP must reference the same cached records without owning separate record copies.

The CALLSIGN MAP value does not use SEQ as an array index. It uses a container that safely handles arbitrary PK values, such as a hash set, a list of record handles, or a separate ordered structure.

If lookup results require business ordering, an explicit sort field such as `event_time` or `received_at` must be defined separately in the Cache Definition. In that case, a group index ordered by that field is maintained at mutation time to avoid paying the sorting cost on every lookup. Group lookups without a defined sort field do not guarantee result order.

Primary operations:

- Look up one history record by SEQ
- Look up all history records in a group by group condition
- `UPDATE` and `DELETE` by SEQ
- Update the SEQ MAP and related group MAPs together on `INSERT`
- Remove from the previous group MAP and add to the new group MAP when a group field changes

## MAP Conditions

Every Cache Definition defines one MAP for each lookup condition. MAP does not mean only an optional secondary index separate from the PK. A PK lookup is itself one MAP, and each additional lookup condition adds another MAP. Both MAPs in which one key references one record and MAPs in which one group key references multiple records are supported.

A MAP is an in-memory lookup structure that uses specified fields or field combinations as a key to access target records directly. Every lookup must be processed through the MAP for its condition, without scanning all records or querying the database again.

```text
Lookup condition 1 ──► MAP 1 ──► Record

Lookup condition 2 ──► MAP 2 ──► Record
```

For example, if a user table contains a numeric `id` and a business identifier `user_id`, and both fields must support lookup, there are two MAPs.

```text
id      ──► ID MAP      ──► User Record
user_id ──► USER_ID MAP ──► User Record
```

Both MAPs reference the same user records. Even though `id` is the Primary Key, it counts as one MAP for `id` lookup; adding the `user_id` lookup MAP produces a total of two MAPs.

When a field used by a MAP is changed by `INSERT`, `UPDATE`, or `DELETE`, the related MAP indexes must be updated immediately with the record.

No fallback path sequentially scans records for a condition not defined by a MAP. When a required lookup condition is added, its MAP must first be added to the Cache Definition.

Each Cache Definition specifies how MAP conditions are represented, whether duplicate keys are allowed, and whether single or multiple results are returned.

## Input Information

When initializing the library, pass a list of Connections and a list of Cache Definitions. Each must contain identifiers that associate them with one another.

### Connection Information

This information is required for database access and data persistence by the Background Thread. A Connection owns the Background Thread and work queue shared by the Caches associated with it.

The specific configuration format and connection and authentication contracts follow [CONNECTION.md](CONNECTION.md).

Public error numbers and the asynchronous DB error delivery contract follow [ERROR.md](ERROR.md).

Record, MAP, initial loading, and mutation structures follow [CACHE.md](CACHE.md).

The Cache Definition JSON draft and examples follow [CACHE_DEFINITION.md](CACHE_DEFINITION.md).

Cache lookup requests and responses using MAPs follow [QUERY.md](QUERY.md).

- Connection identifier
- Database type and connection location
- Authentication information
- Target database or schema
- Connection options such as connection count and timeouts

One Connection definition may be referenced by multiple Caches. In this case, all Caches share the same Background processing path owned by the Connection.

The actual fields and supported databases will be specified separately when the public API is finalized.

### Cache Definition

This defines which data is retained in memory and through which indexes.

- Cache identifier
- Identifier of the Connection to associate
- Target table or dataset
- Cache type: `Single PK` or `SEQ PK + Group Lookup`
- Primary Key field
- SEQ field (this field is the Primary Key in the `SEQ PK + Group Lookup` structure)
- Group lookup fields (for the `SEQ PK + Group Lookup` structure)
- Optional sort field (when group lookup result order must be guaranteed)
- Required MAP list for each lookup condition
- Fields to load into the Cache

Each Cache must reference exactly one Connection.

## Consistency Principles

- Apply a program mutation request as one logical mutation to the cached record and all related indexes.
- A Cache lookup immediately after the call must observe the mutation that was just applied.
- Every lookup uses the MAP corresponding to its lookup condition and never sequentially scans all records.
- Regardless of the name `SEQ`, treat PK values as opaque identifiers without assuming a starting value, continuity, direction of increase, or business ordering.
- Process database operations from multiple Caches belonging to the same Connection through the Connection's shared work queue.
- Database operation order for the same key must match the mutation order processed by the Cache.
- Do not hide operations that the Background Thread failed to process as successful.
- Provide a clear shutdown contract specifying whether pending operations are completed, preserved safely, or returned as errors when the library shuts down.

## Final Artifact

Rust statically links the C++ Cache Core to produce one `cdylib`. The final result is a shared library that Linux programs can link dynamically.

```text
libautobricks_cache.so
```

The expected Cargo configuration is as follows.

```toml
[lib]
crate-type = ["cdylib"]
```

The public header is `autobricks_cache.h`. It exposes initialization,
shutdown, and the five Cache operations: `query`, `insert`, `update`,
`delete`, and `status`. Every operation accepts UTF-8 JSON and returns a
UTF-8 JSON string. Release returned strings with `ab_cache_string_free()`.
Internal Rust types and C++ Cache Core functions are not exposed across the
shared-library boundary.

Applications initialize the library with a Connection JSON object and a Cache
Definition JSON array. The Connection object owns `queue_directory`, its
persistent WRITE Queue, its DB Worker, and its physical Database Connections.

## Current Scope

Version 0.1.104 exposes the C ABI declared in `autobricks_cache.h` and supports
PostgreSQL, MariaDB, SQLite, and SQLCipher through the documented Connection
and Cache Definition formats. Future functionality is not part of the public
contract until it is documented and released.
