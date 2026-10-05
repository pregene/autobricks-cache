# Database Connection

This document describes the Connection definition and execution contract used by Autobricks Cache to connect to databases.

Autobricks Cache handles only DB connections, so HTTP, Unix socket, and program-execution connections are outside the scope of this document.

## Ownership

A Connection owns the following runtime resources in addition to database connection settings.

- SELECT Connection Pool
- One WRITE Connection exclusively owned by the DBWorkerThread
- Background Thread
- Background work queue
- Connection and error states

A single Connection may be shared by multiple Caches. Each Cache references exactly one Connection.

```text
Connection A
 ├── SELECT Connection Pool
 │    └── pool_size - 1 connections
 ├── 1 WRITE Connection
 ├── Background Thread
 ├── Background Work Queue
 ├── Cache A-1
 └── Cache A-2
```

Do not create a separate Background Thread or WRITE Connection for each Cache. WRITE operations from all Caches that reference the same Connection are submitted to a shared work queue and executed serially through the single WRITE Connection owned by the DBWorkerThread. A SELECT borrows a Connection from the prepared SELECT Connection Pool, executes, and returns it.

## Connection Definition

The currently implemented Drivers and connection conditions follow.

| Item | Currently Supported Values |
| --- | --- |
| `kind` | `DATABASE` |
| `driver` | `postgresql`, `mariadb`, `sqlite` |
| Authentication | `username`, `password` |
| `tls_used` | `true`, `false` |
| TLS | Optional: `ca_file`, `cert`, `key` |
| `pool_used` | `true` |
| `pool_size` | Minimum `2` |

The current source supports PostgreSQL, MariaDB, and SQLite. MySQL, Oracle, DB2, and Microsoft SQL Server will be added in later versions. Couchbase and MongoDB are not supported here; they are provided by the separate `jcache` product.

For SQLite, specify a file path in `database` and declare the SQLite-specific `opt` object.

```json
{
  "connection_id": "cache_database_sqlite",
  "name": "cache SQLite database",
  "kind": "DATABASE",
  "driver": "sqlite",
  "host": "",
  "port": 0,
  "database": "data/cache.sqlite3",
  "authentication": { "username": "", "password": "" },
  "tls_used": false,
  "connect_timeout_ms": 2000,
  "query_timeout_ms": 5000,
  "pool_used": true,
  "pool_size": 10,
  "opt": {
    "key": null,
    "open_mode": "READ_WRITE_CREATE",
    "mutex": "FULL",
    "uri": false,
    "cache": "DEFAULT",
    "journal_mode": "WAL",
    "synchronous": "NORMAL",
    "foreign_keys": true
  }
}
```

| `opt` Field | Supported Values |
| --- | --- |
| `key` | `null` selects plain SQLite; a string applies the SQLCipher Key immediately after connection |
| `open_mode` | `READ_ONLY`, `READ_WRITE`, `READ_WRITE_CREATE` |
| `mutex` | `FULL`, `NO` |
| `uri` | `true`, `false` |
| `cache` | `DEFAULT`, `SHARED`, `PRIVATE` |
| `journal_mode` | `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, `WAL`, `OFF` |
| `synchronous` | `OFF`, `NORMAL`, `FULL`, `EXTRA` |
| `foreign_keys` | `true`, `false` |

`query_timeout_ms` is applied as the SQLite `busy_timeout`. The `pool_size - 1` SELECT Connections and the one dedicated DBWorkerThread Connection are opened with the same options.

When creating a Connection, select the Database Adapter corresponding to the `driver` value. After selection, Cache, Connection Pool, Queue, and DBWorkerThread behavior is identical regardless of Driver type.

The following is a complete PostgreSQL connection example.

```json
{
  "connection_id": "77bcb7f1-9bdd-40a8-a2c9-5e50b7b8686d",
  "name": "air_position_db",
  "kind": "DATABASE",
  "driver": "postgresql",
  "host": "position-db.example.test",
  "port": 5432,
  "database": "air_position",
  "authentication": {
    "username": "autobricks_cache",
    "password": "air_position_db_password"
  },
  "tls_used": true,
  "tls": {
    "cert": "/etc/autobricks-cache/pki/db-client-cert.pem",
    "key": "/etc/autobricks-cache/pki/db-client.key",
    "ca_file": "/etc/autobricks-cache/pki/db-ca.pem"
  },
  "connect_timeout_ms": 2000,
  "query_timeout_ms": 1000,
  "pool_used": true,
  "pool_size": 10
}
```

This JSON is a configuration format read by Autobricks Cache, not a configuration file read directly by PostgreSQL. The Rust DB Adapter converts each item to actual driver options.

## Fields

### Basic Information

| Field | Required | Description |
| --- | --- | --- |
| `connection_id` | Yes | Identifier used by a Cache to reference the Connection |
| `name` | Yes | Descriptive name for operators; not used as a reference key |
| `kind` | Yes | Currently only `DATABASE` is allowed |
| `driver` | Yes | Selects the DB Adapter; currently supports `postgresql`, `mariadb`, and `sqlite` |
| `host` | Yes | DB server hostname or address |
| `port` | Yes | DB server port |
| `database` | Yes | Name of the database to connect to |

The exact format and issuing authority for `connection_id` will be finalized in the public interface design. Do not automatically merge distinct Connections merely because they have the same name or host.

### Authentication

```json
{
  "authentication": {
    "username": "autobricks_cache",
    "password": "air_position_db_password"
  }
}
```

| Field | Description |
| --- | --- |
| `username` | DB login account |
| `password` | DB login password |

### Transport Protection

When `tls_used` is `false`, omit `tls`.

```json
"tls_used": false
```

When `tls_used` is `true`, include `tls`.

#### TLS

```json
{
  "tls_used": true,
  "tls": {
    "ca_file": "/etc/autobricks-cache/pki/db-trust.pem"
  }
}
```

#### mTLS

```json
{
  "tls_used": true,
  "tls": {
    "cert": "/etc/autobricks-cache/pki/db-client-chain.pem",
    "key": "/etc/autobricks-cache/pki/db-client.key",
    "ca_file": "/etc/autobricks-cache/pki/db-trust.pem"
  }
}
```

When only `ca_file` is present, connect with TLS. When `cert` and `key` are present, also provide the Client certificate. Apply these three paths directly to the TLS Driver settings provided by PostgreSQL and MariaDB.

### Timeouts

| Field | Description |
| --- | --- |
| `connect_timeout_ms` | Limit for establishing a new DB connection |
| `query_timeout_ms` | Limit for executing a DB operation |
Distinguish connection-establishment limits from query-execution limits. Record an explicit failure state when a timeout expires.

## Secret Binding

Secrets referenced by a Connection definition are supplied separately by the execution environment.

```json
{
  "secret_bindings": {
    "air_position_db_password": {
      "provider": "file",
      "path": "/run/secrets/autobricks-cache/air-position-db-password"
    },
    "air_position_db_client_key": {
      "provider": "file",
      "path": "/run/secrets/autobricks-cache/air-position-db-client.key"
    }
  }
}
```

The process that actually loads the library must be able to read the files above and the certificate files. Do not record passwords, private keys, or sensitive connection information in logs.

## DB Connection

The Connection Runtime separates SELECT and WRITE Connections.

```text
Cache 1..N
   ├── SELECT → Select Connection Pool → borrow → execute → return
   │
   └── WRITE  → Connection Queue → DBWorkerThread → 1 Write Connection
```

- PRELOAD and ON_DEMAND SELECT borrow a Connection from the SELECT Connection Pool.
- The SELECT Connection Pool prepares `pool_size - 1` physical Connections.
- After a SELECT completes, return the borrowed Connection to the same Pool regardless of success or failure.
- INSERT, UPDATE, and DELETE return immediately after insertion into the persistent Queue.
- Only the DBWorkerThread owns the WRITE Connection, and it executes all WRITEs serially.
- This separation supports SQLite's multiple-SELECT, single-WRITE structure and allows only the SELECT Pool size to be expanded later.

### Connection Pool

```json
{
  "pool_used": true,
  "pool_size": 10
}
```

`pool_size` is the total number of physical Connections, including both SELECT and WRITE Connections.

```text
pool_size = 10
├── SELECT Connection Pool = 9 connections
└── WRITE Connection       = 1 connection
```

The formulas follow.

```text
SELECT Connection count = pool_size - 1
WRITE Connection count  = 1
```

- When `pool_used: true`, `pool_size` must be at least `2`.
- `pool_size: 2` provides one SELECT and one WRITE Connection.
- A SELECT Connection is borrowed from the Pool and must be returned after execution.
- The WRITE Connection is not borrowed from the Pool; it is exclusively owned by the DBWorkerThread.
- WRITEs execute serially in Queue order.

### Retained Promise Implementation

The fixed Promise Handler array implementation is retained in the code for future use when synchronous results from Queue operations are required. It is not connected to the current SELECT or WRITE execution paths.

```text
Calling Thread
  → Borrow Promise Index
  → Record Promise Index in a Queue request that requires a synchronous result
  → Insert into Connection Queue
  → Wait on Promise

DBWorkerThread
  → Execute Queue request
  → Set result for the same Promise Index
  → Wake waiting Thread

Calling Thread
  → Receive result
  → Return Promise Index
```

Outside the Promise, expose no Pointer or Handler object; pass only a `u64` Index. The Index represents both the Slot position and generation, so an old Index cannot access a new result even after a returned Slot is reused.

Current INSERT, UPDATE, and DELETE operations do not use Promises because they are Background operations for which the caller does not wait for a DB result.

## Background Processing

When a Cache changes, process it in the following order.

```text
Cache A-1 ─────────┐
                  ├──► Connection A Work Queue
Cache A-2 ─────────┘              │
                                 ▼
                    Connection A Background Thread
                                 │
                                 ▼
                    Connection A WRITE Connection
```

1. The Cache immediately changes the in-memory record and related MAPs.
2. It submits the DB operation to the shared work queue of its referenced Connection.
3. The caller returns without waiting for the DB operation to complete.
4. The Connection's Background Thread executes the operation using its owned WRITE Connection.
5. Record successful completion, retry eligibility, or final failure state.

The order of DB changes to the same record must be preserved. Even when multiple Caches share one Connection, the change order for a specific record must not be reversed. Retry decisions must consider both operation idempotency and the DB outcome so that operations with unknown outcomes are not duplicated unconditionally.

### Queue and DBWorkerThread Structure

A Connection owns both a persistent Queue and the DBWorkerThread that consumes it.

```text
Cache
    │
    │ Queue.push(object)
    ▼
Connection Queue
    │
    │ signal
    ▼
DBWorkerThread
    ├── Queue.peek()
    ├── DB operation and Commit
    └── Queue.pop() only on success
```

- The Queue is a disk-based FIFO consisting of a Header file and a Body file.
- The Header stores the format version, read position, write position, file size, and item count.
- The Worker uses `AtomicBool`, `Mutex`, and `Condvar` to manage execution state and waiting.
- When the Queue is empty, the Worker waits on the Condvar.
- When an operation is added, signal the Worker for that Connection.
- Also inspect the Wake Sequence so that notification is not lost because of Signal races.
- The Worker calls `peek` on the Queue Head and then executes the DB operation.
- Remove the operation from the Queue with `pop` only when the DB operation and Commit succeed.
- If the DB operation fails, retain the Queue Head to allow error inspection and conditional retry.
- When stopping the Worker, do not start a new DB operation; Join the Thread.

The current Queue stores the Cache ID, operation type, SQL, and binding values for the DB Adapter as serialized JSON Bytes.

## PostgreSQL Mapping

Representative PostgreSQL/libpq mappings follow. Verify that the actual Rust Driver provides equivalent validation and behavior.

| Connection Setting | PostgreSQL/libpq Concept |
| --- | --- |
| `host`, `port`, `database` | `host`, `port`, `dbname` |
| `authentication.username` | `user` |
| Resolved `authentication.password` | `password` |
| `PLAIN-TEXT` | `sslmode=disable` |
| TLS/mTLS server validation | `sslmode=verify-full`, `sslrootcert`, `sslcrl` |
| mTLS client certificate and key | `sslcert`, `sslkey`, and `sslpassword` when required |

If the Rust DB Adapter does not support CA, CRL, server-name validation, or client-authentication capabilities required by the configuration, fail Connection registration or initialization. Do not silently ignore unsupported security settings.

## Configuration Validation

Before creating a Connection Runtime, validate at least the following items.

- Whether `connection_id` is duplicated
- Whether `kind` and `driver` are supported
- Required fields and types
- Valid ranges for the port and all timeouts
- Presence of the Secret Bindings required by the authentication method
- Accessibility and format of CA, CRL, certificate, and private-key files
- The Transport mode and field combination for that mode
- Whether each Cache references exactly one existing Connection

Do not activate a Cache connected to a Connection that fails validation. Do not continue execution while omitting security or connection settings.

## Configuration Changes and Shutdown

When Connection settings, certificates, or secrets change, create new DB connections with the new configuration. Do not assume that existing connections use new credentials merely because files were replaced.

For configuration changes, follow this sequence.

1. Validate the new Connection settings and secrets.
2. Prepare new DB connections.
3. Redirect new operations to the new Connection Runtime.
4. Process the previous work queue and in-use connections according to the defined contract.
5. Stop the previous Background Thread and DB connections.

When shutting down the library, stop accepting new operations for each Connection, process its work queue according to the completion, preservation, and failure-handling contract for pending DB operations, and then stop the Background Thread and DB connections.

## Open Items

Public error numbers and the asynchronous DB error-delivery contract follow [ERROR.md](ERROR.md).

The following items are not yet implemented interfaces and will be finalized during Rust API and ABI design.

- Public function and serialization format for supplying Connection settings
- Format and issuing authority for `connection_id`
- List of supported DB Drivers
- Rust DB Driver implementations
- Number of Background Threads and work-queue implementation
- Retry, persistent work-queue, and failure-recovery rules
- API for changing Connection settings without downtime
- Operational-state query API
