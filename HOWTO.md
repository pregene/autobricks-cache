# Using Autobricks Cache

Autobricks Cache provides the following five external Interfaces.

```text
cache.query()
cache.insert()
cache.update()
cache.delete()
cache.status()
```

The Cache internally owns the Connection identified by the Definition's `connection_id`. Users do not separately pass a Connection, MAP name, or field order.

Every Interface accepts JSON and returns JSON.

## Library Initialization and Shutdown

Before calling a Cache Interface, the program initializes the Library by passing both the Connection configuration and Cache configuration.

```text
initialize(connection_config, cache_config)
```

The shared-library entry point is:

```c
char *result = ab_cache_initialize(connection_config, cache_config);
ab_cache_string_free(result);
```

- `connection_config`: A single JSON Object defining the Database connection and Connection Pool
- `cache_config`: A JSON Array containing Cache Definitions that share the same Connection

The Connection definition must include `queue_directory`. The Connection owns
the persistent WRITE Queue files stored in that directory.

A program using configuration files reads both files and passes their respective JSON content.

```rust
let connection_config = std::fs::read_to_string("config/connection.json")?;
let cache_config = std::fs::read_to_string("config/caches.json")?;

initialize(&connection_config, &cache_config);
```

Each array item in `cache_config` defines one Cache. A `cache_id` must be unique within the array, and each item's `connection_id` must match `connection_config.connection_id`.

```json
[
  {
    "cache_id": "active_user_cache",
    "connection_id": "main_database",
    "cache_type": "PRELOAD",
    "retention": {
      "type": "NONE",
      "value": 0
    },
    "primary_key": ["id"],
    "select": {
      "query": "SELECT id, user_id, display_name, status FROM public.users WHERE status = 'ACTIVE'",
      "fields": []
    },
    "insert": {
      "query": "INSERT INTO public.users (id, user_id, display_name, status) VALUES ($1, $2, $3, $4)",
      "fields": ["id", "user_id", "display_name", "status"]
    },
    "update": {
      "query": "UPDATE public.users SET user_id = $1, display_name = $2, status = $3 WHERE id = $4",
      "fields": ["user_id", "display_name", "status", "id"]
    },
    "delete": {
      "query": "DELETE FROM public.users WHERE id = $1",
      "fields": ["id"]
    },
    "maps": [
      ["id"],
      ["user_id"]
    ]
  },
  {
    "cache_id": "flight_position_cache",
    "connection_id": "main_database",
    "cache_type": "ON_DEMAND",
    "retention": {
      "type": "TIMESTAMP",
      "value": 3600
    },
    "primary_key": ["seq"],
    "select": {
      "query": "SELECT seq, callsign, event_time, lat, lng FROM public.flight_positions WHERE callsign = $1",
      "fields": ["callsign"]
    },
    "insert": {
      "query": "INSERT INTO public.flight_positions (seq, callsign, event_time, lat, lng) VALUES ($1, $2, $3, $4, $5)",
      "fields": ["seq", "callsign", "event_time", "lat", "lng"]
    },
    "update": {
      "query": "UPDATE public.flight_positions SET callsign = $1, event_time = $2, lat = $3, lng = $4 WHERE seq = $5",
      "fields": ["callsign", "event_time", "lat", "lng", "seq"]
    },
    "delete": {
      "query": "DELETE FROM public.flight_positions WHERE seq = $1",
      "fields": ["seq"]
    },
    "maps": [
      ["seq"],
      ["callsign"]
    ]
  }
]
```

After initialization completes, use `query()`, `insert()`, `update()`, `delete()`, and `status()`. When the program exits or finishes using the Cache, shut it down without arguments.

```text
uninitialize()
```

The shared-library entry point is:

```c
char *result = ab_cache_uninitialize();
ab_cache_string_free(result);
```

```rust
uninitialize();
```

## Currently Supported Drivers

The current source supports PostgreSQL, MariaDB, and SQLite Database Drivers. The SQLite Adapter uses the bundled SQLCipher Engine and opens either plain SQLite or SQLCipher according to the `key` setting.

```json
{
  "kind": "DATABASE",
  "driver": "postgresql"
}
```

Both Drivers use a Database account and password. The Connection Pool must be enabled, and the minimum `pool_size` is `2`. Of all Connections, `pool_size - 1` are used for SELECT, while the remaining Connection is dedicated to serialized WRITE operations.

For MariaDB, set `driver` to `mariadb`. PostgreSQL Query Parameters use the `$1`, `$2` format, while MariaDB Query Parameters use `?`. The Cache passes configured Queries unchanged to the selected Driver.

### PLAIN Connection

For an unencrypted Database connection, set `tls_used` to `false`. In this case, omit `tls`.

```json
{
  "connection_id": "main_database",
  "name": "main database",
  "kind": "DATABASE",
  "driver": "postgresql",
  "host": "127.0.0.1",
  "port": 5432,
  "database": "application_database",
  "queue_directory": "data/queue",
  "authentication": {
    "username": "cache_service",
    "password": "change-this-password"
  },
  "tls_used": false,
  "connect_timeout_ms": 2000,
  "query_timeout_ms": 5000,
  "pool_used": true,
  "pool_size": 10
}
```

### TLS Connection

For a TLS connection that validates the Server certificate, set `tls_used` to `true` and specify the CA certificate file used to validate the Database Server certificate in `tls.ca_file`.

```json
{
  "connection_id": "main_database",
  "name": "main database tls",
  "kind": "DATABASE",
  "driver": "postgresql",
  "host": "database.example.com",
  "port": 5432,
  "database": "application_database",
  "queue_directory": "data/queue",
  "authentication": {
    "username": "cache_service",
    "password": "change-this-password"
  },
  "tls_used": true,
  "tls": {
    "ca_file": "/etc/autobricks-cache/pki/database-ca.pem"
  },
  "connect_timeout_ms": 2000,
  "query_timeout_ms": 5000,
  "pool_used": true,
  "pool_size": 10
}
```

### mTLS Connection

For an mTLS connection in which Server and Client authenticate each other, set `tls_used` to `true` and specify `ca_file`, `cert`, and `key`. `cert` and `key` must always be used together.

```json
{
  "connection_id": "main_database",
  "name": "main database mtls",
  "kind": "DATABASE",
  "driver": "postgresql",
  "host": "database.example.com",
  "port": 5432,
  "database": "application_database",
  "queue_directory": "data/queue",
  "authentication": {
    "username": "cache_service",
    "password": "change-this-password"
  },
  "tls_used": true,
  "tls": {
    "cert": "/etc/autobricks-cache/pki/cache-client.pem",
    "key": "/etc/autobricks-cache/pki/cache-client.key",
    "ca_file": "/etc/autobricks-cache/pki/database-ca.pem"
  },
  "connect_timeout_ms": 2000,
  "query_timeout_ms": 5000,
  "pool_used": true,
  "pool_size": 10
}
```

All three connection modes apply equally to PostgreSQL and MariaDB. For MariaDB, set `driver` to `mariadb` and `port` to the MariaDB Server Port. The corresponding DBMS and Driver handle the TLS Handshake and Database Server certificate configuration.

MySQL, Oracle, DB2, and Microsoft SQL Server will be added in a later version. Couchbase and MongoDB are outside this product's scope and are provided by the separate `jcache` product.

## Common Return Format

The success code is `0`.

```json
{
  "code": 0,
  "message": "success"
}
```

On failure, the Interface returns an error code defined in `ERROR.md` and an English description.

```json
{
  "code": 9024,
  "message": "query fields do not match a registered MAP"
}
```

Records are returned only in `query()`'s `records`. `insert()`, `update()`, and `delete()` return only success or failure. Use `status()` to check the current Cache record count and memory usage.

## 1. Using Preloaded Records

`PRELOAD` executes `select.query` when creating the Cache and loads all records that will be used.

```json
{
  "cache_id": "active_user_cache",
  "connection_id": "main_database",
  "cache_type": "PRELOAD",
  "retention": {
    "type": "NONE",
    "value": 0
  },
  "primary_key": ["id"],
  "select": {
    "query": "SELECT id, user_id, display_name, status FROM public.users WHERE status = 'ACTIVE'",
    "fields": []
  },
  "insert": {
    "query": "INSERT INTO public.users (id, user_id, display_name, status) VALUES ($1, $2, $3, $4)",
    "fields": ["id", "user_id", "display_name", "status"]
  },
  "update": {
    "query": "UPDATE public.users SET user_id = $1, display_name = $2, status = $3 WHERE id = $4",
    "fields": ["user_id", "display_name", "status", "id"]
  },
  "delete": {
    "query": "DELETE FROM public.users WHERE id = $1",
    "fields": ["id"]
  },
  "maps": [
    ["id"],
    ["user_id"]
  ]
}
```

During Cache creation, SELECT results are stored once in the Record Store, and the `id` MAP and `user_id` MAP reference the same records. `select.fields` for `PRELOAD` must be an empty array.

### Lookup by user_id

The user passes only the known MAP field and value as JSON.

```rust
let result = cache.query(r#"{"user_id":"test_user"}"#);
```

```json
{
  "code": 0,
  "message": "success",
  "records": [
    {
      "id": 100001,
      "user_id": "test_user",
      "display_name": "Test User",
      "status": "ACTIVE"
    }
  ]
}
```

### Lookup by id

```rust
let result = cache.query(r#"{"id":100001}"#);
```

The Cache internally selects the MAP from the input JSON field combination. `{"user_id":"test_user"}` uses the `["user_id"]` MAP, while `{"id":100001}` uses the `["id"]` MAP. A separate MAP name such as `by_user_id` is not used.

If no record exists, an empty array is returned rather than an error.

```json
{
  "code": 0,
  "message": "success",
  "records": []
}
```

## 2. Loading Records On Demand

`ON_DEMAND` starts with an empty Cache. A record is fetched from the DB and loaded into the Cache only when it is absent from the MAP.

```json
{
  "cache_id": "user_cache",
  "connection_id": "main_database",
  "cache_type": "ON_DEMAND",
  "retention": {
    "type": "TIMESTAMP",
    "value": 3600
  },
  "primary_key": ["id"],
  "select": {
    "query": "SELECT id, user_id, display_name, status FROM public.users WHERE user_id = $1 AND status = 'ACTIVE'",
    "fields": ["user_id"]
  },
  "insert": {
    "query": "INSERT INTO public.users (id, user_id, display_name, status) VALUES ($1, $2, $3, $4)",
    "fields": ["id", "user_id", "display_name", "status"]
  },
  "update": {
    "query": "UPDATE public.users SET user_id = $1, display_name = $2, status = $3 WHERE id = $4",
    "fields": ["user_id", "display_name", "status", "id"]
  },
  "delete": {
    "query": "DELETE FROM public.users WHERE id = $1",
    "fields": ["id"]
  },
  "maps": [
    ["id"],
    ["user_id"]
  ]
}
```

Usage is the same as `PRELOAD`.

```rust
let result = cache.query(r#"{"user_id":"test_user"}"#);
```

The internal processing sequence is as follows.

1. Select the `["user_id"]` MAP from the input field.
2. Look up `test_user` in the MAP.
3. If the record exists, return it immediately without using the DB.
4. If the record does not exist, bind `test_user` to `$1` according to `select.fields`.
5. Execute `select.query` through the SELECT Connection Pool owned by the Cache.
6. Store the DB record in the Record Store and register it in every MAP.
7. Look up `test_user` again and return it.

### When MAP and Loading Conditions Differ

A MAP defines the condition for finding the record requested by the user, while `select.fields` defines the range loaded from the DB on a Cache miss. These conditions are independent.

The following configuration looks up results by `callsign`, but loads the entire specified time range on a Cache miss.

```json
{
  "select": {
    "query": "SELECT seq, callsign, event_time, lat, lng FROM public.flight_positions WHERE event_time >= $1::text::timestamp AND event_time < $2::text::timestamp",
    "fields": ["from_time", "to_time"]
  },
  "maps": [
    ["seq"],
    ["callsign"]
  ]
}
```

The user passes the MAP value and loading conditions in one JSON Object.

```rust
let result = cache.query(
    r#"{
      "callsign":"FL042",
      "from_time":"2026-10-04 10:00:00",
      "to_time":"2026-10-04 11:00:00"
    }"#,
);
```

`callsign` is used for MAP selection and lookup. `from_time` and `to_time` are used only for SELECT when a Cache miss occurs. Dates are not used to create a MAP. If `FL042` is already in the Cache, no DB Query is executed.

## 3. Common Record Mutation Rules

`insert()`, `update()`, and `delete()` mutate the Cache first, then enqueue the DB operation in the WRITE Queue of the Connection owned by the Cache.

```text
Application
    -> Cache Record and MAP change
    -> Connection WRITE Queue
    -> Serial DB Worker
    -> Database
```

A success response means that input validation, the Cache mutation, and WRITE Queue registration succeeded. It does not mean the Background DB operation has completed. If Queue registration fails, the Cache mutation is rolled back and error JSON is returned.

### insert()

Pass every field of the new record as JSON. The DB is not queried before INSERT.

```rust
let result = cache.insert(
    r#"{
      "id":100001,
      "user_id":"test_user",
      "display_name":"Test User",
      "status":"ACTIVE"
    }"#,
);
```

```json
{
  "code": 0,
  "message": "success"
}
```

The input must contain the Primary Key, every MAP field, and every field referenced by `insert.fields`. If the same Primary Key exists in the Cache, `9084` is returned. If it is absent from the Cache but exists in the DB, the Background INSERT may fail, so the caller must ensure that the Primary Key is new.

### update()

Pass every field of the record after mutation as JSON. This is not a Patch operation that accepts only selected fields.

```rust
let result = cache.update(
    r#"{
      "id":100001,
      "user_id":"test_user",
      "display_name":"Test User Updated",
      "status":"ACTIVE"
    }"#,
);
```

```json
{
  "code": 0,
  "message": "success"
}
```

The target record must exist in the Cache. For an `ON_DEMAND` record that has not yet been loaded, first load it with `query()`, then call `update()`. If a MAP field value changes, the previous MAP reference is removed and the record is registered in the new MAP. If the target does not exist, `9085` is returned.

### delete()

Pass only the fields and values matching one registered MAP.

```rust
let result = cache.delete(r#"{"user_id":"test_user"}"#);
```

```json
{
  "code": 0,
  "message": "success"
}
```

For a composite MAP, pass all field names and values in one JSON Object.

```rust
let result = cache.delete(
    r#"{"tenant_id":"tenant-a","id":100001}"#,
);
```

If a MAP references multiple records, all matching Cache records are deleted and a DB DELETE operation for each is registered in the Queue. The returned JSON does not include a deletion count. Use `status()` to check the remaining Cache record count.

For `ON_DEMAND`, if the target exists only in the DB, first load it with `query()`, then call `delete()`. If the target is absent from the Cache, `9085` is returned and no DB DELETE is registered in the Queue.

## 4. Drain Configuration

The entire Cache system shares one Retention Drain Thread. This Thread periodically checks registered Caches.

### Extending the Drain Time

For a record with Retention configured, the Drain time is extended when any of the following events occurs.

- An `ON_DEMAND` SELECT result is newly loaded into the Cache
- A record is loaded into the Cache by `PRELOAD`
- A new record is added to the Cache by `insert()`
- An existing Cache record is changed by `update()`
- `query()` finds and returns a record from the Cache

`TIMESTAMP` updates the last-used time to the current time when one of these events occurs. `SCORE` resets the value to the initial Score configured in the Definition.

For example, when the `TIMESTAMP` `value` is `3600`, the record becomes eligible for Drain 3,600 seconds after the last event. If the same record is queried again before removal, another 3,600 seconds starts from that query time.

```text
10:00:00  Record loaded       -> Drain after 11:00:00
10:40:00  Record queried      -> Drain after 11:40:00
11:20:00  Record queried      -> Drain after 12:20:00
12:20:00  No later activity   -> Drain eligible
```

When a group MAP lookup returns multiple records, Retention is updated for every record actually returned. If a lookup for a value absent from the MAP returns an empty result, there is no Cache record to update.

### TIMESTAMP

```json
"retention": {
  "type": "TIMESTAMP",
  "value": 3600
}
```

`value` is the retention duration in seconds. A record is removed when no new event occurs for 3,600 seconds after its last load, mutation, or lookup.

### SCORE

```json
"retention": {
  "type": "SCORE",
  "value": 3600
}
```

Loading, mutating, or querying a record resets its Score to `3600`. The Score decreases on each Drain cycle, and the record is removed when it reaches zero.

### NONE

```json
"retention": {
  "type": "NONE",
  "value": 0
}
```

Automatic removal is disabled. This can be used for a `PRELOAD` Cache that must retain every record continuously.

Drain removes only in-memory Cache Records and MAP references. It does not delete source DB records. When a removed record is requested again, it is reloaded through an `ON_DEMAND` SELECT.

## 5. status() Interface

Call `status()` without input parameters.

```rust
let result = cache.status();
```

Example output:

```json
{
  "code": 0,
  "message": "success",
  "product": "Autobricks Cache",
  "version": "0.1.106",
  "copyright": "(C) 2026 Autobricks, Co.",
  "record_count": 100000,
  "memory_bytes": 111989330
}
```

`record_count` is the number of records currently loaded in the Cache, and `memory_bytes` reports memory managed by the Cache in Bytes.

## 6. Using SQLite

For SQLite, set `driver` to `sqlite` and provide the Database file path in `database`. Because SQLite does not use a Server address or authentication, set `host` and `authentication` to empty strings, `port` to `0`, and `tls_used` to `false`.

For plain SQLite, set `opt.key` to `null`.

```json
{
  "connection_id": "application_sqlite",
  "name": "application SQLite database",
  "kind": "DATABASE",
  "driver": "sqlite",
  "host": "",
  "port": 0,
  "database": "data/application.sqlite3",
  "queue_directory": "data/queue",
  "authentication": {
    "username": "",
    "password": ""
  },
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

A relative `database` path is resolved against the current working directory of the program using the Library. An absolute path may be used in production.

`opt` supports the following values.

| Field | Supported Values | Meaning |
| --- | --- | --- |
| `key` | `null`, string | `null` selects plain SQLite; a string is the SQLCipher Key |
| `open_mode` | `READ_ONLY` | Open an existing file read-only |
| `open_mode` | `READ_WRITE` | Open an existing file for reading and writing; fail if the file does not exist |
| `open_mode` | `READ_WRITE_CREATE` | Open for reading and writing; create the file if it does not exist |
| `mutex` | `FULL`, `NO` | SQLite Connection Mutex Mode |
| `uri` | `true`, `false` | Whether to interpret `database` as an SQLite URI |
| `cache` | `DEFAULT`, `SHARED`, `PRIVATE` | SQLite Page Cache Mode |
| `journal_mode` | `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, `WAL`, `OFF` | Journal Mode |
| `synchronous` | `OFF`, `NORMAL`, `FULL`, `EXTRA` | Disk synchronization level |
| `foreign_keys` | `true`, `false` | Whether Foreign Key validation is enabled |

### SQLite Option Combination Examples

A typical new SQLite file uses the following combination. It creates the file if absent and uses WAL with normal synchronization.

```json
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
```

Use `READ_WRITE` to open only an existing file for reading and writing and fail startup if it is absent. Set `synchronous` to `FULL` when stronger Disk synchronization is required.

```json
"opt": {
  "key": null,
  "open_mode": "READ_WRITE",
  "mutex": "FULL",
  "uri": false,
  "cache": "PRIVATE",
  "journal_mode": "WAL",
  "synchronous": "FULL",
  "foreign_keys": true
}
```

When using an existing Database read-only, specify the Journal Mode already used by the file. The following example opens an existing Database created with WAL in read-only mode.

```json
"opt": {
  "key": null,
  "open_mode": "READ_ONLY",
  "mutex": "NO",
  "uri": false,
  "cache": "PRIVATE",
  "journal_mode": "WAL",
  "synchronous": "NORMAL",
  "foreign_keys": true
}
```

With `uri: true`, `database` may contain an SQLite URI. To let multiple Pool Connections use the same Memory Database, use a shared URI with `cache: SHARED` instead of ordinary `:memory:`.

```json
{
  "database": "file:autobricks-cache?mode=memory&cache=shared",
  "queue_directory": "data/queue",
  "opt": {
    "key": null,
    "open_mode": "READ_WRITE_CREATE",
    "mutex": "FULL",
    "uri": true,
    "cache": "SHARED",
    "journal_mode": "MEMORY",
    "synchronous": "OFF",
    "foreign_keys": true
  }
}
```

Use this shared Memory combination only for Tests or temporary data. Data disappears when the process exits, and `synchronous: OFF` provides no durability.

`journal_mode: OFF` or `synchronous: OFF` does not guarantee recovery or durability after a failure. The generally recommended production Database combination is `WAL` with `FULL`; `WAL` with `NORMAL` may be used to prioritize performance while retaining WAL durability. When `open_mode` and the URI `mode` are used together, configure them with equivalent meanings.

`query_timeout_ms` is applied as SQLite `busy_timeout`. All `pool_size - 1` SELECT Connections and the one Connection dedicated to DBWorkerThread are opened with the same `database` and `opt`.

The Cache Definition's `connection_id` must match the SQLite Connection's `connection_id`. SQLite Queries must use syntax supported by SQLite without PostgreSQL Schema names or Type Casts. Parameters may use the `$1`, `$2` format.

```json
{
  "cache_id": "user_cache",
  "connection_id": "application_sqlite",
  "cache_type": "PRELOAD",
  "retention": { "type": "NONE", "value": 0 },
  "primary_key": ["id"],
  "select": {
    "query": "SELECT id, user_id, display_name, status FROM users",
    "fields": []
  },
  "insert": {
    "query": "INSERT INTO users (id, user_id, display_name, status) VALUES ($1, $2, $3, $4)",
    "fields": ["id", "user_id", "display_name", "status"]
  },
  "update": {
    "query": "UPDATE users SET user_id = $1, display_name = $2, status = $3 WHERE id = $4",
    "fields": ["user_id", "display_name", "status", "id"]
  },
  "delete": {
    "query": "DELETE FROM users WHERE id = $1",
    "fields": ["id"]
  },
  "maps": [["id"], ["user_id"]]
}
```

## 7. Using SQLCipher

SQLCipher also uses `sqlite` as the `driver`. The difference from plain SQLite is that a SQLCipher Key string is specified in `opt.key`. Because the Library uses the bundled SQLCipher Engine, no separate system SQLCipher Library is required at Runtime.

```json
{
  "connection_id": "application_sqlcipher",
  "name": "application SQLCipher database",
  "kind": "DATABASE",
  "driver": "sqlite",
  "host": "",
  "port": 0,
  "database": "/var/lib/application/application.sqlcipher",
  "queue_directory": "/var/lib/application/cache-queue",
  "authentication": {
    "username": "",
    "password": ""
  },
  "tls_used": false,
  "connect_timeout_ms": 2000,
  "query_timeout_ms": 5000,
  "pool_used": true,
  "pool_size": 10,
  "opt": {
    "key": "replace-with-a-protected-database-key",
    "open_mode": "READ_WRITE_CREATE",
    "mutex": "FULL",
    "uri": false,
    "cache": "DEFAULT",
    "journal_mode": "WAL",
    "synchronous": "FULL",
    "foreign_keys": true
  }
}
```

Each Connection is opened in the following sequence.

1. Open the Database file with the specified `open_mode`, `mutex`, `uri`, and `cache` Flags.
2. Disable internal SQLCipher Logging.
3. Apply `opt.key` to the Connection.
4. Verify the SQLCipher Engine with `PRAGMA cipher_version`.
5. Read `sqlite_master` to verify that the Key correctly decrypts the Database.
6. Apply `busy_timeout`, `journal_mode`, `synchronous`, and `foreign_keys`.

Connection initialization fails if the Key is incorrect or if an encrypted Database is accessed with `key: null`. An ordinary `sqlite3` program must also be unable to open the SQLCipher Database.

Examples that store `key` in plain text in a configuration file exist only to explain the configuration format. Production environments must use restricted file permissions or a Key delivery path protected by a Secret Manager or HSM. The Library neither generates the Key nor stores it in a separate file; it uses the supplied Key when opening each SQLCipher Connection.

The Query format and `connection_id` association rules for a SQLCipher Cache Definition are the same as for SQLite above. Use `READ_WRITE_CREATE` for a new Database, `READ_WRITE` when only an existing Database is allowed, and `READ_ONLY` for a read-only service.

### SQLCipher Option Combination Examples

Use `key` with `READ_WRITE_CREATE` when creating a new encrypted Database.

```json
"opt": {
  "key": "replace-with-a-protected-database-key",
  "open_mode": "READ_WRITE_CREATE",
  "mutex": "FULL",
  "uri": false,
  "cache": "DEFAULT",
  "journal_mode": "WAL",
  "synchronous": "FULL",
  "foreign_keys": true
}
```

To allow only an existing encrypted Database, change the mode to `READ_WRITE`. This makes initialization fail rather than creating a new empty encrypted Database when the file path is incorrect.

```json
"opt": {
  "key": "replace-with-a-protected-database-key",
  "open_mode": "READ_WRITE",
  "mutex": "FULL",
  "uri": false,
  "cache": "PRIVATE",
  "journal_mode": "WAL",
  "synchronous": "FULL",
  "foreign_keys": true
}
```

A read-only SQLCipher Pool must also pass the same Key to every Connection.

```json
"opt": {
  "key": "replace-with-a-protected-database-key",
  "open_mode": "READ_ONLY",
  "mutex": "NO",
  "uri": false,
  "cache": "PRIVATE",
  "journal_mode": "WAL",
  "synchronous": "FULL",
  "foreign_keys": true
}
```

Specifying different Keys for the same encrypted file does not result in only some Connections being used. If Key validation fails for any Connection during Connection Pool initialization, initialization of the entire Connection fails.
