# API Reference

## `HranaClient`

The main entry point. Create one instance per application lifetime.

```ts
new HranaClient(config: HranaConfig)
```

### `HranaConfig`

| Field             | Type                   | Default     | Description                                                                                                 |
| ----------------- | ---------------------- | ----------- | ----------------------------------------------------------------------------------------------------------- |
| `url`             | `string`               | —           | Server URL. Scheme determines transport (`http/https` → HTTP pipeline, `ws/wss` → WebSocket). **Required.** |
| `authToken`       | `string`               | `undefined` | Auth token. Sent as `Authorization: Bearer <token>` (HTTP) or `jwt` field (WebSocket).                      |
| `version`         | `"v1" \| "v2" \| "v3"` | `"v3"`      | Protocol version. HTTP transport requires `v2` or `v3`.                                                     |
| `cache`           | `CacheConfig`          | `undefined` | In-memory cache settings. Omit to disable client-level defaults.                                            |
| `persistentCache` | `PersistentCache`      | `undefined` | IDB-backed persistent cache instance. Required to use `cache: { persist: true }`.                           |

### `CacheConfig`

| Field          | Type      | Default  | Description                                                |
| -------------- | --------- | -------- | ---------------------------------------------------------- |
| `ttl`          | `number`  | `60_000` | Default TTL in ms for in-memory cache entries.             |
| `maxSize`      | `number`  | `100`    | Maximum entries. Oldest is evicted when exceeded.          |
| `clearOnWrite` | `boolean` | `false`  | Flush the entire in-memory cache after every `run()` call. |

---

### `db.execute(sql, params?, options?)`

Execute a SQL statement that returns rows.

```ts
execute(
    sql: string,
    params?: SqlParams,
    options?: { cache?: CachePolicy }
): Promise<ResultSet>
```

| Argument        | Type                                     | Description                                     |
| --------------- | ---------------------------------------- | ----------------------------------------------- |
| `sql`           | `string`                                 | SQL statement                                   |
| `params`        | `SqlValue[] \| Record<string, SqlValue>` | Optional positional or named parameters         |
| `options.cache` | `CachePolicy`                            | Cache control — see [CachePolicy](#cachepolicy) |

Returns [`ResultSet`](#resultset).

---

### `db.run(sql, params?)`

Execute a statement that does not return rows (`INSERT` / `UPDATE` / `DELETE` / DDL).

```ts
run(
    sql: string,
    params?: SqlParams
): Promise<{ rowsAffected: number; lastInsertRowid: bigint | null }>
```

If `clearOnWrite: true` is configured, the in-memory cache is flushed after a
successful call.

---

### `db.paginate(sql, params?, options?)`

Execute a paginated query.

```ts
paginate(
    sql: string,
    params?: SqlParams,
    options?: PaginateOptions
): Promise<PaginatedResult>
```

#### `PaginateOptions`

| Field      | Type          | Default     | Description                        |
| ---------- | ------------- | ----------- | ---------------------------------- |
| `page`     | `number`      | `1`         | Page number (1-based)              |
| `pageSize` | `number`      | `20`        | Rows per page                      |
| `cache`    | `CachePolicy` | `undefined` | Cache control for this page result |

Returns [`PaginatedResult`](#paginatedresult).

---

### `db.batch(statements)`

Execute multiple statements and return results in order.

```ts
batch(
    statements: Array<{ sql: string; params?: SqlParams }>
): Promise<ResultSet[]>
```

---

### `db.close()`

Release the underlying connection / WebSocket. Safe to call multiple times.

```ts
close(): Promise<void>
```

---

### `db.cache`

The in-memory `QueryCache` instance.

```ts
db.cache.size                              // number of live entries
db.cache.invalidate(sql, params?)          // remove one entry
db.cache.clear()                           // remove all entries
```

---

## `PersistentCache`

IndexedDB-backed long-term query result cache.

### `PersistentCache.open(options?)`

```ts
static open(options?: PersistentCacheOptions): Promise<PersistentCache>
```

#### `PersistentCacheOptions`

| Field          | Type     | Default                   | Description                                       |
| -------------- | -------- | ------------------------- | ------------------------------------------------- |
| `dbName`       | `string` | `"hrana_cache"`           | IDB database name                                 |
| `defaultTtl`   | `number` | `2_592_000_000` (30 days) | Default TTL in ms                                 |
| `defaultGroup` | `string` | `"default"`               | Default group for entries with no group specified |

Falls back to an in-memory store (with a `console.warn`) if IDB is unavailable.

---

### `store.invalidate(sql, params?)`

Remove the cached entry for a specific SQL + params combination.

```ts
invalidate(sql: string, params?: SqlParams): Promise<void>
```

---

### `store.invalidateGroup(group)`

Remove all entries belonging to the named group.

```ts
invalidateGroup(group: string): Promise<void>
```

---

### `store.purgeExpired()`

Delete all expired entries from this store. Returns the count removed.

```ts
purgeExpired(): Promise<number>
```

---

### `store.clear()`

Remove all entries from this store.

```ts
clear(): Promise<void>
```

---

### `store.inspect()`

Return statistics about the current state of the store.

```ts
inspect(): Promise<InspectResult>
```

#### `InspectResult`

| Field            | Type                     | Description                                |
| ---------------- | ------------------------ | ------------------------------------------ |
| `totalEntries`   | `number`                 | Total entries currently stored             |
| `groups`         | `Record<string, number>` | Entry count per group                      |
| `oldestEntry`    | `Date \| null`           | Creation date of the oldest entry          |
| `newestEntry`    | `Date \| null`           | Creation date of the newest entry          |
| `estimatedBytes` | `number`                 | Best-effort storage estimate (JSON length) |

---

### `PersistentCache.clearAll(dbName)`

Drop an entire IDB database. Useful for "clear cache" UI buttons.

```ts
static clearAll(dbName: string): Promise<void>
```

---

### `PersistentCache.purgeAllExpired(options?)`

Purge expired entries across every IDB database prefixed with `"hrana_cache"`.
Self-throttled via `localStorage`; skips if called within `minInterval`.

```ts
static purgeAllExpired(options?: { minInterval?: number }): Promise<PurgeAllResult>
```

| Option        | Type     | Default             | Description                                       |
| ------------- | -------- | ------------------- | ------------------------------------------------- |
| `minInterval` | `number` | `86_400_000` (24 h) | Minimum ms between purge runs. Pass `0` to force. |

#### `PurgeAllResult`

```ts
type PurgeAllResult =
    | { skipped: true }
    | { skipped: false; stores: number; purged: number }
```

---

## `QueryCache`

In-memory TTL cache exposed on `HranaClient.cache`. Constructed automatically
by `HranaClient` — do not instantiate directly.

| Member                     | Type              | Description                      |
| -------------------------- | ----------------- | -------------------------------- |
| `size`                     | `number` (getter) | Number of live entries           |
| `invalidate(sql, params?)` | `void`            | Remove one entry by SQL + params |
| `clear()`                  | `void`            | Remove all entries               |

---

## `rows(result)`

Wrap a `ResultSet` in a [`ResultRows`](#resultrows) lazy iterator.

```ts
rows<T = Record<string, RowValue>>(result: ResultSet): ResultRows<T>
```

The generic `T` is a cast — columns must actually match the TypeScript type you
provide. Defaults to `Record<string, RowValue>`.

```ts
import { rows } from "@cornejong/hrana"

interface User { id: bigint; name: string }

const result = await db.execute("SELECT id, name FROM users")
rows<User>(result).forEach(u => console.log(u.name))
```

---

## `ResultRows<T>`

A lazy, chainable iterator over `ResultSet` rows. Obtained via `rows(result)`.

### `[Symbol.iterator]()`

Makes `ResultRows` usable in `for...of` loops.

```ts
for (const row of rows(result)) { ... }
```

### `.map<U>(fn)`

Lazily transform each row. Returns a new `ResultRows<U>` — nothing runs until
consumed.

```ts
map<U>(fn: (row: T, index: number) => U): ResultRows<U>
```

### `.filter(predicate)`

Lazily discard rows that do not satisfy `predicate`. Returns a new
`ResultRows<T>` — nothing runs until consumed.

```ts
filter(predicate: (row: T, index: number) => boolean): ResultRows<T>
```

### `.forEach(fn)`

Consume the iterator, calling `fn` for each row. No result array is allocated —
the lowest-overhead way to consume a result set.

```ts
forEach(fn: (row: T, index: number) => void): void
```

### `.toArray()`

Materialize all remaining rows into a plain array.

```ts
toArray(): T[]
```

---

## `HranaError`

Thrown on network failures and protocol errors.

```ts
try {
    await db.execute("SELECT * FROM missing")
} catch (err) {
    if (err instanceof HranaError) {
        console.error(err.message)
    }
}
```

---

## Types

### `SqlValue`

Accepted as SQL parameter values.

```ts
type SqlValue = null | boolean | number | bigint | string | ArrayBuffer | Uint8Array
```

### `SqlParams`

```ts
type SqlParams = SqlValue[] | Record<string, SqlValue>
```

### `RowValue`

Decoded column values returned in result rows.

```ts
type RowValue = null | bigint | number | string | Uint8Array
```

### `ResultSet`

```ts
interface ResultSet {
    columns: string[]
    rows: Record<string, RowValue>[]
    rowsAffected: number
    lastInsertRowid: bigint | null
}
```

### `PaginatedResult`

```ts
interface PaginatedResult {
    columns: string[]
    rows: Record<string, RowValue>[]
    page: number
    pageSize: number
    totalRows: number
    totalPages: number
    hasNextPage: boolean
    hasPrevPage: boolean
}
```

### `CachePolicy`

```ts
type CachePolicy =
    | boolean
    | { ttl?: number; persist?: boolean; group?: string }
```

| Value                      | Behaviour                           |
| -------------------------- | ----------------------------------- |
| `false` / omitted          | No caching                          |
| `true`                     | Memory cache; client default TTL    |
| `{ ttl }`                  | Memory cache; explicit TTL (ms)     |
| `{ persist: true }`        | Persistent cache; store default TTL |
| `{ persist: true, ttl }`   | Persistent cache; explicit TTL      |
| `{ persist: true, group }` | Persistent cache; assigns group     |
