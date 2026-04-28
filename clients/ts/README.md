# @cornejong/hrana

TypeScript/JavaScript client for the [Hrana protocol](https://github.com/libsql/hrana-client-ts/blob/main/HRANA_SPEC.md).
Works in the browser and Node.js. Supports HTTP pipeline and WebSocket transports,
built-in pagination, in-memory caching, and IndexedDB-backed persistent caching.

## Install

```bash
npm install @cornejong/hrana
```

## Quick start

```ts
import { HranaClient } from "@cornejong/hrana"

const db = new HranaClient({
    url: "http://localhost:8080",
    authToken: "your-token",   // optional
})

const result = await db.execute("SELECT * FROM users WHERE id = ?", [1n])
console.log(result.rows)

await db.close()
```

## Transport

The URL scheme selects the transport automatically.

| Scheme           | Transport     | Versions                        |
| ---------------- | ------------- | ------------------------------- |
| `http` / `https` | HTTP pipeline | `v2`, `v3` (default `v3`)       |
| `ws` / `wss`     | WebSocket     | `v1`, `v2`, `v3` (default `v3`) |

## Querying

```ts
// Read — returns rows
const result = await db.execute("SELECT * FROM products WHERE active = ?", [true])
// result.columns → ["id", "name", "price"]
// result.rows    → [{ id: 1n, name: "Widget", price: 9.99 }, ...]

// Write — INSERT / UPDATE / DELETE / DDL
const { rowsAffected, lastInsertRowid } = await db.run(
    "INSERT INTO products (name, price) VALUES (?, ?)",
    ["Gadget", 19.99]
)

// Multiple statements in one call
const [users, posts] = await db.batch([
    { sql: "SELECT * FROM users" },
    { sql: "SELECT * FROM posts WHERE author_id = ?", params: [1n] },
])
```

SQLite integers are always returned as `bigint` to preserve the full int64
range. Use `Number(row.id)` when you need a plain number and values are within
the safe integer range.

## Pagination

```ts
const result = await db.paginate(
    "SELECT * FROM products ORDER BY name",
    [],
    { page: 1, pageSize: 20 }
)

console.log(result.rows)        // rows for page 1
console.log(result.totalRows)   // e.g. 143
console.log(result.totalPages)  // 8
console.log(result.hasNextPage) // true
console.log(result.hasPrevPage) // false
```

Trailing semicolons are stripped automatically. Both positional (`?`) and named
(`:param`) parameters are supported.

## In-memory cache

Cache results for the lifetime of the page to avoid duplicate round-trips.

```ts
const db = new HranaClient({
    url: "http://localhost:8080",
    cache: {
        ttl: 60_000,       // default TTL per entry (ms)
        maxSize: 100,      // max entries before LRU eviction
        clearOnWrite: true // flush cache after every run() call
    },
})

// Opt in per call
const result = await db.execute("SELECT * FROM config", [], { cache: true })

// Per-call TTL override (5 minutes)
const result = await db.execute("SELECT * FROM countries", [], { cache: { ttl: 300_000 } })

// Manual control
db.cache.invalidate("SELECT * FROM config")
db.cache.clear()
```

## Persistent cache (IndexedDB)

Store results across page reloads using IndexedDB. Ideal for reference data that
rarely changes.

```ts
import { HranaClient, PersistentCache } from "@cornejong/hrana"

// Open once at app startup
const store = await PersistentCache.open({
    defaultTtl: 7 * 24 * 60 * 60 * 1000,  // 7 days
})

const db = new HranaClient({
    url: "http://localhost:8080",
    persistentCache: store,
})

// Schedule cleanup — self-throttled to once per day
PersistentCache.purgeAllExpired()

// Opt in per query
const result = await db.execute("SELECT * FROM countries", [], {
    cache: { persist: true, group: "reference" }
})

// Invalidate a group after a write
await db.run("UPDATE countries SET name = ? WHERE id = ?", ["Newland", 1n])
await store.invalidateGroup("reference")
```

When a persistent cache is used the lookup order is:
1. In-memory (L1) — instant
2. IndexedDB (L2) — survives reload, warms L1 on hit
3. Network — result written back to both levels

If IndexedDB is unavailable a `console.warn` is emitted and the instance falls
back to in-memory automatically.

## Browser bundle

No bundler required. Reference the pre-built IIFE bundle directly:

```html
<script src="dist/hrana.bundle.js"></script>
<script>
    const db = new Hrana.HranaClient({ url: "http://localhost:8080" })
    db.execute("SELECT sqlite_version()").then(r => console.log(r.rows))
</script>
```

Build it from source:

```bash
npm run bundle
```

## Build

```bash
npm run build      # compile TypeScript → dist/
npm run typecheck  # type-check without emitting
npm run bundle     # build dist/hrana.bundle.js (IIFE, minified)
```

## Documentation

Full guides and API reference are in the [`docs/`](./docs/) folder.

| Guide                                          | Description                             |
| ---------------------------------------------- | --------------------------------------- |
| [Getting Started](./docs/getting-started.md)   | Installation, transports, lifecycle     |
| [Querying](./docs/querying.md)                 | Parameters, value types, error handling |
| [Pagination](./docs/pagination.md)             | `paginate()`, page controls             |
| [Caching](./docs/caching.md)                   | Memory and persistent cache overview    |
| [Persistent Cache](./docs/persistent-cache.md) | IndexedDB cache, groups, cleanup        |
| [API Reference](./docs/api-reference.md)       | Full type and method reference          |
