# Caching

The client ships with two complementary caching layers. You can use either or
both. Neither is active unless you opt in.

| Layer                | Backed by        | Survives page reload | Max TTL               |
| -------------------- | ---------------- | -------------------- | --------------------- |
| **Memory cache**     | JavaScript `Map` | No                   | Any (default 60 s)    |
| **Persistent cache** | IndexedDB        | Yes                  | Any (default 30 days) |

---

## Memory cache

### Client-level configuration

Pass a `cache` object to `HranaConfig` to set defaults for the in-memory store.

```ts
const db = new HranaClient({
    url: "http://localhost:8080",
    cache: {
        ttl: 60_000,        // default TTL per entry (ms). Default: 60 000
        maxSize: 100,       // max entries; oldest evicted when exceeded. Default: 100
        clearOnWrite: true, // flush entire cache after every run() call. Default: false
    },
})
```

All three fields are optional; omit the `cache` key entirely to disable
client-level defaults (you can still opt in per-call).

### Per-call opt-in

```ts
// Use client TTL default
const users = await db.execute("SELECT * FROM users", [], { cache: true })

// Override TTL for this call only (5 minutes)
const config = await db.execute("SELECT * FROM config", [], { cache: { ttl: 300_000 } })

// Never cache (default — same as omitting the option)
const live = await db.execute("SELECT * FROM live_feed", [], { cache: false })
```

`paginate()` accepts the same `cache` option inside its options object:

```ts
const page = await db.paginate(
    "SELECT * FROM products ORDER BY id",
    [],
    { page: 1, pageSize: 10, cache: true }
)
```

Each page is cached as a separate entry — changing `page` or `pageSize` produces
a different cache key.

### Manual control

The `db.cache` property exposes the `QueryCache` instance.

```ts
// Number of live entries
console.log(db.cache.size)

// Remove one entry
db.cache.invalidate("SELECT * FROM users WHERE role = ?", ["admin"])

// Flush everything
db.cache.clear()
```

### Cache key

Two calls produce the same cache key when:
- The SQL is identical after lowercasing and whitespace-collapsing
- The parameter values and their types are identical

`SELECT * FROM users` and `select  *  from  users` share one entry.
`[1n]` (bigint) and `["1"]` (string) produce different keys.

### `clearOnWrite` behaviour

When `clearOnWrite: true` is set on the client, every successful `run()` call
flushes the entire in-memory cache. This is a blunt but safe approach for
single-table apps where any write invalidates all cached reads.

For finer control, call `db.cache.invalidate()` or `db.cache.clear()` manually
after writes.

---

## Persistent cache

> See [Persistent Cache](./persistent-cache.md) for the full guide.

The persistent cache stores results in IndexedDB so they survive page reloads,
tabs closing, and browser restarts (up to the configured TTL or browser storage
eviction).

### Quick setup

```ts
import { HranaClient, PersistentCache } from "@cornejong/hrana"

// 1. Open the store (async — do this once on app startup)
const store = await PersistentCache.open({
    defaultTtl: 7 * 24 * 60 * 60 * 1000,  // 7 days
})

// 2. Pass it to the client
const db = new HranaClient({
    url: "http://localhost:8080",
    persistentCache: store,
})

// 3. Schedule periodic cleanup (fire-and-forget, self-throttled to once per day)
PersistentCache.purgeAllExpired()
```

### Per-call opt-in

```ts
// Persist with store default TTL
await db.execute("SELECT * FROM countries", [], { cache: { persist: true } })

// Persist with explicit TTL and a group label
await db.execute("SELECT * FROM products", [], {
    cache: { persist: true, ttl: 3 * 24 * 60 * 60 * 1000, group: "products" }
})
```

### Two-level lookup order

When `persist: true` is used, the lookup order is:

1. **Memory (L1)** — instant, no async overhead
2. **IndexedDB (L2)** — survives reload; warms L1 on hit
3. **Network** — result is written to both L1 and L2

---

## CachePolicy reference

The `cache` option accepted by `execute()` and `paginate()`:

| Value                              | Behaviour                                               |
| ---------------------------------- | ------------------------------------------------------- |
| `false` / omitted                  | No caching                                              |
| `true`                             | Memory cache; uses client default TTL                   |
| `{ ttl: number }`                  | Memory cache; explicit TTL (ms)                         |
| `{ persist: true }`                | Persistent (IDB) cache; store default TTL               |
| `{ persist: true, ttl: number }`   | Persistent cache; explicit TTL                          |
| `{ persist: true, group: string }` | Persistent cache; assigns a group for bulk invalidation |
| `{ persist: true, ttl, group }`    | All of the above combined                               |
