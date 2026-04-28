# Persistent Cache

The `PersistentCache` class stores query results in **IndexedDB** so they
survive page reloads and browser restarts. It is designed for databases that
are treated as largely read-only, where reducing server load and improving
perceived performance matters more than having perfectly fresh data.

> For short-lived, in-memory caching see [Caching](./caching.md).

---

## Setup

```ts
import { HranaClient, PersistentCache } from "@cornejong/hrana"

const store = await PersistentCache.open({
    dbName: "hrana_cache",                  // IDB database name. Default: "hrana_cache"
    defaultTtl: 30 * 24 * 60 * 60 * 1000,  // Default TTL (ms). Default: 30 days
    defaultGroup: "default",               // Default group for entries. Default: "default"
})

const db = new HranaClient({
    url: "http://localhost:8080",
    persistentCache: store,
})
```

`PersistentCache.open()` returns a `Promise` because IDB initialisation is
asynchronous. Call it once at app startup and reuse the instance.

---

## Caching queries

Pass `cache: { persist: true }` on any `execute()` or `paginate()` call.

```ts
// Default store TTL + default group
await db.execute("SELECT * FROM countries", [], { cache: { persist: true } })

// Explicit TTL (7 days) and named group
await db.execute("SELECT * FROM products", [], {
    cache: { persist: true, ttl: 7 * 24 * 60 * 60 * 1000, group: "products" }
})

// Paginated
const page = await db.paginate("SELECT * FROM products ORDER BY id", [], {
    page: 1, pageSize: 20, cache: { persist: true, group: "products" }
})
```

---

## Cache groups

Every entry belongs to a group. Groups let you invalidate a set of related
entries in a single call — useful after a write that affects a known table.

```ts
// Tag entries as belonging to "products"
await db.execute("SELECT * FROM products", [], {
    cache: { persist: true, group: "products" }
})
await db.execute("SELECT * FROM categories", [], {
    cache: { persist: true, group: "products" }
})

// After a product update — remove all "products" entries at once
await db.run("UPDATE products SET price = ? WHERE id = ?", [9.99, 1n])
await store.invalidateGroup("products")
```

If no group is specified, the entry is assigned the store's `defaultGroup`
(`"default"` unless overridden in `PersistentCache.open()`).

---

## Invalidation

### One entry

Remove the cached result for a specific SQL + params combination.

```ts
await store.invalidate("SELECT * FROM products WHERE id = ?", [42n])
```

> Paginated entries include the page number and page size in their key.
> Use `invalidateGroup()` or `clear()` to remove all paginated results for a query.

### By group

```ts
await store.invalidateGroup("products")
```

### Everything

```ts
await store.clear()
```

### Drop the entire database

Drops the IDB database entirely — useful for a "clear all cache" button in
settings UI.

```ts
await PersistentCache.clearAll("hrana_cache")
```

---

## Expiry & cleanup

Entries expire after their TTL (30 days by default). Expired entries are
deleted lazily on the next read. To actively reclaim storage, call
`purgeExpired()`.

### Single store

```ts
const purged = await store.purgeExpired()
console.log(`Removed ${purged} expired entries`)
```

### All stores — recommended on app startup

`purgeAllExpired()` is a static method that scans every IDB database whose
name starts with `"hrana_cache"` and purges expired entries from all of them.

It is self-throttled: a last-run timestamp is stored in `localStorage` and
the method skips work if called within the throttle window.

```ts
// Fire-and-forget — non-blocking, safe to call on every page load
PersistentCache.purgeAllExpired()

// Custom throttle window (once per week instead of once per day)
PersistentCache.purgeAllExpired({ minInterval: 7 * 24 * 60 * 60 * 1000 })

// Force a run regardless of the last-run timestamp
PersistentCache.purgeAllExpired({ minInterval: 0 })
```

The return value is useful for logging:

```ts
const result = await PersistentCache.purgeAllExpired()

if (result.skipped) {
    console.log("Purge skipped — ran recently")
} else {
    console.log(`Purged ${result.purged} entries across ${result.stores} store(s)`)
}
```

---

## Introspection

```ts
const info = await store.inspect()

// {
//   totalEntries: 340,
//   groups: { products: 120, session: 5, default: 215 },
//   oldestEntry: Date,
//   newestEntry: Date,
//   estimatedBytes: 204800,
// }
```

`estimatedBytes` is a best-effort figure based on JSON serialisation length
(bigint and Uint8Array are approximated).

---

## Fallback behaviour

If IndexedDB is unavailable (server-side rendering, certain private-browsing
modes), `PersistentCache.open()` logs a warning and returns an instance backed
by an in-memory `Map`. The API surface is identical — no code change is needed
in the caller.

```
[hrana] IndexedDB is not available; PersistentCache falling back to in-memory store.
```

Similarly, if `cache: { persist: true }` is used on a query but no
`persistentCache` was passed to `HranaClient`, a one-time warning is logged
and the memory cache is used instead.

```
[hrana] cache.persist requested but no persistentCache configured on HranaClient. Falling back to memory cache.
```

---

## Important notes

- IndexedDB data is accessible to any JavaScript on the same origin. **Do not
  cache sensitive data** (credentials, PII, session tokens).
- Browsers may evict IDB data under storage pressure. Treat the persistent
  cache as a performance optimisation, not a source of truth.
- Entries are not invalidated automatically when you call `run()`. Invalidate
  groups manually after writes, or use `clearOnWrite: true` on `HranaConfig`
  to flush the memory cache (persistent cache is unaffected by `clearOnWrite`).
