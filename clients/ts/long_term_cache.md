# Long-Term Cache — Design Proposal

## Context

The existing `QueryCache` (on `HranaClient.cache`) is intentionally thin:
in-memory, per-instance, TTL in the minute range. It disappears on page reload
and is scoped to one client object.

This proposal describes a **separate, persistent cache layer** backed by a
browser storage API. It is complementary — the two caches can be used
independently or layered (memory cache in front of persistent cache).

---

## Storage backend options

| Backend                      | Capacity       | Async     | Structured data        | Available in workers | Notes                                                                                         |
| ---------------------------- | -------------- | --------- | ---------------------- | -------------------- | --------------------------------------------------------------------------------------------- |
| `localStorage`               | ~5 MB          | No (sync) | No (strings only)      | No                   | Simple, but capacity and serialisation are limiting. Blocks the main thread on large payloads |
| `sessionStorage`             | ~5 MB          | No (sync) | No                     | No                   | Clears on tab close — not useful for "long-term"                                              |
| `IndexedDB`                  | Hundreds of MB | Yes       | Yes (structured clone) | Yes                  | Best fit. Native support for bigint, Uint8Array, binary blobs without base64 overhead         |
| `Cache API` (Service Worker) | Hundreds of MB | Yes       | HTTP responses only    | Yes (SW context)     | Designed for HTTP caching, awkward for raw query results                                      |

**Recommendation: IndexedDB**, wrapped in a minimal promise-based helper so the
caller never touches IDB internals.

Fallback strategy: if IndexedDB is unavailable (SSR, certain privacy modes),
silently fall back to the existing in-memory cache so the API surface stays
identical.

---

## Cache groups

A **cache group** is a named bucket that holds related entries. Any entry
belongs to exactly one group. Invalidating a group removes all its entries
in a single operation — no need to track individual SQL statements.

Example use-cases:
- `"products"` — all queries against the products table.
- `"session"` — queries tied to the currently logged-in user (clear on logout).
- `"static"` — reference data that almost never changes.

### Group assignment

Groups are opt-in per call, consistent with the existing `CachePolicy` pattern:

```ts
await db.execute("SELECT * FROM products", [], {
    cache: { ttl: 7 * 24 * 60 * 60 * 1000, group: "products" }
})

await db.execute("SELECT * FROM categories", [], {
    cache: { persist: true, group: "products" }   // uses group default TTL
})
```

---

## Proposed API surface

### New top-level export: `PersistentCache`

```ts
const store = await PersistentCache.open({
    dbName: "hrana_cache",   // IDB database name — allows multiple isolated stores
    defaultTtl: 30 * 24 * 60 * 60 * 1000,  // 30 days in ms (default)
    defaultGroup: "default",
})
```

`PersistentCache.open()` returns a promise so IDB initialisation is fully
async. The instance is then passed to `HranaClient`:

```ts
const db = new HranaClient({
    url: "http://localhost:8080",
    persistentCache: store,
})
```

### Per-call usage

`CachePolicy` gains an optional `persist` flag and `group` field:

```ts
// Use persistent cache with default TTL and group
await db.execute(sql, params, { cache: { persist: true } })

// Explicit TTL + group
await db.execute(sql, params, { cache: { persist: true, ttl: 7 * 24 * 60 * 60 * 1000, group: "products" } })
```

When `persist: true` the lookup order is:
1. In-memory cache (if a memory TTL is also set)
2. Persistent cache
3. Network

Result is written back to both levels.

### Invalidation & cleanup

```ts
// Invalidate one entry by SQL + params
await store.invalidate("SELECT * FROM products WHERE id = ?", [42n])

// Invalidate an entire group
await store.invalidateGroup("products")

// Remove all expired entries (safe to call on app startup or periodically)
await store.purgeExpired()

// Wipe everything
await store.clear()

// Standalone convenience — no instance needed, useful for "clear cache" buttons
await PersistentCache.clearAll("hrana_cache")  // drops the named IDB database

// Purge expired entries across every hrana IDB store — call once on app startup.
// Skips automatically if a purge already ran within the throttle window.
await PersistentCache.purgeAllExpired({ minInterval: 24 * 60 * 60 * 1000 })
// Returns: { skipped: true }                         — if within throttle window
// Returns: { stores: number, purged: number }        — if purge actually ran
```

`purgeAllExpired()` enumerates all IDB databases whose name starts with
`hrana_cache` (using `indexedDB.databases()`, supported in all modern browsers)
and runs `purgeExpired()` on each one. It is a static method so no instance is
needed — one line at app startup handles everything regardless of how many
`PersistentCache` instances are open.

#### Throttling with `minInterval`

Iterating every store on every page load would be wasteful when there are many
stores or large datasets. `minInterval` (ms, default `86_400_000` — 24 hours)
prevents redundant work:

- The timestamp of the last successful purge is persisted in `localStorage`
  under the key `hrana_cache:last_purge`. `localStorage` is used deliberately
  here (not IDB) because it is synchronous — the check completes in
  microseconds before any async IDB work begins.
- If `Date.now() - lastPurge < minInterval` the method returns
  `{ skipped: true }` immediately without opening a single IDB database.
- After a successful purge the timestamp is updated.
- If `localStorage` is unavailable (e.g. private browsing with storage blocked)
  the timestamp check is skipped and the purge always runs — safe, just not
  throttled.

```ts
// Typical app startup — fire-and-forget, non-blocking, self-throttles to once a day:
import { PersistentCache } from "hrana"

PersistentCache.purgeAllExpired()

// Override to once per week:
PersistentCache.purgeAllExpired({ minInterval: 7 * 24 * 60 * 60 * 1000 })

// Force a purge regardless of last-run timestamp:
PersistentCache.purgeAllExpired({ minInterval: 0 })
```

Await it only if you need the result for logging or telemetry.

### Introspection

```ts
const info = await store.inspect()
// {
//   totalEntries: 340,
//   groups: { products: 120, session: 5, default: 215 },
//   oldestEntry: Date,
//   newestEntry: Date,
//   estimatedBytes: 204800,  // best-effort via JSON serialisation size
// }
```

---

## IDB schema (internal)

### Object store: `entries`

| Field       | Type             | Description                                                             |
| ----------- | ---------------- | ----------------------------------------------------------------------- |
| `key`       | string (primary) | FNV-1a hash of normalised SQL + params (same algorithm as memory cache) |
| `group`     | string (indexed) | Group name — enables `getAll({ index: "group", value: "products" })`    |
| `expiresAt` | number (indexed) | `Date.now() + ttl` — enables range query for `purgeExpired()`           |
| `createdAt` | number           | For introspection / debugging                                           |
| `result`    | any              | Stored via structured clone: supports bigint, Uint8Array natively       |

Indexes: `group`, `expiresAt`.

No separate "groups" store needed — groups are implicit via the index.

### `localStorage` key (outside IDB)

| Key                      | Value                         | Purpose                               |
| ------------------------ | ----------------------------- | ------------------------------------- |
| `hrana_cache:last_purge` | Unix timestamp (ms) as string | Throttle gate for `purgeAllExpired()` |

This is the only thing written to `localStorage`. Everything else lives in IDB.

---

## TTL & cleanup strategy

- Default TTL: **30 days** (configurable per `PersistentCache.open()`).
- Per-call TTL overrides the default for that entry only.
- **No background sweeper** — browsers do not give reliable background execution
  to non-SW code. Instead, `purgeExpired()` is provided as an explicit call
  and is recommended on app startup.
- On each cache read, if `Date.now() > expiresAt` the entry is deleted and
  treated as a miss (stale-on-expiry, no SWR).
- Browsers may evict IDB data under storage pressure. The cache must
  be treated as a best-effort optimisation, not a source of truth.

---

## What stays out of scope

| Idea                                                                                | Reason excluded                                                                                                             |
| ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Cross-tab cache invalidation (`BroadcastChannel`)                                   | Adds substantial event-plumbing complexity; out of scope for a query cache                                                  |
| Automatic write-through invalidation (detecting which group a `run()` call affects) | Requires SQL parsing; the caller assigns groups explicitly                                                                  |
| Encryption at rest                                                                  | IDB is accessible to any script on the same origin. Sensitive data should not be persisted — this is a UX-facing read cache |
| Node.js / server-side support                                                       | `IndexedDB` is browser-only. Server-side usage keeps the existing in-memory cache                                           |

---

## Decisions

1. **IDB unavailable fallback** — log a `console.warn` so the caller knows
   persistence is not working, then transparently fall back to the existing
   in-memory cache. The API surface stays identical; the caller does not need
   a try/catch.

2. **`dbName` default** — keep it simple (`"hrana_cache"`). No server URL
   encoding. Callers are responsible for calling `PersistentCache.clearAll()`
   when switching environments.

3. **Group TTL** — groups do not declare their own TTL. TTL is set per-call
   only; the store default is the fallback.

4. **`purgeExpired()` call site** — fully the caller's responsibility.
   `HranaClient` never calls it implicitly. Use `PersistentCache.purgeAllExpired()`
   on app startup as the recommended pattern.

5. **Serialisation fallback** — no `localStorage` fallback path, so structured
   clone handles `bigint` / `Uint8Array` natively in IDB with no custom
   serialiser needed.
