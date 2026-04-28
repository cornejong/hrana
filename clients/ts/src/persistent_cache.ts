import type { SqlParams } from "./value.js"
import { makeCacheKey } from "./cache_key.js"

const DB_PREFIX = "hrana_cache"
const LS_LAST_PURGE_KEY = "hrana_cache:last_purge"
const STORE_NAME = "entries"
const DB_VERSION = 1
const DEFAULT_TTL = 30 * 24 * 60 * 60 * 1000   // 30 days
const DEFAULT_GROUP = "default"
const DEFAULT_MIN_INTERVAL = 24 * 60 * 60 * 1000 // 24 hours

// ─── Public types ─────────────────────────────────────────────────────────────

export interface PersistentCacheOptions {
    /** IDB database name. Default: `"hrana_cache"`. */
    dbName?: string
    /** Default TTL in ms for stored entries. Default: 30 days. */
    defaultTtl?: number
    /** Default group assigned to entries that do not specify one. Default: `"default"`. */
    defaultGroup?: string
}

export type PurgeAllResult =
    | { skipped: true }
    | { skipped: false; stores: number; purged: number }

export interface InspectResult {
    totalEntries: number
    groups: Record<string, number>
    oldestEntry: Date | null
    newestEntry: Date | null
    /** Best-effort byte estimate based on JSON serialisation length. */
    estimatedBytes: number
}

// ─── Internal types ───────────────────────────────────────────────────────────

interface IDBEntry {
    key: string
    group: string
    expiresAt: number
    createdAt: number
    result: unknown
}

interface FallbackEntry {
    result: unknown
    expiresAt: number
    group: string
}

// ─── PersistentCache ──────────────────────────────────────────────────────────

/**
 * IndexedDB-backed persistent query result cache.
 *
 * If IndexedDB is unavailable (SSR, restricted privacy mode) a `console.warn`
 * is emitted and the instance falls back to an in-memory store so the API
 * surface remains identical.
 *
 * @example
 * ```ts
 * const store = await PersistentCache.open({ defaultTtl: 7 * 24 * 60 * 60 * 1000 })
 * const db = new HranaClient({ url: "http://localhost:8080", persistentCache: store })
 *
 * // On app startup — throttled to once per day by default:
 * PersistentCache.purgeAllExpired()
 * ```
 */
export class PersistentCache {
    readonly #db: IDBDatabase | null
    readonly #fallback: Map<string, FallbackEntry>
    readonly #defaultTtl: number
    readonly #defaultGroup: string

    private constructor(db: IDBDatabase | null, defaultTtl: number, defaultGroup: string) {
        this.#db = db
        this.#fallback = new Map()
        this.#defaultTtl = defaultTtl
        this.#defaultGroup = defaultGroup
    }

    /**
     * Open (or create) the persistent cache backed by IndexedDB.
     * Falls back to in-memory with a console warning if IDB is unavailable.
     */
    static async open(options?: PersistentCacheOptions): Promise<PersistentCache> {
        const dbName = options?.dbName ?? DB_PREFIX
        const defaultTtl = options?.defaultTtl ?? DEFAULT_TTL
        const defaultGroup = options?.defaultGroup ?? DEFAULT_GROUP

        if (typeof indexedDB === "undefined") {
            console.warn("[hrana] IndexedDB is not available; PersistentCache falling back to in-memory store.")
            return new PersistentCache(null, defaultTtl, defaultGroup)
        }

        try {
            const db = await openIDB(dbName)
            return new PersistentCache(db, defaultTtl, defaultGroup)
        } catch (err) {
            console.warn("[hrana] Failed to open IndexedDB; PersistentCache falling back to in-memory store.", err)
            return new PersistentCache(null, defaultTtl, defaultGroup)
        }
    }

    // ─── Internal read / write ─────────────────────────────────────────────────

    /** @internal */
    async get<T>(key: string): Promise<T | undefined> {
        if (!this.#db) return this.#fallbackGet<T>(key)

        const entry = await idbGet<IDBEntry>(this.#db, STORE_NAME, key)
        if (!entry) return undefined
        if (Date.now() > entry.expiresAt) {
            await idbDelete(this.#db, STORE_NAME, key)
            return undefined
        }
        return entry.result as T
    }

    /** @internal */
    async set<T>(key: string, result: T, ttl?: number, group?: string): Promise<void> {
        const resolvedTtl = ttl ?? this.#defaultTtl
        const resolvedGroup = group ?? this.#defaultGroup

        if (!this.#db) {
            this.#fallback.set(key, { result, expiresAt: Date.now() + resolvedTtl, group: resolvedGroup })
            return
        }

        const entry: IDBEntry = {
            key,
            group: resolvedGroup,
            expiresAt: Date.now() + resolvedTtl,
            createdAt: Date.now(),
            result,
        }
        await idbPut(this.#db, STORE_NAME, entry)
    }

    // ─── Public invalidation API ───────────────────────────────────────────────

    /**
     * Remove the cache entry for a specific SQL statement and parameters.
     *
     * Note: paginated entries include `page` and `pageSize` in the key.
     * Use `invalidateGroup()` or `clear()` to flush all paginated entries at once.
     */
    async invalidate(sql: string, params?: SqlParams): Promise<void> {
        const key = makeCacheKey(sql, params)
        if (!this.#db) {
            this.#fallback.delete(key)
            return
        }
        await idbDelete(this.#db, STORE_NAME, key)
    }

    /** Remove all entries belonging to the named cache group. */
    async invalidateGroup(group: string): Promise<void> {
        if (!this.#db) {
            for (const [key, entry] of this.#fallback) {
                if (entry.group === group) this.#fallback.delete(key)
            }
            return
        }
        await idbDeleteByGroup(this.#db, STORE_NAME, group)
    }

    /** Remove all expired entries. Returns the number of entries deleted. */
    async purgeExpired(): Promise<number> {
        if (!this.#db) {
            let count = 0
            const now = Date.now()
            for (const [key, entry] of this.#fallback) {
                if (now > entry.expiresAt) {
                    this.#fallback.delete(key)
                    count++
                }
            }
            return count
        }
        return idbDeleteExpired(this.#db, STORE_NAME)
    }

    /** Remove all entries from this store. */
    async clear(): Promise<void> {
        if (!this.#db) {
            this.#fallback.clear()
            return
        }
        await idbClear(this.#db, STORE_NAME)
    }

    /** Inspect the current state of the cache. */
    async inspect(): Promise<InspectResult> {
        if (!this.#db) return this.#fallbackInspect()
        return idbInspect(this.#db, STORE_NAME)
    }

    // ─── Static utilities ──────────────────────────────────────────────────────

    /**
     * Drop an entire IDB database by name. Useful for "clear all cache" UI buttons.
     * Has no effect if the database does not exist.
     */
    static async clearAll(dbName: string): Promise<void> {
        if (typeof indexedDB === "undefined") return
        await new Promise<void>((resolve, reject) => {
            const req = indexedDB.deleteDatabase(dbName)
            req.onsuccess = () => resolve()
            req.onerror = () => reject(req.error)
        })
    }

    /**
     * Purge expired entries across **every** IDB database whose name starts with
     * `"hrana_cache"`.
     *
     * Throttled by a timestamp in `localStorage` so repeated calls on the same
     * page load or within the `minInterval` window are no-ops.
     *
     * Call once on app startup — fire-and-forget is fine:
     * ```ts
     * PersistentCache.purgeAllExpired()
     * ```
     *
     * @param options.minInterval  Minimum ms between purge runs (default: 24 h).
     *                             Pass `0` to force a run regardless.
     */
    static async purgeAllExpired(options?: { minInterval?: number }): Promise<PurgeAllResult> {
        const minInterval = options?.minInterval ?? DEFAULT_MIN_INTERVAL

        if (minInterval > 0) {
            try {
                const raw = localStorage.getItem(LS_LAST_PURGE_KEY)
                if (raw !== null) {
                    const lastPurge = parseInt(raw, 10)
                    if (!isNaN(lastPurge) && Date.now() - lastPurge < minInterval) {
                        return { skipped: true }
                    }
                }
            } catch {
                // localStorage unavailable — proceed without throttle
            }
        }

        if (typeof indexedDB === "undefined") {
            return { skipped: false, stores: 0, purged: 0 }
        }

        let stores = 0
        let purged = 0

        try {
            const databases = await indexedDB.databases()
            const hranaDbs = databases.filter((info) => info.name?.startsWith(DB_PREFIX))

            for (const info of hranaDbs) {
                if (!info.name) continue
                try {
                    const db = await openIDB(info.name)
                    const count = await idbDeleteExpired(db, STORE_NAME)
                    db.close()
                    stores++
                    purged += count
                } catch {
                    // Skip stores that fail to open
                }
            }
        } catch {
            // indexedDB.databases() unavailable — skip enumeration
        }

        try {
            localStorage.setItem(LS_LAST_PURGE_KEY, Date.now().toString())
        } catch {
            // localStorage unavailable — ignore
        }

        return { skipped: false, stores, purged }
    }

    // ─── Fallback (in-memory) implementations ─────────────────────────────────

    #fallbackGet<T>(key: string): T | undefined {
        const entry = this.#fallback.get(key)
        if (!entry) return undefined
        if (Date.now() > entry.expiresAt) {
            this.#fallback.delete(key)
            return undefined
        }
        return entry.result as T
    }

    #fallbackInspect(): InspectResult {
        const groups: Record<string, number> = {}
        let estimatedBytes = 0

        for (const entry of this.#fallback.values()) {
            groups[entry.group] = (groups[entry.group] ?? 0) + 1
            try { estimatedBytes += JSON.stringify(entry.result, jsonReplacer).length } catch { /* ignore */ }
        }

        return {
            totalEntries: this.#fallback.size,
            groups,
            oldestEntry: null,
            newestEntry: null,
            estimatedBytes,
        }
    }
}

// ─── IDB helpers ─────────────────────────────────────────────────────────────

function openIDB(dbName: string): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(dbName, DB_VERSION)
        req.onupgradeneeded = () => {
            const db = req.result
            if (!db.objectStoreNames.contains(STORE_NAME)) {
                const store = db.createObjectStore(STORE_NAME, { keyPath: "key" })
                store.createIndex("group", "group", { unique: false })
                store.createIndex("expiresAt", "expiresAt", { unique: false })
            }
        }
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
    })
}

function idbGet<T>(db: IDBDatabase, storeName: string, key: string): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readonly")
        const req = tx.objectStore(storeName).get(key)
        req.onsuccess = () => resolve(req.result as T | undefined)
        req.onerror = () => reject(req.error)
    })
}

function idbPut(db: IDBDatabase, storeName: string, value: IDBEntry): Promise<void> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite")
        const req = tx.objectStore(storeName).put(value)
        req.onsuccess = () => resolve()
        req.onerror = () => reject(req.error)
    })
}

function idbDelete(db: IDBDatabase, storeName: string, key: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite")
        const req = tx.objectStore(storeName).delete(key)
        req.onsuccess = () => resolve()
        req.onerror = () => reject(req.error)
    })
}

function idbDeleteByGroup(db: IDBDatabase, storeName: string, group: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite")
        const index = tx.objectStore(storeName).index("group")
        const req = index.openCursor(IDBKeyRange.only(group))
        req.onsuccess = () => {
            const cursor = req.result
            if (cursor) {
                cursor.delete()
                cursor.continue()
            } else {
                resolve()
            }
        }
        req.onerror = () => reject(req.error)
    })
}

function idbDeleteExpired(db: IDBDatabase, storeName: string): Promise<number> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite")
        const index = tx.objectStore(storeName).index("expiresAt")
        const range = IDBKeyRange.upperBound(Date.now())
        const req = index.openCursor(range)
        let count = 0
        req.onsuccess = () => {
            const cursor = req.result
            if (cursor) {
                cursor.delete()
                count++
                cursor.continue()
            } else {
                resolve(count)
            }
        }
        req.onerror = () => reject(req.error)
    })
}

function idbClear(db: IDBDatabase, storeName: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite")
        const req = tx.objectStore(storeName).clear()
        req.onsuccess = () => resolve()
        req.onerror = () => reject(req.error)
    })
}

function idbInspect(db: IDBDatabase, storeName: string): Promise<InspectResult> {
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readonly")
        const req = tx.objectStore(storeName).getAll()
        req.onsuccess = () => {
            const entries = req.result as IDBEntry[]
            const groups: Record<string, number> = {}
            let oldestTs: number | null = null
            let newestTs: number | null = null
            let estimatedBytes = 0

            for (const entry of entries) {
                groups[entry.group] = (groups[entry.group] ?? 0) + 1
                if (oldestTs === null || entry.createdAt < oldestTs) oldestTs = entry.createdAt
                if (newestTs === null || entry.createdAt > newestTs) newestTs = entry.createdAt
                try { estimatedBytes += JSON.stringify(entry.result, jsonReplacer).length } catch { /* ignore */ }
            }

            resolve({
                totalEntries: entries.length,
                groups,
                oldestEntry: oldestTs !== null ? new Date(oldestTs) : null,
                newestEntry: newestTs !== null ? new Date(newestTs) : null,
                estimatedBytes,
            })
        }
        req.onerror = () => reject(req.error)
    })
}

function jsonReplacer(_key: string, value: unknown): unknown {
    if (typeof value === "bigint") return value.toString()
    if (value instanceof Uint8Array) return `<blob:${value.length}>`
    return value
}
