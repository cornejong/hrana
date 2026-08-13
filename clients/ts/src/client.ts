import { buildWireStmt, decodeValue } from "./value.js"
import { HttpStream } from "./http.js"
import { WsStream } from "./ws.js"
import type { WsCodec } from "./ws.js"
import type { WireStmtResult } from "./types.js"
import type { SqlParams, RowValue } from "./value.js"
import { makeCacheKey } from "./cache_key.js"
import type { PersistentCache } from "./persistent_cache.js"

// ─── Public error type ────────────────────────────────────────────────────────

export class HranaError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "HranaError"
    }
}

// ─── Public result types ──────────────────────────────────────────────────────

export interface ResultSet {
    /** Column names in declaration order. */
    columns: string[]
    /** Rows as plain objects keyed by column name. Values are decoded to JS types. */
    rows: Record<string, RowValue>[]
    /** Number of rows changed by a mutating statement. */
    rowsAffected: number
    /** Last inserted row ID, or null. bigint to preserve int64 range. */
    lastInsertRowid: bigint | null
    /** True when this result was served from the in-memory or persistent cache. */
    fromCache?: boolean
}

// ─── Pagination ──────────────────────────────────────────────────────────────

export interface PaginateOptions {
    /** Current page number, 1-based. Defaults to 1. */
    page?: number
    /** Number of rows per page. Defaults to 20. */
    pageSize?: number
    /** Cache this page result. `true` uses the client default TTL; an object sets an explicit TTL (ms). */
    cache?: CachePolicy
}

export interface PaginatedResult {
    /** Column names in declaration order. */
    columns: string[]
    /** Rows for the current page as plain objects keyed by column name. */
    rows: Record<string, RowValue>[]
    /** Current page number (1-based). */
    page: number
    /** Max rows per page. */
    pageSize: number
    /** Total number of matching rows across all pages. */
    totalRows: number
    /** Total number of pages. */
    totalPages: number
    /** Whether a next page exists. */
    hasNextPage: boolean
    /** Whether a previous page exists. */
    hasPrevPage: boolean
    /** True when this result was served from the in-memory or persistent cache. */
    fromCache?: boolean
}

// ─── Cache ───────────────────────────────────────────────────────────────────

export interface CacheConfig {
    /** Default TTL in ms for cached entries. Default: 60_000 (1 min). */
    ttl?: number
    /** Maximum cache entries; oldest is evicted when exceeded. Default: 100. */
    maxSize?: number
    /**
     * Automatically clear the entire cache after every run() call.
     * Useful for single-table apps where any write invalidates all reads.
     * Default: false.
     */
    clearOnWrite?: boolean
}

/**
 * Per-call cache control passed via `options.cache`.
 * - `true`                       → memory cache using the client-level TTL default.
 * - `{ ttl: number }`            → memory cache with an explicit TTL (ms).
 * - `{ persist: true }`          → persistent (IDB) cache with the store's default TTL.
 * - `{ persist: true, ttl, group }` → persistent cache, explicit TTL and/or group.
 * - `false` / omitted            → never cache.
 */
export type CachePolicy = boolean | { ttl?: number; persist?: boolean; group?: string }

interface CacheEntry<T> {
    result: T
    expiresAt: number
}

/** In-memory TTL cache exposed on `HranaClient.cache`. */
export class QueryCache {
    readonly #ttl: number
    readonly #maxSize: number
    readonly #store: Map<string, CacheEntry<unknown>>

    /** @internal */
    constructor(ttl: number, maxSize: number) {
        this.#ttl = ttl
        this.#maxSize = maxSize
        this.#store = new Map()
    }

    /** Number of live entries currently held. */
    get size(): number {
        return this.#store.size
    }

    /** @internal */
    get<T>(key: string): T | undefined {
        const entry = this.#store.get(key)
        if (!entry) return undefined
        if (Date.now() > entry.expiresAt) {
            this.#store.delete(key)
            return undefined
        }
        return entry.result as T
    }

    /** @internal */
    set<T>(key: string, result: T, ttl?: number): void {
        if (this.#store.size >= this.#maxSize) {
            // Evict the oldest (first-inserted) entry.
            for (const key of this.#store.keys()) {
                this.#store.delete(key)
                break
            }
        }
        this.#store.set(key, { result, expiresAt: Date.now() + (ttl ?? this.#ttl) })
    }

    /**
     * Remove the cache entry for a specific SQL statement and parameters.
     * Note: for paginated results the key also encodes `page` and `pageSize`;
     * use `cache.clear()` to flush all paginated entries at once.
     */
    invalidate(sql: string, params?: SqlParams): void {
        this.#store.delete(makeCacheKey(sql, params))
    }

    /** Remove all cache entries. */
    clear(): void {
        this.#store.clear()
    }
}

// ─── Config ───────────────────────────────────────────────────────────────────

export interface HranaConfig {
    /**
     * The server URL. Scheme determines transport and is required:
     *   http / https → HTTP pipeline (version: v2 or v3, default v3)
     *   ws  / wss   → WebSocket     (version: v1, v2, or v3, default v3)
     */
    url: string
    /** Auth token sent as `Authorization: Bearer <token>` (HTTP) or `jwt` (WS). */
    authToken?: string
    /**
     * Protocol version to use. Must be compatible with the chosen transport.
     * Defaults to "v3".
     */
    version?: "v1" | "v2" | "v3"
    /** Client-side query result cache settings. Omit to disable caching by default. */
    cache?: CacheConfig
    /**
     * Persistent (IndexedDB-backed) query result cache.
     * Create with `await PersistentCache.open(...)` and pass the instance here.
     * Required when using `cache: { persist: true }` on individual queries.
     */
    persistentCache?: PersistentCache
    /**
     * Wire encoding for WebSocket connections.
     * - `"json"` (default) — text frames, human-readable.
     * - `"msgpack"` — binary frames, more compact. Requires the server to
     *   support the `hrana{N}-bin` subprotocol.
     */
    codec?: WsCodec
}

interface ResolvedConfig {
    url: string
    authToken: string | undefined
    version: "v1" | "v2" | "v3"
    codec: WsCodec
    clearOnWrite: boolean
    persistentCache: PersistentCache | undefined
}

// ─── Stream (internal transport handle) ──────────────────────────────────────

type Transport = HttpStream | WsStream

// ─── Client ───────────────────────────────────────────────────────────────────

/**
 * HranaClient provides a high-level async query API over the Hrana protocol.
 *
 * @example
 * ```ts
 * const db = new HranaClient({ url: "http://localhost:8080", authToken: "secret" })
 * const result = await db.execute("SELECT * FROM users WHERE id = ?", [1n])
 * await db.close()
 * ```
 */
export class HranaClient {
    readonly #cfg: ResolvedConfig
    #transport: Transport | null = null

    /** In-memory query result cache. Use `cache.clear()` or `cache.invalidate()` to manage entries. */
    readonly cache: QueryCache
    #warnedPersist = false

    constructor(cfg: HranaConfig) {
        const version = cfg.version ?? "v3"
        this.#cfg = {
            url: cfg.url,
            authToken: cfg.authToken,
            version,
            codec: cfg.codec ?? "json",
            clearOnWrite: cfg.cache?.clearOnWrite ?? false,
            persistentCache: cfg.persistentCache,
        }
        this.cache = new QueryCache(
            cfg.cache?.ttl ?? 60_000,
            cfg.cache?.maxSize ?? 100,
        )
        this.#validateConfig()
    }

    authToken(token: string) {
        this.#cfg.authToken = token
        this.#getTransport().authToken(token)
    }

    /**
     * Execute a SQL statement and return a decoded ResultSet.
     * Parameters can be positional (array) or named (object).
     * Pass `options.cache` to serve the result from the in-memory or persistent cache.
     */
    async execute(sql: string, params?: SqlParams, options?: { cache?: CachePolicy }): Promise<ResultSet> {
        const policy = options?.cache
        if (policy === undefined || policy === false) {
            return this.#executeRaw(sql, params)
        }

        const isPersist = typeof policy === "object" && policy.persist === true
        const ttl = typeof policy === "object" ? policy.ttl : undefined
        const group = typeof policy === "object" ? policy.group : undefined
        const key = makeCacheKey(sql, params)

        if (isPersist) {
            const pc = this.#cfg.persistentCache
            if (!pc) {
                if (!this.#warnedPersist) {
                    console.warn("[hrana] cache.persist requested but no persistentCache configured on HranaClient. Falling back to memory cache.")
                    this.#warnedPersist = true
                }
            } else {
                // 1. Memory (L1)
                const memHit = this.cache.get<ResultSet>(key)
                if (memHit) return { ...memHit, fromCache: true }
                // 2. IDB (L2)
                const idbHit = await pc.get<ResultSet>(key)
                if (idbHit) {
                    this.cache.set(key, idbHit, ttl)  // warm L1
                    return { ...idbHit, fromCache: true }
                }
                // 3. Network — write to both levels
                const result = await this.#executeRaw(sql, params)
                await pc.set(key, result, ttl, group)
                this.cache.set(key, result, ttl)
                return result
            }
        }

        // Memory-only path
        const cached = this.cache.get<ResultSet>(key)
        if (cached) return { ...cached, fromCache: true }
        const result = await this.#executeRaw(sql, params)
        this.cache.set(key, result, ttl)
        return result
    }

    /**
     * Execute a SQL statement that does not return rows (INSERT/UPDATE/DELETE/DDL).
     * If the client was configured with `cache.clearOnWrite: true`, the entire
     * cache is flushed automatically after each call.
     */
    async run(sql: string, params?: SqlParams): Promise<{ rowsAffected: number; lastInsertRowid: bigint | null }> {
        const stmt = buildWireStmt(sql, params, false)
        const result = await this.#getTransport().execute(stmt)
        if (this.#cfg.clearOnWrite) this.cache.clear()
        return {
            rowsAffected: result.affected_row_count,
            lastInsertRowid: result.last_insert_rowid !== null ? BigInt(result.last_insert_rowid) : null,
        }
    }

    /**
     * Execute a paginated query and return a PaginatedResult with rows for the
     * requested page plus metadata useful for building page controls.
     *
     * Two queries are issued in parallel: a COUNT(*) wrap to determine the total
     * row count, and the main query scoped to the requested page via LIMIT/OFFSET.
     *
     * @example
     * ```ts
     * const result = await db.paginate("SELECT * FROM users ORDER BY id", [], { page: 2, pageSize: 10 })
     * console.log(result.totalPages, result.hasNextPage)
     * ```
     */
    async paginate(sql: string, params?: SqlParams, options?: PaginateOptions): Promise<PaginatedResult> {
        const cleanSql = sql.trim().replace(/;+$/, "")
        const page = Math.max(1, options?.page ?? 1)
        const pageSize = Math.max(1, options?.pageSize ?? 20)
        const policy = options?.cache

        if (policy === undefined || policy === false) {
            return this.#paginateRaw(cleanSql, params, page, pageSize)
        }

        const isPersist = typeof policy === "object" && policy.persist === true
        const ttl = typeof policy === "object" ? policy.ttl : undefined
        const group = typeof policy === "object" ? policy.group : undefined
        // Key encodes the base query + params + page + pageSize so each page is a distinct entry.
        const key = makeCacheKey(`${cleanSql}\0p:${page}\0s:${pageSize}`, params)

        if (isPersist) {
            const pc = this.#cfg.persistentCache
            if (!pc) {
                if (!this.#warnedPersist) {
                    console.warn("[hrana] cache.persist requested but no persistentCache configured on HranaClient. Falling back to memory cache.")
                    this.#warnedPersist = true
                }
            } else {
                // 1. Memory (L1)
                const memHit = this.cache.get<PaginatedResult>(key)
                if (memHit) return { ...memHit, fromCache: true }
                // 2. IDB (L2)
                const idbHit = await pc.get<PaginatedResult>(key)
                if (idbHit) {
                    this.cache.set(key, idbHit, ttl)
                    return { ...idbHit, fromCache: true }
                }
                // 3. Network — write to both levels
                const result = await this.#paginateRaw(cleanSql, params, page, pageSize)
                await pc.set(key, result, ttl, group)
                this.cache.set(key, result, ttl)
                return result
            }
        }

        // Memory-only path
        const cached = this.cache.get<PaginatedResult>(key)
        if (cached) return { ...cached, fromCache: true }
        const result = await this.#paginateRaw(cleanSql, params, page, pageSize)
        this.cache.set(key, result, ttl)
        return result
    }

    /**
     * Execute multiple statements in a single round-trip.
     * Returns a ResultSet per statement. Useful for bulk reads.
     */
    async batch(statements: Array<{ sql: string; params?: SqlParams }>): Promise<ResultSet[]> {
        // Each statement goes through the transport individually for now; a future
        // optimisation can pack them into a single pipeline batch request.
        return Promise.all(statements.map(({ sql, params }) => this.execute(sql, params)))
    }

    /** Release the underlying connection / WebSocket. */
    async close(): Promise<void> {
        if (this.#transport) {
            await this.#transport.close()
            this.#transport = null
        }
    }

    // ─── Internal ──────────────────────────────────────────────────────────────

    async #executeRaw(sql: string, params: SqlParams | undefined): Promise<ResultSet> {
        const stmt = buildWireStmt(sql, params, true)
        const result = await this.#getTransport().execute(stmt)
        return decodeResult(result)
    }

    async #paginateRaw(
        cleanSql: string,
        params: SqlParams | undefined,
        page: number,
        pageSize: number,
    ): Promise<PaginatedResult> {
        const offset = (page - 1) * pageSize
        const countSql = `SELECT COUNT(*) AS _count FROM (${cleanSql})`
        const [pagedSql, pagedParams] = buildPagedParams(cleanSql, params, pageSize, offset)

        const [countResult, dataResult] = await Promise.all([
            this.#executeRaw(countSql, params),
            this.#executeRaw(pagedSql, pagedParams),
        ])

        const totalRows = Number(countResult.rows[0]?.["_count"] ?? 0)
        const totalPages = Math.max(1, Math.ceil(totalRows / pageSize))

        return {
            columns: dataResult.columns,
            rows: dataResult.rows,
            page,
            pageSize,
            totalRows,
            totalPages,
            hasNextPage: page < totalPages,
            hasPrevPage: page > 1,
        }
    }

    #getTransport(): Transport {
        if (!this.#transport) {
            this.#transport = this.#createTransport()
        }
        return this.#transport
    }

    #createTransport(): Transport {
        const { url, version, authToken } = this.#cfg
        const scheme = new URL(url).protocol.replace(":", "")

        if (scheme === "http" || scheme === "https") {
            if (version !== "v2" && version !== "v3") {
                throw new HranaError(`HTTP transport only supports v2 or v3, got "${version}"`)
            }
            return new HttpStream(url, version, authToken)
        }

        if (scheme === "ws" || scheme === "wss") {
            return new WsStream(url, version, authToken, this.#cfg.codec)
        }

        throw new HranaError(`unsupported scheme "${scheme}"`)
    }

    #validateConfig(): void {
        const url = this.#cfg.url
        try {
            new URL(url)
        } catch {
            throw new HranaError(`invalid URL: "${url}"`)
        }

        const { version } = this.#cfg
        if (version !== "v1" && version !== "v2" && version !== "v3") {
            throw new HranaError(`unsupported version "${version}"`)
        }
    }
}

// ─── Pagination helpers ─────────────────────────────────────────────────────

/**
 * Builds the paged SQL and merged params for a LIMIT/OFFSET query.
 * Named params use reserved keys `:_hrana_limit` and `:_hrana_offset`;
 * positional params append limit/offset to the existing array.
 */
function buildPagedParams(
    sql: string,
    params: SqlParams | undefined,
    limit: number,
    offset: number,
): [string, SqlParams] {
    if (!params || Array.isArray(params)) {
        return [
            `SELECT * FROM (${sql}) LIMIT ? OFFSET ?`,
            [...(params ?? []), limit, offset],
        ]
    }
    return [
        `SELECT * FROM (${sql}) LIMIT :_hrana_limit OFFSET :_hrana_offset`,
        { ...params, _hrana_limit: limit, _hrana_offset: offset },
    ]
}

// ─── Result decoding ──────────────────────────────────────────────────────────

function decodeResult(raw: WireStmtResult): ResultSet {
    const columns = raw.cols.map((c) => c.name ?? "")
    const rows = raw.rows.map((row) => {
        const obj: Record<string, RowValue> = {}
        for (let i = 0; i < columns.length; i++) {
            obj[columns[i]] = decodeValue(row[i])
        }
        return obj
    })

    return {
        columns,
        rows,
        rowsAffected: raw.affected_row_count,
        lastInsertRowid: raw.last_insert_rowid !== null ? BigInt(raw.last_insert_rowid) : null,
    }
}
