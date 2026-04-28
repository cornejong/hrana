import type { SqlParams, SqlValue } from "./value.js"

/**
 * Builds a stable hex string cache key from a SQL statement and its parameters.
 *
 * - SQL is lowercased and whitespace-collapsed so cosmetically different but
 *   semantically identical queries share the same entry.
 * - Each parameter value is serialised with an explicit type prefix so that,
 *   for example, the integer `1n` and the string `"1"` produce different keys.
 * - Named parameter objects are sorted by key so insertion order doesn't matter.
 */
export function makeCacheKey(sql: string, params: SqlParams | undefined): string {
    const normSql = sql.toLowerCase().replace(/\s+/g, " ").trim()
    return fnv1a(normSql + "\0" + serialiseParams(params)).toString(16)
}

function serialiseParams(params: SqlParams | undefined): string {
    if (!params) return ""
    if (Array.isArray(params)) {
        return params.map(serialiseValue).join(",")
    }
    return Object.entries(params)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => `${k}=${serialiseValue(v)}`)
        .join(",")
}

function serialiseValue(v: SqlValue): string {
    if (v === null) return "null"
    if (typeof v === "bigint") return `i:${v}`
    if (typeof v === "boolean") return `b:${v ? 1 : 0}`
    if (typeof v === "number") return `n:${v}`
    if (typeof v === "string") return `s:${JSON.stringify(v)}`
    // ArrayBuffer or Uint8Array — fingerprint with length + first 16 bytes via FNV-1a.
    const bytes = v instanceof Uint8Array ? v : new Uint8Array(v)
    let sample = ""
    const sampleLen = Math.min(bytes.length, 16)
    for (let i = 0; i < sampleLen; i++) sample += String.fromCharCode(bytes[i])
    return `blob:${bytes.length}:${fnv1a(sample).toString(16)}`
}

/**
 * FNV-1a 32-bit hash — fast, non-cryptographic, suitable for cache keys.
 * https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
 */
export function fnv1a(str: string): number {
    let h = 2166136261 // FNV offset basis
    for (let i = 0; i < str.length; i++) {
        h = (h ^ str.charCodeAt(i)) * 16777619 >>> 0
    }
    return h
}
