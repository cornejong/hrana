import type { WireValue, WireStmt } from "./types.js"

/**
 * The value types accepted as SQL parameters.
 * bigint maps to the Hrana integer wire type (int64-safe).
 */
export type SqlValue = null | boolean | number | bigint | string | ArrayBuffer | Uint8Array

/**
 * A decoded row value returned to the caller.
 * Integers come back as bigint to preserve full int64 range.
 */
export type RowValue = null | bigint | number | string | Uint8Array

// ─── Encode: JS → wire ───────────────────────────────────────────────────────

export function encodeValue(v: SqlValue): WireValue {
    if (v === null || v === undefined) {
        return { type: "null" }
    }
    if (typeof v === "boolean") {
        return { type: "integer", value: v ? "1" : "0" }
    }
    if (typeof v === "bigint") {
        return { type: "integer", value: v.toString(10) }
    }
    if (typeof v === "number") {
        if (Number.isInteger(v) && v >= Number.MIN_SAFE_INTEGER && v <= Number.MAX_SAFE_INTEGER) {
            return { type: "integer", value: v.toString(10) }
        }
        return { type: "float", value: v }
    }
    if (typeof v === "string") {
        return { type: "text", value: v }
    }
    // ArrayBuffer or Uint8Array
    const bytes = v instanceof Uint8Array ? v : new Uint8Array(v)
    return { type: "blob", base64: bytesToBase64(bytes) }
}

// ─── Decode: wire → JS ───────────────────────────────────────────────────────

export function decodeValue(v: WireValue): RowValue {
    switch (v.type) {
        case "null":
            return null
        case "integer":
            return BigInt(v.value)
        case "float":
            return v.value
        case "text":
            return v.value
        case "blob":
            return base64ToBytes(v.base64)
    }
}

// ─── Statement builder ───────────────────────────────────────────────────────

export type SqlParams =
    | SqlValue[]
    | Record<string, SqlValue>

export function buildWireStmt(sql: string, params: SqlParams | undefined, wantRows: boolean): WireStmt {
    if (!params || (Array.isArray(params) && params.length === 0)) {
        return { sql, want_rows: wantRows }
    }

    if (Array.isArray(params)) {
        return { sql, args: params.map(encodeValue), want_rows: wantRows }
    }

    const named_args = Object.entries(params).map(([name, val]) => ({
        name,
        value: encodeValue(val),
    }))
    return { sql, named_args, want_rows: wantRows }
}

// ─── Base64 helpers (browser-safe) ───────────────────────────────────────────

function bytesToBase64(bytes: Uint8Array): string {
    let binary = ""
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i])
    }
    return btoa(binary)
}

function base64ToBytes(b64: string): Uint8Array {
    const binary = atob(b64)
    const bytes = new Uint8Array(binary.length)
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i)
    }
    return bytes
}
