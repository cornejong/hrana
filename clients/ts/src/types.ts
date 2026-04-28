// ─── Wire value types ─────────────────────────────────────────────────────────

export type WireValue =
    | { type: "null" }
    | { type: "integer"; value: string }        // int64 as decimal string
    | { type: "float"; value: number }
    | { type: "text"; value: string }
    | { type: "blob"; base64: string }

// ─── Statement ───────────────────────────────────────────────────────────────

export interface WireNamedArg {
    name: string
    value: WireValue
}

export interface WireStmt {
    sql: string
    args?: WireValue[]
    named_args?: WireNamedArg[]
    want_rows: boolean
}

// ─── Column ──────────────────────────────────────────────────────────────────

export interface WireCol {
    name: string | null
    decltype?: string | null
}

// ─── Statement result ────────────────────────────────────────────────────────

export interface WireStmtResult {
    cols: WireCol[]
    rows: WireValue[][]
    affected_row_count: number
    last_insert_rowid: string | null
}

// ─── HTTP pipeline ───────────────────────────────────────────────────────────

export interface WireStreamRequest {
    type: "execute" | "close"
    stmt?: WireStmt
}

export interface WirePipelineReq {
    baton?: string | null
    requests: WireStreamRequest[]
}

export interface WireStreamResponse {
    type: string
    result?: WireStmtResult
}

export interface WireStreamResult {
    type: "ok" | "error"
    response?: WireStreamResponse
    error?: { message: string; code?: string }
}

export interface WirePipelineResp {
    baton: string | null
    base_url: string | null
    results: WireStreamResult[]
}

// ─── WebSocket messages ───────────────────────────────────────────────────────

export interface WireHelloMsg {
    type: "hello"
    jwt?: string
}

export interface WireHelloOkMsg {
    type: "hello_ok"
}

export interface WireHelloErrorMsg {
    type: "hello_error"
    error: { message: string }
}

export interface WireRequestMsg {
    type: "request"
    request_id: number
    request: unknown
}

export interface WireResponseOkMsg {
    type: "response_ok"
    request_id: number
    response: unknown
}

export interface WireResponseErrorMsg {
    type: "response_error"
    request_id: number
    error: { message: string }
}

export type WireServerMsg =
    | WireHelloOkMsg
    | WireHelloErrorMsg
    | WireResponseOkMsg
    | WireResponseErrorMsg
