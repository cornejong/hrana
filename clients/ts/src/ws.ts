import { encode as msgpackEncode, decode as msgpackDecode } from "@msgpack/msgpack"
import type { WireStmt, WireStmtResult, WireServerMsg } from "./types.js"
import { HranaError } from "./client"

export type WsCodec = "json" | "msgpack"

type PendingReq = {
    payload: unknown
    resolve: (data: unknown) => void
    reject: (err: Error) => void
    retries: number
}

export class WsStream {
    #streamId = -1

    readonly #ws: WebSocket
    readonly #codec: WsCodec

    #authToken: string | undefined
    #reqIdSeq = 0
    #pending = new Map<number, PendingReq>()
    #ready: Promise<void>
    #recovering: Promise<void> | undefined
    readonly #maxStreamRetries = 3

    constructor(url: string, version: "v1" | "v2" | "v3", authToken?: string, codec: WsCodec = "json") {
        this.#codec = codec
        this.#authToken = authToken

        // Select the appropriate subprotocol: -bin variants use msgpack over binary frames.
        const versionNum = version[1]
        const subprotocol = codec === "msgpack" ? `hrana${versionNum}-bin` : `hrana${versionNum}`

        this.#ws = new WebSocket(url, [subprotocol])
        // Receive binary frames as ArrayBuffer rather than Blob so we can pass
        // them directly to the msgpack decoder without async reading.
        this.#ws.binaryType = "arraybuffer"

        this.#ws.onmessage = (ev) => this.#onMessage(ev)
        this.#ws.onerror = () => this.#rejectAll(new HranaError("WebSocket error"))
        this.#ws.onclose = () => this.#rejectAll(new HranaError("WebSocket closed"))

        this.#ready = this.#handshake()
    }

    async authToken(token: string | undefined) {
        this.#authToken = token

        if (this.#ws.readyState !== 1) {
            return
        }

        const helloMsg: { type: string; jwt?: string } = { type: "hello" }
        if (this.#authToken) helloMsg.jwt = this.#authToken
        this.#send(helloMsg)
    }

    async execute(stmt: WireStmt): Promise<WireStmtResult> {
        await this.#ready
        if (this.#recovering) {
            await this.#recovering
        }

        const resp = await this.#sendRequest({ type: "execute", stream_id: this.#streamId, stmt })
        const r = resp as { type: string; result: WireStmtResult }
        return r.result
    }

    async close(): Promise<void> {
        await this.#ready.catch(() => undefined)
        if (this.#recovering) {
            await this.#recovering.catch(() => undefined)
        }
        await this.#sendRequest({ type: "close_stream", stream_id: this.#streamId }).catch(() => undefined)
        this.#ws.close(1000, "done")
    }

    // ─── Internal ──────────────────────────────────────────────────────────────

    async #handshake(): Promise<void> {
        await new Promise<void>((resolve, reject) => {
            if (this.#ws.readyState === WebSocket.OPEN) return resolve()
            this.#ws.addEventListener("open", () => resolve(), { once: true })
            this.#ws.addEventListener("error", () => reject(new HranaError("WebSocket failed to connect")), { once: true })
        })

        const helloMsg: { type: string; jwt?: string } = { type: "hello" }
        if (this.#authToken) helloMsg.jwt = this.#authToken
        this.#send(helloMsg)

        await new Promise<void>((resolve, reject) => {
            const onMsg = (ev: MessageEvent) => {
                const msg = this.#decode(ev.data) as WireServerMsg
                if (msg.type === "hello_ok") {
                    resolve()
                } else if (msg.type === "hello_error") {
                    reject(new HranaError(`auth rejected: ${msg.error.message}`))
                }
                this.#ws.removeEventListener("message", onMsg)
            }
            this.#ws.addEventListener("message", onMsg)
        })

        if (this.#streamId < 0) {
            const resp = await this.#sendRequest({ type: "open_stream_assigned" }) as { stream_id: number }
            this.#streamId = resp.stream_id
        } else {
            await this.#sendRequest({ type: "open_stream", stream_id: this.#streamId })
        }
    }

    #sendRequest(payload: unknown): Promise<unknown> {
        return this.#sendRequestWithRetries(payload, 0)
    }

    #sendRequestWithRetries(payload: unknown, retries: number): Promise<unknown> {
        const id = ++this.#reqIdSeq

        return new Promise<unknown>((resolve, reject) => {
            this.#pending.set(id, { payload, resolve, reject, retries })

            const msg = { type: "request", request_id: id, request: payload }
            try {
                this.#send(msg)
            } catch (err) {
                this.#pending.delete(id)
                reject(err instanceof Error ? err : new HranaError(String(err)))
            }
        })
    }

    /** Encode and send a message using the session codec. */
    #send(msg: unknown): void {
        if (this.#codec === "msgpack") {
            this.#ws.send(msgpackEncode(msg))
        } else {
            this.#ws.send(JSON.stringify(msg))
        }
    }

    /** Decode an incoming WebSocket message frame using the session codec. */
    #decode(data: string | ArrayBuffer): WireServerMsg {
        if (data instanceof ArrayBuffer) {
            return msgpackDecode(new Uint8Array(data)) as WireServerMsg
        }
        return JSON.parse(data) as WireServerMsg
    }

    #onMessage(ev: MessageEvent): void {
        let msg: WireServerMsg
        try {
            msg = this.#decode(ev.data as string | ArrayBuffer)
        } catch (e) {
            console.error("hrana: failed to decode message", e)
            return
        }

        const requestId = "request_id" in msg ? msg.request_id : -1
        const pending = this.#pending.get(requestId)
        switch (msg.type) {
            case "hello_ok":
                break

            case "hello_error":
                break

            case "response_ok":
                if (!pending) return
                this.#pending.delete(msg.request_id)
                pending.resolve(msg.response)
                break

            case "response_error":
                if (!pending) return
                this.#pending.delete(msg.request_id)
                pending.reject(new HranaError(msg.error.message))
                break

            case "stream_superseded_error":
                if (!pending) return
                this.#pending.delete(msg.request_id)

                const streamId = (pending.payload as { stream_id?: number }).stream_id
                if (streamId === undefined || pending.retries >= this.#maxStreamRetries) {
                    pending.reject(new HranaError(msg.error.message))
                    break
                }

                this.#retrySuperseded(pending)
                break

            default:
                /* @ts-expect-error */
                console.warn("hrana: unknown message type", msg.type, msg)
                break
        }
    }

    async #retrySuperseded(pending: PendingReq): Promise<void> {
        try {
            await this.#recoverStream()
            const payload = { ...(pending.payload as object), stream_id: this.#streamId }
            const result = await this.#sendRequestWithRetries(payload, pending.retries + 1)
            pending.resolve(result)
        } catch (err) {
            pending.reject(err instanceof Error ? err : new HranaError(String(err)))
        }
    }

    async #recoverStream(): Promise<void> {
        if (this.#recovering) {
            return this.#recovering
        }

        this.#recovering = this.#doRecoverStream()
        try {
            await this.#recovering
        } finally {
            this.#recovering = undefined
        }
    }

    async #doRecoverStream(): Promise<void> {
        // Close the superseded stream (best effort).
        if (this.#streamId >= 0) {
            await this.#sendRequestWithRetries({ type: "close_stream", stream_id: this.#streamId }, 0)
                .catch(() => undefined)
        }

        // Open a fresh stream on the current database generation.
        const resp = await this.#sendRequestWithRetries({ type: "open_stream_assigned" }, 0) as { stream_id: number }
        this.#streamId = resp.stream_id
    }

    #rejectAll(err: Error): void {
        for (const pending of this.#pending.values()) {
            pending.reject(err)
        }
        this.#pending.clear()
    }
}

