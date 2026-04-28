import type { WireStmt, WireStmtResult, WireServerMsg } from "./types.js"
import { HranaError } from "./client"

type PendingReq = {
    resolve: (data: unknown) => void
    reject: (err: Error) => void
}

export class WsStream {
    readonly #ws: WebSocket
    readonly #authToken: string | undefined
    readonly #streamId = 0

    #reqIdSeq = 0
    #pending = new Map<number, PendingReq>()
    #ready: Promise<void>

    constructor(url: string, version: "v1" | "v2" | "v3", authToken?: string) {
        const subprotocol = `hrana${version[1]}`
        this.#authToken = authToken
        this.#ws = new WebSocket(url, [subprotocol])
        this.#ws.onmessage = (ev) => this.#onMessage(ev)
        this.#ws.onerror = () => this.#rejectAll(new HranaError("WebSocket error"))
        this.#ws.onclose = () => this.#rejectAll(new HranaError("WebSocket closed"))

        this.#ready = this.#handshake()
    }

    async execute(stmt: WireStmt): Promise<WireStmtResult> {
        await this.#ready

        const resp = await this.#sendRequest({ type: "execute", stream_id: this.#streamId, stmt })
        const r = resp as { type: string; result: WireStmtResult }
        return r.result
    }

    async close(): Promise<void> {
        await this.#ready.catch(() => undefined)
        await this.#sendRequest({ type: "close_stream", stream_id: this.#streamId }).catch(() => undefined)
        this.#ws.close(1000, "done")
    }

    // ─── Internal ──────────────────────────────────────────────────────────────

    async #handshake(): Promise<void> {
        // Wait for the socket to open.
        await new Promise<void>((resolve, reject) => {
            if (this.#ws.readyState === WebSocket.OPEN) return resolve()
            this.#ws.addEventListener("open", () => resolve(), { once: true })
            this.#ws.addEventListener("error", () => reject(new HranaError("WebSocket failed to connect")), { once: true })
        })

        // Send hello.
        const helloMsg: { type: string; jwt?: string } = { type: "hello" }
        if (this.#authToken) helloMsg.jwt = this.#authToken
        this.#ws.send(JSON.stringify(helloMsg))

        // Wait for hello_ok / hello_error via a dedicated first-message handler.
        await new Promise<void>((resolve, reject) => {
            const onMsg = (ev: MessageEvent) => {
                const msg = JSON.parse(ev.data as string) as WireServerMsg
                if (msg.type === "hello_ok") {
                    resolve()
                } else if (msg.type === "hello_error") {
                    reject(new HranaError(`auth rejected: ${msg.error.message}`))
                }
                // Any other message type before hello_ok is ignored; the loop handles them.
                this.#ws.removeEventListener("message", onMsg)
            }
            this.#ws.addEventListener("message", onMsg)
        })

        // Open stream 0.
        await this.#sendRequest({ type: "open_stream", stream_id: this.#streamId })
    }

    #sendRequest(payload: unknown): Promise<unknown> {
        const id = ++this.#reqIdSeq

        return new Promise<unknown>((resolve, reject) => {
            this.#pending.set(id, { resolve, reject })

            const msg = { type: "request", request_id: id, request: payload }
            try {
                this.#ws.send(JSON.stringify(msg))
            } catch (err) {
                this.#pending.delete(id)
                reject(err instanceof Error ? err : new HranaError(String(err)))
            }
        })
    }

    #onMessage(ev: MessageEvent): void {
        let msg: WireServerMsg
        try {
            msg = JSON.parse(ev.data as string) as WireServerMsg
        } catch {
            return
        }

        if (msg.type === "response_ok") {
            const pending = this.#pending.get(msg.request_id)
            if (pending) {
                this.#pending.delete(msg.request_id)
                pending.resolve(msg.response)
            }
        } else if (msg.type === "response_error") {
            const pending = this.#pending.get(msg.request_id)
            if (pending) {
                this.#pending.delete(msg.request_id)
                pending.reject(new HranaError(msg.error.message))
            }
        }
    }

    #rejectAll(err: Error): void {
        for (const pending of this.#pending.values()) {
            pending.reject(err)
        }
        this.#pending.clear()
    }
}
