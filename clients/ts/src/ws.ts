import type { WireStmt, WireStmtResult, WireServerMsg } from "./types.js"
import { HranaError } from "./client"

type PendingReq = {
    resolve: (data: unknown) => void
    reject: (err: Error) => void
}

export class WsStream {
    readonly #ws: WebSocket
    readonly #streamId = 0

    #authToken: string | undefined
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

    async authToken(token: string | undefined) {
        this.#authToken = token
        // TODO: Send another Hello message to start using the new auth token for the existing connection

        if (this.#ws.readyState !== 1) {
            // We're not open 
            return
        }

        // Send hello.
        const helloMsg: { type: string; jwt?: string } = { type: "hello" }
        if (this.#authToken) helloMsg.jwt = this.#authToken
        this.#ws.send(JSON.stringify(helloMsg))

        // // Wait for hello_ok / hello_error via a dedicated first-message handler.
        // await new Promise<void>((resolve, reject) => {
        //     const onMsg = (ev: MessageEvent) => {
        //         const msg = JSON.parse(ev.data as string) as WireServerMsg
        //         if (msg.type === "hello_ok") {
        //             resolve()
        //         } else if (msg.type === "hello_error") {
        //             reject(new HranaError(`auth rejected: ${msg.error.message}`))
        //         }
        //         // Any other message type before hello_ok is ignored; the loop handles them.
        //         this.#ws.removeEventListener("message", onMsg)
        //     }
        //     this.#ws.addEventListener("message", onMsg)
        // })
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
        } catch (e) {
            console.error(e)
            return
        }

        /* @ts-ignore-error The request_id field only exists on response type messages */
        const pending = this.#pending.get(msg.request_id ?? -1)
        switch (msg.type) {
            case "hello_ok":

                break;

            case "hello_error":
                break;

            case "response_ok":
                if (!pending) return

                this.#pending.delete(msg.request_id)
                pending.resolve(msg.response)
                break;

            case "response_error":
                // let pending = this.#pending.get(msg.request_id)
                if (!pending) return

                this.#pending.delete(msg.request_id)
                pending.reject(new HranaError(msg.error.message))
                break;

            default:
                /* @ts-expect-error since we currently cover all cases of teh msg.type value TS marks this branch as Never and complains about type not existing on never */
                console.warn("unknown message type", msg.type, msg)
                break;
        }


    }

    #rejectAll(err: Error): void {
        for (const pending of this.#pending.values()) {
            pending.reject(err)
        }
        this.#pending.clear()
    }
}
