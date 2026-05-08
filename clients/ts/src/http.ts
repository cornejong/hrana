import type { WirePipelineReq, WirePipelineResp, WireStmt, WireStmtResult } from "./types.js"
import { HranaError } from "./client"

export class HttpStream {
    readonly #baseUrl: string
    readonly #version: "v2" | "v3"

    #authToken: string | undefined
    #baton: string | null = null

    constructor(baseUrl: string, version: "v2" | "v3", authToken?: string) {
        this.#baseUrl = baseUrl.replace(/\/$/, "")
        this.#version = version
        this.#authToken = authToken
    }

    authToken(token: string | undefined) {
        this.#authToken = token
    }

    async execute(stmt: WireStmt): Promise<WireStmtResult> {
        const resp = await this.#pipeline([{ type: "execute", stmt }])
        const r = resp.results[0]

        if (r.type === "error") {
            throw new HranaError(r.error?.message ?? "unknown pipeline error")
        }
        if (!r.response?.result) {
            throw new HranaError("missing result in pipeline response")
        }

        return r.response.result
    }

    async close(): Promise<void> {
        if (this.#baton === null) return
        await this.#pipeline([{ type: "close" }]).catch(() => undefined)
        this.#baton = null
    }

    async #pipeline(requests: WirePipelineReq["requests"]): Promise<WirePipelineResp> {
        const body: WirePipelineReq = {
            baton: this.#baton,
            requests,
        }

        const endpoint = `${this.#baseUrl}/${this.#version}/pipeline`
        const headers: HeadersInit = { "Content-Type": "application/json", "Accept": "application/json" }
        if (this.#authToken) {
            headers["Authorization"] = `Bearer ${this.#authToken}`
        }

        const res = await fetch(endpoint, {
            method: "POST",
            headers,
            body: JSON.stringify(body),
        })

        if (!res.ok) {
            const msg = await res.text().catch(() => res.statusText)
            throw new HranaError(`server returned ${res.status}: ${msg}`)
        }

        const data: WirePipelineResp = await res.json()

        // Rotate baton and follow base_url if the server provides one.
        this.#baton = data.baton
        if (data.base_url) {
            // base_url replaces the origin for the next request.
            const base = data.base_url.replace(/\/$/, "")
            Object.defineProperty(this, "_baseUrl", { value: base })
        }

        return data
    }
}
