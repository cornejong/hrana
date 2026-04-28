# Getting Started

## Installation

```bash
npm install @cornejong/hrana
```

## Quick start

```ts
import { HranaClient } from "@cornejong/hrana"

const db = new HranaClient({
    url: "http://localhost:8080",
    authToken: "your-auth-token",   // optional
})

const result = await db.execute("SELECT sqlite_version()")
console.log(result.rows[0]["sqlite_version()"])

await db.close()
```

## Choosing a transport

The URL scheme determines which transport is used. No extra configuration is required.

| Scheme           | Transport     | Supported versions              |
| ---------------- | ------------- | ------------------------------- |
| `http` / `https` | HTTP pipeline | `v2`, `v3` (default `v3`)       |
| `ws` / `wss`     | WebSocket     | `v1`, `v2`, `v3` (default `v3`) |

```ts
// HTTP
const db = new HranaClient({ url: "https://db.example.com" })

// WebSocket
const db = new HranaClient({ url: "wss://db.example.com" })

// Explicit version
const db = new HranaClient({ url: "https://db.example.com", version: "v2" })
```

## Connection lifecycle

`HranaClient` is designed to be **long-lived**. Create one instance for the
lifetime of your app (or component) and reuse it. WebSocket connections are
kept open between queries.

```ts
// app.ts — create once
export const db = new HranaClient({ url: "http://localhost:8080" })

// Somewhere in your cleanup / teardown
await db.close()
```

Call `db.close()` when you no longer need the client. It is safe to call even
if no queries have been made.

## Using the browser bundle

If you are not bundling your own code, reference the pre-built IIFE bundle:

```html
<script src="dist/hrana.bundle.js"></script>
<script>
    const db = new Hrana.HranaClient({ url: "http://localhost:8080" })
    db.execute("SELECT 1").then(r => console.log(r.rows))
</script>
```

All public exports are available on the global `Hrana` object.
