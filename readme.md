# hrana

A Go package that serves the [Hrana protocol](https://github.com/libsql/hrana-client-ts/blob/main/HRANA_SPEC.md) (V1, V2, V3) over both HTTP and WebSockets. It wraps a standard `*sql.DB` connection pool and implements the full specification including batches, stored SQL, sequences, cursors, and JWT authentication.

## Install

```bash
go get github.com/cornejong/hrana
```

> Requires CGo for `github.com/mattn/go-sqlite3`. Ensure a C compiler is available.

## Quick start

```go
package main

import (
    "database/sql"
    "log"
    "net/http"

    "github.com/cornejong/hrana"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    db, err := sql.Open("sqlite3", "file:app.db?cache=shared&mode=rwc")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    srv := hrana.New(db, nil) // nil → default config
    defer srv.Close()

    log.Println("Hrana listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", srv))
}
```

`*Server` implements `http.Handler`, so it can be mounted directly on any HTTP mux.

---

## Configuration

```go
srv := hrana.New(db, &hrana.Config{
    // Called for every HTTP request (Authorization: Bearer <token>)
    // and for every WebSocket hello message.
    // Return a non-nil error to reject the connection.
    AuthFunc: func(token string) error {
        if token != "secret" {
            return errors.New("unauthorized")
        }
        return nil
    },

    // How long an HTTP stream (baton) lives without activity.
    // Default: 10s
    BatonTTL: 30 * time.Second,
})
```

### JWT authentication example

```go
import "github.com/golang-jwt/jwt/v5"

var jwtSecret = []byte("supersecret")

srv := hrana.New(db, &hrana.Config{
    AuthFunc: func(token string) error {
        if token == "" {
            return errors.New("missing token")
        }
        _, err := jwt.Parse(token, func(t *jwt.Token) (any, error) {
            if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
                return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
            }
            return jwtSecret, nil
        })
        return err
    },
})
```

---

## HTTP API

### Mounting on a sub-path

```go
mux := http.NewServeMux()
mux.Handle("/db/", http.StripPrefix("/db", srv))
// Endpoints become: /db/v1/execute, /db/v2/pipeline, etc.
log.Fatal(http.ListenAndServe(":8080", mux))
```

### Exposed endpoints

| Method | Path           | Protocol | Description                                   |
| ------ | -------------- | -------- | --------------------------------------------- |
| `POST` | `/v1/execute`  | Hrana 1  | Execute a single statement (ephemeral stream) |
| `POST` | `/v1/batch`    | Hrana 1  | Execute a batch (ephemeral stream)            |
| `GET`  | `/v2`          | Hrana 2  | Health / version check                        |
| `POST` | `/v2/pipeline` | Hrana 2  | Stateful stream pipeline (baton)              |
| `GET`  | `/v3`          | Hrana 3  | Health / version check                        |
| `POST` | `/v3/pipeline` | Hrana 3  | Stateful stream pipeline (baton)              |
| `POST` | `/v3/cursor`   | Hrana 3  | Streaming batch results (NDJSON)              |

### `POST /v1/execute` – single statement

```bash
curl -s -X POST http://localhost:8080/v1/execute \
  -H "Content-Type: application/json" \
  -d '{
    "stmt": {
      "sql": "SELECT 1 + 1 AS result",
      "want_rows": true
    }
  }'
```

```json
{
  "result": {
    "cols": [{"name": "result", "decltype": null}],
    "rows": [[{"type": "integer", "value": "2"}]],
    "affected_row_count": 0,
    "last_insert_rowid": null,
    "rows_read": 1,
    "rows_written": 0,
    "query_duration_ms": 0.12
  }
}
```

### `POST /v1/execute` – parameterised statement

```bash
# Positional args
curl -s -X POST http://localhost:8080/v1/execute \
  -H "Content-Type: application/json" \
  -d '{
    "stmt": {
      "sql": "INSERT INTO users (name, age) VALUES (?, ?)",
      "args": [
        {"type": "text",    "value": "Alice"},
        {"type": "integer", "value": "30"}
      ],
      "want_rows": false
    }
  }'

# Named args
curl -s -X POST http://localhost:8080/v1/execute \
  -H "Content-Type: application/json" \
  -d '{
    "stmt": {
      "sql": "INSERT INTO users (name, age) VALUES (:name, :age)",
      "named_args": [
        {"name": "name", "value": {"type": "text",    "value": "Bob"}},
        {"name": "age",  "value": {"type": "integer", "value": "25"}}
      ],
      "want_rows": false
    }
  }'
```

### `POST /v1/batch` – conditional batch (transaction pattern)

```bash
curl -s -X POST http://localhost:8080/v1/batch \
  -H "Content-Type: application/json" \
  -d '{
    "batch": {
      "steps": [
        {
          "stmt": {"sql": "BEGIN", "want_rows": false}
        },
        {
          "condition": {"type": "ok", "step": 0},
          "stmt": {"sql": "INSERT INTO accounts (id, balance) VALUES (1, 1000)", "want_rows": false}
        },
        {
          "condition": {"type": "ok", "step": 1},
          "stmt": {"sql": "INSERT INTO accounts (id, balance) VALUES (2, 500)",  "want_rows": false}
        },
        {
          "condition": {"type": "ok", "step": 2},
          "stmt": {"sql": "COMMIT", "want_rows": false}
        },
        {
          "condition": {"type": "error", "step": 2},
          "stmt": {"sql": "ROLLBACK", "want_rows": false}
        }
      ]
    }
  }'
```

### `POST /v2/pipeline` – stateful stream with baton

The first request creates a new stream (`"baton": null`). Every response carries a new baton that must be used in the next request.

```bash
# Step 1 – open a new stream and run a statement
curl -s -X POST http://localhost:8080/v2/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "baton": null,
    "requests": [
      {
        "type": "execute",
        "stmt": {"sql": "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)", "want_rows": false}
      }
    ]
  }'
# → {"baton":"<TOKEN>","base_url":null,"results":[{"type":"ok","response":{"type":"execute","result":{...}}}]}

# Step 2 – reuse the stream with the returned baton
curl -s -X POST http://localhost:8080/v2/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "baton": "<TOKEN>",
    "requests": [
      {
        "type": "execute",
        "stmt": {"sql": "INSERT INTO kv VALUES ('"'"'hello'"'"', '"'"'world'"'"')", "want_rows": false}
      },
      {
        "type": "execute",
        "stmt": {"sql": "SELECT * FROM kv", "want_rows": true}
      },
      {"type": "close"}
    ]
  }'
# Baton in response will be null because the stream was closed.
```

### `POST /v2/pipeline` – stored SQL

```bash
# Store a reusable SQL text (sql_id scoped to this stream)
curl -s -X POST http://localhost:8080/v2/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "baton": null,
    "requests": [
      {"type": "store_sql", "sql_id": 1, "sql": "SELECT * FROM users WHERE id = ?"},
      {"type": "execute",   "stmt": {"sql_id": 1, "args": [{"type": "integer", "value": "42"}], "want_rows": true}},
      {"type": "close_sql", "sql_id": 1},
      {"type": "close"}
    ]
  }'
```

### `POST /v2/pipeline` – sequence (multi-statement script)

```bash
curl -s -X POST http://localhost:8080/v2/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "baton": null,
    "requests": [
      {
        "type": "sequence",
        "sql": "CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);"
      },
      {"type": "close"}
    ]
  }'
```

### `POST /v3/cursor` – streaming batch results (NDJSON)

The response is newline-delimited JSON. The first line is the cursor metadata; subsequent lines are `CursorEntry` objects streamed as rows become available.

```bash
curl -s -N -X POST http://localhost:8080/v3/cursor \
  -H "Content-Type: application/json" \
  -d '{
    "baton": null,
    "batch": {
      "steps": [
        {"stmt": {"sql": "SELECT * FROM users", "want_rows": true}}
      ]
    }
  }'
```

```ndjson
{"baton":"<TOKEN>","base_url":null}
{"type":"step_begin","step":0,"cols":[{"name":"id","decltype":"INTEGER"},{"name":"name","decltype":"TEXT"}]}
{"type":"row","row":[{"type":"integer","value":"1"},{"type":"text","value":"Alice"}]}
{"type":"row","row":[{"type":"integer","value":"2"},{"type":"text","value":"Bob"}]}
{"type":"step_end","affected_row_count":0,"last_insert_rowid":null}
```

---

## WebSocket API

`*Server` exposes `ServeConn(net.Conn)` for raw WebSocket connections. The typical integration pattern is to detect an upgrade request in your HTTP handler and pass the hijacked connection to the server:

```go
import (
    "net/http"
    "github.com/cornejong/hrana"
)

func main() {
    db, _ := sql.Open("sqlite3", "app.db")
    srv := hrana.New(db, nil)
    defer srv.Close()

    http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
        if r.Header.Get("Upgrade") != "websocket" {
            http.Error(w, "websocket required", http.StatusBadRequest)
            return
        }
        // Hijack the connection and hand it to the Hrana server.
        hj, ok := w.(http.Hijacker)
        if !ok {
            http.Error(w, "hijacking not supported", http.StatusInternalServerError)
            return
        }
        conn, _, err := hj.Hijack()
        if err != nil {
            return
        }
        // ServeConn reads the buffered HTTP request, completes the WS
        // handshake, and then runs the Hrana protocol loop.
        srv.ServeConn(conn)
    })

    // HTTP endpoints on the same mux:
    http.Handle("/", srv)

    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### Supported WebSocket subprotocols

The server negotiates the highest mutually-supported version:

| Subprotocol | Version | Encoding |
| ----------- | ------- | -------- |
| `hrana1`    | 1       | JSON     |
| `hrana2`    | 2       | JSON     |
| `hrana3`    | 3       | JSON     |

### WebSocket message flow

```
Client                          Server
  │                               │
  │── hello {"jwt": "..."}  ────▶ │  AuthFunc called
  │◀── hello_ok              ───  │
  │                               │
  │── request {open_stream} ────▶ │
  │◀── response_ok           ───  │
  │                               │
  │── request {execute}     ────▶ │
  │◀── response_ok           ───  │
  │                               │
  │── request {close_stream}────▶ │
  │◀── response_ok           ───  │
  │                               │
  │── [close frame]         ────▶ │  all streams closed
```

The client may pipeline multiple requests without waiting for responses.  
Responses may arrive out of order; `request_id` is used to correlate them.

### WS V3 cursor example

```json
// open_cursor
{
  "type": "request",
  "request_id": 1,
  "request": {
    "type": "open_cursor",
    "stream_id": 0,
    "cursor_id": 0,
    "batch": {
      "steps": [{"stmt": {"sql": "SELECT * FROM users", "want_rows": true}}]
    }
  }
}

// fetch_cursor (up to 100 entries at a time)
{
  "type": "request",
  "request_id": 2,
  "request": {
    "type": "fetch_cursor",
    "cursor_id": 0,
    "max_count": 100
  }
}

// close_cursor when done is true
{
  "type": "request",
  "request_id": 3,
  "request": {
    "type": "close_cursor",
    "cursor_id": 0
  }
}
```

---

## Value types

All SQL values in the protocol use a tagged-union JSON encoding:

| Hrana type | JSON                                  | Go type   |
| ---------- | ------------------------------------- | --------- |
| `null`     | `{"type":"null"}`                     | `nil`     |
| `integer`  | `{"type":"integer","value":"42"}`     | `int64`   |
| `float`    | `{"type":"float","value":3.14}`       | `float64` |
| `text`     | `{"type":"text","value":"hello"}`     | `string`  |
| `blob`     | `{"type":"blob","base64":"aGVsbG8="}` | `[]byte`  |

> 64-bit integers are encoded as strings to avoid precision loss in JavaScript clients.

---

## Batch conditions

Conditions control whether a step in a batch is executed:

| Type            | Description                                    |
| --------------- | ---------------------------------------------- |
| `ok`            | True if the referenced step succeeded          |
| `error`         | True if the referenced step failed             |
| `not`           | Logical negation of an inner condition         |
| `and`           | Logical conjunction of multiple conditions     |
| `or`            | Logical disjunction of multiple conditions     |
| `is_autocommit` | True if the stream is not inside a transaction |

```json
// Run step 2 only if both step 0 and step 1 succeeded
{
  "condition": {
    "type": "and",
    "conds": [
      {"type": "ok", "step": 0},
      {"type": "ok", "step": 1}
    ]
  },
  "stmt": {"sql": "COMMIT", "want_rows": false}
}
```

---

## Error responses

All error responses follow the Hrana `Error` structure:

```json
{"message": "hrana: sql_id 99 not found", "code": null}
```

HTTP errors use the status code to indicate the class of failure:

| Status | Meaning                            |
| ------ | ---------------------------------- |
| 400    | Bad request (invalid JSON / baton) |
| 401    | Authentication failed              |
| 500    | Internal server error              |

---

## Spec compliance

| Feature                         | V1  | V2  | V3      |
| ------------------------------- | --- | --- | ------- |
| Execute statement               | ✓   | ✓   | ✓       |
| Execute batch + conditions      | ✓   | ✓   | ✓       |
| Stored SQL (`store_sql`)        | –   | ✓   | ✓       |
| Sequence (multi-statement)      | –   | ✓   | ✓       |
| Describe statement              | –   | ✓   | ✓       |
| Stateful HTTP pipeline (baton)  | –   | ✓   | ✓       |
| Streaming cursor (HTTP)         | –   | –   | ✓       |
| WS cursor (`open/fetch/close`)  | –   | –   | ✓       |
| `get_autocommit`                | –   | –   | ✓       |
| `is_autocommit` batch condition | –   | –   | ✓       |
| Protobuf encoding               | –   | –   | planned |

---

## License

MIT
