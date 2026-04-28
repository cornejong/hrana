# hrana – Python client

Python DB-API 2.0 client for the [Hrana protocol](https://github.com/tursodatabase/hrana-client-spec).

## Installation

```bash
pip install hrana                     # HTTP transport only
pip install "hrana[websocket]"        # HTTP + WebSocket transport
```

## Quick start

```python
import hrana

# HTTP pipeline (v3 by default)
con = hrana.connect("http://localhost:8080?token=secret")

cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
cur.execute("SELECT * FROM users")
print(cur.fetchall())

con.close()
```

### Context manager

```python
with hrana.connect("https://my-db.example.com?token=secret") as con:
    con.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
    # commit is called automatically on __exit__
```

### WebSocket transport

```python
con = hrana.connect("ws://localhost:8080?token=secret")
# or
con = hrana.connect("wss://my-db.example.com?token=secret")
```

### Version selection

| Scheme     | Supported versions | Default |
| ---------- | ------------------ | ------- |
| http/https | v2, v3             | v3      |
| ws/wss     | v1, v2, v3         | v3      |

Append `?version=v2` (or `v1`) to the DSN to choose an older version.

## Parameters

```python
# Positional (qmark style)
cur.execute("SELECT * FROM users WHERE id = ?", (1,))

# Named (dict)
cur.execute("SELECT * FROM users WHERE name = :name", {"name": "Alice"})
```

## Dependencies

- Python ≥ 3.9 (standard library only for HTTP transport)
- [`websocket-client`](https://pypi.org/project/websocket-client/) ≥ 1.6 for WebSocket transport
