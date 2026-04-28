# Querying

## `execute()` — read queries

Use `execute()` for any statement that returns rows (`SELECT`).

```ts
const result = await db.execute("SELECT * FROM users")

console.log(result.columns)   // ["id", "name", "email"]
console.log(result.rows)      // [{ id: 1n, name: "Alice", email: "alice@example.com" }, ...]
```

### ResultSet shape

| Field             | Type                         | Description                                     |
| ----------------- | ---------------------------- | ----------------------------------------------- |
| `columns`         | `string[]`                   | Column names in declaration order               |
| `rows`            | `Record<string, RowValue>[]` | Each row as a plain object keyed by column name |
| `rowsAffected`    | `number`                     | Rows changed (always `0` for `SELECT`)          |
| `lastInsertRowid` | `bigint \| null`             | Last inserted row ID (`null` for `SELECT`)      |

## `run()` — write queries

Use `run()` for statements that do not return rows (`INSERT`, `UPDATE`, `DELETE`, DDL).

```ts
const { rowsAffected, lastInsertRowid } = await db.run(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    ["Bob", "bob@example.com"]
)

console.log(rowsAffected)      // 1
console.log(lastInsertRowid)   // 42n
```

## `batch()` — multiple statements

Execute several statements in a single call. Results are returned in the same
order as the input array.

```ts
const [users, posts] = await db.batch([
    { sql: "SELECT * FROM users" },
    { sql: "SELECT * FROM posts WHERE author_id = ?", params: [1n] },
])
```

> Internally each statement uses the same transport connection, saving
> per-request overhead compared to multiple `execute()` calls.

## Parameters

### Positional (`?`)

Pass an array. Values are bound left-to-right.

```ts
await db.execute("SELECT * FROM users WHERE id = ? AND active = ?", [42n, true])
```

### Named (`:name`)

Pass a plain object. Keys match the parameter names in the SQL (without `:` prefix).

```ts
await db.execute(
    "SELECT * FROM users WHERE id = :id AND active = :active",
    { id: 42n, active: true }
)
```

## Value type mapping

JavaScript values are automatically encoded to the correct Hrana wire type and
decoded back when results are returned.

| JavaScript (input)           | Wire type             | JavaScript (output) |
| ---------------------------- | --------------------- | ------------------- |
| `null` / `undefined`         | `null`                | `null`              |
| `boolean`                    | `integer` (`0` / `1`) | `bigint`            |
| `bigint`                     | `integer`             | `bigint`            |
| Safe integer `number`        | `integer`             | `bigint`            |
| Other `number`               | `float`               | `number`            |
| `string`                     | `text`                | `string`            |
| `ArrayBuffer` / `Uint8Array` | `blob`                | `Uint8Array`        |

> **Important:** SQLite integers are always decoded as `bigint` to preserve the
> full int64 range. Use `Number(row.id)` when you need a plain number and are
> certain the value is within the safe integer range.

## Iterating rows with `rows()`

For large result sets you may not want to convert every row upfront. The `rows()`
helper wraps a `ResultSet` in a **lazy iterator** — rows are processed one at a
time, only when consumed. Chaining `.filter()` before `.map()` means rejected
rows never even reach the mapper.

```ts
import { rows } from "@cornejong/hrana"

const result = await db.execute("SELECT id, name, active FROM users")

// Typed for-of — zero extra allocation:
interface User { id: bigint; name: string; active: bigint }

for (const user of rows<User>(result)) {
    console.log(user.name)
}
```

### Lazy chaining

Every `.filter()` / `.map()` call returns a new `ResultRows` instance. Nothing
runs until you call `.forEach()`, `.toArray()`, or iterate with `for...of`.

```ts
// Single pass: filter then map, nothing materialised in between.
const names = rows<User>(result)
    .filter(u => u.active !== 0n)
    .map(u => u.name as string)
    .toArray()
```

### Lowest-overhead consumption

`.forEach()` drives the iterator without allocating a result array, making it
the cheapest option when you do not need the values collected:

```ts
rows<User>(result).forEach(u => sendToSocket(u))
```

### Using without a generic type

Omit the type parameter to get `Record<string, RowValue>` — identical to
accessing `result.rows` directly but lazy:

```ts
rows(result).forEach(row => console.log(row["name"]))
```

> **Note:** The generic `T` is a TypeScript cast only. The runtime shape is
> always `Record<string, RowValue>` as returned by the server.

## Error handling

All network and protocol errors are thrown as `HranaError`.

```ts
import { HranaError } from "@cornejong/hrana"

try {
    await db.execute("SELECT * FROM nonexistent_table")
} catch (err) {
    if (err instanceof HranaError) {
        console.error("Query failed:", err.message)
    }
}
```

On error, the underlying transport stream may be in a bad state. The client
will create a fresh connection on the next query automatically.
