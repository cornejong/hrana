# Pagination

## Basic usage

`paginate()` wraps your SQL in a `COUNT(*)` subquery and a `LIMIT / OFFSET`
subquery, firing both in parallel. It returns a `PaginatedResult` with
everything you need to drive page buttons.

```ts
const result = await db.paginate(
    "SELECT * FROM products ORDER BY name",
    [],            // params (optional)
    { page: 1, pageSize: 10 }
)

console.log(result.rows)        // up to 10 rows for page 1
console.log(result.page)        // 1
console.log(result.pageSize)    // 10
console.log(result.totalRows)   // e.g. 47
console.log(result.totalPages)  // 5
console.log(result.hasNextPage) // true
console.log(result.hasPrevPage) // false
```

## PaginatedResult shape

| Field         | Type                         | Description                          |
| ------------- | ---------------------------- | ------------------------------------ |
| `columns`     | `string[]`                   | Column names in declaration order    |
| `rows`        | `Record<string, RowValue>[]` | Rows for the current page            |
| `page`        | `number`                     | Current page (1-based)               |
| `pageSize`    | `number`                     | Max rows per page                    |
| `totalRows`   | `number`                     | Total matching rows across all pages |
| `totalPages`  | `number`                     | Total number of pages (minimum 1)    |
| `hasNextPage` | `boolean`                    | Whether a next page exists           |
| `hasPrevPage` | `boolean`                    | Whether a previous page exists       |

## Parameters

Parameters work exactly the same as with `execute()`.

```ts
// Positional
const result = await db.paginate(
    "SELECT * FROM products WHERE category = ? ORDER BY name",
    ["electronics"],
    { page: 2, pageSize: 20 }
)

// Named
const result = await db.paginate(
    "SELECT * FROM products WHERE category = :cat ORDER BY name",
    { cat: "electronics" },
    { page: 2, pageSize: 20 }
)
```

Trailing semicolons in the SQL are stripped automatically, so `SELECT * FROM products;` works as expected.

## Defaults

| Option     | Default |
| ---------- | ------- |
| `page`     | `1`     |
| `pageSize` | `20`    |

## Driving page controls

```ts
async function loadPage(page: number) {
    const result = await db.paginate("SELECT * FROM products ORDER BY id", [], { page, pageSize: 10 })

    renderRows(result.rows)

    // Enable / disable prev & next buttons
    prevBtn.disabled = !result.hasPrevPage
    nextBtn.disabled = !result.hasNextPage

    // "Page 2 of 5"
    pageLabel.textContent = `Page ${result.page} of ${result.totalPages}`
}

prevBtn.addEventListener("click", () => loadPage(currentPage - 1))
nextBtn.addEventListener("click", () => loadPage(currentPage + 1))
```

## With caching

Each page result can be independently cached. See [Caching](./caching.md) for details.

```ts
const result = await db.paginate(
    "SELECT * FROM products ORDER BY name",
    [],
    { page: 1, pageSize: 10, cache: true }
)
```

## How it works

Given:
```sql
SELECT * FROM products WHERE active = ? ORDER BY name
```

Two queries are fired in parallel:

```sql
-- Count query (determines totalRows / totalPages)
SELECT COUNT(*) AS _count FROM (
    SELECT * FROM products WHERE active = ? ORDER BY name
)

-- Data query (scoped to the requested page)
SELECT * FROM (
    SELECT * FROM products WHERE active = ? ORDER BY name
) LIMIT ? OFFSET ?
```

The original parameters are passed to both queries. The `LIMIT` and `OFFSET`
values are appended automatically (or injected as named params `:_hrana_limit`
and `:_hrana_offset` when named params are used).
