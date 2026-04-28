import type { ResultSet } from "./client.js"
import type { RowValue } from "./value.js"

function escapeField(value: RowValue): string {
    if (value === null) return ""

    const str = typeof value === "bigint"
        ? value.toString()
        : value instanceof Uint8Array
            ? btoa(String.fromCharCode(...value))
            : String(value)

    // Wrap in quotes if the value contains a comma, double-quote, or newline
    if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
        return `"${str.replace(/"/g, '""')}"`
    }

    return str
}

/**
 * Converts a ResultSet to a CSV string.
 * The first row is the header row with column names.
 * Null values are represented as empty fields.
 * Blob values are base64-encoded.
 */
export function resultSetToCsv(result: ResultSet): string {
    const lines: string[] = []

    lines.push(result.columns.map(escapeField).join(","))

    for (const row of result.rows) {
        const fields = result.columns.map((col) => escapeField(row[col] ?? null))
        lines.push(fields.join(","))
    }

    return lines.join("\n")
}
