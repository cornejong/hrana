import type { RowValue } from "./value.js"
import type { ResultSet } from "./client.js"

/**
 * A lazy, chainable iterator over ResultSet rows.
 *
 * Transformations are applied JIT — no intermediate arrays are created until
 * you call `toArray()` or consume the iterator via `for...of`. Chaining
 * `.filter()` before `.map()` means only matching rows ever reach the mapper.
 *
 * @example
 * ```ts
 * const result = await db.execute("SELECT id, name FROM users WHERE active = 1")
 *
 * // Typed iteration — zero extra allocation:
 * for (const user of rows<User>(result)) {
 *     console.log(user.name)
 * }
 *
 * // Lazy filter → map → collect:
 * const names = rows(result)
 *     .filter(r => r.active !== 0n)
 *     .map(r => r.name as string)
 *     .toArray()
 *
 * // forEach with no result array:
 * rows(result).forEach(r => send(r))
 * ```
 */
export class ResultRows<T> implements Iterable<T> {
    readonly #source: Iterable<T>

    /** @internal */
    constructor(source: Iterable<T>) {
        this.#source = source
    }

    [Symbol.iterator](): Iterator<T> {
        return this.#source[Symbol.iterator]()
    }

    /**
     * Lazily transform each row. Returns a new `ResultRows<U>`; nothing runs
     * until the result is consumed.
     */
    map<U>(fn: (row: T, index: number) => U): ResultRows<U> {
        const source = this.#source
        return new ResultRows<U>({
            [Symbol.iterator](): Iterator<U> {
                const iter = source[Symbol.iterator]()
                let i = 0
                return {
                    next(): IteratorResult<U> {
                        const step = iter.next()
                        if (step.done) return { done: true, value: undefined as never }
                        return { done: false, value: fn(step.value, i++) }
                    },
                }
            },
        })
    }

    /**
     * Lazily filter rows by a predicate. Returns a new `ResultRows<T>`; nothing
     * runs until the result is consumed.
     */
    filter(predicate: (row: T, index: number) => boolean): ResultRows<T> {
        const source = this.#source
        return new ResultRows<T>({
            [Symbol.iterator](): Iterator<T> {
                const iter = source[Symbol.iterator]()
                let i = 0
                return {
                    next(): IteratorResult<T> {
                        for (; ;) {
                            const step = iter.next()
                            if (step.done) return { done: true, value: undefined as never }
                            if (predicate(step.value, i++)) return { done: false, value: step.value }
                        }
                    },
                }
            },
        })
    }

    /**
     * Iterate over all rows, calling `fn` for each. No result array is
     * allocated — the lowest-overhead way to consume a result set.
     */
    forEach(fn: (row: T, index: number) => void): void {
        let i = 0
        for (const row of this) fn(row, i++)
    }

    /** Materialize all (remaining) rows into a plain array. */
    toArray(): T[] {
        return [...this]
    }
}

/**
 * Wrap a `ResultSet` in a {@link ResultRows} lazy iterator.
 *
 * The generic `T` is a cast — columns must actually match the TypeScript type
 * you provide. Defaults to `Record<string, RowValue>` (same as `result.rows`).
 *
 * @example
 * ```ts
 * interface User { id: bigint; name: string; active: bigint }
 *
 * const result = await db.execute("SELECT * FROM users")
 * rows<User>(result).forEach(u => console.log(u.name))
 * ```
 */
export function rows<T = Record<string, RowValue>>(result: ResultSet): ResultRows<T> {
    return new ResultRows<T>(result.rows as unknown as Iterable<T>)
}
