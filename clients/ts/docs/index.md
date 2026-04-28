# @cornejong/hrana — Documentation

TypeScript / browser client for the [Hrana protocol](https://github.com/libsql/hrana-client-ts).
Supports HTTP pipeline and WebSocket transports, pagination, in-memory caching,
and IndexedDB-backed persistent caching.

## Contents

| Guide                                     | Description                                                                                 |
| ----------------------------------------- | ------------------------------------------------------------------------------------------- |
| [Getting Started](./getting-started.md)   | Installation, quick start, transport selection, browser bundle                              |
| [Querying](./querying.md)                 | `execute()`, `run()`, `batch()`, `rows()` iterator, parameters, value types, error handling |
| [Pagination](./pagination.md)             | `paginate()`, driving page controls, how it works under the hood                            |
| [Caching](./caching.md)                   | Memory cache and persistent cache overview, `CachePolicy` reference                         |
| [Persistent Cache](./persistent-cache.md) | IndexedDB-backed cache, groups, invalidation, expiry & cleanup                              |
| [API Reference](./api-reference.md)       | Full type and method reference                                                              |
