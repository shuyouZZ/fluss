# 07: Deliver Spark 3.2 batch behavior and legacy filtering

**What to build:** An internal Spark 3.2 user can configure the Fluss catalog, complete supported batch reads and writes, and obtain correct query results with safe legacy Filter pushdown.

**Blocked by:** 06: Assemble the Spark 3.2 adapter artifact.

**Status:** resolved

- [x] A packaged Spark 3.2 connector can configure the Fluss catalog and complete representative batch read and write operations.
- [x] Convertible legacy filters are accepted by the connector; unsupported filters remain Spark residuals and return correct rows.
- [x] LIMIT results remain correct without connector-side limit pushdown.
- [x] Spark 3.4 and 3.5 retain V2 Predicate and limit-pushdown behavior.

## Completion evidence

Validated append and PK batch read/write, legacy accepted/residual filter contracts, projection, count and Spark-owned LIMIT. Existing V2 filtering and limit behavior are covered on Spark 3.4/3.5. Shared UT cases are reused rather than copied.
