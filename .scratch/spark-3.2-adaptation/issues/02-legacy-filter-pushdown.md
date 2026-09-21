# 02: Support safe legacy filter pushdown

**What to build:** Spark 3.2 queries can use the connector's legacy filter contract to reduce scans when safe, while all unsupported or ambiguous filters remain evaluated by Spark and therefore return correct query results.

**Blocked by:** 01: Establish the Spark 3.2 connector artifact.

**Status:** superseded

**Superseded by:** 05: Expand the Spark compatibility seam; 07: Deliver Spark 3.2 batch behavior and legacy filtering.

- [ ] Supported legacy filters are reported as accepted only when the connector can preserve their semantics.
- [ ] Unsupported filters remain residual Spark filters, and mixed supported/unsupported predicates return the same rows as an unoptimized query.
- [ ] Queries using LIMIT return correct results without the connector advertising pushed-limit support.
- [ ] Spark 3.4 and 3.5 retain their existing V2 Predicate pushdown behavior.
