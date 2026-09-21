# 01: Establish the Spark 3.2 connector artifact

**What to build:** An independently compiled and packaged Spark 3.2 / Scala 2.12 Fluss connector that an internal user can add to a Spark session, configure as a Fluss catalog, and use for basic batch reads and writes without loading classes compiled for Spark 3.4 or 3.5.

**Blocked by:** None (can start immediately).

**Status:** superseded

**Superseded by:** 05: Expand the Spark compatibility seam; 06: Assemble the Spark 3.2 adapter artifact.

- [ ] The build resolves the approved internal Spark 3.2 / Scala 2.12 dependency coordinate and produces a distinct connector artifact.
- [ ] A packaged-artifact smoke test configures a Fluss catalog and completes a basic batch read and write against a Fluss test cluster.
- [ ] The Spark 3.2 compilation path has no dependency on V2 Predicate pushdown, pushed-limit, or AvailableNow APIs.
- [ ] Existing Spark 3.4 and 3.5 artifact build paths remain unchanged.
