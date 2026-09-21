# 05: Expand the Spark compatibility seam

**What to build:** Shared Spark connector behavior reaches version-sensitive Spark facilities through a small adapter seam, so the shared module no longer exposes Spark 3.4-or-newer scan, streaming, Catalyst, or type contracts directly.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] Existing Spark 3.4 and 3.5 behavior remains unchanged through the default adapter implementation.
- [x] Shared connector code has one adapter seam for each release-varying capability rather than scattered direct version checks.
- [x] Regression tests prove current Spark 3.4 and 3.5 catalog, read, write, and streaming behavior still works through the seam.
- [x] The seam is small enough that a new Spark release needs adapter work only at that seam.

## Completion evidence

Implemented the Flink-style binary reuse boundary with scan, streaming, SQL and namespace adapters. Removed the V2-only forwarding adapter. Default Spark 3.4/3.5 catalog, read, write, streaming and adapter regressions pass. See ../implementation-notes.md for the final design and version-matrix evidence.
