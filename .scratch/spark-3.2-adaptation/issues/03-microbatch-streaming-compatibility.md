# 03: Qualify Spark 3.2 micro-batch streaming

**What to build:** Spark 3.2 Structured Streaming users can run standard micro-batch Fluss reads through checkpoints and restarts, while AvailableNow is rejected with a clear unsupported-capability response rather than a linkage failure.

**Blocked by:** 01: Establish the Spark 3.2 connector artifact.

**Status:** superseded

**Superseded by:** 05: Expand the Spark compatibility seam; 08: Deliver Spark 3.2 micro-batch behavior.

- [ ] A standard micro-batch query reads Fluss data and resumes correctly from its checkpoint after restart.
- [ ] Streaming writes retain the established Fluss commit behavior on the Spark 3.2 artifact.
- [ ] An AvailableNow request receives an actionable unsupported-capability response.
- [ ] Spark 3.4 and 3.5 streaming behavior remains covered by their existing regression tests.
