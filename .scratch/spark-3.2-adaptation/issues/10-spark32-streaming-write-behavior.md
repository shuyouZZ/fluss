# 10: Deliver Spark 3.2 streaming writes

**What to build:** An internal Spark 3.2 user can write Fluss data through Structured Streaming without loading newer Spark streaming-write coordinator APIs.

**Blocked by:** 06: Assemble the Spark 3.2 adapter artifact.

**Status:** ready-for-agent

- [ ] The Spark 3.2 artifact supplies a streaming-write adapter compiled only against Spark 3.2 interfaces.
- [ ] Append and primary-key streaming writes create the existing Fluss data writers and preserve their commit and abort behavior.
- [ ] The connector does not reference the newer streaming-write coordinator API when loaded by Spark 3.2.
- [ ] Spark 3.4 and 3.5 retain their existing streaming-write behavior.
