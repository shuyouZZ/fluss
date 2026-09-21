# 09: Qualify the Spark 3.2.4 release line

**What to build:** The platform can continuously build, test, package, and operate the Apache Spark 3.2.4 connector without changing the public Spark release matrix.

**Blocked by:** 07: Deliver Spark 3.2 batch behavior and legacy filtering; 08: Deliver Spark 3.2 micro-batch behavior; 10: Deliver Spark 3.2 streaming writes.

**Status:** resolved

- [x] CI builds and tests the exact Apache Spark 3.2.4 / Scala 2.12 dependency closure through the explicit Spark 3.2 profile.
- [x] Behavior-oriented tests cover catalog setup, batch read/write, filter correctness, micro-batch recovery, streaming writes, and the AvailableNow exclusion.
- [x] Artifact isolation and internal usage documentation are verified as part of the release gate.
- [x] The public Spark 3.4 and 3.5 release matrix remains unchanged.

## Completion evidence

Added the standalone internal-spark32 CI workflow without changing the public matrix. The
same local sequence rebuilt all 21 reactor prerequisites, checked the actual Spark 3.2.4 /
Scala 2.12.18 test closure, and passed clean verify (167 ordinary + 15 packaged cases).
The dependency checker passes seven maintained positive/negative tests. Internal usage,
artifact coordinate, exclusions and the main comparison are documented in
[compatibility-matrix.md](../compatibility-matrix.md). Remote Actions was not dispatched;
this closes the implementation and local qualification, not a remote deployment approval.
