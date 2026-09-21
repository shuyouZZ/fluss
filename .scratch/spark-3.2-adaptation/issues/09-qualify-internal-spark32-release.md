# 09: Qualify the Spark 3.2.4 release line

**What to build:** The platform can continuously build, test, package, and operate the Apache Spark 3.2.4 connector without changing the public Spark release matrix.

**Blocked by:** 07: Deliver Spark 3.2 batch behavior and legacy filtering; 08: Deliver Spark 3.2 micro-batch behavior; 10: Deliver Spark 3.2 streaming writes.

**Status:** ready-for-agent

- [ ] CI builds and tests the exact Apache Spark 3.2.4 / Scala 2.12 dependency closure through the explicit Spark 3.2 profile.
- [ ] Behavior-oriented tests cover catalog setup, batch read/write, filter correctness, micro-batch recovery, streaming writes, and the AvailableNow exclusion.
- [ ] Artifact isolation and internal usage documentation are verified as part of the release gate.
- [ ] The public Spark 3.4 and 3.5 release matrix remains unchanged.
