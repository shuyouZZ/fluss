# 04: Qualify and document the Spark 3.2 release line

**What to build:** The internal platform has a continuously verified Spark 3.2 connector release line with behavior-oriented tests, package isolation checks, CI coverage, and documentation of its supported and excluded capabilities.

**Blocked by:** 02: Support safe legacy filter pushdown; 03: Qualify Spark 3.2 micro-batch streaming.

**Status:** superseded

**Superseded by:** 09: Qualify the internal Spark 3.2 release line.

- [ ] CI compiles and tests the approved internal Spark 3.2 / Scala 2.12 artifact independently from the Spark 3.4 and 3.5 lines.
- [ ] End-to-end tests cover catalog setup, batch read/write, filter correctness, standard micro-batch recovery, and the AvailableNow exclusion without relying on Catalyst internal-plan shapes.
- [ ] Packaging verification proves the artifact contains its intended Fluss payload and excludes Spark runtime classes.
- [ ] Internal usage documentation states the artifact coordinate, runtime prerequisite, supported capabilities, and explicit exclusions.
