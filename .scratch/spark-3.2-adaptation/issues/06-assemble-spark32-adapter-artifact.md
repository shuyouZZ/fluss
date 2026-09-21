# 06: Assemble the Spark 3.2 adapter artifact

**What to build:** An internal Spark 3.2 / Scala 2.12 artifact packages the shared connector implementation with Spark 3.2 adapter classes, without copying the shared connector source tree.

**Blocked by:** 05: Expand the Spark compatibility seam.

**Status:** resolved

- [x] The Apache Spark 3.2.4 coordinate is isolated behind an explicit Spark 3.2 build profile.
- [x] The Spark 3.2 artifact packages one shared connector implementation and only its version-specific adapter implementation.
- [x] The former full Spark 3.2 common-source copy is removed.
- [x] Packaging checks prove Spark runtime classes are not bundled in the connector artifact.

## Completion evidence

The profile assembles the common connector/client payload with only the four Spark 3.2 adapters. Full parser/stream copies were removed. Spark32ArtifactTest verifies packaged class selection and absence of bundled Spark runtime classes on the real Spark 3.2.4 classpath.
