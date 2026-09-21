# Internal Spark 3.2 connector compatibility

Status: ready-for-agent

## Latest scope clarification (2026-09-20)

Follow the Flink version-module model: reuse compatible common bytecode, expose only small
version-sensitive adapter entry points, and compile Spark 3.2 implementations against 3.2.4.
Existing adapters are within redesign scope. Common changes are limited to necessary adapter
hooks; Spark 3.4/3.5 keep their default behavior. Do not duplicate whole common business classes.
Retain extension capabilities and reuse `fluss-spark-ut` tests wherever their API assumptions
hold. Version-specific tests live in the connector module; final shaded-JAR acceptance runs
after package in integration-test/verify, without a separate version-specific UT module. Add
focused adapter contracts and version-specific behavior tests. See
[implementation notes](implementation-notes.md).

## Problem Statement

The platform's first Spark 3.2 target is Apache Spark 3.2.4 on Scala 2.12. Fluss currently publishes and tests Spark connector artifacts only for Spark 3.4 and 3.5. Reusing either artifact with the Spark 3.2 runtime is unsafe because the connector is compiled against newer Spark DataSource V2 interfaces.

The platform needs one internally supportable connector artifact that can run catalog, batch read, structured-streaming read and write workloads on the approved internal Spark 3.2 runtime, without changing Fluss server, client, RPC, Lake, or Flink behavior.

## Solution

Add an internal Spark 3.2 / Scala 2.12 compatibility line that produces a distinct `fluss-spark-3.2_2.12` connector artifact. Its source is compiled against the exact internal Spark 3.2 Maven coordinate selected by the build, rather than reusing a connector JAR compiled for Spark 3.4 or 3.5.

The minimum compatibility boundary is deliberately narrow:

- Preserve catalog operations, SQL parsing and extensions, batch reads, standard micro-batch streaming reads, batch writes, and streaming writes where their Spark 3.2 APIs are already available.
- Adapt filter pushdown from the newer V2 Predicate API to Spark 3.2's legacy `Filter` API, preserving result correctness. A filter that cannot be represented or safely accepted is returned to Spark for evaluation.
- Do not implement V2 predicate pushdown, source-level limit pushdown, or AvailableNow trigger support in the Spark 3.2 line. Spark may still apply filtering and limits after scanning; this is a performance reduction, not a correctness change.
- Compile the Spark 3.2 adapters against 3.2.4 and test the assembled connector with only Spark 3.2 runtime dependencies; shared connector bytecode is reused through stable adapter contracts.

## Approved Adapter Design

The Spark 3.2 connector follows the existing Flink compatibility model. Shared connector behavior remains in one Spark common module. Version-sensitive Spark APIs are reached only through a small adapter seam; each supported Spark line supplies the adapter implementation compiled against its runtime.

- The shared module owns Fluss catalog behavior, read and write planning, row conversion, and other behavior that does not vary by Spark release.
- The adapter seam owns scan filter exchange, scan capability declaration, streaming source lifecycle differences, Catalyst expression and parser differences, and Spark type differences.
- The Spark 3.2 adapter uses the legacy `Filter` contract and the Spark 3.2 streaming and Catalyst shapes. The Spark 3.4 and 3.5 adapters retain the V2 Predicate, limit-pushdown, and AvailableNow behavior they support today.
- A version artifact shades shared connector behavior with its own adapter, just as the Flink version artifacts shade the shared Flink connector with version-specific adapter classes.
- The previously attempted full Spark 3.2 common-source copy is superseded. The compatibility line must not duplicate the shared connector implementation.

## User Stories

1. As an internal Spark user, I want a connector artifact built for Spark 3.2 and Scala 2.12, so that I can load Fluss without Spark linkage failures.
2. As a platform engineer, I want the connector build to pin one internal Spark 3.2 Maven coordinate, so that every compile and test uses the runtime that will be deployed.
3. As a Spark SQL user, I want to configure a Fluss catalog and execute supported catalog DDL, so that existing SQL integrations remain usable on the internal runtime.
4. As a data engineer, I want to read append and primary-key tables in batch mode, so that Spark 3.2 jobs can consume Fluss data.
5. As a streaming user, I want standard micro-batch reads to continue from checkpoints, so that long-running Structured Streaming jobs remain supported.
6. As a writer, I want batch and streaming writes to use the existing Fluss commit semantics, so that Spark-version adaptation does not alter data durability behavior.
7. As a query user, I want filter results to remain correct when a predicate is not pushed down, so that compatibility never trades correctness for optimization.
8. As a query user, I want supported legacy filters to be pushed down when safe, so that Spark 3.2 retains useful scan reduction without relying on unavailable V2 Predicate classes.
9. As a query user, I want `LIMIT` to return the correct rows even when the connector does not push the limit to Fluss, so that removing the unavailable optimization is transparent semantically.
10. As a streaming user, I want an explicit error or documented exclusion for AvailableNow triggers, so that unsupported execution modes do not fail with linkage errors.
11. As a test maintainer, I want Spark 3.2 tests to assert user-visible SQL and streaming behavior rather than Catalyst tuple shapes, so that tests are stable across supported Spark lines.
12. As a release engineer, I want CI to compile and run the dedicated Spark 3.2 test suite, so that a future Spark-common change cannot silently break the internal artifact.
13. As an operator, I want the Spark 3.2 artifact to shade the same Fluss client payload as the existing Spark artifacts, so that deployment remains a single connector JAR plus the Spark runtime.
14. As a maintainer, I want unsupported Spark 3.2-only capabilities documented separately from upstream Spark 3.4/3.5 support, so that internal compatibility claims do not alter the public release matrix.

## Implementation Decisions

- The first supported target is Apache Spark 3.2.4 on Scala 2.12 only. Compatibility with the internal Dream Spark fork is a later, separate qualification effort.
- The Spark 3.2 artifact is a real adapter compilation target. Like the Flink version artifacts, it reuses common bytecode and replaces the default adapters explicitly at packaging time. Binary compatibility is verified on the real Spark 3.2 runtime.
- Shared connector behavior is kept shared only where its compiled Spark API surface is compatible. Spark-version-sensitive code is isolated behind a small compatibility boundary rather than selected at runtime by reflection.
- The compatibility boundary owns scan filter exchange, optional scan optimizations, and source-trigger capability declarations. Catalog, Fluss client interaction, read planning, record conversion, and write semantics stay outside that boundary unless a compile-time incompatibility proves otherwise.
- Spark 3.2 filter handling implements the legacy DataSource V2 filter contract. It reports accepted filters separately from residual filters; residual filters remain Spark-owned.
- Spark 3.2 does not advertise V2 Predicate support, pushed limit support, or AvailableNow trigger support. Requests relying on AvailableNow fail with an actionable unsupported-capability message rather than a missing-class error.
- Existing Spark 3.4 and 3.5 artifact names, source behavior, packaging, and CI lanes remain unchanged.
- The Spark 3.2 artifact follows the existing connector packaging convention: it contains the compatible connector implementation and shaded Fluss client dependencies, while Spark dependencies remain provided by the runtime.
- The work does not change Fluss server APIs, table metadata contracts, RPC protocols, Lake integration, Flink connectors, or the default distribution.
- Internal documentation identifies the artifact coordinate, Scala binary version, runtime prerequisite, supported capability set, and excluded capabilities. Public documentation remains unchanged unless the compatibility line is later approved for public release.

## Testing Decisions

- The primary acceptance seam is a Spark 3.2 session using the packaged connector JAR against a Fluss test cluster. It verifies catalog initialization, SQL DDL, batch read, batch write, micro-batch read, and streaming write behavior.
- Filter tests assert returned rows and whether a supported legacy filter is accepted; they do not assert Spark internal Predicate classes or Catalyst plan extractor arity.
- Limit tests prove correct result cardinality with connector-side limit pushdown disabled.
- Streaming tests cover ordinary micro-batch checkpoint and restart behavior. AvailableNow has a dedicated negative test that proves a clear unsupported response.
- Adapter compilation resolves Spark 3.2 / Scala 2.12 APIs. Packaging and runtime checks verify the selected adapter classes do not reference unavailable APIs; unused default-only common helpers are not mistaken for the selected adapter implementation.
- The existing 3.4 and 3.5 test suites remain regression gates. Their tests continue to cover the higher-version V2 Predicate and optimization behavior.
- Packaging verification confirms that the Spark 3.2 artifact includes the intended Fluss classes and client payload while excluding Spark runtime classes.

## Out of Scope

- Public release, Maven Central publication, or public support-matrix changes for Spark 3.2.
- Scala 2.13 support for Spark 3.2.
- AvailableNow trigger behavior on Spark 3.2.
- V2 Predicate pushdown or pushed-limit optimization on Spark 3.2.
- Performance parity with Spark 3.4 or 3.5 where their newer scan APIs enable additional optimization.
- Changes to Fluss server, client API, RPC protocol, Lake storage, Flink connector, or distribution modules.
- Qualification of a Spark 3.2 runtime other than the selected internal Maven coordinate.

## Further Notes

- Repository evidence shows that the existing connector source imports newer V2 Predicate and filter-pushdown interfaces, while the locally available internal Spark 3.2 build exposes the legacy filter contract. This is the motivating compatibility split.
- The current shared Spark test suite also relies on Catalyst plan details that differ in Spark 3.2; the Spark 3.2 line needs its own behavior-oriented tests rather than copied internal-plan assertions.
- The adapter artifact must establish an explicit Spark 3.2 compilation and packaging gate before functional qualification. Compatible common bytecode is reused; selected adapters and runtime dependencies are verified against Spark 3.2.
- Tickets 01 through 04 are superseded by the adapter-seam design and retained only as history. Tickets 05 through 09 are the active implementation sequence.
- Ticket 10 separately qualifies streaming writes because Spark 3.2 lacks the newer streaming-write coordinator API used by the shared implementation.
