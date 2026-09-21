# Spark 3.2 tickets 05–08

## Final implementation boundary

The latest user direction is the Flink version-module pattern: reuse the common JAR, keep public
behavior shared, and replace only the small Spark API adapters. This supersedes both the broad
common refactor and the intermediate whole-file source overlays. Existing adapters were reviewed
rather than preserved automatically.

Spark 3.2 compiles four adapter files against Spark 3.2.4. The POM excludes common's corresponding
classes (including Scala companions) when shading, and includes the shared connector/client
payload. It does not recompile or copy the common source tree. Scala 2.12.18 and the existing Java
build settings are inherited without profile overrides. Spark 3.4/3.5 module POMs are unchanged.

## Adapter responsibilities

- `SparkScanBuilderAdapter`: selects the legacy Filter scan on 3.2; default keeps V2 filters and
  LIMIT pushdown. Fluss planners and batch readers remain common. ARROW pruning accepts safe
  scalar comparisons, but all filters remain Spark residuals because pruning is batch-level.
- `SparkMicroBatchStreamAdapter`: an abstract superclass with a stable no-argument constructor
  and `currentEndOffset(): Offset` hook. Default owns admission control and AvailableNow; 3.2
  owns the legacy latestOffset callback. Checkpoint, offset retrieval and partition planning
  remain in the shared stream implementation.
- `SparkSqlAdapter`: timestamp-without-time-zone access, executable RuntimeReplaceable nodes,
  and version-specific Origin/ParseException construction. Parser, TVF and type conversion
  business logic remains common.
- `SparkNamespaceAdapter`: adapts dropNamespace method signatures and namespace-listing behavior.
  The 3.2 variant retains the two-argument binary entry point used by shared Scala forwarders.

The former `SparkFilterPushDownAdapter` only forwarded V2-specific operations without isolating
3.2 differences. It was removed; default V2 scan builders call their existing utilities directly.
Partition conversion uses equivalent stable SQL types instead of newer internal physical types.
No separate 3.2 copies of parser, type visitors, TVF, namespace, procedure execution, streaming
write, projection helpers, partition helpers, or micro-batch business logic remain.

## Tests and Maven lifecycle

The shared `fluss-spark-ut` module remains the reusable test JAR. Spark 3.2 owns its version
fixtures under `fluss-spark-3.2/src/test`; there is no separate `fluss-spark-3.2-ut` module.
Shared test dependencies are excluded transitively and Spark 3.2 runtime/test JARs are selected
explicitly. Fluss test dependencies remain ahead of Spark SQL so the cluster loads Fluss's
RocksDB JNI, preserving the previous test-module classpath order. The unchanged shared StreamingTest source is recompiled against Spark 3.2 because
the upstream StreamTest test-library ABI differs. Catalog and TVF cases use small subclasses
with release-specific expectations; default Spark 3.4/3.5 coverage remains unchanged.

- `test`: runs 171 ScalaTest cases, including shared behavior and adapter contracts.
- `package`: assembles the shaded connector JAR.
- `integration-test`: the existing Surefire execution runs the JUnit5 Spark32ArtifactITCase
  bridge, which executes 7 ScalaTest acceptance cases (artifact selection, batch queries and
  checkpoint recovery). Its main classpath entry is the final JAR, and separate common/client
  JARs are excluded. Class-origin assertions prove that the catalog, client and all selected
  adapters came from the exact artifact built in this invocation.
- `verify`: succeeds only after the preceding acceptance execution succeeds. Failures in the
  bridge propagate to Maven; no failure-ignore option is used.

Unit and artifact reports are separate (`target/surefire-reports` and
`target/artifact-test-reports`). Acceptance cases are excluded from the ordinary ScalaTest suite
list, so moving them after package does not duplicate the 178-case behavior coverage. The Java JUnit
bridge method is a wrapper, not an additional connector behavior case. All acceptance suites
run in one ScalaTest Runner invocation. Surefire uses the classpath rather than the module path
because shared and version-specific test classes use the same packages.

Shared V2 read suites that require V2 Predicate objects or pushed LIMIT are not reused unchanged
on 3.2, which lacks those capabilities. Dedicated SQL tests retain result-correctness coverage.

## Reproduce

Use JDK 11 from the repository root. If shared artifacts are not installed yet:

```bash
mvn -pl fluss-spark/fluss-spark-common,fluss-spark/fluss-spark-ut install -DskipTests
```

Then run the Spark 3.2 module directly:

```bash
mvn -Pinternal-spark-3.2 -pl fluss-spark/fluss-spark-3.2 clean test
mvn -Pinternal-spark-3.2 -pl fluss-spark/fluss-spark-3.2 clean verify
```

`test` does not perform packaged-artifact acceptance; `verify` does. Adding `-am` also runs the
selected lifecycle in upstream modules, including the shared UT module. Activate the profile
with `-pl` to limit the selected modules; the profile alone does not disable other Spark lines.
The `suites` property can select an ordinary ScalaTest suite for targeted runs; packaged checks
remain in the integration-test execution.

## Verification

| Runtime | Result | Evidence |
| --- | --- | --- |
| Spark 3.2.4 / Scala 2.12.18 / Java 11.0.30 | 178/178 distinct cases passed; 171 ordinary + 7 packaged acceptance | 164 shared cases, 14 adaptation cases; only Spark 3.2.4 runtime JARs |
| Spark 3.4.3 / Java 11.0.30 | 76 distinct regression cases passed | Catalog 9, log 25, PK 14, write 3, streaming 6, TVF 8, procedure 7, adapter 4 |
| Spark 3.5.7 / Java 11.0.30 | 288 distinct cases passed across regression runs | 131 cases in completed suites from the initial full run, plus 184 in the final remainder, minus 27 repeated converter cases |

After merging the test module, the full ordinary phase passed 171/171 in `clean verify`.
The integration bridge initially exposed a Surefire module-path split-package failure; its
configuration was corrected to use the classpath. The final targeted `verify` passed all 7
packaged acceptance cases plus 5 repeated adapter contracts, with Checkstyle, Spotless and
RAT enabled. This is coverage across runs, not a claim that the initial full invocation passed.
An earlier negative artifact-origin check failed Maven as expected, confirming that acceptance
failures cannot silently pass the build.

The final Spark 3.5 remainder completed 184/184 with no failed, cancelled, ignored or pending
cases. This is full distinct-case coverage assembled across runs, not a claim that the initial
single-process full run succeeded.

The original common at commit 8353c448 also reproduced the existing PK fixture failure after
CREATE and before the first empty scan. PK fixtures now use independent table names and
FlussClusterExtension.waitUntilTableReady before the empty projection scan. All schemas,
values, queries apart from table names, and assertions are preserved. The final Spark 3.4 and
Spark 3.5 PK runs both pass 14/14 without the earlier metadata failures or flush stalls. No
client/server production code was changed to work around those failures.

Spotless, Checkstyle, root Maven validate (including RAT), and diff checks passed. Standards
and spec reviews have no remaining findings; the fixture isolation delta was reviewed too.
Spark 3.2 still emits the existing ANTLR generator/runtime version notice when reusing the
common parser; its shared parser/procedure tests pass.

Spark 3.2 cannot represent Fluss local TIMESTAMP columns; use TIMESTAMP_LTZ or a newer Spark
runtime. AvailableNow is absent from Spark 3.2; use ProcessingTime or Once. Full streaming-write
qualification remains ticket 10.
