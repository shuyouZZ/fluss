# Internal Fluss tiering on Flink 1.18 and Iceberg 1.6

Status: ready-for-agent

## Problem Statement

The internal platform needs to operate Fluss with Lake tiering on its Flink 1.18.2 internal build (Maven version `1.18.2-SNAPSHOT`) and Iceberg 1.6.1. The current Fluss baseline provides a Flink 1.18 connector but its Lake and tiering runtime defaults target newer Flink and Iceberg versions. Deploying the existing artifacts unchanged would create an unverified version matrix and can expose duplicate classes or an undiscoverable Iceberg lake plugin.

The platform needs an internally supportable release that preserves Fluss tiering recovery semantics, uses the internal Iceberg Catalog and FileIO implementation, and gives operators a reproducible deployment and rollback path.

## Solution

Create an internal maintenance line with one locked compatibility matrix: the selected Fluss baseline, internal Flink 1.18.2 build (`1.18.2-SNAPSHOT`), and Iceberg 1.6.1. Build and test dedicated internal artifacts for the Flink connector, Lake tiering job, Iceberg lake adapter, and Fluss distribution.

Run tiering in a dedicated Flink 1.18.2 internal-build deployment. Install the Fluss connector and the Iceberg lake adapter with the same Catalog, FileIO, and Hadoop client dependencies used by the Fluss service-side Iceberg plugin. Submit a separate tiering job to that deployment. Treat the tiering job, from configuration parsing through Iceberg commit and Fluss LakeSnapshot acknowledgement, as the primary observable acceptance seam.

Configure the Iceberg Catalog exclusively through the existing `datalake.iceberg.*` contract. Generate both Fluss service configuration and tiering-job arguments from one logical internal configuration source, including credential references, warehouse, Catalog, FileIO, and temporary I/O directories.

## User Stories

1. As a platform engineer, I want a single approved Fluss, Flink, and Iceberg version matrix, so that every environment resolves the same supported artifacts.
2. As a platform engineer, I want the Fluss connector compiled for the internal Flink 1.18.2 build, so that business jobs can use Fluss without runtime linkage errors.
3. As a platform engineer, I want the tiering job compiled and tested for the internal Flink 1.18.2 build, so that lake tiering does not rely on the newer Flink runtime used by upstream defaults.
4. As a platform engineer, I want the Iceberg lake adapter bound to Iceberg 1.6.1, so that the service and tiering planes share the approved Iceberg behavior.
5. As a release engineer, I want version locking to be represented by an internal build profile or BOM, so that CI and release builds cannot silently drift by command-line overrides.
6. As a Fluss operator, I want the Fluss service distribution to load the Iceberg lake plugin, so that Fluss can create, validate, and read Lake-enabled tables.
7. As a tiering operator, I want the Flink tiering deployment to discover the same Iceberg lake plugin, so that the tiering job can create writers and committers for the `iceberg` format.
8. As a platform engineer, I want a dedicated tiering Flink deployment, so that its Iceberg dependency set does not conflict with arbitrary business-job dependencies.
9. As an internal Catalog owner, I want the Catalog and FileIO implementation to be visible to both Fluss and the tiering job, so that table metadata and data files resolve consistently.
10. As a security operator, I want credentials and endpoints sourced from one logical configuration model, so that credentials are not duplicated between Fluss and tiering-job configuration.
11. As a table owner, I want Fluss-managed Lake tables to retain a stable schema, partition, and bucket contract, so that tiering can write without format incompatibility.
12. As a table owner, I want append, primary-key upsert, and delete behavior preserved in Iceberg, so that Lake data remains semantically equivalent to the corresponding Fluss table.
13. As a reliability engineer, I want tiering to recover after failures before and after an Iceberg commit, so that data is neither lost nor made incorrectly visible.
14. As an operator, I want Fluss-owned Iceberg commits distinguishable from external commits, so that recovery and incident diagnosis can identify tiering progress.
15. As a data platform owner, I want an explicit policy for external writers to Fluss-managed Iceberg tables, so that concurrent schema and commit ownership is not ambiguous.
16. As an operator, I want small-file maintenance assigned to a supported Iceberg maintenance mechanism, so that enabling tiering does not imply unsupported automatic compaction.
17. As a release engineer, I want classpath and ServiceLoader checks in CI, so that plugin-discovery failures are detected before deployment.
18. As an operations engineer, I want tier lag, commit failures, snapshot growth, and file growth monitored during rollout, so that the rollout can be paused before service objectives are affected.
19. As a platform engineer, I want a rollback-compatible release record, so that the previously approved matrix can be restored without mixing artifact versions.

## Implementation Decisions

- The supported matrix is the selected internal Fluss baseline, the Flink 1.18.2 internal build with Maven version `1.18.2-SNAPSHOT`, and Iceberg 1.6.1. Any later change to one member is a new compatibility qualification, not a patch-level deployment choice.
- Version selection is centralized in an internal build profile or BOM. The relevant connector, Lake, and tiering modules consume that single source of truth.
- The shared Flink common module remains compiled against its upstream Flink 1.20 compatibility baseline. Flink 1.18 compatibility is supplied by the existing version-specific connector adapter module; the internal profile must not attempt to recompile the common module against Flink 1.18.
- The internal release produces four independently identifiable artifacts: a Flink 1.18 connector, a tiering job, an Iceberg lake adapter, and a Fluss server distribution containing the Iceberg plugin.
- The Iceberg-only server distribution is a separate internal module so the default Fluss distribution retains its full filesystem, metrics, and lake-plugin contents.
- Tiering runs on a dedicated Flink deployment. Its parent-visible classpath contains exactly one compatible Fluss connector and one Iceberg lake adapter dependency closure.
- The dedicated tiering deployment does not include the native Iceberg Flink runtime by default. If native Iceberg SQL is required in that deployment, classpath ownership and compatibility become a separately approved decision.
- The Fluss service-side plugin and tiering-side adapter use the same Iceberg version and the same internal Catalog, FileIO, filesystem, authentication, and Hadoop client dependencies.
- The internal Catalog and FileIO are configured through the existing `datalake.iceberg.*` property namespace. Their implementation classes are supplied as platform dependencies rather than hard-coded into Fluss.
- A configuration renderer derives Fluss service configuration and tiering-job parameters from one logical source. Secrets are referenced through the platform's approved secret mechanism and are not copied into artifacts.
- The primary acceptance seam is an end-to-end tiering job: it discovers the `iceberg` LakeStoragePlugin, obtains a LakeTieringFactory, writes Iceberg files, commits a snapshot, and records the corresponding Fluss LakeSnapshot.
- New Lake-enabled tables are created and evolved through Fluss. Existing Iceberg-table adoption and migration are excluded from the initial release.
- The release supports the Fluss Iceberg schema, partition, and bucket contract. Unsupported bucket rescaling, incompatible partition specifications, and unmanaged concurrent schema changes fail explicitly.
- External writers may read Fluss-managed Iceberg tables. External writes require a documented ownership protocol before enablement, including commit-concurrency and schema-change rules.
- Tiering recovery remains based on Fluss LakeSnapshot progress and Iceberg snapshots tagged as Fluss tiering commits. The tiering job must preserve the full-restart behavior required by its stateless committer path.
- Automated compaction for clean-schema Iceberg tables is not assumed. Small-file maintenance is executed by a separately operated Iceberg maintenance workflow.

## Testing Decisions

- Tests validate observable contracts, not internal object construction: successful plugin discovery, Catalog initialization, tiering output, Iceberg visibility, Fluss LakeSnapshot progress, recovery behavior, and classpath consistency.
- The highest test seam is the tiering job integrated with a Fluss cluster and an Iceberg Catalog. Existing Fluss tiering integration tests and Iceberg Lake tests are the primary test patterns.
- Build verification covers dependency convergence, dependency-tree inspection, duplicate-class detection, service-provider resource preservation, and package-level smoke startup on the internal Flink 1.18.2 build.
- Functional verification covers append-only tables, primary-key tables with updates and deletes, partitioned tables, union reads, and supported schema evolution.
- Recovery verification injects failures before the Iceberg commit, after Iceberg commit but before Fluss acknowledgement, and during transient Catalog or FileIO failures. Each scenario proves that a restart resumes correctly without data loss or incorrect duplicate visibility.
- Catalog verification uses the internal Catalog, authentication, and FileIO implementation rather than an in-memory-only substitute.
- Operational verification measures tier lag, tiering failures, Iceberg snapshot counts, data-file counts, and temporary-disk use under representative load.
- A staging rollout verifies the dedicated deployment topology before production traffic is enabled. Production rollout begins with low-risk newly created tables and uses a defined pause and rollback threshold.

## Out of Scope

- Upgrading Flink beyond the internal 1.18.2 build baseline.
- Upgrading Iceberg beyond 1.6.1.
- Migrating existing externally managed Iceberg tables into Fluss ownership.
- Supporting arbitrary external writers to Fluss-managed Iceberg tables without an approved ownership protocol.
- Making unsupported tiering-managed compaction work for clean-schema Iceberg tables.
- Replacing the internal Catalog, FileIO, filesystem, or secret-management platform.
- Changing Fluss core table semantics, RPC contracts, or server storage architecture beyond compatibility fixes required by the locked matrix.

## Further Notes

- The internal Iceberg source checkout must be pinned to the approved 1.6.1 source or artifact baseline; the currently available local Iceberg checkout is not evidence of that version.
- The current Fluss checkout path used for this work is the local workspace checkout, not the alternate path mentioned initially.
- The first implementation ticket must establish the build matrix and prove compile-time compatibility before deployment, Catalog integration, or feature expansion tickets begin.
