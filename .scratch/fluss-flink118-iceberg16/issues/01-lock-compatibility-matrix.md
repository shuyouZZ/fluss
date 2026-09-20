# 01: Lock the internal compatibility matrix and build gate

**What to build:** An internal build profile or BOM that resolves the selected Fluss baseline, the internal Flink 1.18.2 build (Maven version `1.18.2-SNAPSHOT`), and Iceberg 1.6.1 consistently for the connector, tiering job, Iceberg lake adapter, and Fluss distribution.

**Blocked by:** None (can start immediately).

**Status:** completed

- [x] The selected compatibility matrix is represented by one internal source of truth and all target artifacts consume it.
- [x] The shared Flink common module remains on its upstream compatibility baseline; the version-specific Flink 1.18 connector remains the adaptation boundary.
- [x] A build proves that no Flink 1.20 or Iceberg 1.10 dependency remains in the target artifact closure.
- [x] Dependency convergence, duplicate-class, and service-provider-resource checks are part of the target build gate.
