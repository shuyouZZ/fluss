# 04: Deliver the first Iceberg append tiering path

**What to build:** A dedicated tiering deployment on the internal Flink 1.18.2 build that discovers the Iceberg LakeStoragePlugin, tiers an append Fluss table into Iceberg, and records the committed LakeSnapshot in Fluss.

**Blocked by:** 02: Deliver the Flink 1.18 Fluss connector smoke path; 03: Deliver the service-side Iceberg Catalog path.

**Status:** ready-for-agent

- [ ] The dedicated tiering deployment has one compatible connector and Iceberg adapter dependency closure visible to plugin discovery.
- [ ] A tiering job obtains the Iceberg LakeTieringFactory and commits append data to the internal Iceberg Catalog.
- [ ] The resulting Iceberg snapshot is readable and Fluss records corresponding LakeSnapshot progress.
- [ ] The same logical Lake configuration produces the required service and tiering-job settings.
