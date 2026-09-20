# 03: Deliver the service-side Iceberg Catalog path

**What to build:** A Fluss server distribution that discovers the Iceberg lake plugin and uses the internal Catalog and FileIO implementation to create and validate a new Lake-enabled table.

**Blocked by:** 01: Lock the internal compatibility matrix and build gate.

**Status:** ready-for-agent

- [ ] The Fluss service distribution includes the compatible Iceberg lake plugin and its internal Catalog/FileIO dependency closure.
- [ ] A service-side configuration using the `datalake.iceberg.*` contract initializes the internal Catalog without embedded credentials.
- [ ] Fluss creates and validates a new Lake-enabled table with the supported schema, partition, and bucket contract.
