# 05: Qualify primary-key, partition, and schema tiering semantics

**What to build:** End-to-end qualification of the supported Fluss-to-Iceberg semantics for primary-key updates and deletes, partitioned tables, union reads, and supported schema evolution on the locked matrix.

**Blocked by:** 04: Deliver the first Iceberg append tiering path.

**Status:** ready-for-agent

- [ ] Primary-key updates and deletes produce the expected visible Iceberg table state.
- [ ] Partitioned Lake-enabled tables tier and remain readable through the supported union-read path.
- [ ] Supported schema evolution remains compatible between Fluss and Iceberg.
- [ ] Incompatible partition specifications, bucket changes, and unsupported schema changes fail explicitly.
