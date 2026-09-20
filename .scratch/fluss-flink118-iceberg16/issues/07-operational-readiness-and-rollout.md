# 07: Complete operational readiness and staged rollout

**What to build:** An operable release with monitoring, a supported small-file maintenance path, and a staged rollout and rollback procedure for the internal Fluss-to-Iceberg tiering service.

**Blocked by:** 05: Qualify primary-key, partition, and schema tiering semantics; 06: Prove tiering recovery and external-writer boundaries.

**Status:** ready-for-agent

- [ ] Operators can observe tier lag, commit failures, snapshot growth, data-file growth, and tiering temporary-disk use.
- [ ] Clean-schema Iceberg table maintenance uses an explicit supported maintenance workflow rather than assumed tiering auto-compaction.
- [ ] The release defines staging verification, low-risk-table rollout, pause thresholds, rollback steps, and the prior compatible artifact set.
- [ ] A staging deployment proves the runbook before production rollout begins.
