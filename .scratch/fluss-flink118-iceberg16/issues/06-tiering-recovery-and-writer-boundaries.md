# 06: Prove tiering recovery and external-writer boundaries

**What to build:** Demonstrated recovery of the Iceberg tiering path through commit-boundary failures, plus an enforceable operating contract for external readers, writers, schema changes, and commits on Fluss-managed Iceberg tables.

**Blocked by:** 04: Deliver the first Iceberg append tiering path.

**Status:** ready-for-agent

- [ ] Failures before an Iceberg commit, after a successful Iceberg commit, and during temporary Catalog/FileIO errors recover without lost or incorrectly duplicated visible data.
- [ ] Restarted tiering resumes from Fluss LakeSnapshot and Fluss-owned Iceberg commit evidence.
- [ ] The external-writer contract states which reads, writes, schema changes, and commit-concurrency patterns are allowed.
- [ ] Tests demonstrate that unsupported concurrent ownership fails predictably rather than silently corrupting tiering progress.
