# 02: Deliver the Flink 1.18 Fluss connector smoke path

**What to build:** A validated connector artifact for the internal Flink 1.18.2 build that a representative Flink job can load to read and write a regular Fluss table without runtime linkage errors.

**Blocked by:** 01: Lock the internal compatibility matrix and build gate.

**Status:** ready-for-agent

- [ ] The internal Flink 1.18.2 build deployment loads exactly one compatible Fluss connector artifact.
- [ ] A representative job reads and writes a non-Lake Fluss table successfully.
- [ ] The deployment check detects duplicate or incompatible Fluss and Flink classes before job submission.
