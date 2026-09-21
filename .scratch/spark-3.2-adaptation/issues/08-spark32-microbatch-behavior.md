# 08: Deliver Spark 3.2 micro-batch behavior

**What to build:** An internal Spark 3.2 user can run standard Fluss micro-batch reads through checkpoint recovery, with unsupported AvailableNow behavior made explicit.

**Blocked by:** 06: Assemble the Spark 3.2 adapter artifact.

**Status:** resolved

- [x] A standard micro-batch query resumes from its checkpoint after restart.
- [x] AvailableNow is rejected or documented at the adapter seam without linkage errors.
- [x] Spark 3.4 and 3.5 streaming behavior remains unchanged.

## Completion evidence

Validated append and PK checkpoint restart without replay, plus all six shared streaming cases. AvailableNow is explicitly excluded on Spark 3.2; default adapter contracts verify admission control and frozen bounds. The common stream implementation is shared.
