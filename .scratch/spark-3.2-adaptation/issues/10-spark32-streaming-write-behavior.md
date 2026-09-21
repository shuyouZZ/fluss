# 10: Deliver Spark 3.2 streaming writes

**What to build:** An internal Spark 3.2 user can write Fluss data through Structured Streaming using the shared writer lifecycle without invoking unavailable Spark APIs.

**Blocked by:** 06: Assemble the Spark 3.2 adapter artifact.

**Status:** resolved

- [x] The packaged Spark 3.2 artifact reuses compatible streaming writers; add a version adapter only if a real incompatible API invocation requires one.
- [x] Append and primary-key streaming writes create the existing Fluss data writers and preserve their commit and abort behavior.
- [x] The Spark 3.2 execution path does not invoke the unavailable StreamingWrite.useCommitCoordinator API; retain each runtime's native coordinator policy.
- [x] Spark 3.4 and 3.5 retain their existing streaming-write behavior.

## Clarification from API qualification

Spark 3.2.4 and 3.4.3 both use BatchWrite's default coordinator policy. Only 3.5.7
calls StreamingWrite.useCommitCoordinator. Fluss's existing extra method is binary-compatible
on the older releases; adding an empty adapter would not change Spark's policy or improve
compatibility. This replaces the original implementation-prescriptive adapter requirement
under the latest minimal-change direction.

The packaged gate reuses SparkStreamingTest and adds writer lifecycle checks for append and
primary-key tables. Shared FlussDataWriterTest covers successful flush acknowledgement and
propagating asynchronous write failures with their original cause. Normal checkpoint restart
is qualified; transaction rollback and end-to-end exactly-once are not promised.

## Completion evidence

The final full clean verify passed all 15 packaged cases, including six reused streaming
cases and two real writer lifecycle cases. The shared two-case writer contract passed on
Spark 3.2, 3.4 and 3.5; its negative regression reproduced the lost asynchronous failure
before the one-line initialization fix. See [implementation-notes.md](../implementation-notes.md)
and [compatibility-matrix.md](../compatibility-matrix.md) for evidence and semantic limits.
