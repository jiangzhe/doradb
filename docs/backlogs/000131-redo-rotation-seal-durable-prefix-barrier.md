# Backlog: Make redo log rotation seal a durable prefix barrier

## Summary

Redo recovery currently has an ambiguity across log rotation boundaries: rotated-file sealing is side work, not part of the durable commit prefix, while replay treats an unsealed checksum/tail condition as a stream boundary. This can let a newer file contain physically persisted and even previously published transactions while an older unsealed file stops recovery before those newer records are considered. Fix the redo durability and recovery model so unsealed tails, seal persistence, runtime initialization, and repeated recovery runs have one coherent invariant.

## Reference

Deferred from the redo direct-IO read-ahead task after reviewing RedoLogStream tail handling and log rotation ordering.

Relevant current code shape:
- doradb-storage/src/log/mod.rs: RedoLogWriter::finalize_finished_prefix dispatches SealDispatch as side work, then the following new-file header can complete and later group prefixes can publish without waiting for old-file seal persistence.
- doradb-storage/src/log/mod.rs: RedoLog::enqueue_precommit_group rotates on StorageFileCapacityExceeded, creates a new file, and enqueues LogFileBoundary with ended_log_file plus the new file header write.
- doradb-storage/src/recovery/stream.rs: SegmentReadState::read_group_start treats checksum failure in a group-start block as LogFileCorrupted, while append_continuation_payload treats checksum/deser failure in an unsealed continuation block as IncompleteTail.
- doradb-storage/src/recovery/stream.rs: RedoReplayPlanner plans a suffix of discovered files but does not enforce that only the final planned segment may use unsealed crash-tail EOF semantics.
- doradb-storage/src/conf/trx.rs and doradb-storage/src/log/mod.rs: prepare_recovery computes next_file_seq before recovery; RedoLogInitializer::finish creates the next runtime file with create_or_fail, so recovery-side truncation/reuse of an existing unsealed tail file must be coordinated before finish.
- docs/redo-log.md documents incomplete open groups as discarded crash tails, but does not yet define durable-prefix seal semantics across rotation or multi-run recovery repair.

## Deferred From (Optional)

docs/tasks/000188-redo-direct-io-read-ahead-reader.md; docs/rfcs/0021-redo-log-fixed-block-read-write-path.md phase 3; docs/redo-log.md

## Deferral Context (Optional)

- Defer Reason: This is larger than the current direct-IO read-ahead cleanup because it changes redo durability semantics, log rotation prefix ordering, recovery repair behavior, startup initializer sequencing, and repeated recovery guarantees. It should be planned as a correctness task rather than folded into the reader refactor.
- Findings:
  Findings from current investigation:
  
  - Current write path can physically persist new-file records without the old file's seal being guaranteed durable, because seal work is staged by LogFileSealer as side work while the new-file header acts as the prefix barrier for later groups.
  - Current recovery can stop at an old unsealed file and ignore a newer file. RedoLogStream maps unsealed zero/incomplete tail to ReplayEof, which stops the whole stream, and planning does not currently prove the unsealed segment is the newest planned segment.
  - Repeated recovery can hit the same unsealed ambiguity again because recovery does not currently seal accepted unsealed files before RedoLogInitializer::finish opens the next runtime file.
  - Tail checksum policy is inconsistent by block role: read_group_start maps checksum failure to LogFileCorrupted, while append_continuation_payload maps checksum/deser failure to IncompleteTail for unsealed files.
  - RedoLogInitializer currently creates a new file with create_or_fail using next_file_seq derived before replay. Recovery repair that truncates/reuses an existing tail file must update initializer state or provide a finish path that can recreate the chosen active file.
  - SparseFile::create_or_trunc exists, so truncating/recreating a tail file is mechanically available, but must be integrated carefully with discovered descriptors and next-file sequencing.
- Direction Hint:
  Recommended direction for future planning:
  
  - Treat seal persistence as an ordered durability barrier at rotation boundaries. The prefix should not complete new-file transactions until the old-file seal write and required sync policy have succeeded.
  - Define a recovery-side sealing routine that scans unsealed files to the last complete checksum-valid group, records min/max redo CTS for accepted groups, writes the inactive sealed super-block slot, syncs according to the required recovery policy, and excludes the incomplete/checksum-failed group from durable_end_offset.
  - Make RedoLogStream or a lower-level scanner return enough terminal metadata for recovery sealing: last accepted end offset, redo CTS range, and whether termination was clean file end, zero tail, checksum/deser tail, or corruption.
  - Make the unsealed-file policy explicit. Enforce at runtime that there cannot be an arbitrary chain of unsealed rotated files, and make recovery reject or truncate any sequence shape outside the supported invariant.
  - Design multi-run tests first. Build fixtures that run recovery, drop the engine, and recover again to prove repair persists and the second recovery does not depend on in-memory state from the first run.

## Scope Hint

Design and implement the redo correctness fix across write, recovery, and startup boundaries:

1. Make single-block and multi-block crash-tail semantics consistent. A checksum/deserialization failure at the first block of an unsealed tail group and a checksum/deserialization failure in a continuation block should both mean the durable replay prefix ends before that group, not one case hard-corrupting recovery and the other succeeding.

2. Make rotated-file seal persistence part of the durable prefix. New-file header writes and new-file group writes may be physically submitted, but transactions in the new file must not be published as durable/committed until the previous file's seal metadata is durably persisted. Seal failure must poison/fail the relevant prefix before new-file transactions are completed.

3. Make successful recovery repair unsealed log files before initializing runtime redo. Recovery should seal accepted unsealed files up to the last checksum-valid complete group, excluding any checksum-failed or incomplete group from the sealed durable_end_offset. If the newest unsealed file is only a discarded tail/new runtime candidate, truncate or recreate it before RedoLogInitializer::finish so startup leaves only the new active unsealed file.

4. Establish and enforce the unsealed-file invariant. Prefer a runtime invariant that at most the previous rotated file and current active file can be unsealed after a crash. Recovery should still be defensive if more unsealed files are discovered: either reject as corruption or truncate/drop everything after the accepted durable boundary by explicit policy.

5. Update docs/redo-log.md to describe rotation seal as a durable-prefix barrier, the unsealed-tail repair process, and repeated recovery guarantees.

## Acceptance Hint

Acceptance criteria:

- A checksum failure in an unsealed group-start block at the crash tail is treated consistently with a checksum failure in an unsealed continuation block: recovery succeeds by discarding that incomplete group and everything after the accepted durable prefix.
- A checksum failure or incomplete group in a sealed segment remains corruption unless the segment is skipped as obsolete by replay-floor planning.
- Transactions in a new redo file are not reported committed/durable unless the previous file's seal metadata is durably persisted as part of the ordered prefix.
- After successful recovery, all replayed/accepted historical redo files are sealed up to the accepted durable boundary, and runtime starts with exactly one active unsealed redo file.
- If recovery sees two unsealed files, it can recover deterministically: accepted records before the tail are sealed, the non-durable tail/newest file is truncated or recreated as the runtime file according to the final design, and a second recovery observes the same durable state without re-stopping at the old ambiguity.
- Multi-time recovery tests cover one unsealed file, two unsealed files, a file sealed by recovery, checksum-failed group-start tail, checksum-failed continuation tail, and a second recovery after a successful repair.

## Notes (Optional)


## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
