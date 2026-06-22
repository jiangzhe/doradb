---
id: 000186
title: Redo Request-Id Prefix Scheduler
status: proposal
created: 2026-06-21
github_issue: 749
---

# Task: Redo Request-Id Prefix Scheduler

## Summary

Implement RFC-0021 Phase 1 by replacing the redo writer's CTS-keyed IO
scheduling identity with writer-generated request ids and explicit prefix
state. The scheduler must preserve the current redo format, one physical write
per redo-bearing commit group, mmap recovery, ordered commit visibility,
no-log group behavior, fatal cleanup, file rotation, sealing, and shutdown
drain semantics.

The result should make physical IO request identity independent from
transaction metadata. Commit groups remain logical ordered units that carry CTS
for transaction publication and durable metadata, while physical writes,
headers, and seal writes are tracked by `RedoRequestId`. This prepares
RFC-0021 Phase 2 to attach multiple fixed-block writes to one logical group
without another scheduler rewrite.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Parent RFC:
- docs/rfcs/0021-redo-log-fixed-block-read-write-path.md

RFC Phase:
- Phase 1: Request-Id Prefix Scheduler

Source Backlogs:
- docs/backlogs/000127-redo-io-request-id-prefix-sequencing.md

RFC-0021 Phase 1 starts from the RFC-0020 sealed-segment redo baseline. Today
`RedoLogWriter` tracks group IO in `BTreeMap<TrxID, SyncGroup>`, while initial
and rotation header writes use side queues/counters and rotated-file seal
writes use `RedoFileSealer` side state. That split is workable for the current
one-group one-write model, but it cannot naturally represent future
multi-block groups because CTS is transaction metadata, not physical IO request
identity.

This task resolves the RFC phase-local scheduler choices:
- use a `VecDeque`-backed logical prefix tracker for ordered commit
  publication;
- assign writer-generated `RedoRequestId` values to physical redo write,
  header, and seal submissions;
- keep metadata writes capacity-consuming through the existing
  `LogWriteDriver`/`SubmissionDriver` path;
- avoid broad completion-drain or sync-batching API changes in this phase.

Following phase preserved:
- RFC-0021 Phase 2 can model one logical group as multiple fixed-block write
  requests, aggregate those request completions in the group entry, and publish
  the group only when the commit prefix reaches it.

No RFC phase-plan edit is required. The accepted design keeps Phase 1
scheduler-only and preserves Phase 2 prerequisites.

## Goals

- Introduce a small `RedoRequestId` generated only by the redo log thread.
- Make completions identify physical requests by request ownership instead of
  `TrxID`.
- Replace the CTS-keyed group inflight map with logical prefix entries ordered
  by scheduler insertion order.
- Keep `SyncGroup.max_cts` and real redo CTS ranges as transaction/durability
  metadata only; do not use CTS as scheduler identity.
- Model no-log groups as logical prefix entries that are ready immediately and
  have no physical request id.
- Model redo-bearing groups as logical prefix entries with request aggregation.
  Phase 1 groups own exactly one write request, but the structure must allow
  Phase 2 groups to own multiple requests.
- Model header writes as prefix entries because groups in a file must not be
  published before that file's super-block header is durable.
- Dispatch rotated-file seal work from the prefix after the old file's durable
  group metadata is complete, but complete the seal as side metadata work so it
  does not block new-file group publication.
- Preserve current fatal semantics: redo write/header/seal or sync failures
  poison storage, close admission, fail not-yet-published precommit groups, and
  retain in-flight buffers until their completions return.
- Preserve clean shutdown behavior, including pending redo drain and
  best-effort active-file sealing.

## Non-Goals

- Do not change the persisted redo data format.
- Do not split commit groups across fixed-size redo blocks.
- Do not change commit-group admission policy.
- Do not replace `MmapLogReader` or change recovery APIs.
- Do not implement sync batching or timed group-commit waits.
- Do not add cross-file logical groups.
- Do not implement redo log truncation/deletion.
- Do not require `SubmissionDriver` wait-many or nonblocking drain APIs unless
  a very small local wrapper is necessary for clean ownership flow.

## Plan

1. Add request and prefix identity types in `doradb-storage/src/log/mod.rs`.
   - Add a compact `RedoRequestId` newtype over `u64`.
   - Add a compact `RedoPrefixEntryId` newtype over `u64`.
   - Add request-owner metadata carried inside `LogWriteSubmission`, for
     example:

     ```rust
     struct RedoRequestOwner {
         request_id: RedoRequestId,
         entry_id: Option<RedoPrefixEntryId>,
         kind: RedoRequestKind,
         group_write_idx: Option<usize>,
     }
     ```

   - Keep the owner inside the returned `LogWriteSubmission` so completion
     handling does not need a separate `request_id -> entry_id` map.
   - Use `entry_id: None` for side metadata work that is not part of the
     transaction publication prefix, such as pending rotated-file seal writes.

2. Assign request ids in the redo log thread, not in foreground admission.
   - Add `next_request_id` to `RedoLogWriter` or to a small scheduler object it
     owns for the duration of the log thread loop.
   - Assign ids when writer-owned scheduler work becomes a physical IO request:
     header write, group write, and side seal write.
   - If the backend driver has no capacity, keep the same request id attached
     to the pending submission and retry later.
   - Do not derive request ids from CTS, file offsets, or backend tokens.

3. Replace `inflight: BTreeMap<TrxID, SyncGroup>` with a prefix tracker.
   - Introduce a structure similar to:

     ```rust
     struct RedoPrefixTracker {
         front_entry_id: RedoPrefixEntryId,
         next_entry_id: RedoPrefixEntryId,
         entries: VecDeque<RedoPrefixEntry>,
     }
     ```

   - Locate completed entries by computing the deque index from
     `entry_id - front_entry_id`, validating that the index is in range and the
     stored id matches.
   - Keep entry lookup O(io_depth). A secondary entry map is unnecessary unless
     implementation finds the deque math makes the code less clear.

4. Define prefix entries around logical publication, not physical IO.
   - `RedoPrefixEntry::Header` owns a header completion and one request id. It
     is ready after the header write succeeds.
   - `RedoPrefixEntry::Group` owns one `SyncGroup` and a request aggregation
     counter. It is ready when all group requests have completed successfully.
     In Phase 1, redo-bearing groups have one request and no-log groups have
     zero requests.
   - `RedoPrefixEntry::SealDispatch` is a zero-IO marker. It owns the ended
     `RedoLogFile` and the accumulator snapshot needed to build the sealed
     header after all earlier old-file groups are durable.
   - Do not make rotated-file seal completion a prefix entry. Once
     `SealDispatch` is reached after old-file group publication, enqueue a side
     `PendingSeal` request and pop the marker so the new-file header and groups
     can continue through the normal prefix.

5. Update `SyncGroup` to be request-aggregation friendly.
   - Keep existing transaction fields, completion, `max_cts`, `log_fd`,
     `log_bytes`, and `write_meta`.
   - Replace the single completion flag with request aggregation state such as
     `outstanding_requests`.
   - Keep returned write buffers in the group until finalization can recycle
     them.
   - Preserve no-log groups as immediately ready, zero-byte entries.
   - Preserve failure state so a failed group ends the durable transaction
     prefix, while later in-flight buffers are still recovered when their
     physical completions return.

6. Keep seal work side-tracked but request-id based.
   - Change `RedoFileSealer` from independent submission/counter state into a
     side metadata queue/tracker that uses `LogWriteSubmission` with
     `RedoRequestOwner`.
   - Add a side `PendingSeal` state for a submitted seal write. It should own:
     - `request_id`;
     - ended `RedoLogFile`;
     - sealed-header target/offset/buffer ownership through the submission;
     - enough context to run the configured seal sync after write completion.
   - Seal write completion should:
     - validate the write result;
     - run `fsync`, `fdatasync`, or no sync according to `log_sync`;
     - drop the ended file only after seal write/sync completion;
     - poison storage and fail not-yet-published work on failure.
   - Already published commit groups remain published if a later side seal
     failure arrives, matching current fatal-poison semantics for asynchronous
     rotated-file seal work.

7. Rewrite submission flow around the prefix tracker.
   - Convert fetched `Commit::LogFileBoundary` into ordered prefix work:
     `SealDispatch` for the old file when present, then `Header` for the new
     file.
   - Convert fetched `Commit::Group` into a `Group` prefix entry.
   - Stage prefix-entry requests into `LogWriteDriver` while capacity is
     available.
   - Stage side seal requests through the same driver capacity path.
   - Keep `write_driver.submit_ready()` as the backend handoff point.

8. Rewrite completion handling.
   - `LogWriteDriver::wait_one()` should return a completion whose submission
     includes `RedoRequestOwner`.
   - Header/group completions locate their prefix entry through `entry_id`.
   - Side seal completions update `PendingSeal` directly through owner kind and
     request id.
   - A short write or backend error becomes `FatalError::RedoWrite` and enters
     the existing poison/fail-pending flow.
   - Do not complete commit waiters from IO completion handlers. IO completion
     only makes prefix entries ready; prefix finalization publishes logical
     completion.

9. Rewrite prefix finalization.
   - Walk entries from the front of the prefix tracker.
   - Remove ready headers and complete their header waiters.
   - Collect ready group entries until the first unfinished group, failed
     group, header barrier, or seal-dispatch marker.
   - Sync only a ready redo-bearing group prefix for one file descriptor; do
     not cross file boundaries in one sync operation.
   - After sync succeeds:
     - record durable redo-bearing group metadata into the current file
       accumulator;
     - advance `persisted_cts` to the last published group `max_cts`;
     - commit transactions;
     - hand committed payloads to purge before waking waiters;
     - recycle returned direct buffers;
     - update stats.
   - When `SealDispatch` reaches the front, snapshot/reset the old file
     accumulator, enqueue the side seal request, pop the marker, and continue
     without waiting for seal completion.
   - On sync failure, poison storage as `FatalError::RedoSync`, fail the ready
     group prefix, fail all later accepted groups as cleanup-only, and stop
     accepting new work.

10. Preserve shutdown and fatal cleanup semantics.
    - `fail_pending` must drain:
      - unsubmitted prefix entries;
      - side pending seal submissions that have not reached the driver;
      - group-commit queue entries still under `GroupCommit`;
      - in-flight prefix groups whose buffers may return later.
    - Shutdown must still drain pending group/header work before attempting
      best-effort active-file sealing.
    - Active-file best-effort sealing can remain a direct one-off helper if the
      driver is otherwise drained, but it should use the same request-owner
      mechanics when practical.

11. Keep documentation comments and naming aligned.
    - Update local comments in `doradb-storage/src/log/mod.rs` and
      `doradb-storage/src/trx/group.rs` so they describe request ids as IO
      identity and CTS as transaction metadata.
    - Do not update broad redo docs in this task unless comments would become
      actively misleading; RFC-0021 Phase 5 owns final documentation cleanup.

## Implementation Notes


## Impacts

- `doradb-storage/src/log/mod.rs`
  - `LogWriteSubmission`, `LogWriteKind`, and `LogWriteCompletion` should carry
    request-owner metadata instead of group CTS identity.
  - `LogWriteDriver::wait_one()` should return owner-aware completions.
  - `RedoLogWriter` should own request id allocation, prefix tracking,
    submission, completion routing, prefix finalization, and fail-pending
    updates.
  - `RedoFileSealer` should become side request-id tracked metadata work rather
    than an independent submission/counter path.
  - `shrink_inflight` should be removed or replaced with prefix-tracker
    finalization helpers.

- `doradb-storage/src/trx/group.rs`
  - `CommitGroup::into_sync_group()` should no longer create a submission whose
    scheduler identity is CTS.
  - `SyncGroup` should expose enough operations to attach one or more request
    ids, mark request completion, keep returned buffers, and report prefix
    readiness.

- `doradb-storage/src/trx/sys.rs`
  - `log_loop()` should no longer rely on special old-file drain behavior to
    make rotation safe. It should let the writer process the logical prefix
    stream and side seal work while preserving shutdown behavior.

- `doradb-storage/src/io/mod.rs`
  - No broad API change is expected. The existing `SubmissionDriver` returns
    the original submission on completion, which is enough to carry request
    ownership.

- `docs/rfcs/0021-redo-log-fixed-block-read-write-path.md`
  - No design edit is planned during task creation. During `task resolve`,
    synchronize Phase 1 task doc/issue/status and note whether implementation
    changed any phase-local choice.

- `docs/backlogs/000127-redo-io-request-id-prefix-sequencing.md`
  - This is the source backlog and should be closed during `task resolve` if
    the implementation completes the planned scheduler work.

## Test Cases

1. Request ids are assigned by the redo writer and are monotonic across header,
   group, and side seal submissions.
2. Completion of a redo-bearing group write marks only the owning group prefix
   entry ready; it does not wake the commit waiter before prefix finalization.
3. A no-log group at the front of the prefix is published without backend IO.
4. A no-log group behind an unfinished redo-bearing group does not publish
   early.
5. Out-of-order completion of later group request ids does not advance
   `persisted_cts` or wake later waiters before earlier prefix entries finish.
6. A header prefix entry blocks publication of following groups until the
   header write completes successfully.
7. Rotation converts the old file into a seal-dispatch marker and the new file
   into a header prefix entry in stream order.
8. Reaching a seal-dispatch marker after old-file groups are durable enqueues a
   side seal request and allows new-file header/group publication to continue
   without waiting for seal completion.
9. Side seal write failure poisons storage as `FatalError::RedoWrite`, closes
   admission, and fails not-yet-published groups.
10. Side seal sync failure poisons storage as `FatalError::RedoSync`, closes
    admission, and fails not-yet-published groups.
11. Already published groups remain completed if a later asynchronous side seal
    failure occurs.
12. Redo write failure on a group fails that group and later accepted groups as
    cleanup-only while retaining in-flight buffers until completion returns.
13. Header write failure completes the header waiter with a fatal completion
    error and enters the same storage-poison/fail-pending path.
14. Prefix finalization syncs only ready redo-bearing groups for one file
    descriptor and does not cross a file boundary in one sync call.
15. Durable seal accumulator state is updated only after group prefix sync
    succeeds and ignores ordered no-log groups.
16. Clean shutdown drains pending prefix work and still attempts best-effort
    active-file sealing.
17. Dropped commit waiters after handoff still finish session cleanup or
    failed-precommit cleanup through existing ownership rules.
18. Existing recovery tests continue to pass because persisted redo format and
    mmap reader behavior are unchanged.

Validation commands:

```bash
cargo nextest run -p doradb-storage
cargo nextest run -p doradb-storage --no-default-features --features libaio
```

## Open Questions

- Exact helper names and struct boundaries are implementation-local. Prefer
  keeping new types inside `log/mod.rs` unless a narrow test helper in
  `trx/group.rs` materially improves clarity.
- If implementation finds that `RedoLogWriter` lifetime across loop iterations
  makes request id allocation awkward, move the monotonic allocator into the
  long-lived log-loop state rather than deriving ids from CTS or offsets.
- If side seal failure after some newer groups have already been published
  exposes a stronger recovery guarantee requirement than current behavior, stop
  and update RFC-0021 instead of silently making seal completion a commit
  prefix barrier.
