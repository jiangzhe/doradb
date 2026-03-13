---
id: 000064
title: Readonly Cache Cancellation-Safe Inflight Miss Loading
status: implemented  # proposal | implemented | superseded
created: 2026-03-12
github_issue: 419
---

# Task: Readonly Cache Cancellation-Safe Inflight Miss Loading

## Summary

Make readonly cache miss loading independent from caller future lifetime. A
readonly miss attempt should be owned by readonly inflight state rather than
by the initiating request, so request/session cancellation cannot release the
reserved frame early or fail unrelated waiters. Hard IO and validation
failures remain terminal for that shared attempt and are returned to all
callers attached to it.

## Context

Task `000061` moved persisted-page validation to the readonly cache miss
boundary, but `GlobalReadonlyBufferPool` still models inflight dedup as an
event-only map while the first caller executes the miss load inline.
`ReservedMissFrameGuard` cleanup and publish ordering therefore remain coupled
to the caller-driven async path in `doradb-storage/src/buffer/readonly.rs`.

This is the wrong ownership boundary for request cancellation. In normal
storage-engine operation, a client session can abort or cancel its request
future without implying any storage corruption or IO failure. If the readonly
miss attempt is tied to that future, other waiters can observe a released
frame, lose the shared load attempt, or fail for a reason unrelated to the
underlying storage state.

The desired behavior is narrower and stricter:

1. Request cancellation is local to that caller and must not become the
   shared result of the miss attempt.
2. Once the readonly subsystem starts a miss attempt, it should continue to
   completion independently of caller cancellation.
3. Real IO failure or validation failure is terminal for the shared attempt
   and should be returned to all attached callers.
4. `inflight_loads` should track only active attempts, not retain completed
   results after the attempt finishes.

Optional issue metadata for `tools/issue.rs create-issue-from-doc`:
`Issue Labels:`
`- type:task`
`- priority:medium`
`- codex`

## Goals

1. Make readonly cache miss attempts survive initiating or waiting caller
   cancellation.
2. Ensure reserved miss frames remain owned until success publish or failure
   cleanup completes exactly once.
3. Share real IO and validation failures across all callers attached to the
   same inflight attempt.
4. Remove inflight entries promptly on terminal outcome so completed results
   do not remain in `inflight_loads`.
5. Keep the fix scoped to readonly miss loading without reopening generic
   `FileIO` or file-format design.

## Non-Goals

1. A generic cancellation-safe redesign of
   `doradb-storage/src/file/mod.rs` or
   `doradb-storage/src/file/cow_file.rs`.
2. Automatic retry, backoff, or recovery policy for IO or validation failure.
3. Changes to persisted-page validation rules introduced by task `000061`.
4. Changes to readonly hit-path semantics or broad `BufferPool` interface
   redesign.
5. File-format, checkpoint, or recovery protocol changes.

## Unsafe Considerations (If Applicable)

This task changes the ownership and completion ordering around existing unsafe
direct-read code in the readonly miss path.

1. Affected modules and why `unsafe` matters here:
   - `doradb-storage/src/buffer/readonly.rs`
   - `doradb-storage/src/file/cow_file.rs`
   - `doradb-storage/src/file/mod.rs`
   The direct read still targets page-sized, aligned memory owned by a
   reserved miss frame. The task should keep that destination valid for the
   entire background miss attempt rather than for the lifetime of one caller
   future.
2. Review must confirm:
   - the reserved miss frame or equivalent owned state outlives the direct
     read until publish or cleanup;
   - caller cancellation does not drop the underlying destination pointer or
     release the frame back to residency;
   - inflight completion writes terminal state, removes the map entry, and
     notifies waiters exactly once;
   - any new helper that publishes or cleans up after a background load has
     updated `// SAFETY:` comments that describe pointer lifetime and
     exclusive frame ownership across awaits.
3. Inventory refresh and validation scope:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo test -p doradb-storage`
   - `cargo test -p doradb-storage --no-default-features`

Reference:
- `docs/unsafe-usage-principles.md`
- `docs/process/unsafe-review-checklist.md`

## Plan

1. Replace the current `InflightLoad { ev: Event }` in
   `doradb-storage/src/buffer/readonly.rs` with a shared inflight-attempt
   object that contains:
   - an `Event` for waiter wakeup;
   - a lock-protected state machine with at least
     `Running` and `Completed(Result<PageID>)`.
2. Change `inflight_loads` semantics so the map tracks only active attempts.
   - callers clone the `Arc<InflightLoad>` while waiting;
   - the background miss attempt removes the key from `inflight_loads`
     immediately on terminal success or failure;
   - waiters read the terminal result from their own `Arc`, so the map does
     not become a completed-result cache.
3. On cache-miss vacancy, create the inflight entry and spawn one detached
   readonly-owned background task.
   - the task owns the reserved miss frame, page source handle, and optional
     validation context;
   - it performs the direct read, optional validation, publish on success, or
     cleanup on failure.
4. Make both the initial caller and followers use the same shared-attempt
   path.
   - caller cancellation only drops that caller's wait future;
   - caller cancellation must not mutate inflight state, release the reserved
     frame, or convert the shared attempt into an error result.
5. Treat terminal storage failures as shared attempt failures.
   - IO failure and validation failure become `Completed(Err(...))`;
   - all callers attached to that attempt receive the same error;
   - no implicit retry occurs within the same inflight attempt.
6. Add a one-time completion guard inside the background task.
   - it must write a terminal state if the task exits unexpectedly before
     storing the real result;
   - it must remove the map entry and notify waiters exactly once;
   - unexpected internal abort maps to `Error::InternalError`, which remains
     distinct from IO or validation error.
7. Preserve correct cleanup when there are no remaining waiters.
   - if the initial caller is canceled and no other waiter exists, the
     background attempt still completes;
   - after terminal completion removes the map entry, the inflight object
     drops naturally when no `Arc` holders remain;
   - no stale completed entry should remain in `inflight_loads`.

## Implementation Notes

1. Implemented the readonly miss path in `doradb-storage/src/buffer/readonly.rs`
   with a shared `InflightLoad` state machine and detached background owner
   task. `inflight_loads` now tracks only active attempts, the reserved miss
   frame remains owned until publish or cleanup completes, and terminal
   results are stored in shared state so caller cancellation no longer drops
   the underlying load attempt.
2. Added resolve-phase tests for the critical cancellation and shared-result
   cases:
   - canceled initiating loader with a surviving waiter that still succeeds;
   - canceled single loader with no stale completed entry left in
     `inflight_loads`;
   - shared IO failure propagation to all waiters;
   - shared validation failure propagation to all waiters.
   Existing readonly cache tests remained green.
3. Verification completed with:
   - `tools/unsafe_inventory.rs --write docs/unsafe-usage-baseline.md`
   - `cargo test -p doradb-storage --no-default-features`
   - `cargo test -p doradb-storage`
   Both crate-wide test runs passed after the readonly changes.
4. Follow-up design work for a broader IO-thread-owned completion framework
   was deferred and tracked separately in
   `docs/backlogs/000053-io-thread-owned-completion-callback-framework-for-file-io.md`.
5. No parent RFC linkage was present for this task at resolve time.

## Impacts

1. `doradb-storage/src/buffer/readonly.rs`
2. readonly-cache tests in `doradb-storage/src/buffer/readonly.rs`
3. internal readonly helper signatures if a background-owned page-source
   handle needs minor plumbing adjustments

## Test Cases

1. Cancel the initial caller after the miss load has started but before the
   direct read completes; another caller waiting on the same key still
   succeeds and the reserved frame is not reused early.
2. Attach multiple waiters to one miss load; cancel one waiter and verify the
   others still receive the successful result.
3. Attach multiple callers to one validated miss load; force validation
   failure and verify all attached callers receive the same validation error
   and no readonly mapping remains.
4. Force direct-read IO failure for one shared miss load and verify all
   attached callers receive the same IO error and the reserved frame is
   cleaned up exactly once.
5. Verify completed attempts do not remain in `inflight_loads` after success
   or failure when no waiters remain.
6. Keep existing readonly miss dedup, reload, invalidation, and corruption
   tests green.

## Open Questions

1. Generic static-read cancellation safety outside the readonly cache remains
   deferred to
   `docs/backlogs/000053-io-thread-owned-completion-callback-framework-for-file-io.md`,
   which tracks the larger RFC-scale design for IO-thread-owned completion
   callbacks and shared completion semantics.
2. If transient IO retry or backoff is needed in the future, that policy
   should be designed separately from this task's shared terminal-result
   semantics.
