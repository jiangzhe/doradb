# Backlog: Fix Flaky Readonly Pool Eviction/Reload Test

## Summary

Investigate and fix intermittent CI failure in `buffer::readonly::tests::test_readonly_pool_drop_only_eviction_and_reload`.

## Reference

1. User-reported CI panic log:
   `thread 'buffer::readonly::tests::test_readonly_pool_drop_only_eviction_and_reload' (...) panicked at doradb-storage/src/buffer/readonly.rs:1272:53: called Option::unwrap() on a None value`
2. Related code/test location:
   - `doradb-storage/src/buffer/readonly.rs`

## Scope Hint

- Reproduce flaky behavior locally (stress/repeat test runs).
- Identify why `Option::unwrap()` at `readonly.rs:1272` can observe `None`.
- Fix root cause (state synchronization, eviction/reload ordering, or test assumptions) without weakening test intent.
- Add targeted assertions/instrumentation and regression coverage to reduce recurrence.

## Acceptance Hint

The test is stable in repeated runs and no longer shows intermittent `Option::unwrap()` panic in CI.

## Notes (Optional)

This backlog item is standalone and not a follow-up of existing task `000041`.

## Close Reason

- Type: stale
- Detail: This flaky readonly-pool drop/eviction/reload test issue has not been observed again, so keeping a dedicated backlog item open is no longer useful.
- Closed By: backlog close
- Reference: User request to close backlog 000004 as stale on 2026-06-11.
- Closed At: 2026-06-11
