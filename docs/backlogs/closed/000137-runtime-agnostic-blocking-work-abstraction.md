# Backlog: Introduce runtime-agnostic blocking work abstraction

## Summary

Audit blocking filesystem and other potentially blocking operations reachable from async storage APIs, then introduce a storage-owned runtime abstraction for offloading blocking work without coupling the engine to a specific async runtime.

## Reference

Discussion during task 000197 implementation: cleanup_obsolete_redo_files is called from async TransactionSystem::truncate_redo_log and performs obsolete_redo_log_files_below_marker directory glob traversal plus repeated fs::remove_file calls on the executor thread. The narrow redo cleanup fix could use smol::unblock, but that would deepen runtime coupling and does not address the common pattern across the storage engine.

## Deferred From (Optional)

docs/tasks/000197-session-redo-log-truncation-api.md

## Deferral Context (Optional)

- Defer Reason: Task 000197 is focused on correctness of the initial redo truncation API, marker publication, and checkpoint/truncation exclusion. A runtime abstraction and cross-codebase blocking-IO audit is broader than the immediate truncation fix and should be planned separately.
- Findings: The immediate issue is valid: redo obsolete cleanup currently runs synchronous glob traversal and unlink syscalls directly inside an async maintenance API. For a small obsolete prefix this is negligible, but a large accumulated prefix can starve other async tasks. Similar blocking filesystem cleanup patterns exist elsewhere, so a one-off smol::unblock call would fix only the symptom and would couple production storage code to the current executor choice.
- Direction Hint: Prefer a small storage runtime facade or execution service with an offload_blocking style API, explicit shutdown semantics, and test hooks. Keep the facade independent from smol-specific types at public and core storage boundaries. Evaluate whether existing background worker infrastructure, file-system configuration, or engine runtime ownership is the right home before migrating call sites.

## Scope Hint

Design and implement a runtime-agnostic blocking-work abstraction owned by the storage engine or configuration layer, then migrate async production paths that perform blocking filesystem scans, metadata reads, unlink, or similar work to use that abstraction. Include redo obsolete cleanup as a motivating call site, but audit other async entrypoints before finalizing scope.

## Acceptance Hint

Future work should define the abstraction contract, default implementation, shutdown behavior, error propagation, and tests proving async executor threads are not blocked by long filesystem cleanup. The implementation should keep the storage engine runtime-agnostic and should not require public callers to use smol, tokio, or another specific runtime.

## Close Reason

- Type: wontfix
- Detail: The engine-owned ThreadPool already supports finite synchronous computations and asynchronous tasks with explicit shutdown and error handling. Per user decision, future storage I/O should use the existing asynchronous I/O paths; a separate runtime-agnostic blocking-work abstraction and its broad call-site migration are not planned.
- Closed By: backlog close
- Reference: User decision on 2026-10-04 after investigating backlog 000137; docs/tasks/000308-support-finite-sync-and-async-thread-pool-jobs.md; doradb-storage/src/runtime/thread_pool.rs
- Closed At: 2026-10-04
