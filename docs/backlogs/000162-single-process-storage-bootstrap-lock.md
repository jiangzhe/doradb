# Backlog: Enforce single-process engine bootstrap per storage root

## Summary

Engine startup validates storage-layout.toml before component construction and revalidates or creates it after construction, but no inter-process lock prevents another process from opening or mutating the same storage root during that interval or while the engine remains active. Marker creation also writes directly to the final path without syncing the file or parent directory, so failure or process termination can expose a partial or non-durable marker.

## Reference

docs/tasks/000228-typed-infrastructure-error-boundaries.md; docs/rfcs/0023-storage-error-boundary-propagation-migration.md Phase 1; doradb-storage/src/conf/engine.rs ResolvedStoragePaths::validate_marker_if_present and persist_marker; doradb-storage/src/engine.rs EngineConfig::build_inner; code-review discussion of marker validation races and atomic publication

## Deferred From (Optional)

docs/tasks/000228-typed-infrastructure-error-boundaries.md; docs/rfcs/0023-storage-error-boundary-propagation-migration.md Phase 1

## Deferral Context (Optional)

- Defer Reason: A durable cross-process lifecycle lease and crash-safe marker publication are broader than the narrow marker and error-boundary review and need design of acquisition order, ownership, shutdown, crash recovery, filesystem, and platform behavior.
- Findings: Reading the marker directly would remove the local exists/read gap and prevent Path::exists from suppressing metadata errors, but it cannot stop another process from replacing the marker or mutating storage later. The current preflight and post-build revalidation are not mutual exclusion, and the durable marker is configuration rather than a lifetime lock. persist_marker creates the final path before writing, does not sync the file or parent directory, and can leave a partial marker after a write failure or process termination. Publishing a synced same-directory temporary file by ordinary rename would improve crash consistency, but rename may overwrite an existing marker and therefore cannot replace explicit storage-root ownership or no-clobber semantics.
- Direction Hint: Prefer an OS-backed advisory or exclusive lease represented by an RAII engine component, acquired for the canonical storage root before any component opens storage files and held for the full engine lifetime. Under that lease, publish a new marker through an exclusively created, uniquely named temporary file in the marker directory: write all bytes, sync the file, atomically install it without clobbering an existing validated marker, then open and sync the parent directory. Preserve Report-based stage and path context, clean up temporary files on recoverable failures, and define startup handling for crash-left temporary files plus the outcome of a parent-sync failure after rename. Do not release the lease after bootstrap or use marker creation as the lock. Define unsupported-filesystem behavior and error classification explicitly.

## Scope Hint

Design and implement an OS-backed exclusive lease for the resolved storage root; integrate acquisition before marker validation and storage file access, retain ownership in the engine lifecycle through shutdown, and define errors and lock-file placement and cleanup. Make marker publication atomic and durable with a same-directory temporary file, file and parent-directory synchronization, no-clobber installation, and stale-temporary-file handling. Add crash-oriented and cross-process coverage.

## Acceptance Hint

For the same canonical storage root, exactly one live engine process can acquire ownership; a second process fails before opening or mutating storage, lock release permits a later owner after shutdown or process termination, and different roots remain independent. Marker publication never exposes partial contents at storage-layout.toml, does not overwrite an existing validated marker, syncs both marker contents and the parent directory entry, reports the failing publication stage and paths, and leaves no live temporary file after handled failures. Multi-process and crash-oriented tests cover contention, release, write and sync failures, stale temporary files, and restart-visible marker state.

## Notes (Optional)

Atomic rename alone is not a durability or concurrency boundary: the directory entry must be synced, and ordinary rename can replace the destination on supported platforms. Future design must specify the installed-marker state returned after a rename succeeds but parent-directory sync fails, rather than assuming temporary-file cleanup can roll that publication back.

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
