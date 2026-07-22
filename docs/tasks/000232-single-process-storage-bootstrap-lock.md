---
id: 000232
title: Enforce Single-Process Storage Bootstrap Lock
status: implemented
created: 2026-07-22
github_issue: 874
---

# Task: Enforce Single-Process Storage Bootstrap Lock

## Summary

Make one engine process the exclusive owner of a canonical `storage_root`
before startup reads or opens engine storage, retain that ownership until all
components have shut down, and publish `storage-layout.toml` through a durable
atomic no-clobber protocol. The engine will use a persistent `storage.lock`
file whose OS lock is authoritative and whose synced diagnostic record reports
the owning PID and acquisition time. Marker creation will move all writes to a
same-directory temporary file, sync the completed contents, install the final
name with `hard_link`, remove the temporary name, and sync the root directory.

## Context

`bootstrap_inner` currently resolves paths, checks whether
`storage-layout.toml` exists, creates storage directories, and constructs every
engine component before it checks or creates the marker again. Those two
marker checks detect some local configuration changes, but they do not reserve
the storage root. Another process can open the same catalog, table, redo, or
swap files at any point during bootstrap or while the first engine remains
active.

`ResolvedStoragePaths::validate_marker_if_present` also calls `Path::exists`
before reading the marker. That suppresses metadata errors and leaves an
exists/read race. `ResolvedStoragePaths::persist_marker` exclusively creates
the final marker path and writes directly into it, but does not sync the file
or its parent directory. A failed write or process exit can therefore expose a
partial marker at the authoritative name, while a successful return does not
establish durable marker contents or a durable directory entry.

The component registry already provides the lifecycle needed by a storage-root
lease. `RegistryBuilder` shuts down and drops registered owners when a later
build step fails, and successful engines shut components down in reverse
registration order. Registering the lease first makes it release last on
normal shutdown, failed bootstrap, and owner drop without adding a runtime
handle to `EngineInner`.

This work was deferred from
`docs/tasks/000228-typed-infrastructure-error-boundaries.md` and RFC-0023 Phase
1 because ownership, crash behavior, filesystem semantics, and lifecycle
tests were beyond that error-boundary task. RFC-0023 Phases 1 and 2 are now
complete and provide the required `IoResult`, fieldless error contexts, source
preservation, and public build convergence. This task is a standalone backlog
resolution, not an RFC phase: it must not add `Parent RFC` metadata or edit the
RFC phase plan.

Issue Labels:

- type:task
- priority:high
- codex

Source Backlogs:

- docs/backlogs/closed/000162-single-process-storage-bootstrap-lock.md

## Goals

1. Ensure at most one live DoraDB engine owns a canonical storage root,
   including engines in the same process and processes that reach the root
   through relative, `..`, or symbolic-link aliases.
2. Acquire ownership after the root directory exists but before marker reads,
   subordinate directory creation, or any file, buffer, catalog, redo,
   recovery, or transaction component build.
3. Retain ownership through the complete engine lifetime and release it only
   after reverse-order component shutdown has completed. Failed bootstrap,
   successful explicit shutdown, normal owner drop, and process termination
   must all release the OS lock.
4. Keep `storage.lock` as a persistent control file. After acquiring its
   exclusive lock, replace its contents with a synced, versioned diagnostic
   record containing the owner PID and lock-acquisition timestamp.
5. Report lock contention as a stable lifecycle failure and include valid
   owner diagnostics when available, without treating PID or file contents as
   ownership evidence.
6. Read an existing layout marker without `Path::exists`; only a direct
   `NotFound` result is normal absence, while all other read failures retain
   their IO source below the existing Config context.
7. Publish a new layout marker atomically without overwriting an existing
   marker, sync both its contents and directory entry, remove handled
   temporary files, and define every pre-install and post-install failure
   state.
8. Remove recognized crash-left marker temporary names under the acquired
   lease without promoting their contents, and durably sync successful stale
   cleanup before bootstrap continues.
9. Fail closed before exposing an engine when the filesystem cannot provide
   the required exclusive lock, hard-link installation, or directory
   synchronization semantics.
10. Add deterministic same-process, cross-process, lifecycle, operation-fault,
    and process-exit tests without scheduler sleeps or global test hooks.

## Non-Goals

1. Enforcing mandatory exclusion against tools or processes that ignore the
   advisory DoraDB lock, or defending against a hostile actor that can replace
   storage paths while the engine runs.
2. Supporting shared/read-only owners, lock upgrades, fencing epochs,
   distributed leases, or multiple writers.
3. Introducing a general directory-handle capability layer, migrating all
   storage IO to `openat`-style APIs, or creating a transactional storage
   manifest service. Those changes cross file, buffer, log, and recovery
   subsystem boundaries and require separate RFC design.
4. Adding a configuration switch that disables storage-root locking or marker
   durability checks.
5. Changing `storage-layout.toml` fields or version, catalog/table/redo formats,
   checkpoint semantics, or recovery decisions.
6. Using the lock-file PID or timestamp for stale-lock detection. A persistent
   record can describe the previous owner after release; only successful OS
   lock acquisition proves ownership.
7. Recording a release timestamp or making lock-record update failure a
   best-effort warning. A successful bootstrap requires the acquired-owner
   record to be written and synced.
8. Adding a generic production filesystem fault-injection trait or coupling
   startup to a particular async runtime. Minimal per-call `#[cfg(test)]`
   stage hooks are sufficient for deterministic failure and crash coverage.
9. Claiming power-loss simulation from process-exit tests. The task verifies
   crash-visible state transitions and invokes the required sync operations;
   actual media guarantees remain the filesystem contract.
10. Expanding the current Linux/Unix storage platform scope. Unsupported
    filesystems or platform behavior return typed startup errors rather than a
    weaker fallback.

## Plan

### 1. Reserve the storage control namespace and prepare canonical paths

1. Add private constants in `doradb-storage/src/root.rs`:
   - `STORAGE_LOCK_FILE_NAME = "storage.lock"`;
   - a marker temporary prefix such as
     `.storage-layout.toml.tmp.`;
   - lock diagnostic record version `1`;
   - a small fixed maximum when reading contender diagnostics, so a malformed
     or externally enlarged lock file cannot allocate without bound.
2. Extend resolved-path validation in
   `doradb-storage/src/root.rs` so configured data, log, and swap paths
   cannot consume `storage.lock`, descend through it, or create root-level
   files matching the reserved marker-temporary grammar. Preserve the existing
   rule that `data_dir` or `log_dir` may be the storage root itself; reject the
   reserved control entry, not the root that contains it.
3. Have `ResolvedStoragePaths` retain the normalized root-relative path inputs
   needed to rebuild every absolute data, log, and swap path after the root is
   canonicalized. Do not recover relative paths through unchecked string
   manipulation.
4. Add an IO-typed preparation step that:
   - creates `storage_root` and its missing parents;
   - calls `fs::canonicalize` after creation;
   - rebases every resolved absolute path onto that canonical root;
   - retains native filesystem errors and attaches the operation, configured
     root, and canonicalization phase.
5. Keep the durable marker representation root-relative. Canonicalization must
   not change marker contents or break whole-root relocation.

### 2. Acquire the persistent storage-root lease and write owner diagnostics

1. Add a crate-private `StorageRootLease` owned by the bootstrap/control-plane
   path. Open `<canonical-root>/storage.lock` with read and write access and
   `create(true)`, but never use truncate-on-open. On Unix, use
   `OpenOptionsExt` to reject a final-component symlink without introducing an
   unsafe block, and require the opened object to be a regular file.
2. Call nonblocking `File::try_lock` and return an IO-typed neutral attempt
   result, for example:
   - `Acquired(StorageRootLease)`;
   - `Contended(StorageRootOwnerDiagnostic)`.
   `WouldBlock` is contention, not an IO exception. Any other open, metadata,
   or lock syscall failure remains `IoResult` with the native source attached.
3. Only the successful lock holder may mutate `storage.lock`. After acquiring
   the lock:
   - capture `std::process::id()` and milliseconds since `UNIX_EPOCH`;
   - serialize a small stable diagnostic record with fields `version`, `pid`,
     and `acquired_unix_ms`;
   - seek to offset zero, truncate, write all bytes, and call `sync_all`;
   - fail bootstrap and drop the file, thereby releasing the lock, if record
     construction, truncation, seek, write, or sync fails.
4. A contending opener must never truncate or write the file. After
   `try_lock` returns `WouldBlock`, read at most the configured diagnostic
   limit and parse the record on a best-effort basis. Return valid typed owner
   fields to `bootstrap_inner`; retain a concise unavailable or
   invalid diagnostic otherwise. A contender can race with the owner writing
   its initial record, so diagnostic parse failure must not replace or weaken
   the authoritative contention result.
5. The record remains after release and is overwritten only after the next
   owner acquires the OS lock. Never unlink `storage.lock` during cleanup or
   shutdown.
6. Store the locked `File` behind a tiny `parking_lot::Mutex<Option<File>>` or
   an equivalent idempotent owner-only release cell. `StorageRootLease` does
   not expose file access to sessions or `EngineInner`.

### 3. Integrate the lease into component and engine lifecycle ordering

1. Implement `Component` for `StorageRootLease` without exposing a runtime
   dependency handle:
   - the already acquired lease is its build config/owned value;
   - registration is infallible;
   - `Access` is `()` or an equivalently empty private handle;
   - `shutdown` takes and drops the locked file exactly once.
2. Reorder `bootstrap_inner` to:
   - resolve and validate configuration paths;
   - prepare/canonicalize the root;
   - acquire the OS lock and write/sync owner diagnostics;
   - create `RegistryBuilder` and register `StorageRootLease` first;
   - perform stale-temp cleanup and sync the root directory;
   - validate the marker;
   - create subordinate directories and build the existing components in their
     current relative order;
   - revalidate an initially present marker, or publish an initially absent
     marker durably;
   - finish the registry and construct `Engine`.
3. Convert `Contended` only at `bootstrap_inner`, the mixed public
   startup owner, into a new fieldless
   `LifecycleError::StorageRootInUse`. Attach canonical root, lock path, and
   valid owner PID/timestamp when available. Public callers observe
   `ErrorKind::Lifecycle`; do not add a new public `ErrorKind` or a general
   multi-domain carrier.
4. If a marker was present before component construction, require the
   post-build read to remain present and compatible. Map disappearance to the
   existing Config mismatch boundary with phase context rather than silently
   treating it as a fresh store. If the marker was initially absent, any final
   name that appears before no-clobber installation causes publication to
   abort.
5. Keep the lease as the first registry entry. Reverse shutdown then stops
   transaction workers, transaction system, catalog, lock manager, pool and IO
   workers, pools, filesystem, poisoner, and finally the storage-root lease.
   `Engine::shutdown` therefore permits a later owner even if the shut-down
   `Engine` value remains allocated.
6. Preserve `RegistryBuilder` failure behavior: any error after lease
   registration invokes reverse shutdown and releases the lease only after all
   already registered components stop. Owner-drop violation handling likewise
   releases it only through the registry's final shutdown pass.
7. Update the component-order documentation in
   `doradb-storage/src/component.rs` and
   `docs/engine-component-lifetime.md` to list `StorageRootLease` first and to
   state that root ownership brackets all other component activity.

### 4. Replace marker probing and clean stale temporary files

1. Replace the `Path::exists` probe in
   `ResolvedStoragePaths::validate_marker_if_present` with a direct open/read:
   - `NotFound` returns normal absence;
   - all other standard IO errors become `IoError` beneath
     `ConfigError::StorageLayoutMarkerRead`;
   - malformed TOML and durable-layout mismatch retain their current Config
     classifications.
2. Recognize only the exact generated temporary-name grammar under the root,
   including numeric PID and nonce components. Before marker validation, scan
   the root while the lease is held and remove recognized non-directory stale
   entries without reading or promoting them.
3. Never recursively delete an entry. A directory occupying an exact reserved
   temporary name is not publisher output and must fail startup with its path
   rather than risking unrelated data removal.
4. Sync the canonical root directory after stale cleanup. Perform the root
   directory sync even when no stale file was removed so bootstrap:
   - persists a newly created `storage.lock` directory entry;
   - re-establishes a visible marker entry after an earlier
     post-install-sync failure;
   - rejects unsupported directory-sync behavior before component storage is
     opened.
5. Attach scan, classify, unlink, and directory-sync stages plus every relevant
   root/temp path to IO reports. A handled pre-install cleanup must not silently
   discard its original error if cleanup itself also fails.

### 5. Publish `storage-layout.toml` atomically and durably

1. Replace direct final-path writes with a uniquely named regular temporary
   file in the canonical root. Use `create_new(true)` and a name formed from
   the reserved prefix, current PID, and a process-local atomic nonce. The
   lease and prior stale cleanup make collisions exceptional; report rather
   than overwrite any collision.
2. Serialize the existing `DurableStorageLayout`, write all bytes to the
   temporary file, and call `sync_all` before exposing a final marker name.
3. Install the final name with
   `fs::hard_link(temp_path, marker_path)`:
   - a hard link creates a second directory entry for the already written and
     synced inode; it does not copy the bytes;
   - creation of the final entry is atomic;
   - an existing `storage-layout.toml` causes `AlreadyExists` instead of being
     replaced;
   - placing both names in the root guarantees the same filesystem;
   - ordinary `rename` is not an acceptable fallback because it may overwrite
     an existing marker on Unix.
4. After the hard link succeeds, remove the temporary name. Both names refer
   to the same complete file between installation and cleanup, so a process
   exit in that interval can leave an extra name but cannot expose partial
   contents at the final name.
5. Open and `sync_all` the canonical root directory after final-name creation
   and temp-name removal. File sync before linking establishes contents;
   directory sync after cleanup establishes the final link and removal.
6. Track publication progress explicitly with a private typed attachment such
   as `MarkerPublicationState` plus a named stage:
   - before successful `hard_link`, state is `NotInstalled`;
   - after successful `hard_link` but before successful directory sync, state
     is `InstalledDurabilityUnknown`;
   - success is returned only after directory sync.
7. On a failure before installation, close and remove the temporary name and
   sync that cleanup. Preserve the initiating IO report and attach any cleanup
   failure rather than replacing the original stage.
8. On a failure after installation, never remove or roll back the final marker.
   Attempt safe temporary-name cleanup, return the IO failure with the
   installed-state attachment, and let a later leased startup sync the root and
   validate the visible marker. If required filesystem semantics are
   unsupported, abort; do not fall back to direct final writes or overwriting
   rename.

### 6. Preserve narrow error boundaries and testability

1. Add only the fieldless `LifecycleError::StorageRootInUse` to
   `doradb-storage/src/error.rs`. Lock and marker paths, owner PID/timestamp,
   publication stages, and installed state belong in typed or printable
   attachments, not variant fields.
2. Keep root preparation, lock open/syscall/diagnostic write, stale cleanup,
   hard-link publication, and sync operations IO-typed. Keep marker syntax and
   durable-layout compatibility Config-typed. Public disclosure remains in
   `bootstrap_inner`.
3. Preserve native standard-library error sources rather than formatting them
   away. Attach stable keys such as `operation`, `phase`, `storage_root`,
   `lock_path`, `marker_path`, `temp_path`, `pid`, and
   `publication_state` at the owner that knows them.
4. Define minimal scoped, thread-local `#[cfg(test)]` hooks inside the root
   tests module at meaningful lease-record and marker-publication stages. They
   may inject a typed IO failure or terminate a helper process after a named
   side effect. Do not add process-global mutable hooks, widen production
   structs for tests, or sleep to arrange races.
5. No unsafe code or new dependency is expected. Use stable
   `std::fs::File` locking, standard filesystem operations, existing `toml`,
   and existing synchronization dependencies. If implementation nevertheless
   introduces or changes unsafe code, follow
   `docs/process/unsafe-review-checklist.md` and refresh the unsafe inventory.

### 7. Verify behavior and documentation

1. Keep focused lease/marker unit tests beside
   `doradb-storage/src/root.rs`; keep public engine lifecycle and error
   assertions beside `doradb-storage/src/engine.rs`.
2. Implement a deterministic subprocess helper by re-executing the current
   unit-test binary with an exact helper test name and role/root environment.
   Use piped readiness and release messages, `Child::wait`, and bounded
   watchdogs. Do not use elapsed time to make lock or crash predicates true.
3. Deduplicate subprocess setup, owner-record parsing, temporary-name
   discovery, and publication-state assertions before final review.
4. Update `docs/engine-component-lifetime.md` and relevant inline bootstrap
   comments. Remove the temporary comments that say callers must provide
   exclusive root ownership after the implementation enforces it.
5. Run the repository-authoritative validation:

   ```bash
   rtk cargo fmt
   rtk cargo clippy --workspace --all-targets -- -D warnings
   rtk cargo nextest run --workspace
   rtk cargo clippy -p doradb-storage --no-default-features --features libaio --all-targets -- -D warnings
   rtk cargo nextest run -p doradb-storage --no-default-features --features libaio
   tools/style_audit.rs
   tools/coverage_focus.rs --path doradb-storage/src/root.rs --path doradb-storage/src/engine.rs
   rtk git diff --check
   ```

   Treat `.config/nextest.toml` as the timeout authority. If a subprocess test
   is flaky, reproduce it with a focused no-retry stress run and fix the
   missing semantic synchronization rather than adding sleeps or retries.

### Rejected alternatives

1. A lease-only task was rejected because it would leave direct partial and
   non-durable marker publication unresolved even though the lease is exactly
   the boundary needed to clean stale temporary state safely.
2. Using `storage-layout.toml` itself as the lock was rejected because the
   durable configuration marker and lifetime ownership have different
   semantics, and a missing marker is valid during first bootstrap.
3. Unlinking `storage.lock` at shutdown was rejected because deleting an
   inode-based lock file can let a contender create and lock a different inode
   while the previous owner still holds the old one.
4. Writing diagnostics before `try_lock` or opening with truncation was
   rejected because a losing contender could destroy the active owner's
   record.
5. Ordinary rename was rejected because it may replace an existing marker.
   Platform-specific `renameat2(RENAME_NOREPLACE)` would require a narrower
   platform syscall/dependency path; a same-directory hard link provides the
   required stable-standard-library no-clobber installation and fails closed
   when unsupported.
6. A broad storage-root capability/manifest service was rejected for this task
   because it would cross major file, buffer, log, and recovery interfaces and
   requires an RFC-scale multi-phase program.

## Implementation Notes

- Storage-root configuration projection and resolution now live in the
  dedicated `root` module. Preparation creates and canonicalizes the root,
  rebases configured data/log/swap paths from retained relative inputs, keeps
  durable marker contents root-relative, and rejects reserved lock and marker
  temporary names before storage components open files.
- `StorageRootLease` opens the persistent `storage.lock` without following a
  final-component symlink, requires a regular file, acquires a nonblocking OS
  lock, and writes and syncs a versioned PID/acquisition-time record only after
  ownership succeeds. Contenders never mutate the record and return optional
  bounded diagnostics beneath `LifecycleError::StorageRootInUse`.
- Bootstrap registers the acquired lease as the first component before marker
  access or subordinate storage setup. Reverse component shutdown therefore
  releases it last after normal shutdown, failed construction, ordinary drop,
  and fatal busy-drop teardown; explicit shutdown permits a new owner while
  the old `Engine` allocation remains alive.
- Marker validation now reads directly and treats only `NotFound` as absence.
  Startup removes only recognized non-directory stale temporaries under the
  lease and syncs the canonical root before validation, preserving native IO
  sources and operation/stage/path diagnostics for other failures.
- New markers are serialized into a same-directory `create_new` temporary,
  fully written and synced, installed without clobber through `hard_link`,
  unlinked from the temporary name, and committed with a root-directory sync.
  Pre-install and post-install failures retain explicit publication stage and
  `NotInstalled` or `InstalledDurabilityUnknown` state, preserve the initiating
  error, and attach any cleanup failure without rolling back a visible final
  marker.
- Deterministic thread-local hooks and subprocess helpers cover lease record
  faults, same-process and cross-process contention, alias canonicalization,
  shutdown and crash release, stale cleanup, no-clobber installation, handled
  publication failures, and crash-visible publication stages without sleeps
  or retry-based synchronization. Component-lifetime and example documentation
  now describe the ownership boundary and persistent control files.
- Resolution review found that the style audit counted the type-to-associated
  item separator in paths such as `tests::Enum::Variant` as excess module
  qualification. The audit now permits a type root after at most one module
  segment for enum, struct, trait, and associated-item paths while retaining
  diagnostics for deeper module paths; the three genuinely undocumented
  crate-visible root methods were documented.
- Verification passed 1,494 default workspace tests and 1,419 alternate
  `libaio` storage tests. Default and `libaio` Clippy passed with warnings
  denied, and the style gate passed all 31 branch-diff Rust files. Focused line
  coverage was 82.81% for `root.rs`, 95.90% for `engine.rs`, and 89.57% in
  aggregate. `git diff --check` also passed.
- No unsafe code, dependency, persisted marker/catalog/table/redo format, or
  RFC phase plan changed. No implementation follow-up was deferred; source
  backlog 000162 is closed as implemented by this task.

## Impacts

| Area | Expected impact |
| --- | --- |
| `doradb-storage/src/root.rs` | Canonical root preparation, reserved namespace validation, lease acquisition/diagnostics, direct marker reads, stale cleanup, durable marker publication, root constants, and focused tests |
| `doradb-storage/src/conf/engine.rs` | `EngineConfig`, its configuration methods, and the storage-path resolution entrypoint |
| `doradb-storage/src/engine.rs` | Bootstrap ordering, first lease registration, contention-to-Lifecycle convergence, explicit-shutdown release tests, and removal of temporary ownership warnings |
| `doradb-storage/src/component.rs` | Component topology documentation with `StorageRootLease` as the first owner and last shutdown target |
| `doradb-storage/src/error.rs` | New fieldless `LifecycleError::StorageRootInUse`; no new public kind |
| `docs/engine-component-lifetime.md` | Root ownership in build order, reverse shutdown, failed bootstrap, and owner-drop behavior |
| `.config/nextest.toml` | No expected change; its existing 60-second global timeout remains authoritative for subprocess tests |
| RFC-0023 | No change; completed phase prerequisites are consumed without reopening phase choices |
| Durable storage | Adds persistent `storage.lock` and transient reserved marker names; does not change marker, catalog, table, or redo formats |

The only intentional pre-marker mutation is creation/canonicalization of the
root and creation/update of `storage.lock`. An incompatible or malformed layout
marker may therefore leave a persistent unlocked diagnostic file after failed
startup, but it cannot modify existing durable engine data. Whole-root moves
remain supported because both the lock and marker move with the root and the
marker stores only relative layout fields.

The supported-filesystem contract becomes stricter: a storage root that cannot
honor the standard exclusive file lock, regular-file hard link, or directory
`sync_all` operations will fail startup even if it worked before. This is an
intentional correctness boundary. Advisory-lock semantics on a filesystem that
silently lies about cross-process exclusion cannot be detected by DoraDB and
remain outside the cooperative-engine guarantee.

## Test Cases

1. **Path preparation and reserved namespace**
   - Relative, `..`, and symbolic-link aliases rebase to the same canonical
     root without changing relative durable marker contents.
   - Whole-root relocation continues to work.
   - Data/log/swap configurations cannot consume `storage.lock` or generate a
     root-level marker-temporary name, while `data_dir = "."` and
     `log_dir = "."` remain valid.
   - Lock-file symlinks and non-regular objects fail before marker access.

2. **Lease acquisition and owner diagnostics**
   - The successful owner writes a parseable version-1 record whose PID equals
     the current process and whose acquisition timestamp is populated, then
     syncs it before returning `Acquired`.
   - A second independently opened handle in the same process receives
     `Contended`; its diagnostic read cannot alter the file bytes.
   - Injected truncate, write, and sync failures release the OS lock and fail
     before marker validation.
   - A malformed, oversized, or partially written diagnostic does not hide
     contention or allocate beyond the read bound.

3. **Cross-process ownership**
   - A helper child acquires the root, writes diagnostics, signals readiness,
     and blocks on a pipe. The parent and a second helper both fail to acquire
     the same root and observe `StorageRootInUse` with the child's PID when the
     record is valid.
   - Canonical and symlink aliases contend; independently created roots can be
     locked concurrently.
   - After an orderly child release and `wait`, another process acquires the
     root immediately.
   - After the parent terminates and waits for a holding child, another process
     acquires the root without deleting `storage.lock`.

4. **Engine bootstrap and shutdown ordering**
   - A second `Engine::bootstrap` on an active root returns public
     `ErrorKind::Lifecycle` with `StorageRootInUse` before an alternate
     data/log/swap path from its configuration is created.
   - A successful `Engine::shutdown` releases the root after all other
     components stop; a new engine can start while the shut-down owner value
     remains alive.
   - Existing component-build failure cases release the lease and still leave
     no layout marker.
   - Normal engine drop releases the lease. Fatal busy-drop handling stops all
     registered components before the lease shutdown executes.

5. **Direct marker reads**
   - Missing marker returns normal absence.
   - Permission, directory-at-marker, and other read errors retain IO beneath
     `StorageLayoutMarkerRead` and identify the marker path.
   - Malformed and mismatched markers retain current Config classifications.
   - An initially present marker that disappears or changes during build does
     not get silently recreated.

6. **No-clobber marker publication**
   - A synced temporary marker is hard-linked to an absent final name, the temp
     name is removed, the directory is synced, and the final bytes parse and
     match exactly.
   - An existing valid or arbitrary final marker is never overwritten;
     `AlreadyExists` identifies both source and destination and leaves the
     original inode/bytes unchanged.
   - File-sync or hard-link unsupported/failure paths abort before installation
     and leave no handled temporary name.

7. **Handled publication failures**
   - Injected create, write, file-sync, and pre-link failures return
     `NotInstalled`, remove the temporary file, and sync cleanup.
   - Injected temp-unlink and parent-sync failures after `hard_link` return
     `InstalledDurabilityUnknown`, never remove the final marker, and expose
     only complete marker bytes at the final path.
   - A cleanup failure preserves the initiating error, reports the cleanup
     path/stage, and leaves a name recognized by the next stale cleanup.

8. **Crash-visible marker states**
   - A child exiting after a partial temporary write leaves no final marker;
     the next leased startup removes the stale temp instead of promoting it.
   - A child exiting after temp sync but before `hard_link` has the same absent
     final state and cleanup behavior.
   - A child exiting after `hard_link` but before temp unlink may leave both
     names, but both reference one complete synced inode; the next startup
     removes the stale name and validates the final marker.
   - A child exiting after temp unlink but before directory sync leaves at most
     a complete final marker; the next startup syncs the root before validation
     and either proceeds with the matching marker or fails closed on sync.

9. **Regression and validation**
   - Existing failed-startup, durable-layout mismatch, marker relocation,
     shutdown idempotence, registry teardown, and both IO-backend tests remain
     green.
   - Subprocess tests use readiness pipes and process wait/termination
     predicates, contain bounded watchdogs only for hang detection, and do not
     use sleeps or retries to establish ownership state.
   - Focused coverage for changed config/bootstrap files meets the repository
     default review bar, and both default and `libaio` nextest/clippy passes
     succeed.

## Open Questions

None. Lock placement, diagnostic contents and authority, acquisition/release
order, root canonicalization, stale-temp policy, marker installation primitive,
post-install failure state, unsupported-filesystem behavior, error
classification, RFC relationship, and test synchronization are resolved above.
