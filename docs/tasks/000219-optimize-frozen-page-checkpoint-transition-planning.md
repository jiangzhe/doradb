---
id: 000219
title: Optimize frozen page checkpoint transition planning
status: proposal
created: 2026-07-10
github_issue: 836
---

# Task: Optimize frozen page checkpoint transition planning

## Summary

Optimize the user-table frozen-page checkpoint transition introduced by tasks
000217 and 000218. The current successful path may traverse each selected
page's undo chains during cutoff validation, transition delete-marker capture,
and LWC visibility-bitmap construction. It also holds every selected page-state
write lock across validation and transition capture, so one slow page can block
foreground deletes and move updates across the complete frozen batch.

Replace that path with optimistic preparation that takes no page-state write
lock, backed by a paired-bump frozen-page mutation version. One analyzer will
produce an owned,
cutoff-specific transition plan containing the image cutoff proof, LWC deletion
bitmap, and pending overlay markers. Immediately compare the page version after
analysis and discard an already-invalid plan. After whole-batch image readiness,
revalidate each retained plan under only that page's state write lock, rebuild
only invalidated pages, and transition the pages individually. LWC and secondary
sidecar construction will consume the prepared bitmap without another undo-chain
walk.

Preserve the existing normal delay boundary before transition, fatal fail-closed
behavior after workflow transition admission, table-owned checkpoint workflow,
and atomic publication of the table root and all companion persistent state.

## Context

Issue Labels:
- type:task
- priority:medium
- codex

Source Backlogs:
- docs/backlogs/000151-optimize-frozen-page-checkpoint-transition-planning.md

Related implemented tasks:

- `docs/tasks/000217-redesign-checkpoint-frozen-page-cutoff-validation.md`
  replaced the unsafe `max_ins_sts` readiness shortcut with actual undo-status
  cutoff validation, cached stable image proofs, structured retryable delays,
  and checked transition views.
- `docs/tasks/000218-unify-table-freeze-checkpoint-workflow-state.md` made the
  table own one canonical frozen batch and original fence, moved publish
  admission before page transition, and defined reversible versus irreversible
  checkpoint phases.

The current `Table::validate_and_set_loaded_frozen_pages_to_transition` first
acquires every selected page's state write lock. While retaining the complete
lock set, it scans unchecked undo histories, checks the cached cutoff
requirements, acquires publish admission, changes the whole batch to
`TRANSITION`, and scans the histories again to capture lock/delete overlays.
`Table::build_lwc_blocks` later invokes `RowPage::vector_view_in_transition`,
which walks the histories again to construct a cutoff-visible deletion bitmap;
a block-split retry can construct that view more than once.

Page-state locks are synchronous `parking_lot::RwLock` guards. Foreground hot
row mutations retain a page-state read guard through `RowWriteAccess`, so the
batch-wide write-lock set synchronously stalls writers on every selected page.
The state lock is still the correct final transition exclusion mechanism, but
holding it during optimistic preparation does not keep the resulting plan valid
after the lock is released. Preparation should therefore avoid the page-state
write lock and rely on explicit mutation-version validation.

Frozen pages reject new insert and in-place update images. Foreground updates
become a delete of the old frozen RowID plus an insert into the active suffix;
ordinary deletes, provisional locks, rollback, and undo purge may still change
the frozen page representation. A stable image cutoff proof therefore remains
valid after it is established, while the transition bitmap and overlay markers
are versioned, attempt-local artifacts.

Shared transaction status publication is not a structural page mutation and
does not bump the page version. This is safe for a retained ready plan:

- an unresolved image-producing operation never produces a ready plan;
- an unresolved pre-fence lock/delete blocks initial readiness, but ownership
  observed after a stable proof is representable because its mutation acquired
  the already-`FROZEN` page state;
- an accepted post-fence lock/delete owns a cloned `Arc<SharedTrxStatus>`
  marker, and a transaction still active at the selected cutoff can only commit
  after that exclusive cutoff.

The mutation version is not a conventional odd/even seqlock. Row writers on
different rows may overlap, so an even value does not prove writer quiescence.
Correctness depends only on full-value equality plus the final page-state write
lock, which waits for every page-state reader and therefore every paired
post-mutation bump before authorizing plan reuse.

## Goals

1. Fuse frozen-page image validation, transition marker selection, and LWC
   bitmap construction into one analyzer with one set of undo-chain semantics.
2. Prepare page plans optimistically without acquiring a page-state write lock.
3. Bracket every mutation performed while a page is `FROZEN` with a version
   increment before the mutation and another increment after it.
4. Compare the version immediately after each optimistic preparation and drop
   a plan already known to be invalid.
5. Revalidate each retained plan under that page's state write lock immediately
   before transition; rebuild only absent, stale, or cutoff-mismatched plans.
6. Keep whole-batch image readiness reversible: any unresolved or future
   insert/update image returns the existing normal delay before publish
   admission and before any page enters `TRANSITION`.
7. After whole-batch readiness, transition one page at a time so writers on a
   transitioned prefix wait for cold routing while deletes on the remaining
   frozen suffix continue.
8. Make LWC construction and secondary-index sidecar collection consume the
   exact same prepared visibility bitmap without rescanning undo histories.
9. Preserve task-000217 cutoff semantics, task-000218 workflow ownership and
   publish/drop ordering, fatal post-transition failure handling, recovery
   behavior, and atomic table-root publication.
10. Add deterministic regression coverage that proves plan reuse, immediate
    invalid-plan rejection, page-local rebuild, paired mutation ordering, and
    mixed `TRANSITION`/`FROZEN` routing behavior.

## Non-Goals

1. Do not add checkpoint benchmarks, performance thresholds, benchmark CLI
   workloads, or benchmark output changes in this task.
2. Do not add `max_writer_sts`, `max_committed_cts`, a raw-page O(1) readiness
   fast path, or a general `PageModificationTracker` retained by transactions.
3. Do not modify transaction effects, ordered commit, active-STS retirement,
   purge-horizon publication, or global timestamp allocation.
4. Do not change public `freeze_table` or `checkpoint_table` APIs, canonical
   batch ownership, or checkpoint outcome types.
5. Do not change LWC, table-file, redo, catalog, or recovery formats and do not
   add a durable checkpoint-workflow state.
6. Do not implement an automatic tuple-mover actor, checkpoint scheduler,
   transaction status notification index, or maintenance queue.
7. Do not make page-state or row-version latches asynchronous.
8. Do not change which lock/delete overlays are representable or how the
   deletion buffer resolves marker conflicts.
9. Do not broaden this task into rollback-on-`TRANSITION` behavior or another
   row-page lifecycle redesign.

## Plan

### 1. Add an ordered frozen-page mutation version

Add a private `AtomicU64` mutation version to `RowVersionMap` with narrow load
and increment helpers. The version is meaningful for prepared checkpoint state
only while the page is `FROZEN`; active pages have no prepared plan and should
not pay atomic increment overhead.

Integrate paired version publication into `RowWriteAccess`, the common
production boundary for hot-row mutation, row rollback, and undo purge:

1. acquire the page-state read guard;
2. acquire the row-version write latch;
3. if the retained page state is `FROZEN`, increment the mutation version before
   returning mutable access to the caller;
4. perform all page-image and undo-chain changes while both guards remain held;
5. increment the version again from RAII drop before either guard is released.

Use an internal RAII token or a `RowWriteAccess::drop` implementation so every
early return and unwind that obtained frozen-page mutable access publishes the
closing increment. A no-op or failed mutation may conservatively add two
increments; false invalidation is acceptable, while a missed mutation is not.
Audit all production `RowVersionMap::write_latch` and direct row-page mutation
call sites. Any frozen-page mutation path that does not use `RowWriteAccess`
must be moved behind the common boundary or explicitly use the same RAII token.
Test-only direct latch setup does not widen the production API.

Use ordering equivalent to:

- optimistic and final version loads: `Acquire`;
- pre-mutation increment: `AcqRel`;
- post-mutation increment: `Release`.

The pre-increment must occur after the writer owns its page-state read guard and
row write latch but before any mutation. The post-increment must occur before
releasing those guards. The final transition path acquires the page-state write
lock before its version load, so it cannot observe a writer that still owes its
post-increment. Never interpret version parity as writer state. Treat version
wraparound as a fatal internal invariant rather than allowing ABA plan reuse.

### 2. Define an owned, cutoff-specific transition plan

Extend the private canonical batch state in `checkpoint_workflow.rs` with an
optional prepared plan aligned to each `FrozenPage`. Use a shape equivalent to:

```rust
struct PreparedTransitionPage {
    page_id: PageID,
    cutoff_ts: TrxID,
    observed_version: u64,
    required_cutoff_ts: Option<TrxID>,
    del_bitmap: Vec<u64>,
    overlay_markers: Vec<(RowID, DeleteMarker)>,
}
```

The plan must be fully owned. `DeleteMarker::Ref` may clone the relevant
`Arc<SharedTrxStatus>`, but the plan must not retain `RowUndoRef`, row latches,
page guards, or `RowVersionMap` references. Preserve the existing stable-page
count as an image-proof metric; a page may have a stable image proof but no
usable prepared plan after optimistic invalidation.

A plan is reusable only when all of these match:

- canonical page id and row-id range;
- selected exclusive `cutoff_ts`;
- current full mutation version.

Plans retained across a delayed checkpoint attempt remain an optimization only.
A newer cutoff or any version mismatch drops or rebuilds the representation
without weakening the independently cached image proof.

### 3. Fuse undo analysis into one semantic implementation

Replace the separate `validate_frozen_page`,
`capture_delete_markers_for_transition`, and transition-view chain logic with
one analyzer. It takes the page, frame context, an analysis mode, selected
exclusive `cutoff_ts`, and the version observed before analysis. Readiness mode
uses the original `frozen_ts`; stable-plan mode relies on the monotonic stable
proof. It scans each row under the existing row-version read latch and returns
either a structured delay/proof result or a ready owned plan.

For each row, copy the current physical delete bitmap as the starting state and
trace leading undo entries through the first image-producing operation:

- `Lock` does not change the cutoff image. An unresolved pre-fence lock blocks
  initial readiness. Once readiness is stable, later ownership observed on the
  frozen page is representable regardless of writer STS. The first unresolved
  lock that must survive routing becomes an owned `DeleteMarker::Ref` candidate.
- `Delete` does not set the required image cutoff. The newest relevant delete
  determines whether the row is deleted at the selected cutoff: a committed
  delete below the cutoff stays deleted; a future or unresolved delete restores
  the row in the prepared LWC bitmap. Future committed deletes become
  `DeleteMarker::Committed`; unresolved representable deletes retain a cloned
  `DeleteMarker::Ref`. Continue behind the delete to prove the image source.
- `Insert` or `Update` is the image boundary. An unresolved image blocks. A
  committed image contributes `cts + 1` to the exclusive required cutoff and
  is safe only when that requirement is at most the selected cutoff.

Record at most the first overlay marker required for each row, matching current
transition ownership behavior. Centralize these rules so the bitmap, marker,
and image proof cannot diverge between helpers.

### 4. Cache readiness, then refresh every plan optimistically

In the reversible checkpoint phase, retain the loaded shared page guards but do
not acquire page-state write guards for preparation. First run an incremental
readiness pass in canonical order:

1. reuse `Stable` image proofs without rescanning their undo chains;
2. reanalyze only `Unchecked` or `Blocked` pages;
3. stop at the first unsafe page and return its structured cutoff delay with
   the stable-prefix count, leaving later pages unchecked;
4. if a `Stable` page only needs a newer cutoff, return its cached cutoff delay
   without rescanning until the selected cutoff reaches the requirement.

Only when every page is ready, run one full optimistic plan-refresh pass before
acquiring the first page-state write lock. For every canonical page:

1. assert page id, row range, and `FROZEN` state invariants;
2. load `version_before` with Acquire ordering;
3. reuse a retained plan only for the same page, cutoff, and version; otherwise
   run the fused analyzer under per-row read latches;
4. load `version_after` with Acquire ordering;
5. retain the plan only when the full versions are equal;
6. immediately discard its bitmap and markers on mismatch while retaining the
   independently valid stable image proof;
7. preserve `Stable` readiness: later lock/delete ownership may change the plan
   but cannot produce another cutoff delay.

Do not spin until a continuously modified page yields a stable optimistic
window. A ready page whose plan was discarded remains batch-ready and is
rebuilt later under its page-local transition lock. This guarantees progress
without a global or page-wide quiet-window requirement.

Immediate version equality only proves the plan was not already stale when its
analysis finished. A writer may begin afterward, so it never authorizes
transition by itself.

### 5. Revalidate and transition one page at a time

After every page has a safe image proof, enter page-local transition in canonical
order:

1. Call `TableCheckpointWorkflow::try_begin_transition`, acquire lifecycle
   publish admission, and arm the irreversible checkpoint guard.
2. For each page, acquire only its state write lock and assert it is still
   `FROZEN` with the expected page identity.
3. Load the current mutation version under that lock.
4. Reuse the prepared plan only when its page identity, cutoff, and version all
   match. Otherwise rebuild the plan while retaining this page lock. The lock
   waits for all page-state readers, which guarantees every in-flight modifier
   has published its closing version bump.
5. Assert that locked regeneration produces a plan. A missing plan would mean
   a new image-producing mutation appeared after stable readiness, which frozen
   page mutation rules prohibit.
6. Change the page to `TRANSITION`, install its prepared overlay markers,
   retain or move its prepared bitmap for LWC construction, and release the
   page lock immediately.

Once workflow transition admission succeeds, any invariant, marker-install, or
later checkpoint error remains fatal and must wake transition-route waiters
through storage poison. Locked plan regeneration is guaranteed by the stable
readiness invariant: a later rebuild may differ only in representable
lock/delete overlay state because frozen pages cannot accept a new
image-producing mutation.

The full optimistic refresh reduces avoidable locked rebuilds but does not
authorize transition: it can race immediately after any page is visited. The
authoritative revalidation is still the comparison made under each page's state
write lock immediately before that page transitions.

Update the workflow invariant for the irreversible phase. `Transition` means
the canonical batch owns publish admission and contains a monotonically growing
`TRANSITION` prefix followed by a still-`FROZEN` suffix. Foreground operations
on the transitioned prefix wait for cold-route publication; deletes and move
updates reaching the frozen suffix retain their current frozen-page behavior.
Drop continues to wait for the one publish lease, and final table-root
publication remains the only durable visibility boundary.

### 6. Build LWC and secondary sidecars from the prepared bitmap

Add a restricted internal `PageVectorView` constructor or equivalent view type
that uses the prepared deletion bitmap and the immutable frozen-page values.
Change `build_lwc_blocks` to consume the prepared plans in canonical page order
instead of calling `vector_view_in_transition`.

Use the same prepared view for:

- LWC row inclusion and bitmap encoding;
- the visible-row callback that constructs secondary-index sidecar entries;
- retrying a page append after an LWC block split.

A block split may clone or reborrow the owned bitmap, but it must not rescan undo
chains. Remove obsolete transition-only scanning helpers when they have no
remaining runtime consumer; retain only focused checked helpers that still
serve an actual invariant or test purpose. Do not add a hidden second
correctness scan to the successful path.

### 7. Preserve publication and recovery boundaries

Do not change checkpoint work after page planning except where it consumes the
prepared artifacts. Continue to publish, in one existing table-root outcome:

- LWC blocks;
- `ColumnBlockIndex` state and pivot routing;
- cold-delete checkpoint metadata;
- secondary-index `DiskTree` roots;
- allocation reachability and replay bounds.

Retain publish admission through root publication, runtime route installation,
old-root retention, and checkpoint transaction commit. Normal delays occur
before admission. Recovery continues to initialize an idle volatile workflow
and reconstruct state from the same durable table root and redo boundaries.

### 8. Synchronize living documentation

Update `docs/checkpoint-and-recovery.md`, `docs/transaction-system.md`, and
`docs/table-file.md` to describe:

- optimistic plan preparation without a page-state write lock;
- paired mutation-version increments and equality-only validation;
- immediate post-analysis plan rejection plus final locked revalidation;
- page-local rebuild and a mixed `TRANSITION`/`FROZEN` irreversible batch;
- reuse of one prepared visibility representation for LWC and secondary
  sidecars;
- unchanged publish/drop admission, fatal boundary, root atomicity, and
  recovery format.

## Implementation Notes

- Added deterministic test-only analyzer-row, immediate-version-comparison,
  readiness-complete, and full-refresh-complete hooks. All new correctness
  races use hooks, channels, scoped threads, or explicit predicate checks
  rather than timing sleeps.
- Mutation-version coverage now exercises a writer finishing during analysis,
  a writer beginning after its row analysis, post-comparison mutation, even
  versions with overlapping writers, active-page non-effects, repeated frozen
  mutations, unwind cleanup, statement/transaction rollback boundaries, and
  undo purge (`trx::row::tests` and `table::tests`).
- Page-local checkpoint coverage now exercises continuous invalidation of every
  prepared page, cross-page delete progress during a locked rebuild, a mixed
  transitioned-prefix/frozen-suffix batch with real foreground writers,
  stable-prefix reuse before a blocked middle page, deferred suffix analysis,
  cutoff-only retry without rescanning, mutation at both optimistic phase
  boundaries, and successful marker regeneration for frozen-observed pre-fence
  ownership (`table::persistence::tests`).
- Analyzer coverage now includes unresolved pre-fence lock/delete and
  unresolved insert/update, post-fence marker `Arc` identity across later
  commit, and leading lock/delete marker selection. Existing future-image,
  future-delete, and committed-delete tests cover the remaining cutoff cases.
- Prepared-bitmap coverage now checks unique, non-unique, and zero-index table
  layouts. The non-unique split test snapshots the analyzer count at build
  admission and proves LWC block splitting does not trigger another analysis.
- Validation completed on 2026-07-11:
  - `cargo fmt --all -- --check` passed;
  - `cargo clippy --workspace --all-targets -- -D warnings` passed (the
    repository toolchain still reports its existing unknown allowed-lint
    warning for `clippy::manual_assert_eq`);
  - `cargo nextest run --workspace --no-fail-fast` passed 1,342/1,342 tests;
  - `cargo nextest run -p doradb-storage --no-default-features --features
    libaio --no-fail-fast` passed 1,266/1,266 tests;
  - focused coverage for row-version, transition, persistence, rollback,
    purge, and recovery paths was 10,385/11,306 lines (91.85%); every requested
    file exceeded 80%;
  - `tools/style_audit.rs` passed for 13 branch-diff Rust files.
- Public event-driven maintenance progress waits and migration of polling test
  helpers remain deferred to
  `docs/backlogs/000156-event-driven-maintenance-progress-waits.md`.

## Impacts

- `doradb-storage/src/trx/ver_map.rs`
  - add the private frozen-page mutation version and ordered operations;
  - document equality-only semantics and wraparound invariant.
- `doradb-storage/src/trx/row.rs`
  - bracket frozen-page `RowWriteAccess` with paired version increments;
  - guarantee the closing increment precedes row-latch and state-guard release.
- `doradb-storage/src/table/checkpoint_workflow.rs`
  - extend canonical batch state with optional cutoff-specific prepared plans;
  - keep stable image proof counts distinct from plan availability.
- `doradb-storage/src/table/mod.rs`
  - replace separate validation, marker capture, and bitmap analysis with the
    fused analyzer;
  - implement optimistic preparation, immediate version comparison,
    page-local locked revalidation, marker installation, and transition;
  - remove the batch-wide page-state write-lock set.
- `doradb-storage/src/table/persistence.rs`
  - sequence reversible optimistic preparation before page-local irreversible
    transition;
  - carry prepared plans into LWC and secondary-sidecar construction.
- `doradb-storage/src/row/vector_scan.rs`
  - add the prepared-bitmap vector-view boundary;
  - remove redundant transition undo scans from the successful build path.
- `doradb-storage/src/table/hot.rs`, `doradb-storage/src/table/rollback.rs`, and
  `doradb-storage/src/trx/purge.rs`
  - audit that every frozen-page structural mutation uses the paired
    `RowWriteAccess` boundary without duplicating version logic.
- `doradb-storage/src/table/persistence.rs`, `doradb-storage/src/table/access.rs`,
  `doradb-storage/src/trx/row.rs`, and adjacent test modules
  - add deterministic concurrency, visibility, rollback, purge, and recovery
    coverage using hooks/events rather than timing sleeps.
- `docs/checkpoint-and-recovery.md`, `docs/transaction-system.md`, and
  `docs/table-file.md`
  - synchronize the new preparation and transition protocol.

## Test Cases

1. A frozen page with no overlapping mutation has equal immediate and final
   versions, reuses its plan, and invokes the undo analyzer exactly once.
2. A mutation that starts before optimistic analysis and finishes during it
   changes the version; the immediate comparison discards the plan.
3. A mutation that begins during analysis is detected even though it published
   its first bump before changing the page image.
4. A mutation that begins after immediate comparison is caught by the final
   comparison under the page-state write lock.
5. Multiple concurrent row writers may temporarily leave an even version, but
   equality plus the final state lock detects their paired bumps; no code or
   test treats parity as quiescence.
6. Active-page inserts, updates, deletes, rollback, and purge do not increment
   the frozen-plan version because no plan can exist for an active page.
7. A frozen-page lock or delete publishes exactly one pre- and one
   post-mutation increment, including repeated mutations by one transaction
   STS.
8. Statement rollback and transaction rollback publish paired increments and
   invalidate an otherwise reusable plan.
9. Undo purge on a frozen page publishes paired increments and invalidates an
   otherwise reusable plan.
10. A conservative no-op `RowWriteAccess` may invalidate a plan but cannot
    leave an unmatched pre-increment on early return or unwind.
11. A plan invalidated by its immediate comparison is not retried in a busy
    optimistic loop; the page remains image-ready and rebuilds under its final
    page-local lock.
12. When one page changes after preparation, only that page is reanalyzed; all
    unchanged plans are reused.
13. Continuous frozen-page deletes may invalidate plans but do not require a
    globally quiet batch and do not prevent final page-local rebuild progress.
14. While one page plan is rebuilt under its state write lock, a delete on a
    different frozen page completes.
15. After the first page transitions and before the next page does, update or
    delete on the transitioned prefix waits for cold routing while a delete on
    the frozen suffix completes.
16. During initial readiness, an unresolved pre-fence lock/delete and any
    unresolved insert/update return the existing structured delay before
    publish admission; all pages remain `FROZEN`.
17. A committed insert/update whose CTS is at or beyond the selected cutoff
    reports the required exclusive cutoff and delays with no transition.
18. A future committed delete remains visible in the prepared LWC bitmap and
    installs the matching committed deletion-buffer marker.
19. An unresolved representable post-fence lock/delete retains the same
    `Arc<SharedTrxStatus>` in the prepared marker and remains correct if the
    status commits after preparation.
20. A committed delete below the cutoff is deleted in the prepared bitmap and
    requires no transition overlay marker; analysis still proves the older
    image source.
21. Leading lock/delete chains select the same first marker and cutoff-visible
    row state as the task-000217 behavior they replace.
22. LWC row membership and secondary-index sidecar entries consume the same
    prepared bitmap for unique, non-unique, and no-secondary-index layouts.
23. An LWC block-split retry reuses the prepared bitmap without another undo
    analysis.
24. A blocked middle page stops incremental validation, retains the stable
    prefix, and leaves the suffix unchecked; retry skips that prefix and then
    analyzes the former blocker and suffix.
25. A stable page waiting only for a newer cutoff is not rescanned on retries
    until the selected cutoff reaches its cached requirement.
26. Once every page is ready, one full optimistic refresh runs before the first
    page-state write lock and represents pre-fence ownership added after
    readiness without returning another delay.
27. Pre-fence ownership added after the full refresh is caught by final
    revalidation, regenerated under the affected page's state write lock, and
    published with the same shared-status marker.
28. A new cutoff or later version bump prevents stale prepared-plan reuse on
    retry.
29. Stable-plan analysis accepts pre-fence Lock/Delete ownership but still
    rejects unresolved Insert/Update; final locked regeneration asserts the
    latter cannot appear after stable readiness.
30. Any forced locked-analysis/build/publication/commit failure after workflow
    transition admission preserves fatal storage poison and wakes route waiters.
31. Drop either closes the reversible frozen workflow before publish admission
    or waits for the already-admitted page-local transition publisher.
32. Successful checkpoint publishes the same atomic LWC, block-index,
    cold-delete, secondary-root, allocation, and replay-bound state and returns
    the workflow to `Idle`.
33. Restart recovers the checkpointed rows, delete visibility, and secondary
    roots with no new durable workflow or version state.
34. Concurrent tests use explicit hooks, channels, events, or predicate
    signaling and do not use sleeps for correctness coordination.
35. Run `cargo fmt --all -- --check`.
36. Run `cargo clippy --workspace --all-targets -- -D warnings`.
37. Run `cargo nextest run --workspace`.
38. Run `cargo nextest run -p doradb-storage --no-default-features --features libaio`
    because checkpoint transition and persistence sequencing are changed.
35. Run focused coverage for the changed row-version, transition analyzer,
    workflow, persistence, rollback, purge, and recovery paths; meet the 80%
    focused coverage review bar or explain definition-heavy exceptions with
    covered consumers.

## Open Questions

No blocking design questions remain.

Commit-maintained `max_writer_sts` and `max_committed_cts` summaries and an O(1)
raw-page fast path remain outside this task. They require separate planning
across transaction effects, ordered commit, active-STS retirement, and purge
horizon publication if future evidence justifies that foreground cost.
