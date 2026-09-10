# Backlog: Repair dangling RowUndoRef and deferred Lock-to-Delete mismatch

## Summary

Fix two checkpoint-transition correctness bugs found during task000300 review: rollback can free an OwnedRowUndo while a retained hot page still references it, and a deferred update can convert a hot Lock undo into Delete through cold finalization without synchronizing the original page or applying its delete bit. Both bugs were acknowledged by the user; implementation is intentionally deferred because a complete fix changes transition ownership semantics. The proposed design below is planning input, not an approved implementation plan.

## Reference

- User discussion on 2026-09-10 in worktree `.worktrees/000300`: investigation started from repeated `table.mem.pivot_row_id()` calls during forward-link rollback. The user confirmed both bugs and requested a backlog item after reviewing the transition impact.
- [Task 000300](../tasks/000300-fix-stale-unique-read-current-lookups-across-row-replacement.md): current unique read-current and forward-link work.
- [Task 000272](../tasks/000272-row-undo-rollback-through-page-transition.md) and [closed backlog 000185](closed/000185-row-undo-rollback-through-page-transition.md): introduced transition-route waiting and cold-marker-only rollback completion. This follow-up corrects the retained-page lifetime assumption; do not reopen the old item as if its implementation had not shipped.
- [Task 000219](../tasks/000219-optimize-frozen-page-checkpoint-transition-planning.md): frozen mutation tracking, prepared visibility plans, and transition publication.
- [Related backlog 000198](000198-share-unique-mutation-execution-across-user-and-mem-catalog-tables.md) concerns broader mutation-engine consolidation and remains separate.
- `doradb-storage/src/trx/undo/row.rs`: `RowUndoLogs::rollback`, `OwnedRowUndo`, `RowUndoRef`, `ForwardLinkUndo`.
- `doradb-storage/src/table/mem_table.rs`: exact-generation page access, `try_restore_forward_link`, `try_rollback_hot_row_undo`, retained-page deallocation.
- `doradb-storage/src/table/index_mutate.rs`: `apply_deferred_index_update`; `table/access.rs`: `finish_owned_cold_delete_effects`, `update_owned_cold_row`, snapshot candidate lookup.
- `doradb-storage/src/trx/row.rs`: `rollback_first_undo`, `with_forward_source`, row access and undo purge; `table/deletion_buffer.rs`: marker ownership.
- `doradb-storage/src/table/page_transition.rs`: `scan_frozen_page`, `apply_page_transition`, `install_transition_markers`; `table/persistence.rs`: LWC/sidecar construction, route publication, retirement, and transition rollback tests.
- `doradb-storage/src/trx/purge.rs`, `trx/sys.rs`, `trx/stmt.rs`, `trx/mod.rs`: GC retention and statement/terminal/failed-precommit cleanup.
- `docs/checkpoint.md`, `docs/shutdown-and-poison.md`, `docs/garbage-collect.md`: contracts to revisit.

## Deferred From (Optional)

[Task 000300](../tasks/000300-fix-stale-unique-read-current-lookups-across-row-replacement.md), implementation review in worktree `.worktrees/000300`.

## Deferral Context (Optional)

- Defer Reason: The user accepted that both bugs are real but considered the proposed correction too complex to fold into the current task. Fixing them requires coordinated reasoning about retained pages, checkpoint visibility, CDB ownership, deferred mutation, rollback, cancellation, and poison behavior. Defer implementation and final design selection to dedicated planning; this backlog is not authorization to edit runtime code or resolve task000300.
- Findings: Two concrete bugs and the supporting lifetime/transition evidence are preserved in the Notes below. Checkpoint routing can change while an active writer still protects the original page and owns its undo. Routing below the pivot does not authorize freeing a still-linked undo. A separate deferred-update path changes an originally hot undo through cold finalization without maintaining the retained hot page. Existing tests pass but do not assert these retained-page invariants.
- Direction Hint: Evaluate a transition contract that freezes column values and the prepared checkpoint bitmap while permitting narrowly scoped changes to existing ownership metadata. Prefer exact original-page rollback over pivot-driven completion, with synchronized CDB cleanup and a consistent deferred hot-lock-to-cold-delete handoff. Do not simply remove the pivot checks or the Transition rejection: the existing cold finalization violates ordinary hot Delete rollback assumptions. Reassess complexity and alternatives before selecting the final design; no general foreground writes or forward links in CDB are proposed.

## Scope Hint

Address retained-page undo unlinking, exact source forward-link restoration, and deferred completion of a hot lock after cold-route publication as one correctness follow-up. Define allowed Transition operations and their lock order; coordinate row restoration with owner-checked CDB cleanup; preserve reverse index/row undo ordering, partial statement rollback, cancellation ownership, GC fences, and first-fatal handling. Update transition/rollback documentation and the relevant task000300 design references. Keep shared user/mem/catalog mutation-engine consolidation, CDB forward-link optimization, durable-format redesign, and unrelated checkpoint performance work separate.

## Acceptance Hint

- Deterministically capture an old hot route, publish checkpoint, roll back, and prove the retained page has no freed undo reference before resuming snapshot lookup or scan. Use structural unlink assertions before any potentially dangling-pointer dereference.
- Cover rollback before final transition, after marker installation but before route publication, and after publication. Assert restored row state, bitmap accounting, CDB ownership, unchanged checkpoint-visible data, and correct secondary indexes.
- Cover deferred hot Lock completion through cold storage for commit, rollback, callback/error cleanup, and concurrent retained-page readers. The undo kind and original page state must remain consistent and synchronized.
- Cover forward-link restoration after source transition, multiple links/sources, statement rollback preserving an earlier source delete, and any remaining same-transaction ownership.
- Cover terminal, abandoned, statement, and failed-precommit cleanup; cancellation during page access or between undo entries; eviction/reload errors; missing-generation invariants; and checkpoint-poison outcomes without freeing unresolved ownership.
- Verify GC cannot reclaim original pages before required cleanup and eventually reclaims them after active readers/writers finish. Verify restart recovery and both hot/cold index visibility after the affected rollback and deferred-update sequences.
- Run focused deterministic regressions, workspace tests, the libaio backend suite, formatting, and strict Clippy. Passing current tests alone does not establish these fixes.

## Notes (Optional)

### Bug 1: cold routing skips retained-page undo unlink

At review time, the user-table branch of `RowUndoLogs::rollback` checks `entry.row_id < table.mem.pivot_row_id()`, removes the CDB marker, then pops the entry. `OwnedRowUndo` owns a Box, whereas `RowUndoRef` is a non-owning raw pointer. The old page's head can therefore still reference the freed Box.

Concrete interleaving: reader B captures a hot page route; writer A has a Delete or Lock undo on that page; checkpoint installs Transition and CDB ownership, then advances the pivot; A's rollback removes the marker and frees its undo without touching the retained page; B resumes using the old page. Snapshot candidate lookup validates page row range but does not reject Transition, and scan cursors can retain hot pages. The gap is therefore relevant to actual readers, not only diagnostics.

The analogous forward-journal branch skips source restoration below the pivot. That is a related cleanup decision, but the dangling Box above comes from ordinary row-undo completion.

### Bug 2: deferred hot Lock becomes an inconsistent Delete

`IndexMutator::apply_deferred_index_update` retains an original hot Lock across deferred application. Checkpoint can copy that ownership to CDB and publish the cold route. The deferred operation then calls `update_owned_cold_row`, which calls `finish_owned_cold_delete_effects`. That function rewrites the original Lock to Delete without changing its original page identity, acquiring the source row latch, or setting the source physical delete bit.

Unconditional `rollback_first_undo` is insufficient: its Delete arm expects the bit to be set and decrements the approximate deleted count. There is also a synchronization problem in changing a kind reachable through the original page without its row latch. Existing deferred-transition tests check final logical outcomes but do not inspect this retained-page consistency.

### Verified lifetime and checkpoint facts

- The active writer stays registered with GC until row rollback completes. Checkpoint retirement is attached to a system transaction committed after route publication, and reclamation waits for the GC watermark. This protects original page identity through rollback.
- Eviction is distinct from reclamation. An evicted page can require reload; its frame generation and version-map context are retained. A genuine original-generation miss must not be treated as successful cleanup merely because the pivot advanced.
- The writer owns active undo; GC retention does not protect a Box explicitly freed by rollback.
- Final checkpoint revalidation, Transition publication, and CDB marker installation run under the page-state write lock. Row writes take page-state read access before the row latch. Frozen writes publish paired mutation-version increments.
- An unresolved Insert or Update image, including one underneath leading Lock/Delete entries, blocks transition. Post-freeze ownership/deletion can instead be represented by a CDB status reference.
- LWC construction and its secondary-index callback use the owned prepared deletion bitmap and stable column values. They do not rescan the live undo chain or use the live physical delete bit for row inclusion.
- Prepared overlay markers are installed once during transition, not again during root publication. Later rollback must not cause marker reinstallation.
- Undo purge already accesses retained Transition pages under row access and unlinks obsolete history. The page's entire runtime metadata is not immutable today.

### Candidate design and questions to revisit

1. Roll back originally hot entries against their exact retained page regardless of pivot. Restore row state and unlink undo before freeing it. Originally cold entries remain CDB-only.
2. Permit rollback of existing Lock/Delete and restoration of existing forward-link before-images on Transition pages through specific methods. Unresolved Insert/Update in Transition is an invariant violation; new hot ownership and column changes remain prohibited.
3. Coordinate state and CDB using page pin, page-state read lock, row write latch, then CDB operation. If rollback wins while Frozen, mutation-version tracking makes checkpoint refresh its plan. If checkpoint wins, rollback observes installed Transition ownership and cleans up the retained row and CDB without waiting for publication. No await should split local restoration, unlinking, and corresponding marker cleanup.
4. Check marker ownership and preserve any remaining ownership of the same transaction. Restoring a destination's forward journal must not release a source delete owned by an earlier surviving statement.
5. For deferred cold finalization with an original hot page, consider applying the delete bit, accounting, and Lock-to-Delete change together under that page's latch while retaining the existing CDB marker. Preserve cold redo and replacement behavior. This makes the existing Delete inverse meaningful without adding another undo/CDB history structure.
6. Revisit poison semantics explicitly. Safe local cleanup would no longer depend on successful checkpoint publication. Existing tests that require a transition wait or automatic retained rollback on checkpoint poison may need new expectations. Actual access or invariant failure must preserve residual ownership and the canonical fatal outcome.
7. Compare against waiting for publication and then cleaning the retained page, or delaying undo destruction. Waiting still requires retained-page mutation because its state remains Transition; delayed destruction adds a lifetime mechanism and does not by itself reconcile rolled-back row state.
8. Account for performance: pivot reads use an optimistic latch and are not free; simply hoisting an immutable pivot into the current retry loops can loop forever after transition. Exact-page cleanup removes those routing waits but may add a reload where the current cold branch only removes CDB. Do not introduce permanent page pins or new per-row CDB storage without evidence.

### Validation and related work

Read-only investigation ran `cargo nextest run -p doradb-storage -E 'test(transition) | test(rollback) | test(frozen_page)'`: 75 passed, 1800 skipped. No new regression was written and neither bug was dynamically reproduced during that investigation; the findings are from code inspection and concrete interleavings.

Task000272 and its closed source backlog document the previous wait-and-route policy. Backlog000196 is the stale unique lookup source for task000300; backlog000198 tracks broader shared mutation logic. Neither replaces this transition correctness follow-up.

An initial broad-title duplicate scan matched open items 000075, 000095, 000114, 000146, 000196, and 000198 through generic keywords. Their scopes were inspected and differ from these two bugs. The final, specific-title scan covered 51 open documents and returned no candidates.

