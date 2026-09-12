# Backlog: Share unique mutation execution across user and mem/catalog tables

## Summary

Centralize unique MVCC row selection and mutation execution for user tables and standalone MemTable/catalog tables. Remove the legacy MemTable update/delete/upsert entry points after migrating their consumers, while retaining thin catalog convenience methods where they express catalog-specific contracts. This remains a separate follow-up to task000300; task000301 delivered its metadata/runtime ownership prerequisite, while the shared execution architecture is not selected yet.

## Reference

- User discussion on 2026-09-10 during task000300 review: repeated changes to hot-row selection exposed duplicate orchestration in MemTable and UniqueMutator. The user selected removal of legacy MemTable APIs and a separate follow-up, then requested a backlog item without further design.
- [Task 000300: Fix stale unique read-current lookups across row replacement](../tasks/000300-fix-stale-unique-read-current-lookups-across-row-replacement.md).
- [Task 000301: Refactor Memory Table Metadata and Runtime Layouts](../tasks/000301-refactor-memory-table-metadata-and-runtime-layouts.md), the implemented metadata prerequisite that intentionally leaves this execution work open.
- [Task 000299: Unify Unique-Key MVCC Mutation API](../tasks/000299-unify-unique-key-mvcc-mutation-api.md), which explicitly retained internal catalog/MemTable contracts.
- Historical context: [closed backlog 000140](closed/000140-share-table-accessor-lookup-mvcc-logic.md), implemented by [task000204](../tasks/000204-share-table-lookup-mutation-paths.md).
- `doradb-storage/src/table/unique_mutate.rs`: UniqueMutator, CurrentRowSelection, callback and owned-action dispatch.
- `doradb-storage/src/table/mem_table.rs`: update_unique_mvcc_input, update_unique_mvcc, upsert_unique_mvcc, delete_unique_mvcc, move-update and index-maintenance continuations.
- `doradb-storage/src/table/hot.rs` and `doradb-storage/src/table/access.rs`: shared row admission, owned hot/cold mutation, lazy rows, and user-table layout/root handling.
- `doradb-storage/src/trx/stmt.rs` and `doradb-storage/src/catalog/storage/`: catalog statement boundaries and production callers.

## Deferred From (Optional)

[Task 000300](../tasks/000300-fix-stale-unique-read-current-lookups-across-row-replacement.md), implementation review in worktree `.worktrees/000300`.

[Task 000301](../tasks/000301-refactor-memory-table-metadata-and-runtime-layouts.md), the independently implemented metadata prerequisite selected during review in worktree `.worktrees/000301`.

## Deferral Context (Optional)

- Defer Reason: Task000300 addresses stale unique read-current lookup correctness. Unifying mutation APIs, catalog adapters, and physical continuations is a broader refactor. The user explicitly chose a separate follow-up and requested that design be deferred.
- Findings at task000300 completion, before the metadata refactor: Task000299 unified the public user-table callback API but deliberately preserved internal mem/catalog paths. MemTable update and upsert had only test callers and were reserved for future memory-only user tables; catalog delete, batch delete, and delete-then-insert replacement were production paths. Task000300 shared CurrentRowSelection and HotRowMutator admission, including explicit DeletedBeforeSnapshot, Successor, and Unresolved outcomes, but UniqueMutator and the two MemTable update/delete loops still independently orchestrated lookup, forward traversal, prepare waits, retries, and mutation continuation. UniqueMutator was bound to UserTableAccessor, captured layout/root state, and cold LWC/CDB handling. MemTable used direct metadata and memory indexes, rejected persisted-row routing, and carried catalog key-based redo policy. Full-row upsert input and catalog replacement also had distinct ownership and statement semantics that a shared engine must account for.
- Direction Hint: Prefer one central unique-mutation implementation for both table families, covering selection and action execution rather than only extracting another small rejection helper. Remove the legacy MemTable update/delete/upsert entry points and migrate tests to the shared interface; keep catalog convenience operations as thin adapters. Future planning should make metadata/root binding, cold-row support, index maintenance, redo policy, and typed error boundaries explicit. Preserve full-row input ownership without unnecessary conversion to sparse assignments. Do not choose an adapter/trait design or broaden the public mutation action set in this backlog.

Task000301 deferral context:

- Defer Reason: The user split metadata ownership and unique-mutation execution into two independently reviewable stages. Completing the prerequisite does not complete this backlog.
- Findings: RowStore now owns physical resources and stable columns. Both table families use TableRuntimeLayout with exact index bindings; memory layouts directly own fixed runtimes. Shared WriteIndexKeySet derivation supplies owned values and exact identities, including catalog MVCC insert/delete consumers. Indexed-column read sets are cached per layout. User cold decoding, roots, ownership proofs, and separate mutation drivers remain in their existing owners.
- Direction Hint: Build on these concrete components and shared derivation without a flat capability trait. Re-evaluate current-row selection, action validation, hot operations, moves, and complete index effects by their inputs and ownership. A hot user update can claim a key formerly owned by a cold row, so row residency alone cannot define the shared mutation boundary. Keep user roots, cold claims, routing/waits, allocation, proof consumption, and statement settlement explicit.

## Scope Hint

Design a shared execution boundary for unique lookup, read-current ownership and forward traversal, callback/action dispatch, owned update/delete, move-update continuation, and secondary-index maintenance. Migrate standalone MemTable tests and production catalog deletion/replacement consumers, and remove obsolete internal mutation entry points and result types where no callers remain. Keep nontransactional recovery operations and range-mutation semantics outside this follow-up unless later research establishes a necessary shared primitive.

## Acceptance Hint

Both user-table and mem/catalog unique mutations use one selection and action-dispatch engine without duplicated retry loops. Legacy MemTable update/delete/upsert entry points are removed. Tests cover hot and cold user rows, catalog operations, key changes and physical moves, full-row input ownership, deletion timestamp boundaries, forward chains, duplicate/write conflicts, prepare waits, rollback, cancellation, and callback-at-most-once behavior. Preserve catalog primary-key validation, key-based redo and typed errors, batch statement atomicity, delete-then-insert replacement semantics, user-table layout/root safety, and existing cold-row behavior. Standard workspace tests, formatting, and strict Clippy pass.

## Notes (Optional)

The open backlog duplicate scan matched 000087 (recovery replay), 000104 (CREATE INDEX builds), 000109 (block reclamation policy), and 000196 (stale unique lookup correctness) by shared keywords; none covers this execution/API consolidation. Backlog000196 remains the correctness source for task000300. Closed backlog000140 is historical context, not an open item to merge or reopen.
