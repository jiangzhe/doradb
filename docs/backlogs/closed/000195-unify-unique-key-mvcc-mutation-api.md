# Backlog: Unify unique-key MVCC mutation API

## Summary

Consolidate the public `Transaction::table_upsert_unique_mvcc`, `table_update_unique_mvcc`, and `table_delete_unique_mvcc` methods behind a unified `table_unique_mutate_mvcc` API while retaining specialized unique-point execution and existing MVCC ownership, index maintenance, and rollback machinery.

Prefer a dedicated unique-point API initially. Evaluate whether it should accept a direct mutation request or an occupied/missing-entry callback. Also assess a later merger into `table_index_mutate_mvcc` through explicit point-versus-range selection; reducing method names alone must not force point operations through range traversal or silently change their semantics.

## Reference

- User discussion on 2026-09-06 investigated consolidating the three public unique mutation methods and optionally merging them into index mutation. The user accepted recording the recommendation as a standalone follow-up; this was not deferred from active task/RFC implementation.
- `doradb-storage/src/trx/interface.rs`: public transaction entry points and operation-specific results; `doradb-storage/src/trx/stmt.rs`: index admission, DML validation, table-data intent-exclusive locking, and statement settlement.
- `doradb-storage/src/table/access.rs`: `upsert_unique_mvcc`, `update_unique_mvcc_input`, `delete_unique_mvcc`, `table_index_mutate_mvcc`, `LazyRowBuffer`, and `read_latest_cold_row`.
- `doradb-storage/src/index/secondary_index.rs`: `UniqueSecondaryIndex::lookup` and dual-tree candidate merging; `doradb-storage/src/index/borrowed_stream.rs`: operation-local range mutation traversal.
- `doradb-storage/src/table/index_mutate.rs`: owned-row callbacks, empty-update cancellation, and deferred unique-driver key changes; `doradb-storage/src/table/hot.rs`, `doradb-storage/src/trx/row.rs`, and `doradb-storage/src/table/deletion_buffer.rs`: physical mutation and ownership rules.
- `doradb-storage/src/row/ops.rs`: `RowUpdateInput`, `RowMutation`, `TableMutationOutcome`, and point-operation outcomes; `docs/public-api.md`: documented public contracts.
- Historical design: `docs/tasks/000202-unique-key-mvcc-upsert-table-access-api.md`, `docs/tasks/000205-optimize-transaction-row-value-ownership.md`, `docs/tasks/000265-index-driven-mvcc-mutation-api.md`, and `docs/tasks/000271-index-mutation-unique-driver-key-changes.md`.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

- Design and implement the unified public unique mutation boundary, its statement/accessor integration, result types, and migration of affected callers, examples, and API documentation. Reuse existing hot/cold mutation, undo/redo, unique-index claims, and row-move primitives; no new durable format is required by this consolidation.
- Compare a direct request enum covering upsert, keyed sparse update, and keyed delete with a callback design that can distinguish occupied and missing entries. The request enum is the smallest route to retaining existing input validation, upsert key derivation, and execution behavior. A callback adds conditional mutation capability and needs an explicit contract.
- Preserve a distinct point executor. Treat optional unification under the index-mutation method name as a design assessment, not a prerequisite: explicit unique-key and range targets must select their respective executors before range state is constructed.
- Keep catalog-table APIs and full-table mutation outside this follow-up unless a narrowly required shared primitive changes. Do not introduce gap locks, automatic retry-as-update after an insert race, or statement-wide unique-key permutation planning.

## Acceptance Hint

- One documented unique mutation entry point covers full-row upsert, sparse update, and delete. Preserve missing-target outcomes and inserted/updated physical `RowID` results, including replacement IDs after hot moves or cold-to-hot updates; specify skipped-entry outcomes if callbacks are supported.
- Explicitly decide and test cold committed-delete and empty-update semantics. Any intentional normalization must be documented and reviewed as a behavior change, rather than hidden behind equal-range specialization.
- Deterministic tests cover hot, frozen, and cold rows; key changes and other unique-index conflicts; missing-key insertion races; active/preparing ownership; old-snapshot visibility; and statement/transaction rollback. If callbacks are chosen, also cover at-most-once invocation, ownership before row exposure, invalid decisions, callback errors, and cancellation.
- Compare the existing point APIs with the chosen unified implementation using focused benchmarks for hot hits, misses, upsert insert/replace, sparse updates, deletes, wide rows, key changes, empty updates, and cold rows. Measure relevant allocation and index-access behavior. Retain MemIndex-hit short-circuiting, immediate point key changes, and owned full-row payload reuse; explain any material regression before replacing the old surface.
- Record the decision on optional index-API merger. A merged API must distinguish point/range selection and outcomes without collecting per-row IDs for ordinary ranges or allocating range traversal state for unique points. Update public docs and affected consumers, and pass the repository checks appropriate to the implementation.

## Notes (Optional)

- Upsert already shares `update_unique_mvcc_input` with sparse update through `RowUpdateInput::FullRow` versus `Sparse`. On a true miss, it recovers the owned full row and passes it to insert. Preserve this ownership optimization instead of expanding full rows into `Vec<UpdateCol>` or cloning payloads across retries.
- Unique lookup returns on a MemIndex hit and consults DiskTree only on a miss. Range mutation constructs a dual-tree stream, candidate batches, key revalidation state, a row cache, and deferred-update machinery. An equal-bound range currently does not provide the same execution cost as a point lookup.
- `LazyRowBuffer::new` eagerly allocates arrays proportional to column count. A constant callback returning delete or update should not inherit that cost automatically. Consider lazy scratch allocation or a direct-action path; generic callback dispatch alone is not proof of performance equivalence.
- Point updates apply driver-key changes immediately. Unique-driver range updates retain ownership and defer changes until traversal completes so unread candidates remain discoverable. Point execution does not need that deferral.
- There are existing semantic differences: point cold update/delete can return `WriteConflict` for a delete committed after the writer snapshot, while range mutation skips a cold row already observed as committed-deleted. Both still enforce ownership when claiming rows. Empty point updates enter ordinary mutation machinery, can retain undo ownership, and can move frozen/cold rows; range callbacks explicitly release provisional ownership for empty updates.
- For a callback design, consider `FnOnce(Option<&mut LazyRow>)` with skip, delete, sparse update, and full-row put decisions. Acquire definitive ownership before exposing an occupied row. Define actions on missing entries; a miss grants no gap lock, so insertion still uses normal unique claims and propagates races without rerunning the callback. If a separate lookup key accompanies a full-row put, validate their agreement for the selected unique index, or retain a row-shaped request that derives the key internally.
- Existing `RowMutation` only supports skip/delete/sparse update, and `TableMutationOutcome` only counts deletes/updates. Full-row insertion and point results require explicit type design. If sharing the index method name, use a result variant or typed request output rather than adding unnecessary per-row result storage to ranges.
- Investigation validation: 20 focused existing tests covering upsert, cold point conflicts, and index mutation passed. No implementation or comparative benchmark was performed. Performance findings above come from source inspection.

## Close Reason

- Type: implemented
- Detail: Implemented via docs/tasks/000299-unify-unique-key-mvcc-mutation-api.md
- Closed By: backlog close
- Reference: User decision
- Closed At: 2026-09-06
