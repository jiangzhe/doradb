# Backlog: Cache managed table definitions in current catalog state

## Summary

Cache each live managed table definition in the Catalog user_tables current logical state so binding resolution and managed DDL can read coherent runtime metadata and descriptor state without querying persistent descriptor rows.

## Reference

Discovered while implementing docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md for docs/rfcs/0031-compact-numeric-catalog-table-definitions.md Phase 7. CurrentTableState::Live already owns pointer-identical current metadata and the live Table, making it the natural cache boundary.

## Deferred From (Optional)

docs/tasks/000294-managed-table-bindings-and-versioned-resolution.md; docs/rfcs/0031-compact-numeric-catalog-table-definitions.md Phase 7

## Deferral Context (Optional)

- Defer Reason: The active task establishes persistent bindings and versioned resolution. Adding a new authoritative runtime-definition cache broadens DDL publication and recovery state, so it should be designed and validated as a focused follow-up instead of expanding the current task.
- Findings: CurrentTableState::Live metadata is pointer-identical to the current Table runtime-layout metadata, and index-DDL publication already coordinates the user_tables entry with the layout/history update while target metadata-X excludes readers. Caching directly in physical TableMetadata would mix opaque logical descriptors into table-file metadata and retain payloads in old layouts/history. Recovery initially constructs tables before all catalog redo is complete, so cache hydration must wait until parent, descriptor, and table-file/catalog metadata validation finishes. The cache cannot replace the namespace/key binding lookup because user_tables is keyed by TableID.
- Direction Hint: Prefer an immutable shared managed-definition value stored beside metadata in CurrentTableState::Live. Make durable descriptor effects and cache publication derive from the same validated value, use the cache for binding full resolution plus managed-DDL preflight/revalidation, and fail closed on an admitted managed table with absent or inconsistent cache state. Keep catalog.table_descriptors authoritative for durability, checkpoint, recovery, and integrity validation. Future descriptor-only DDL must update the cache atomically and extend the public definition version beyond storage_epoch.

## Scope Hint

Add an immutable cached managed definition adjacent to metadata in CurrentTableState::Live; publish it atomically with CREATE TABLE and managed index-DDL layout/history changes; hydrate it after final recovery validation; use it for binding resolution and all online managed-definition readers. Keep binding-key lookup catalog-backed and descriptor rows durable.

## Acceptance Hint

All online managed-definition reads use the current-state cache with no read-through fallback; cache publication is coherent with layout/history publication; recovery hydrates caches before foreground admission; missing or mismatched cache state fails as data integrity; tests cover no descriptor-catalog reads, DDL races, rollback, recovery, and unchanged public/on-disk formats.

After the cache lands, revisit the binding-resolution benchmarks introduced by
task 000296. Repeat narrow/full resolution with one and 64 targets across the
existing worker/session topologies, including the user-table lock controls.
Recalibrate operation counts and compare lock-store variants using identical
cache-enabled resolution paths. Keep benchmark results and supporting artifacts
only under the ignored `target/` directory.

## Notes (Optional)

Review of task 000296 treats hot built-in catalog resources under frequent
uncached resolution as an expected contention limit and an acceptable tradeoff
for that task. Higher-layer callers are expected to cache versioned definitions
where their cache-validity rules permit reuse. The returned version is an
optimistic comparison token; it does not independently notify a caller of
invalidation. Revalidating every cache hit through narrow resolution still
performs the catalog binding lookup.

This backlog changes the cost of full resolution by removing descriptor-row
reads. Binding-key lookup remains catalog-backed, so the benchmark should
reassess the resulting bottlenecks after implementation rather than carry
forward conclusions from the earlier uncached path.
