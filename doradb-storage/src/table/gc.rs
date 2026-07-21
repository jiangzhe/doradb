use super::{Table, TableRootSnapshot, TableRuntimeLayout};
use crate::buffer::{BufferPool, EvictableBufferPool, PoolGuard, PoolGuards};
use crate::catalog::TableMetadata;
use crate::error::{
    DataIntegrityError, InternalResult, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult,
    RuntimeResult,
};
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::{
    ColumnBlockIndex, MemIndexEntry, NonUniqueMemIndex, ResolvedColumnRow, SecondaryIndex,
    UniqueMemIndex,
};
use crate::session::SessionPin;
use crate::trx::TrxReadProof;
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::sync::Arc;

/// Aggregate result for a full-scan user-table secondary MemIndex cleanup pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecondaryMemIndexCleanupStats {
    /// One row per secondary index scanned by this pass.
    pub indexes: Vec<SecondaryMemIndexCleanupIndexStats>,
}

/// Cleanup result for one secondary MemIndex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryMemIndexCleanupIndexStats {
    /// Table-local secondary-index number.
    pub index_no: usize,
    /// Whether the scanned index is unique.
    pub unique: bool,
    /// Number of MemIndex entries processed as cleanup candidates.
    pub scanned: usize,
    /// Number of MemIndex entries physically removed.
    pub removed: usize,
    /// Number of MemIndex entries intentionally retained.
    pub retained: usize,
    /// Number of live MemIndex entries skipped before key materialization.
    pub skipped_live: usize,
    /// Number of hot delete overlays skipped before key materialization.
    pub skipped_hot_deleted: usize,
}

impl SecondaryMemIndexCleanupIndexStats {
    #[inline]
    fn new(index_no: usize, unique: bool) -> Self {
        Self {
            index_no,
            unique,
            scanned: 0,
            removed: 0,
            retained: 0,
            skipped_live: 0,
            skipped_hot_deleted: 0,
        }
    }

    #[inline]
    fn record(&mut self, decision: CleanupDecision) {
        self.scanned += 1;
        match decision {
            CleanupDecision::Remove => self.removed += 1,
            CleanupDecision::Retain => self.retained += 1,
        }
    }

    #[inline]
    fn record_skipped_live(&mut self, count: usize) {
        self.skipped_live += count;
    }

    #[inline]
    fn record_skipped_hot_deleted(&mut self, count: usize) {
        self.skipped_hot_deleted += count;
    }
}

struct MemIndexCleanupSnapshot<'ctx> {
    root: TableRootSnapshot<'ctx>,
    root_metadata: Arc<TableMetadata>,
    layout: Arc<TableRuntimeLayout>,
    min_active_sts: TrxID,
}

impl MemIndexCleanupSnapshot<'_> {
    #[inline]
    fn is_visible_to(&self, cleanup_sts: TrxID) -> bool {
        self.root.root_is_visible_to(cleanup_sts)
    }

    #[inline]
    fn root_ts(&self) -> TrxID {
        self.root.root_ts()
    }

    #[inline]
    fn effective_ts(&self) -> TrxID {
        self.root.effective_ts()
    }

    #[inline]
    fn pivot_row_id(&self) -> RowID {
        self.root.pivot_row_id()
    }

    #[inline]
    fn column_block_index_root(&self) -> BlockID {
        self.root.column_block_index_root()
    }

    #[inline]
    fn deletion_cutoff_ts(&self) -> TrxID {
        self.root.deletion_cutoff_ts()
    }

    #[inline]
    fn secondary_index_root(&self, index_no: usize) -> InternalResult<BlockID> {
        self.root.secondary_index_root(index_no)
    }

    #[inline]
    fn root_index_is_active(&self, index_no: usize) -> bool {
        self.root_metadata.idx.index_spec(index_no).is_some()
    }

    #[inline]
    fn layout(&self) -> &TableRuntimeLayout {
        &self.layout
    }
}

struct MemIndexCleanupContext<'a, 'ctx> {
    snapshot: &'a MemIndexCleanupSnapshot<'ctx>,
    metadata: &'a TableMetadata,
    clean_live_entries: bool,
    column_index: Option<&'a ColumnBlockIndex<'a>>,
    index_pool_guard: &'a PoolGuard,
    disk_pool_guard: &'a PoolGuard,
}

enum CleanupDecision {
    Remove,
    Retain,
}

enum DeleteOverlayProof {
    NotProven,
    Obsolete,
    ColdRowValues(Vec<Val>),
}

impl Table {
    /// Full-scan cleanup for user-table secondary MemIndex entries.
    ///
    /// This pass removes only entries proven redundant or obsolete against one
    /// captured table-file root. It never mutates DiskTree state, and it treats
    /// missing delete proof as a retention decision for delete overlays.
    ///
    /// When `clean_live_entries` is `true`, redundant live MemIndex entries are
    /// removed as part of the pass. When `false`, live MemIndex cache entries
    /// are retained and only obsolete delete overlays are cleaned.
    pub(crate) async fn cleanup_secondary_mem_indexes(
        &self,
        session: SessionPin,
        clean_live_entries: bool,
    ) -> RuntimeOrFatalResult<SecondaryMemIndexCleanupStats> {
        let trx_sys = session.engine.trx_sys.clone();
        let pool_guards = session.pool_guards();
        loop {
            let mut trx = session
                .begin_trx()
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=cleanup_secondary_mem_indexes, table_id={}, phase=begin_transaction",
                        self.table_id()
                    )
                })
                .map_err(RuntimeOrFatalError::from)?;
            let cleanup_sts = trx.sts();
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let cleanup_res = {
                let checkout = trx
                    .checkout()
                    .change_context(RuntimeError::TableAccess)
                    .attach_with(|| {
                        format!(
                            "operation=cleanup_secondary_mem_indexes, table_id={}, phase=checkout_transaction",
                            self.table_id()
                        )
                    })
                    .map_err(RuntimeOrFatalError::from)?;
                let proof = checkout.inner().ctx().read_proof();
                let snapshot = self.capture_mem_index_cleanup_snapshot(min_active_sts, &proof);
                if !snapshot.is_visible_to(cleanup_sts) {
                    drop(snapshot);
                    drop(checkout);
                    trx.rollback_table_maintenance().await?;
                    continue;
                }
                let cleanup_res = self
                    .cleanup_secondary_mem_indexes_at_snapshot(
                        &pool_guards,
                        &snapshot,
                        clean_live_entries,
                    )
                    .await;
                drop(snapshot);
                drop(checkout);
                cleanup_res
            };
            let rollback_res = trx.rollback_table_maintenance().await;
            return finish_secondary_mem_index_cleanup(cleanup_res, rollback_res);
        }
    }

    #[inline]
    fn capture_mem_index_cleanup_snapshot<'ctx>(
        &self,
        min_active_sts: TrxID,
        proof: &TrxReadProof<'ctx>,
    ) -> MemIndexCleanupSnapshot<'ctx> {
        let layout = self.layout_snapshot();
        let (root, root_metadata) = self.with_active_root(proof, |root| {
            (
                TableRootSnapshot::from_active_root(root, proof),
                Arc::clone(&root.metadata),
            )
        });
        MemIndexCleanupSnapshot {
            root,
            root_metadata,
            layout,
            min_active_sts,
        }
    }

    #[inline]
    async fn cleanup_secondary_mem_indexes_at_snapshot(
        &self,
        guards: &PoolGuards,
        snapshot: &MemIndexCleanupSnapshot<'_>,
        clean_live_entries: bool,
    ) -> RuntimeResult<SecondaryMemIndexCleanupStats> {
        debug_assert!(snapshot.deletion_cutoff_ts() <= snapshot.root_ts());

        let layout = snapshot.layout();
        let metadata = layout.metadata();
        let column_index = self.cleanup_column_index(guards, snapshot);
        let index_pool_guard = self
            .mem
            .index_pool_guard(guards)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=cleanup_secondary_mem_indexes, table_id={}, phase=resolve_index_pool_guard",
                    self.table_id()
                )
            })?;
        let disk_pool_guard = guards.disk_guard();
        let cleanup_context = MemIndexCleanupContext {
            snapshot,
            metadata,
            clean_live_entries,
            column_index: column_index.as_ref(),
            index_pool_guard,
            disk_pool_guard,
        };
        let mut stats = SecondaryMemIndexCleanupStats {
            indexes: Vec::with_capacity(metadata.idx.active_index_count()),
        };

        for (_, index) in layout.active_secondary_indexes() {
            let index_no = index.index_no();
            if !snapshot.root_index_is_active(index_no) {
                continue;
            }
            let secondary_root = snapshot
                .secondary_index_root(index_no)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=cleanup_secondary_mem_indexes, table_id={}, index_no={index_no}, phase=resolve_secondary_root",
                        self.table_id()
                    )
                })?;
            let mut index_stats =
                SecondaryMemIndexCleanupIndexStats::new(index_no, index.is_unique());
            match index {
                SecondaryIndex::Unique { .. } => {
                    self.cleanup_unique_secondary_mem_index(
                        &cleanup_context,
                        index_no,
                        index,
                        secondary_root,
                        &mut index_stats,
                    )
                    .await?;
                }
                SecondaryIndex::NonUnique { .. } => {
                    self.cleanup_non_unique_secondary_mem_index(
                        &cleanup_context,
                        index_no,
                        index,
                        secondary_root,
                        &mut index_stats,
                    )
                    .await?;
                }
            }
            stats.indexes.push(index_stats);
        }

        Ok(stats)
    }

    #[inline]
    async fn cleanup_unique_secondary_mem_index(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_, '_>,
        index_no: usize,
        index: &SecondaryIndex<EvictableBufferPool>,
        secondary_root: BlockID,
        stats: &mut SecondaryMemIndexCleanupIndexStats,
    ) -> RuntimeResult<()> {
        let disk = index
            .disk_runtime()
            .open_unique_at(secondary_root, cleanup_context.disk_pool_guard)?;
        let mem = index.unique_mem()?;
        let mut scan = mem.cleanup_scan(
            cleanup_context.index_pool_guard,
            cleanup_context.snapshot.pivot_row_id(),
            cleanup_context.clean_live_entries,
        );
        while let Some(batch) = scan.next_batch().await? {
            stats.record_skipped_live(batch.skipped_live);
            stats.record_skipped_hot_deleted(batch.skipped_hot_deleted);
            for entry in batch.entries {
                let decision = if entry.deleted {
                    // Delete-shadows are removable only after we prove the overlay
                    // is obsolete. Whole-row deletion is one proof; for cold rows,
                    // an immutable LWC row whose current unique key encodes
                    // differently proves this scanned shadow no longer protects a
                    // visible owner. Hot row-page version checks stay in
                    // transaction index GC, where undo visibility is available.
                    if self
                        .cleanup_unique_delete_overlay_is_obsolete(
                            cleanup_context,
                            index_no,
                            mem,
                            &entry,
                        )
                        .await?
                    {
                        compare_delete_unique_cleanup_entry(
                            mem,
                            cleanup_context.index_pool_guard,
                            &entry,
                            cleanup_context.snapshot.min_active_sts,
                        )
                        .await?
                    } else {
                        CleanupDecision::Retain
                    }
                } else {
                    debug_assert!(cleanup_context.clean_live_entries);
                    debug_assert!(entry.row_id < cleanup_context.snapshot.pivot_row_id());
                    // Live entries need a matching cold mapping before cleanup can
                    // treat the MemIndex copy as redundant.
                    match disk.lookup_encoded(&entry.encoded_key).await {
                        Ok(Some(row_id)) if row_id == entry.row_id => {
                            compare_delete_unique_cleanup_entry(
                                mem,
                                cleanup_context.index_pool_guard,
                                &entry,
                                cleanup_context.snapshot.min_active_sts,
                            )
                            .await?
                        }
                        Ok(_) => CleanupDecision::Retain,
                        Err(err) => return Err(err),
                    }
                };
                stats.record(decision);
            }
        }
        Ok(())
    }

    #[inline]
    async fn cleanup_non_unique_secondary_mem_index(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_, '_>,
        index_no: usize,
        index: &SecondaryIndex<EvictableBufferPool>,
        secondary_root: BlockID,
        stats: &mut SecondaryMemIndexCleanupIndexStats,
    ) -> RuntimeResult<()> {
        let disk = index
            .disk_runtime()
            .open_non_unique_at(secondary_root, cleanup_context.disk_pool_guard)?;
        let mem = index.non_unique_mem()?;
        let mut scan = mem.cleanup_scan(
            cleanup_context.index_pool_guard,
            cleanup_context.snapshot.pivot_row_id(),
            cleanup_context.clean_live_entries,
        );
        while let Some(batch) = scan.next_batch().await? {
            stats.record_skipped_live(batch.skipped_live);
            stats.record_skipped_hot_deleted(batch.skipped_hot_deleted);
            for entry in batch.entries {
                let decision = if entry.deleted {
                    // Non-unique delete marks use the same cold-row-only proof as
                    // unique shadows. If the persisted row exists but its current
                    // exact key no longer matches this encoded key+row-id pair,
                    // the old delete mark is obsolete. We deliberately avoid hot
                    // row-page key proof here; transaction index GC owns that path.
                    if self
                        .cleanup_non_unique_delete_overlay_is_obsolete(
                            cleanup_context,
                            index_no,
                            mem,
                            &entry,
                        )
                        .await?
                    {
                        compare_delete_non_unique_cleanup_entry(
                            mem,
                            cleanup_context.index_pool_guard,
                            &entry,
                            cleanup_context.snapshot.min_active_sts,
                        )
                        .await?
                    } else {
                        CleanupDecision::Retain
                    }
                } else {
                    debug_assert!(cleanup_context.clean_live_entries);
                    debug_assert!(entry.row_id < cleanup_context.snapshot.pivot_row_id());
                    // Live exact entries are redundant only when the same exact
                    // key is already present in the captured cold root.
                    match disk.contains_exact_encoded(&entry.encoded_key).await {
                        Ok(true) => {
                            compare_delete_non_unique_cleanup_entry(
                                mem,
                                cleanup_context.index_pool_guard,
                                &entry,
                                cleanup_context.snapshot.min_active_sts,
                            )
                            .await?
                        }
                        Ok(false) => CleanupDecision::Retain,
                        Err(err) => return Err(err),
                    }
                };
                stats.record(decision);
            }
        }
        Ok(())
    }

    #[inline]
    fn cleanup_column_index<'a>(
        &'a self,
        guards: &'a PoolGuards,
        snapshot: &MemIndexCleanupSnapshot<'_>,
    ) -> Option<ColumnBlockIndex<'a>> {
        if snapshot.column_block_index_root() == SUPER_BLOCK_ID
            || snapshot.effective_ts() >= snapshot.min_active_sts
        {
            return None;
        }
        Some(ColumnBlockIndex::new(
            snapshot.column_block_index_root(),
            snapshot.pivot_row_id(),
            self.file().file_kind(),
            self.file().sparse_file(),
            self.disk_pool(),
            guards.disk_guard(),
        ))
    }

    #[inline]
    async fn cleanup_unique_delete_overlay_is_obsolete(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_, '_>,
        index_no: usize,
        index: &UniqueMemIndex<EvictableBufferPool>,
        entry: &MemIndexEntry,
    ) -> RuntimeResult<bool> {
        match self
            .cleanup_delete_overlay_proof(cleanup_context, index_no, entry.row_id)
            .await?
        {
            DeleteOverlayProof::NotProven => Ok(false),
            DeleteOverlayProof::Obsolete => Ok(true),
            DeleteOverlayProof::ColdRowValues(values) => {
                Ok(!index.encoded_key_matches(&values, &entry.encoded_key))
            }
        }
    }

    #[inline]
    async fn cleanup_non_unique_delete_overlay_is_obsolete(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_, '_>,
        index_no: usize,
        index: &NonUniqueMemIndex<EvictableBufferPool>,
        entry: &MemIndexEntry,
    ) -> RuntimeResult<bool> {
        match self
            .cleanup_delete_overlay_proof(cleanup_context, index_no, entry.row_id)
            .await?
        {
            DeleteOverlayProof::NotProven => Ok(false),
            DeleteOverlayProof::Obsolete => Ok(true),
            DeleteOverlayProof::ColdRowValues(values) => {
                Ok(!index.encoded_exact_key_matches(&values, entry.row_id, &entry.encoded_key))
            }
        }
    }

    #[inline]
    async fn cleanup_delete_overlay_proof(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_, '_>,
        index_no: usize,
        row_id: RowID,
    ) -> RuntimeResult<DeleteOverlayProof> {
        let snapshot = cleanup_context.snapshot;
        // A globally purgeable row tombstone proves the delete overlay is no
        // longer protecting any transaction-visible row, independent of where
        // the row currently falls relative to the cold/hot pivot.
        if self
            .deletion_buffer()
            .delete_marker_is_globally_purgeable(row_id, snapshot.min_active_sts)
        {
            return Ok(DeleteOverlayProof::Obsolete);
        }
        // Full-scan cleanup only proves key obsolescence for persisted LWC
        // rows. Hot row pages require undo-chain checks, which transaction
        // index GC already performs while holding the row-page context.
        if row_id >= snapshot.pivot_row_id() {
            return Ok(DeleteOverlayProof::NotProven);
        }
        // The captured column root can prove durable row absence or a cold key
        // mismatch only after it is older than every active snapshot. Otherwise
        // removing the overlay could expose this newer cold-root fact to a
        // transaction that still depends on the MemIndex delete marker.
        if snapshot.effective_ts() >= snapshot.min_active_sts {
            return Ok(DeleteOverlayProof::NotProven);
        }
        let Some(column_index) = cleanup_context.column_index else {
            return Ok(DeleteOverlayProof::NotProven);
        };
        let Some(row) = column_index.locate_and_resolve_row(row_id).await? else {
            return Ok(DeleteOverlayProof::Obsolete);
        };
        let values = self
            .cleanup_read_cold_index_values(cleanup_context, index_no, row)
            .await?;
        Ok(DeleteOverlayProof::ColdRowValues(values))
    }

    #[inline]
    async fn cleanup_read_cold_index_values(
        &self,
        cleanup_context: &MemIndexCleanupContext<'_, '_>,
        index_no: usize,
        row: ResolvedColumnRow,
    ) -> RuntimeResult<Vec<Val>> {
        let metadata = cleanup_context.metadata;
        let index_spec = metadata.idx.index_spec(index_no).unwrap_or_else(|| {
            panic!(
                "active cleanup index must exist in captured metadata: table_id={}, index_no={index_no}",
                self.table_id()
            )
        });
        let read_set = index_spec
            .cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let file_kind = self.file().file_kind();
        let block_id = row.block_id();
        let persisted = self
            .storage
            .load_lwc_block(cleanup_context.disk_pool_guard, block_id)
            .await?;
        let block = persisted.block();
        if block.row_shape_fingerprint() != row.row_shape_fingerprint() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, \
                     reason=row_shape_fingerprint_mismatch"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach(format!(
                    "operation=cleanup_secondary_mem_indexes, table_id={}, index_no={index_no}",
                    self.table_id()
                )));
        }
        block
            .decode_row_values(metadata.col.as_ref(), row.row_idx(), &read_set)
            .attach_with(|| format!("file={file_kind}, block=lwc_block, block_id={block_id}"))
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=cleanup_secondary_mem_indexes, table_id={}, index_no={index_no}",
                    self.table_id()
                )
            })
    }
}

#[inline]
fn finish_secondary_mem_index_cleanup(
    cleanup_res: RuntimeResult<SecondaryMemIndexCleanupStats>,
    rollback_res: RuntimeOrFatalResult<()>,
) -> RuntimeOrFatalResult<SecondaryMemIndexCleanupStats> {
    match (cleanup_res, rollback_res) {
        (Ok(stats), Ok(())) => Ok(stats),
        (Err(err), Ok(())) => Err(RuntimeOrFatalError::from(err)),
        (_, Err(err)) => Err(err),
    }
}

#[inline]
async fn compare_delete_unique_cleanup_entry<P: BufferPool>(
    index: &UniqueMemIndex<P>,
    index_pool_guard: &PoolGuard,
    entry: &MemIndexEntry,
    min_active_sts: TrxID,
) -> RuntimeResult<CleanupDecision> {
    if index
        .compare_delete_encoded_entry(
            index_pool_guard,
            &entry.encoded_key,
            entry.row_id,
            entry.deleted,
            min_active_sts,
        )
        .await?
    {
        Ok(CleanupDecision::Remove)
    } else {
        Ok(CleanupDecision::Retain)
    }
}

#[inline]
async fn compare_delete_non_unique_cleanup_entry<P: BufferPool>(
    index: &NonUniqueMemIndex<P>,
    index_pool_guard: &PoolGuard,
    entry: &MemIndexEntry,
    min_active_sts: TrxID,
) -> RuntimeResult<CleanupDecision> {
    if index
        .compare_delete_encoded_entry(
            index_pool_guard,
            &entry.encoded_key,
            entry.deleted,
            min_active_sts,
        )
        .await?
    {
        Ok(CleanupDecision::Remove)
    } else {
        Ok(CleanupDecision::Retain)
    }
}

#[cfg(test)]
mod tests {
    use super::finish_secondary_mem_index_cleanup;
    use crate::catalog::IndexNo;
    use crate::error::{
        DataIntegrityError, LifecycleError, OperationError, RuntimeError, RuntimeOrFatalError,
    };
    use crate::id::{RowID, TrxID};
    use crate::index::{IndexInsert, NonUniqueIndex, UniqueIndex};
    use crate::session::tests::{SessionTestExt, assert_checkpoint_published};
    use crate::table::tests::*;
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::value::Val;
    use error_stack::Report;
    use tempfile::TempDir;

    #[test]
    fn test_secondary_mem_index_cleanup_rollback_error_overrides_cleanup_error() {
        let cleanup_err = Report::new(DataIntegrityError::InvalidPayload)
            .change_context(RuntimeError::TableAccess);
        let rollback_err =
            Report::new(LifecycleError::Shutdown).change_context(RuntimeError::TableAccess);
        let err = finish_secondary_mem_index_cleanup(
            Err(cleanup_err),
            Err(RuntimeOrFatalError::Runtime(rollback_err)),
        )
        .unwrap_err();
        let RuntimeOrFatalError::Runtime(err) = err else {
            panic!("Runtime rollback failure must remain Runtime");
        };
        assert_eq!(
            err.downcast_ref::<LifecycleError>().copied(),
            Some(LifecycleError::Shutdown)
        );
        assert!(err.downcast_ref::<DataIntegrityError>().is_none());
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_redundant_live_unique_entries() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let row_count = 4;
            insert_rows(table_id, &mut session, 0, row_count, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert!(!session.in_trx().unwrap());
            assert_eq!(stats.indexes.len(), 1);
            assert_eq!(stats.indexes[0].index_no, 0);
            assert!(stats.indexes[0].unique);
            assert_eq!(stats.indexes[0].scanned, row_count as usize);
            assert_eq!(stats.indexes[0].removed, row_count as usize);
            assert_eq!(stats.indexes[0].retained, 0);
            assert_eq!(stats.indexes[0].skipped_live, 0);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);

            for key_value in 0..row_count {
                let key = single_key(key_value);
                let disk_row_id = unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &pool_guards,
                    &key,
                )
                .await
                .unwrap();
                assert_eq!(
                    index.lookup(&key.vals, MAX_SNAPSHOT_TS,).await.unwrap(),
                    Some((disk_row_id, false))
                );
            }
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_requires_idle_session() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let trx = session.begin_trx().unwrap();

            let err = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap_err();
            let operation_error = err.report().downcast_ref::<OperationError>().copied();
            let was_in_trx = session.in_trx().unwrap();

            let internal_err = table_for_internal_assertion(&engine, table_id)
                .cleanup_secondary_mem_indexes(session.pin().unwrap(), true)
                .await
                .unwrap_err();
            let RuntimeOrFatalError::Runtime(internal_err) = internal_err else {
                panic!("existing transaction must remain a recoverable table-access failure");
            };

            trx.rollback().await.unwrap();
            assert_eq!(operation_error, Some(OperationError::NotSupported));
            assert_eq!(*internal_err.current_context(), RuntimeError::TableAccess);
            assert_eq!(
                internal_err.downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::ExistingTransaction)
            );
            assert!(was_in_trx);
            assert!(!session.in_trx().unwrap());
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_redundant_live_non_unique_entries() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let row_count = 5;
            insert_rows(table_id, &mut session, 0, row_count, "same-name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                1,
            );
            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes.len(), 2);
            assert_eq!(stats.indexes[1].index_no, 1);
            assert!(!stats.indexes[1].unique);
            assert_eq!(stats.indexes[1].scanned, row_count as usize);
            assert_eq!(stats.indexes[1].removed, row_count as usize);
            assert_eq!(stats.indexes[1].retained, 0);
            assert_eq!(stats.indexes[1].skipped_live, 0);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);

            let key = name_key("same-name");
            let disk_rows = non_unique_disk_tree_prefix_scan(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &key,
            )
            .await;
            assert_eq!(disk_rows.len(), row_count as usize);
            let mut lookup_rows = Vec::new();
            index
                .lookup(&key.vals, &mut lookup_rows, MAX_SNAPSHOT_TS)
                .await
                .unwrap();
            assert_eq!(lookup_rows, disk_rows);
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_aggregates_bounded_batches() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let name = "batch-name-".repeat(120);
            let row_count = 80;
            insert_rows(table_id, &mut session, 0, row_count, &name).await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[1].scanned, row_count as usize);
            assert_eq!(stats.indexes[1].removed, row_count as usize);
            assert_eq!(stats.indexes[1].retained, 0);
            assert_eq!(stats.indexes[1].skipped_live, 0);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_can_retain_live_cache_entries() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let row_count = 4;
            insert_rows(table_id, &mut session, 0, row_count, "same-name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let unique_index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            let non_unique_index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                1,
            );
            let stats = session
                .cleanup_secondary_mem_indexes(table_id, false)
                .await
                .unwrap();
            assert_eq!(stats.indexes.len(), 2);
            for index_stats in &stats.indexes {
                assert_eq!(index_stats.scanned, 0);
                assert_eq!(index_stats.removed, 0);
                assert_eq!(index_stats.retained, 0);
                assert_eq!(index_stats.skipped_live, row_count as usize);
                assert_eq!(index_stats.skipped_hot_deleted, 0);
            }

            let unique_key = single_key(0i32);
            let unique_row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &unique_key,
            )
            .await
            .unwrap();
            assert_eq!(
                unique_index
                    .lookup(&unique_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((unique_row_id, false))
            );

            let name_key = name_key("same-name");
            let disk_rows = non_unique_disk_tree_prefix_scan(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &name_key,
            )
            .await;
            let mut lookup_rows = Vec::new();
            non_unique_index
                .lookup(&name_key.vals, &mut lookup_rows, MAX_SNAPSHOT_TS)
                .await
                .unwrap();
            assert_eq!(lookup_rows, disk_rows);
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_retains_unique_delete_shadow_without_delete_proof() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;

            let current_key = single_key(0i32);
            let stale_key = single_key(-1i32);
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            let row_id = index
                .lookup(&current_key.vals, MAX_SNAPSHOT_TS)
                .await
                .unwrap()
                .unwrap()
                .0;
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[0].scanned, 0);
            assert_eq!(stats.indexes[0].removed, 0);
            assert_eq!(stats.indexes[0].retained, 0);
            assert_eq!(stats.indexes[0].skipped_live, 1);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 1);
            assert_eq!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, true))
            );
            assert_eq!(
                index
                    .lookup(&current_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, false))
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_with_purgeable_marker() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let current_key = single_key(0i32);
            let stale_key = single_key(-1i32);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &current_key,
            )
            .await
            .unwrap();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(1))
                .unwrap();

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[0].scanned, 2);
            assert_eq!(stats.indexes[0].removed, 2);
            assert_eq!(stats.indexes[0].retained, 0);
            assert_eq!(stats.indexes[0].skipped_live, 0);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                None
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_delete_shadow_when_live_cleanup_disabled() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let current_key = single_key(0i32);
            let stale_key = single_key(-1i32);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &current_key,
            )
            .await
            .unwrap();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(1))
                .unwrap();

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, false)
                .await
                .unwrap();
            assert_eq!(stats.indexes[0].scanned, 1);
            assert_eq!(stats.indexes[0].removed, 1);
            assert_eq!(stats.indexes[0].retained, 0);
            assert_eq!(stats.indexes[0].skipped_live, 1);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                index
                    .lookup(&current_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, false))
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_with_matching_cold_entry() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let current_key = single_key(0i32);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &current_key,
            )
            .await
            .unwrap();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            assert!(
                index
                    .mask_as_deleted(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[0].scanned, 1);
            assert_eq!(stats.indexes[0].removed, 0);
            assert_eq!(stats.indexes[0].retained, 1);
            assert_eq!(stats.indexes[0].skipped_live, 0);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup(&current_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, true))
            );

            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(1))
                .unwrap();
            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[0].scanned, 1);
            assert_eq!(stats.indexes[0].removed, 1);
            assert_eq!(stats.indexes[0].retained, 0);
            assert_eq!(stats.indexes[0].skipped_live, 0);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup(&current_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, false))
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_unique_delete_shadow_when_cold_row_key_differs() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            session
                .wait_for_gc_horizon_after(checkpoint_ts)
                .await
                .unwrap();

            let current_key = single_key(0i32);
            let stale_key = single_key(-1i32);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &current_key,
            )
            .await
            .unwrap();
            assert_eq!(
                unique_disk_tree_lookup(
                    &table_for_internal_assertion(&engine, table_id),
                    &pool_guards,
                    &stale_key
                )
                .await,
                None
            );
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            assert_eq!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, true))
            );
            assert_eq!(
                index
                    .lookup(&current_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, false))
            );

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[0].scanned, 2);
            assert_eq!(stats.indexes[0].removed, 2);
            assert_eq!(stats.indexes[0].retained, 0);
            assert_eq!(stats.indexes[0].skipped_live, 0);
            assert_eq!(stats.indexes[0].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                index
                    .lookup(&current_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, false))
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_propagates_cold_delete_overlay_proof_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            session
                .wait_for_gc_horizon_after(checkpoint_ts)
                .await
                .unwrap();

            let current_key = single_key(0i32);
            let stale_key = single_key(-1i32);
            let table = table_for_internal_assertion(&engine, table_id);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(&table, &pool_guards, &current_key)
                .await
                .unwrap();
            let block_id = {
                let snapshot = column_block_index_snapshot(&engine, table_id);
                let column_index = snapshot.index(pool_guards.disk_guard());
                column_index
                    .locate_block(row_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .block_id()
            };
            let index = bound_unique_index(&table, &pool_guards, 0);
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_lwc_row_shape_fingerprint(table_file_path, block_id);
            let _ = table
                .disk_pool()
                .invalidate_block(table.file().sparse_file().file_id(), block_id);

            let err = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap_err();
            assert_table_data_integrity(
                err,
                "lwc_block",
                block_id,
                DataIntegrityError::InvalidPayload,
            );
            assert_eq!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some((row_id, true))
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_retains_non_unique_delete_mark_without_delete_proof() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "current").await;

            let pk = single_key(0i32);
            let pool_guards = session.pool_guards();
            let row_id = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                0,
            )
            .lookup(&pk.vals, MAX_SNAPSHOT_TS)
            .await
            .unwrap()
            .unwrap()
            .0;
            let stale_key = name_key("stale");
            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                stale_key.index_no,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[1].scanned, 0);
            assert_eq!(stats.indexes[1].removed, 0);
            assert_eq!(stats.indexes[1].retained, 0);
            assert_eq!(stats.indexes[1].skipped_live, 1);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 1);
            assert_eq!(
                index
                    .lookup_unique(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(false)
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_with_purgeable_marker() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "current").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let pk = single_key(0i32);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &pk,
            )
            .await
            .unwrap();
            let stale_key = name_key("stale");
            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                stale_key.index_no,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(1))
                .unwrap();

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[1].scanned, 2);
            assert_eq!(stats.indexes[1].removed, 2);
            assert_eq!(stats.indexes[1].retained, 0);
            assert_eq!(stats.indexes[1].skipped_live, 0);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup_unique(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                None
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_with_matching_cold_entry() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "current").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let pk = single_key(0i32);
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &pk,
            )
            .await
            .unwrap();
            let current_key = name_key("current");
            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                current_key.index_no,
            );
            assert!(
                index
                    .mask_as_deleted(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[1].scanned, 1);
            assert_eq!(stats.indexes[1].removed, 0);
            assert_eq!(stats.indexes[1].retained, 1);
            assert_eq!(stats.indexes[1].skipped_live, 0);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup_unique(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(false)
            );

            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(1))
                .unwrap();
            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[1].scanned, 1);
            assert_eq!(stats.indexes[1].removed, 1);
            assert_eq!(stats.indexes[1].retained, 0);
            assert_eq!(stats.indexes[1].skipped_live, 0);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup_unique(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(true)
            );
        });
    }

    #[test]
    fn test_secondary_mem_index_cleanup_removes_non_unique_delete_mark_when_cold_row_key_differs() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "current").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let checkpoint_ts = assert_checkpoint_published(&mut session, table_id).await;
            session
                .wait_for_gc_horizon_after(checkpoint_ts)
                .await
                .unwrap();

            let pk = single_key(0i32);
            let current_key = name_key("current");
            let stale_key = name_key("stale");
            let pool_guards = session.pool_guards();
            let row_id = unique_disk_tree_lookup(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &pk,
            )
            .await
            .unwrap();
            assert!(
                non_unique_disk_tree_prefix_scan(
                    &table_for_internal_assertion(&engine, table_id),
                    &pool_guards,
                    &stale_key
                )
                .await
                .is_empty()
            );
            assert_eq!(
                non_unique_disk_tree_prefix_scan(
                    &table_for_internal_assertion(&engine, table_id),
                    &pool_guards,
                    &current_key
                )
                .await,
                vec![row_id]
            );
            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                stale_key.index_no,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            assert_eq!(
                index
                    .lookup_unique(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(false)
            );
            assert_eq!(
                index
                    .lookup_unique(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(true)
            );

            let stats = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert_eq!(stats.indexes[1].scanned, 2);
            assert_eq!(stats.indexes[1].removed, 2);
            assert_eq!(stats.indexes[1].retained, 0);
            assert_eq!(stats.indexes[1].skipped_live, 0);
            assert_eq!(stats.indexes[1].skipped_hot_deleted, 0);
            assert_eq!(
                index
                    .lookup_unique(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                index
                    .lookup_unique(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(true)
            );
        });
    }

    #[test]
    fn test_lwc_unique_index_purge_uses_purgeable_delete_marker_fast_path() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(0i32);
            let reader = session.begin_trx().unwrap();
            let pool_guards = session.pool_guards();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &key,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            assert!(
                index
                    .mask_as_deleted(&key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(10))
                .unwrap();

            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    true,
                    TrxID::new(11),
                )
                .await
                .unwrap();
            assert!(deleted);
            // A reinsertion attempt must not merge a stale MemIndex delete overlay;
            // after purge it falls through to the immutable cold root instead.
            assert_eq!(
                index
                    .insert_if_not_exists(&key.vals, row_id, true, TrxID::new(11),)
                    .await
                    .unwrap(),
                IndexInsert::DuplicateKey(row_id, false)
            );
        });
    }

    #[test]
    fn test_lwc_unique_index_purge_compares_persisted_key_when_marker_is_not_purgeable() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let current_key = single_key(0i32);
            let stale_key = single_key(-1i32);
            let reader = session.begin_trx().unwrap();
            let pool_guards = session.pool_guards();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &current_key,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                current_key.index_no,
            );
            let _ = index
                .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS)
                .await
                .unwrap();
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    stale_key.index_no,
                    &stale_key.vals,
                    row_id,
                    true,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap();
            assert!(deleted);
            assert!(
                index
                    .lookup(&stale_key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_none()
            );

            assert!(
                index
                    .mask_as_deleted(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(100))
                .unwrap();
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    current_key.index_no,
                    &current_key.vals,
                    row_id,
                    true,
                    TrxID::new(100),
                )
                .await
                .unwrap();
            assert!(!deleted);
            assert!(matches!(
                index
                    .lookup(&current_key.vals,
                        MAX_SNAPSHOT_TS,
                    )
                    .await
                    .unwrap(),
                Some((actual_row_id, true)) if actual_row_id == row_id
            ));
        });
    }

    #[test]
    fn test_lwc_non_unique_index_purge_compares_persisted_key_when_marker_is_not_purgeable() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "current").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let pk = single_key(0i32);
            let current_key = name_key("current");
            let stale_key = name_key("stale");
            let reader = session.begin_trx().unwrap();
            let pool_guards = session.pool_guards();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                &pk,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                current_key.index_no,
            );
            let _ = index
                .insert_if_not_exists(&stale_key.vals, row_id, false, MAX_SNAPSHOT_TS)
                .await
                .unwrap();
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    stale_key.index_no,
                    &stale_key.vals,
                    row_id,
                    false,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap();
            assert!(deleted);
            assert!(
                index
                    .lookup_unique(&stale_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_none()
            );

            assert!(
                index
                    .mask_as_deleted(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(row_id, TrxID::new(200))
                .unwrap();
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    current_key.index_no,
                    &current_key.vals,
                    row_id,
                    false,
                    TrxID::new(200),
                )
                .await
                .unwrap();
            assert!(!deleted);
            assert!(matches!(
                index
                    .lookup_unique(&current_key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(false)
            ));
        });
    }

    #[test]
    fn test_index_purge_removes_delete_marked_unique_entry_when_row_is_not_found() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();
            let key = single_key(9999i32);
            let row_id = 9999;
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            let _ = index
                .insert_if_not_exists(&key.vals, RowID::new(row_id), false, MAX_SNAPSHOT_TS)
                .await
                .unwrap();
            assert!(
                index
                    .mask_as_deleted(&key.vals, RowID::new(row_id), MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    key.index_no,
                    &key.vals,
                    RowID::new(row_id),
                    true,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap();
            assert!(deleted);
            assert!(
                index
                    .lookup(&key.vals, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn test_dropped_unique_index_purge_delete_is_noop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "redo_testsys_lightweight").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let key = single_key(1i32);
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("name")],
            )
            .await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let mut hold_session = engine.new_session().unwrap();
            let hold_trx = hold_session.begin_trx().unwrap();
            expect_delete_committed(table_id, &mut session, &key).await;
            let min_active_sts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap(),
            ) + 1;
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            assert!(matches!(
                index
                    .lookup(&key.vals,
                        MAX_SNAPSHOT_TS,
                    )
                    .await
                    .unwrap(),
                Some((actual_row_id, true)) if actual_row_id == row_id
            ));

            session.drop_index(table_id, 0).await.unwrap();

            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    true,
                    min_active_sts,
                )
                .await
                .unwrap();
            assert!(!deleted);
            hold_trx.commit().await.unwrap();
        })
    }

    #[test]
    fn test_dropped_non_unique_index_purge_delete_is_noop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys_non_unique")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let pk = single_key(1i32);
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("name")],
            )
            .await;
            let key = name_key("name");
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let mut hold_session = engine.new_session().unwrap();
            let hold_trx = hold_session.begin_trx().unwrap();
            expect_delete_committed(table_id, &mut session, &pk).await;
            let min_active_sts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap(),
            ) + 1;
            let pool_guards = session.pool_guards();
            let index = bound_non_unique_index_no(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            assert!(matches!(
                index
                    .lookup_unique(&key.vals, row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap(),
                Some(false)
            ));

            session
                .drop_index(table_id, key.index_no as IndexNo)
                .await
                .unwrap();

            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &pool_guards,
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    min_active_sts,
                )
                .await
                .unwrap();
            assert!(!deleted);
            hold_trx.commit().await.unwrap();
        })
    }
}
