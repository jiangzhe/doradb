mod access;
mod checkpoint_workflow;
mod deletion_buffer;
mod dml_validator;
mod gc;
mod hot;
mod layout;
mod lifecycle;
mod mem_table;
mod page_transition;
mod persistence;
mod recover;
mod rollback;
mod storage;
pub use access::LazyRow;
pub(crate) use access::*;
pub use checkpoint_workflow::{FreezeOutcome, FrozenPageBatchInfo};
use checkpoint_workflow::{FrozenPage, FrozenPageBatch, TableCheckpointWorkflow};
pub(crate) use deletion_buffer::*;
pub(crate) use dml_validator::*;
pub use gc::{SecondaryMemIndexCleanupIndexStats, SecondaryMemIndexCleanupStats};
pub(crate) use layout::{RetiredSecondaryIndex, TableRuntimeLayout};
pub use lifecycle::CheckpointCancelReason;
#[cfg(test)]
pub(crate) use lifecycle::CheckpointPublishLease;
#[cfg(test)]
pub(crate) use lifecycle::TableTerminal;
pub(crate) use lifecycle::{
    TableCheckpointRootMutationLease, TableDropDrain, TableLifecycle, TableMetadataChangeLease,
};
pub(crate) use mem_table::{MemTable, NoTrxUpsertChange, RowPageDescriptor};
pub use persistence::*;
pub(crate) use rollback::IndexRollback;
pub(crate) use storage::ColumnStorage;
#[cfg(test)]
pub(crate) use tests::{test_hooks, test_user_table_id};

use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::{EvictableBufferPool, PoolGuard, PoolGuards, PoolRole, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, InternalError, InternalResult, OperationResult,
    RuntimeError, RuntimeResult,
};
use crate::file::table_file::{ActiveRoot, TableFile};
use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
use crate::index::{
    BlockIndex, NonUniqueMemIndex, RowLocation, SecondaryDiskTreeRuntime, SecondaryIndex,
    UniqueMemIndex,
};
use crate::map::FastHashMap;
use crate::obs;
use crate::quiescent::QuiescentGuard;
use crate::row::ops::{RowUpdateInput, RowUpdateView, SelectKey, UpdateCol};
use crate::row::{RowPage, RowRead, var_len_for_insert};
use crate::trx::{TrxContext, TrxReadProof};
use crate::value::{PAGE_VAR_LEN_INLINE, Val};
use error_stack::{Report, ResultExt};
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::mem::take;
use std::result::Result as StdResult;
use std::sync::Arc;

/// Copied replay floor fields from one user-table active root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableRedoReplayFloor {
    /// Lower bound for replaying heap row-page redo.
    pub(crate) heap_redo_start_ts: TrxID,
    /// Lower bound for replaying persisted cold-delete metadata.
    pub(crate) deletion_cutoff_ts: TrxID,
}

impl TableRedoReplayFloor {
    /// Earliest table redo timestamp that may still affect recovered state.
    #[inline]
    pub(crate) fn replay_start_ts(self) -> TrxID {
        self.heap_redo_start_ts.min(self.deletion_cutoff_ts)
    }
}

/// Replay floor snapshot for one resident live user table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveTableRedoReplayFloor {
    /// User table id.
    pub(crate) table_id: TableID,
    /// Replay floor copied from the table active root.
    pub(crate) floor: TableRedoReplayFloor,
}

/// Runtime handle for a user table, combining in-memory and persisted storage.
pub(crate) struct Table {
    /// Hot row-store and in-memory index runtime.
    pub(crate) mem: MemTable<EvictableBufferPool, EvictableBufferPool>,
    /// Persisted column-store runtime and table file binding.
    pub(crate) storage: ColumnStorage,
    layout: Mutex<Arc<TableRuntimeLayout>>,
    retired_secondary_indexes: Mutex<Vec<RetiredSecondaryIndex>>,
    /// Runtime lifecycle gates for foreground, checkpoint, and drop operations.
    pub(crate) lifecycle: TableLifecycle,
    /// Canonical volatile freeze-to-checkpoint workflow state.
    checkpoint_workflow: TableCheckpointWorkflow,
}

impl Table {
    /// Create a new table.
    #[inline]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool_guard: &PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        file: Arc<TableFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> RuntimeResult<Self> {
        // `catalog_load_boundary`: runtime table construction uses the loaded
        // root to seed metadata and hot/cold secondary-index state.
        let active_root = file.active_root_unchecked();
        let metadata = Arc::clone(&active_root.metadata);
        let secondary_index_count = metadata.idx.index_slot_count();
        let sec_idx = build_dual_tree_secondary_indexes(
            index_pool,
            index_pool_guard,
            Arc::clone(&metadata),
            Arc::clone(&file),
            disk_pool.clone(),
            active_root.root_ts,
        )
        .await
        .change_context(RuntimeError::TableAccess)
        .attach_with(|| format!("operation=build_secondary_indexes, table_id={table_id}"))?;
        let mem = MemTable {
            table_id,
            metadata: Arc::clone(&metadata),
            mem_pool: mem_pool.clone(),
            row_pool_role: mem_pool.row_pool_role(),
            index_pool_role: PoolRole::Index,
            blk_idx,
            sec_idx: Box::new([]),
        };
        let storage = ColumnStorage::new(file, disk_pool)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| format!("operation=create_column_storage, table_id={table_id}"))?;
        assert_eq!(
            storage.secondary_index_runtimes().len(),
            secondary_index_count,
            "table construction invariant violated: cold runtime slots do not match metadata, runtime_slots={}, metadata_slots={secondary_index_count}",
            storage.secondary_index_runtimes().len()
        );
        assert_eq!(
            sec_idx.len(),
            secondary_index_count,
            "table construction invariant violated: hot runtime slots do not match metadata, runtime_slots={}, metadata_slots={secondary_index_count}",
            sec_idx.len()
        );
        let layout = TableRuntimeLayout::new(0, Arc::clone(&metadata), sec_idx);
        Ok(Table {
            mem,
            storage,
            layout: Mutex::new(Arc::new(layout)),
            retired_secondary_indexes: Mutex::new(Vec::new()),
            lifecycle: TableLifecycle::new(),
            checkpoint_workflow: TableCheckpointWorkflow::new(),
        })
    }

    /// Returns the logical table id of this user-table runtime.
    #[inline]
    pub fn table_id(&self) -> TableID {
        self.mem.table_id()
    }

    /// Ensures a foreground operation may access this table after logical locks.
    #[inline]
    pub(crate) fn check_foreground_live(&self) -> OperationResult<()> {
        self.lifecycle
            .check_foreground_live()
            .attach_with(|| format!("table_id={}", self.table_id()))
    }

    /// Acquires the reversible metadata-change gate for future index DDL.
    #[inline]
    pub(crate) async fn begin_metadata_change(
        &self,
    ) -> OperationResult<TableMetadataChangeLease<'_>> {
        self.lifecycle.begin_metadata_change(self.table_id()).await
    }

    /// Attempts to enter the checkpoint table-root mutation section.
    #[inline]
    pub(crate) fn try_begin_checkpoint_root_mutation(
        &self,
    ) -> StdResult<TableCheckpointRootMutationLease<'_>, CheckpointCancelReason> {
        self.lifecycle.try_begin_checkpoint_root_mutation()
    }

    /// Starts terminal drop admission and closes the checkpoint workflow.
    #[inline]
    pub(crate) fn start_drop_lifecycle(&self) -> OperationResult<TableDropDrain<'_>> {
        let drain = self.lifecycle.start_drop(self.table_id())?;
        self.checkpoint_workflow.close();
        Ok(drain)
    }

    /// Marks this table lifecycle as fully dropped.
    #[inline]
    pub(crate) fn mark_dropped_lifecycle(&self) {
        self.checkpoint_workflow.assert_closed();
        self.lifecycle.mark_dropped(self.table_id())
    }

    /// Closes an idle workflow for offline recovery or staged-runtime cleanup.
    #[inline]
    pub(crate) fn close_checkpoint_workflow_offline(&self) {
        self.checkpoint_workflow.close_offline();
    }

    /// Consume and destroy all in-memory runtime state for a dropped table.
    ///
    /// At runtime this is called by purge after the table has been logically
    /// removed from catalog and no crate-private runtime pins remain. Recovery
    /// can also use it before normal admission opens. Any runtime error means
    /// purge may have partially traversed owned memory/index structures, so the
    /// purge caller treats failure as fatal storage poison rather than retrying
    /// inline.
    #[inline]
    pub(crate) async fn destroy_dropped_runtime(self, guards: &PoolGuards) -> RuntimeResult<()> {
        let Table {
            mem,
            storage: _storage,
            layout,
            retired_secondary_indexes,
            lifecycle: _lifecycle,
            checkpoint_workflow,
        } = self;
        checkpoint_workflow.assert_closed();
        let index_pool_guard = guards.index_guard();
        for retired in retired_secondary_indexes.into_inner() {
            let _retired_identity = (retired.index_no, retired.retired_generation);
            let index = Arc::try_unwrap(retired.index).unwrap_or_else(|index| {
                panic!(
                    "retired secondary index still referenced during runtime destroy: index_no={}, strong_count={}",
                    index.index_no(),
                    Arc::strong_count(&index)
                )
            });
            index.destroy(index_pool_guard).await?;
        }
        let layout = Arc::try_unwrap(layout.into_inner()).unwrap_or_else(|layout| {
            panic!(
                "table runtime layout still referenced during runtime destroy: generation={}, strong_count={}",
                layout.generation(),
                Arc::strong_count(&layout)
            )
        });
        let indexes = layout.into_secondary_indexes();
        for index in indexes.iter().flatten() {
            assert_eq!(
                Arc::strong_count(index),
                1,
                "secondary index still referenced during runtime destroy: index_no={}",
                index.index_no()
            );
        }
        for index in indexes.into_iter().flatten() {
            let index = Arc::try_unwrap(index).unwrap_or_else(|index| {
                panic!(
                    "secondary index still referenced during runtime destroy: index_no={}, strong_count={}",
                    index.index_no(),
                    Arc::strong_count(&index)
                )
            });
            index.destroy(index_pool_guard).await?;
        }
        mem.destroy(guards).await
    }

    /// Build a lightweight operation accessor over an already captured layout.
    #[inline]
    pub(crate) fn accessor_with_layout<'a>(
        &'a self,
        layout: &'a TableRuntimeLayout,
    ) -> UserTableAccessor<'a> {
        UserTableAccessor::new(self, layout)
    }

    /// Capture the current user-table runtime layout.
    #[inline]
    pub(crate) fn layout_snapshot(&self) -> Arc<TableRuntimeLayout> {
        Arc::clone(&self.layout.lock())
    }

    /// Returns a current-layout metadata owner for this table.
    #[inline]
    pub(crate) fn metadata(&self) -> Arc<TableMetadata> {
        Arc::clone(self.layout_snapshot().metadata_arc())
    }

    /// Copy replay-bound fields from one active-root observation.
    #[inline]
    pub(crate) fn redo_replay_floor_snapshot(&self) -> TableRedoReplayFloor {
        // Redo retention planning is a maintenance dry run. It copies only
        // durable replay-bound fields and does not expose the active root.
        let active_root = self.storage.file().active_root_unchecked();
        TableRedoReplayFloor {
            heap_redo_start_ts: active_root.heap_redo_start_ts,
            deletion_cutoff_ts: active_root.deletion_cutoff_ts,
        }
    }

    /// Bind one active root observation under a transaction read proof.
    #[inline]
    pub(crate) fn with_active_root<'ctx, R, F>(&self, proof: &TrxReadProof<'ctx>, f: F) -> R
    where
        F: for<'root> FnOnce(&'root ActiveRoot) -> R,
    {
        self.storage.with_active_root(proof, f)
    }

    /// Capture an owned table-root snapshot for this table.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "proof-bound root snapshot tests and future read paths use this helper"
        )
    )]
    pub(crate) fn root_snapshot<'ctx>(
        &self,
        proof: &TrxReadProof<'ctx>,
    ) -> TableRootSnapshot<'ctx> {
        self.with_active_root(proof, |root| {
            TableRootSnapshot::from_active_root(root, proof)
        })
    }

    /// Returns the deletion buffer tracking persisted-row tombstones.
    #[inline]
    pub(crate) fn deletion_buffer(&self) -> &ColumnDeletionBuffer {
        self.storage.deletion_buffer()
    }

    /// Returns whether any retired secondary-index runtimes are queued.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "future index DDL tests and maintenance use this helper"
        )
    )]
    pub(crate) fn has_retired_secondary_indexes(&self) -> bool {
        !self.retired_secondary_indexes.lock().is_empty()
    }

    /// Installs a new user-table runtime layout for future index DDL paths.
    #[inline]
    pub(crate) fn install_runtime_layout(
        &self,
        expected_generation: u64,
        new_layout: TableRuntimeLayout,
    ) -> Arc<TableRuntimeLayout> {
        new_layout.assert_valid();
        assert!(
            new_layout.generation() > expected_generation,
            "table runtime layout install invariant violated: generation did not advance, expected_generation={expected_generation}, new_generation={}",
            new_layout.generation()
        );

        let new_layout = Arc::new(new_layout);
        let old_layout = {
            let mut guard = self.layout.lock();
            assert_eq!(
                guard.generation(),
                expected_generation,
                "table runtime layout install invariant violated: stale generation, expected_generation={expected_generation}, actual_generation={}",
                guard.generation()
            );
            assert!(
                new_layout.index_slot_count() >= guard.index_slot_count(),
                "table runtime layout install invariant violated: sparse slots shrank, old_slots={}, new_slots={}",
                guard.index_slot_count(),
                new_layout.index_slot_count()
            );
            let old_layout = Arc::clone(&guard);
            *guard = Arc::clone(&new_layout);
            old_layout
        };

        let mut retired = Vec::new();
        for (index_no, old_index) in old_layout.secondary_indexes().iter().enumerate() {
            let Some(old_index) = old_index else {
                continue;
            };
            let unchanged = new_layout
                .secondary_indexes()
                .get(index_no)
                .and_then(Option::as_ref)
                .is_some_and(|new_index| Arc::ptr_eq(old_index, new_index));
            if !unchanged {
                retired.push(RetiredSecondaryIndex {
                    index_no,
                    retired_generation: old_layout.generation(),
                    index: Arc::clone(old_index),
                });
            }
        }
        if !retired.is_empty() {
            self.retired_secondary_indexes.lock().extend(retired);
        }
        new_layout
    }

    /// Destroys retired secondary-index runtimes whose old layout snapshots drained.
    pub(crate) async fn cleanup_retired_secondary_indexes(
        &self,
        guards: &PoolGuards,
    ) -> RuntimeResult<usize> {
        let mut ready = Vec::new();
        {
            let mut queued = self.retired_secondary_indexes.lock();
            let mut pending = Vec::with_capacity(queued.len());
            for retired in queued.drain(..) {
                if Arc::strong_count(&retired.index) > 1 {
                    pending.push(retired);
                } else {
                    ready.push(retired);
                }
            }
            *queued = pending;
        }

        let index_pool_guard = guards.index_guard();
        let mut cleaned = 0usize;
        while let Some(retired) = ready.pop() {
            let index_no = retired.index_no;
            let retired_generation = retired.retired_generation;
            let index = match Arc::try_unwrap(retired.index) {
                Ok(index) => index,
                Err(index) => {
                    self.retired_secondary_indexes
                        .lock()
                        .push(RetiredSecondaryIndex {
                            index_no,
                            retired_generation,
                            index,
                        });
                    continue;
                }
            };
            if let Err(err) = index.destroy(index_pool_guard).await {
                // The failing index has been consumed by destroy. Keep the
                // remaining ready entries queued so a later maintenance pass
                // can continue after the caller handles the error.
                self.retired_secondary_indexes.lock().extend(ready);
                return Err(err);
            }
            let _destroyed_identity = (index_no, retired_generation);
            cleaned += 1;
        }
        Ok(cleaned)
    }

    /// Returns the readonly disk buffer pool used by persisted table data.
    #[inline]
    pub(crate) fn disk_pool(&self) -> &QuiescentGuard<ReadonlyBufferPool> {
        self.storage.disk_pool()
    }

    /// Returns the backing table file for this user table.
    #[inline]
    pub(crate) fn file(&self) -> &Arc<TableFile> {
        self.storage.file()
    }

    /// Find the current hot or persisted location for a row id.
    #[inline]
    pub(crate) async fn find_row(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
    ) -> RuntimeResult<RowLocation> {
        self.mem
            .blk_idx
            .find_row(
                guards.meta_guard(),
                Some(guards.disk_guard()),
                row_id,
                Some(&self.storage),
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=find_row, table_id={}, row_id={row_id}",
                    self.table_id()
                )
            })
    }

    /// Returns total number of row pages.
    #[inline]
    pub(crate) async fn total_row_pages(&self, guards: &PoolGuards) -> RuntimeResult<usize> {
        let mut res = 0usize;
        let pivot_row_id = self.mem.pivot_row_id();
        let meta_pool_guard = guards.meta_guard();
        let mut cursor = self.mem.blk_idx().mem_cursor(meta_pool_guard);
        cursor.seek(pivot_row_id).await?;
        while let Some(leaf) = cursor.next().await? {
            // A cursor leaf is protected by its held parent and cannot be stale
            // before this immediate shared-lock conversion.
            let g = leaf.lock_shared_async().await.unwrap();
            debug_assert!(g.page().is_leaf());
            res += g
                .page()
                .leaf_entries()
                .iter()
                .filter(|entry| entry.row_id >= pivot_row_id)
                .count();
        }
        Ok(res)
    }

    async fn mem_scan<F>(&self, guards: &PoolGuards, mut page_action: F) -> RuntimeResult<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        // With cursor, we lock two pages in block index and one row page
        // when scanning rows.
        // Admitted table scans always carry the metadata guard; catalog-only
        // bundles are partial only for roles the catalog does not access.
        let meta_pool_guard = guards.meta_guard();
        let pivot_row_id = self.mem.pivot_row_id();
        let mut cursor = self.mem.blk_idx().mem_cursor(meta_pool_guard);
        cursor.seek(pivot_row_id).await?;
        while let Some(leaf) = cursor.next().await? {
            let g = leaf.lock_shared_async().await.unwrap_or_else(|| {
                panic!(
                    "cursor-held row-page-index leaf could not be locked: operation=table_mem_scan, table_id={}",
                    self.table_id()
                )
            });
            debug_assert!(g.page().is_leaf());
            let entries = g.page().leaf_entries();
            for page_entry in entries {
                if page_entry.row_id < pivot_row_id {
                    continue;
                }
                let page_guard = self
                    .mem
                    .must_get_row_page_shared(guards, page_entry.page_id)
                    .await?;
                if !page_action(page_guard) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn recover_row_insert_to_page(
        &self,
        metadata: &TableMetadata,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
    ) -> DataIntegrityResult<()> {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        debug_assert!(metadata.col.col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);
        if !page.row_id_in_valid_range(row_id) {
            return Err(recovery_page_invariant_error(
                "insert",
                page_id,
                row_id,
                cts,
                "row id outside page range",
            ));
        }
        let row_idx = page.row_idx(row_id);
        if !page_guard
            .try_rmap()
            .ok_or_else(|| {
                recovery_page_invariant_error("insert", page_id, row_id, cts, "missing recover map")
            })?
            .is_vacant(row_idx)
        {
            return Err(recovery_page_invariant_error(
                "insert",
                page_id,
                row_id,
                cts,
                "row slot is not vacant",
            ));
        }
        let var_len = var_len_for_insert(metadata.col.as_ref(), cols);
        let (var_offset, var_end) = if let Some(var_offset) = page.request_free_space(var_len) {
            (var_offset, var_offset + var_len)
        } else {
            return Err(recovery_page_invariant_error(
                "insert",
                page_id,
                row_id,
                cts,
                "insufficient row page space",
            ));
        };
        // update count field to include current row id.
        page_guard.page_mut().update_count_to_include_row_id(row_id);
        // insert CTS.
        page_guard
            .try_rmap_mut()
            .ok_or_else(|| {
                recovery_page_invariant_error("insert", page_id, row_id, cts, "missing recover map")
            })?
            .insert_at(row_idx, cts);
        let page = page_guard.page_mut();
        let row_idx = page.row_idx(row_id);
        let mut row = page.row_mut_exclusive(row_idx, var_offset, var_end);
        if !row.is_deleted() {
            return Err(recovery_page_invariant_error(
                "insert",
                page_id,
                row_id,
                cts,
                "row slot is not deleted",
            ));
        }
        for (user_col_idx, user_col) in cols.iter().enumerate() {
            row.update_col(metadata.col.as_ref(), user_col_idx, user_col, false);
        }
        row.finish_insert();
        Ok(())
    }

    #[inline]
    fn recover_row_update_to_page(
        &self,
        metadata: &TableMetadata,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cols: &[UpdateCol],
        cts: TrxID,
    ) -> DataIntegrityResult<()> {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        // column indexes must be in range
        debug_assert!(
            {
                cols.iter()
                    .all(|uc| uc.idx < page.header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                cols.is_empty()
                    || cols
                        .iter()
                        .zip(cols.iter().skip(1))
                        .all(|(l, r)| l.idx < r.idx)
            },
            "update columns should be in order"
        );
        if !page.row_id_in_valid_range(row_id) {
            return Err(recovery_page_invariant_error(
                "update",
                page_id,
                row_id,
                cts,
                "row id outside page range",
            ));
        }
        let row_idx = page.row_idx(row_id);
        if page.row(row_idx).is_deleted() {
            return Err(recovery_page_invariant_error(
                "update",
                page_id,
                row_id,
                cts,
                "row is deleted",
            ));
        }
        let var_len = page.var_len_for_update(row_idx, RowUpdateView::Sparse(cols));
        let (var_offset, var_end) = if let Some(var_offset) = page.request_free_space(var_len) {
            (var_offset, var_offset + var_len)
        } else {
            return Err(recovery_page_invariant_error(
                "update",
                page_id,
                row_id,
                cts,
                "insufficient row page space",
            ));
        };
        if page_guard
            .try_rmap()
            .ok_or_else(|| {
                recovery_page_invariant_error("update", page_id, row_id, cts, "missing recover map")
            })?
            .at(row_idx)
            .is_none()
        {
            return Err(recovery_page_invariant_error(
                "update",
                page_id,
                row_id,
                cts,
                "missing recover CTS",
            ));
        }
        // update CTS.
        page_guard
            .try_rmap_mut()
            .ok_or_else(|| {
                recovery_page_invariant_error("update", page_id, row_id, cts, "missing recover map")
            })?
            .update_at(row_idx, cts);
        let page = page_guard.page_mut();
        let mut row = page.row_mut_exclusive(row_idx, var_offset, var_end);
        debug_assert_eq!(row_id, row.row_id());

        for uc in cols {
            row.update_col(metadata.col.as_ref(), uc.idx, &uc.val, true);
        }
        row.finish_update();
        Ok(())
    }

    #[inline]
    fn recover_row_delete_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cts: TrxID,
    ) -> DataIntegrityResult<()> {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return Err(recovery_page_invariant_error(
                "delete",
                page_id,
                row_id,
                cts,
                "row id outside page range",
            ));
        }
        let row_idx = page.row_idx(row_id);
        let was_deleted = page.is_deleted(row_idx);
        if was_deleted {
            return Err(recovery_page_invariant_error(
                "delete",
                page_id,
                row_id,
                cts,
                "row is already deleted",
            ));
        }
        if page_guard
            .try_rmap()
            .ok_or_else(|| {
                recovery_page_invariant_error("delete", page_id, row_id, cts, "missing recover map")
            })?
            .at(row_idx)
            .is_none()
        {
            return Err(recovery_page_invariant_error(
                "delete",
                page_id,
                row_id,
                cts,
                "missing recover CTS",
            ));
        }
        page_guard
            .try_rmap_mut()
            .ok_or_else(|| {
                recovery_page_invariant_error("delete", page_id, row_id, cts, "missing recover map")
            })?
            .update_at(row_idx, cts);
        let page = page_guard.page_mut();
        page.set_deleted_exclusive(row_idx, true);
        if !was_deleted {
            page.inc_approx_deleted();
        }
        Ok(())
    }
}

/// Owned projection of one proof-gated user-table active root.
///
/// The snapshot contains only runtime read contract fields copied from a
/// single active-root observation. Publication and allocation internals remain
/// behind the table-file boundary.
pub(crate) struct TableRootSnapshot<'ctx> {
    root_ts: TrxID,
    effective_ts: TrxID,
    pivot_row_id: RowID,
    column_block_index_root: BlockID,
    secondary_index_roots: Vec<BlockID>,
    deletion_cutoff_ts: TrxID,
    _proof: PhantomData<&'ctx TrxContext>,
}

impl<'ctx> TableRootSnapshot<'ctx> {
    #[inline]
    fn from_active_root(root: &ActiveRoot, _proof: &TrxReadProof<'ctx>) -> Self {
        Self {
            root_ts: root.root_ts,
            effective_ts: root.effective_ts(),
            pivot_row_id: root.pivot_row_id,
            column_block_index_root: root.column_block_index_root,
            secondary_index_roots: root.secondary_index_roots.clone(),
            deletion_cutoff_ts: root.deletion_cutoff_ts,
            _proof: PhantomData,
        }
    }

    /// Returns the durable timestamp carried by the captured root.
    ///
    /// This is a root publication timestamp, not always a transaction commit
    /// timestamp. Initial create-table roots and checkpoint roots use the
    /// publishing transaction STS, while index-DDL roots use the DDL commit CTS.
    #[inline]
    pub(crate) fn root_ts(&self) -> TrxID {
        self.root_ts
    }

    /// Returns the runtime timestamp proving when the captured root became
    /// observable through the table-file active-root pointer.
    #[inline]
    pub(crate) fn effective_ts(&self) -> TrxID {
        self.effective_ts
    }

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.pivot_row_id
    }

    /// Returns the persisted column-block-index root from the captured root.
    #[inline]
    pub(crate) fn column_block_index_root(&self) -> BlockID {
        self.column_block_index_root
    }

    /// Returns the cold-row deletion replay cutoff from the captured root.
    #[inline]
    pub(crate) fn deletion_cutoff_ts(&self) -> TrxID {
        self.deletion_cutoff_ts
    }

    /// Returns the captured DiskTree root for one secondary index.
    #[inline]
    pub(crate) fn secondary_index_root(&self, index_no: usize) -> InternalResult<BlockID> {
        self.secondary_index_roots
            .get(index_no)
            .copied()
            .ok_or_else(|| missing_secondary_index(index_no, self.secondary_index_roots.len()))
    }

    /// Returns whether the captured root was observable before the supplied
    /// snapshot time.
    #[inline]
    pub(crate) fn root_is_visible_to(&self, sts: TrxID) -> bool {
        self.effective_ts < sts
    }
}

/// Stages newly built dual-tree secondary indexes until the caller publishes them.
struct SecondaryIndexScopedBuilder {
    staged: Vec<Option<SecondaryIndex<EvictableBufferPool>>>,
}

impl SecondaryIndexScopedBuilder {
    #[inline]
    fn new(capacity: usize) -> Self {
        let mut staged = Vec::with_capacity(capacity);
        staged.resize_with(capacity, || None);
        Self { staged }
    }

    #[inline]
    fn push(&mut self, index_no: usize, index: SecondaryIndex<EvictableBufferPool>) {
        debug_assert!(self.staged[index_no].is_none());
        self.staged[index_no] = Some(index);
    }

    #[inline]
    async fn rollback(&mut self, pool_guard: &PoolGuard) {
        for index in take(&mut self.staged).into_iter().rev().flatten() {
            let index_no = index.index_no();
            // Keep the original construction error as the function result,
            // but observe this terminal best-effort cleanup report first.
            if let Err(report) = index.destroy(pool_guard).await {
                let report = report.attach(format!(
                    "operation=rollback_secondary_index_build, index_no={index_no}"
                ));
                obs::error!(
                    "event=secondary_index_cleanup component=table action=destroy_staged result=error error={report:?}"
                );
            }
        }
    }

    #[inline]
    fn publish(self) -> Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]> {
        self.staged
            .into_iter()
            .map(|index| index.map(Arc::new))
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }
}

enum UpdateUniqueMvcc {
    Updated(RowID),
    NotFound(RowUpdateInput),
}

/// Build user-table dual-tree secondary indexes from fresh MemIndex backends
/// paired with the table file's checkpointed DiskTree runtimes.
#[inline]
pub(crate) async fn build_dual_tree_secondary_indexes(
    index_pool: QuiescentGuard<EvictableBufferPool>,
    index_pool_guard: &PoolGuard,
    metadata: Arc<TableMetadata>,
    file: Arc<TableFile>,
    disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    index_ts: TrxID,
) -> RuntimeResult<Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]>> {
    let mut builder = SecondaryIndexScopedBuilder::new(metadata.idx.index_slot_count());
    for (index_no, index_spec) in metadata.idx.active_indexes() {
        let runtime = match SecondaryDiskTreeRuntime::new(
            index_no,
            Arc::clone(&metadata),
            Arc::clone(&file),
            disk_pool.clone(),
        ) {
            Ok(runtime) => runtime,
            Err(err) => {
                builder.rollback(index_pool_guard).await;
                return Err(err);
            }
        };
        let ty_infer = |col_no: usize| metadata.col.col_type(col_no);
        let index = if index_spec.unique() {
            let mem = match UniqueMemIndex::new(
                index_pool.clone(),
                index_pool_guard,
                index_spec,
                ty_infer,
                index_ts,
            )
            .await
            {
                Ok(mem) => mem,
                Err(err) => {
                    builder.rollback(index_pool_guard).await;
                    return Err(err);
                }
            };
            SecondaryIndex::Unique { mem, disk: runtime }
        } else {
            let mem = match NonUniqueMemIndex::new(
                index_pool.clone(),
                index_pool_guard,
                index_spec,
                ty_infer,
                index_ts,
            )
            .await
            {
                Ok(mem) => mem,
                Err(err) => {
                    builder.rollback(index_pool_guard).await;
                    return Err(err);
                }
            };
            SecondaryIndex::NonUnique { mem, disk: runtime }
        };
        builder.push(index_no, index);
    }
    Ok(builder.publish())
}

#[inline]
fn validate_page_row_range(
    page_guard: &PageSharedGuard<RowPage>,
    page_id: PageID,
    row_id: RowID,
) -> bool {
    if page_guard.page_id() != page_id {
        return false;
    }
    page_guard.page().row_id_in_valid_range(row_id)
}

#[inline]
fn recovery_page_invariant_error(
    op: &str,
    page_id: PageID,
    row_id: RowID,
    cts: TrxID,
    reason: &str,
) -> Report<DataIntegrityError> {
    Report::new(DataIntegrityError::InvalidRootInvariant).attach(format!(
        "recover row {op}: page_id={page_id}, row_id={row_id}, cts={cts}, reason={reason}"
    ))
}

#[inline]
fn row_len(metadata: &TableMetadata, cols: &[Val]) -> usize {
    let var_len = metadata
        .col
        .var_cols()
        .iter()
        .map(|idx| {
            let val = &cols[*idx];
            match val {
                Val::Null => 0,
                Val::VarByte(var) => {
                    if var.len() <= PAGE_VAR_LEN_INLINE {
                        0
                    } else {
                        var.len()
                    }
                }
                _ => unreachable!(),
            }
        })
        .sum::<usize>();
    metadata.col.fix_len() + var_len
}

#[inline]
fn index_key_is_changed(
    index_spec: &IndexSpec,
    index_change_cols: &FastHashMap<usize, Val>,
) -> bool {
    index_spec
        .cols
        .iter()
        .any(|key| index_change_cols.contains_key(&(key.col_no as usize)))
}

#[inline]
fn index_key_replace(
    index_spec: &IndexSpec,
    key: &SelectKey,
    updates: &FastHashMap<usize, Val>,
) -> SelectKey {
    let vals: Vec<Val> = index_spec
        .cols
        .iter()
        .zip(&key.vals)
        .map(|(ik, val)| {
            let col_no = ik.col_no as usize;
            updates.get(&col_no).cloned().unwrap_or_else(|| val.clone())
        })
        .collect();
    SelectKey::new(key.index_no, vals)
}

#[inline]
fn read_latest_index_key(
    metadata: &TableMetadata,
    index_no: usize,
    page_guard: &PageSharedGuard<RowPage>,
    row_id: RowID,
) -> SelectKey {
    let index_spec = metadata
        .idx
        .index_spec(index_no)
        .expect("active index spec must exist for latest key read");
    let mut new_key = SelectKey::null(index_no, index_spec.cols.len());
    for (pos, key) in index_spec.cols.iter().enumerate() {
        let access = page_guard.read_row_by_id(row_id);
        let val = access.row().val(metadata.col.as_ref(), key.col_no as usize);
        new_key.vals[pos] = val;
    }
    new_key
}

#[inline]
fn unique_key_from_full_row(
    metadata: &TableMetadata,
    unique_index_no: usize,
    cols: &[Val],
    operation: &'static str,
) -> SelectKey {
    debug_assert!(cols.len() == metadata.col.col_count());
    debug_assert!(
        cols.iter()
            .enumerate()
            .all(|(idx, val)| metadata.col.col_type_match(idx, val))
    );
    let index_spec = metadata
        .idx
        .index_spec(unique_index_no)
        .unwrap_or_else(|| {
            panic!(
                "unique-key construction requires an active index: operation={operation}, index_no={unique_index_no}"
            )
        });
    assert!(
        index_spec.unique(),
        "unique-key construction requires a unique index: operation={operation}, index_no={unique_index_no}"
    );
    let vals = index_spec
        .cols
        .iter()
        .map(|key| cols[key.col_no as usize].clone())
        .collect();
    SelectKey::new(unique_index_no, vals)
}

#[inline]
fn missing_secondary_index(index_no: usize, index_count: usize) -> Report<InternalError> {
    Report::new(InternalError::SecondaryIndexOutOfBounds)
        .attach(format!("index_no={index_no}, index_count={index_count}"))
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::buffer::page::PAGE_SIZE;
    use crate::buffer::{PoolGuard, PoolGuards, PoolRole, ReadonlyBufferPool};
    use crate::catalog::tests::table2;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
        USER_TABLE_ID_START,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{
        CompletionErrorBridge, DataIntegrityError, Error, FatalError, OperationError, Result,
        RuntimeResult,
    };
    use crate::file::block_integrity::{BLOCK_INTEGRITY_HEADER_SIZE, write_block_checksum};
    use crate::file::cow_file::{COW_FILE_PAGE_SIZE, SUPER_BLOCK_ID};
    use crate::file::table_file::ActiveRoot;
    use crate::file::{FileKind, SparseFile};
    use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
    use crate::index::{
        COLUMN_BLOCK_HEADER_SIZE, COLUMN_BLOCK_LEAF_HEADER_SIZE, ColumnBlockIndex,
        IndexBatchStream, IndexCompareExchange, IndexInsert, IndexLookupCandidate, KeyRange,
        NonUniqueIndex, RowLocation, UniqueIndex,
    };
    use crate::io::{
        IOKind, StdIoResult, StorageBackendFileIdentity, StorageBackendOp, StorageBackendTestHook,
    };
    use crate::lock::tests::{LockDebugEntryState, debug_snapshot};
    use crate::lock::{LockMode, LockOwner, LockResource};
    use crate::quiescent::QuiescentGuard;
    use crate::row::ops::{DeleteMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::session::{Session, tests::SessionTestExt};
    use crate::table::{
        CheckpointPublishLease, DeleteMarker, DmlValidationError, FreezeOutcome,
        FrozenPageBatchInfo, Table, TableCheckpointRootMutationLease, TableRuntimeLayout,
    };
    use crate::trx::Transaction;
    use crate::trx::stmt::Statement;
    use crate::value::{Val, ValKind};
    use smol::Timer;
    use std::fs::OpenOptions;
    use std::io::{Error as IoError, Read, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;

    pub(crate) mod test_hooks {
        use crate::error::{InternalError, RuntimeError, RuntimeResult};
        use crate::id::PageID;
        use error_stack::Report;
        use std::cell::{Cell, RefCell};

        type FreezePageHook = Box<dyn FnOnce(PageID) + 'static>;
        type FrozenPageScanHook = Box<dyn FnMut(PageID) + 'static>;
        type FrozenPageRowScanHook = Box<dyn FnMut(PageID, usize) + 'static>;
        type OptimisticPagePlanComparisonHook = Box<dyn FnMut(PageID, u64, u64, bool) + 'static>;
        type FrozenPagePhaseHook = Box<dyn FnOnce() + 'static>;
        type TransitionPageHook = Box<dyn FnOnce(PageID) + 'static>;
        type HotRowWriteHook = Box<dyn FnOnce() + 'static>;

        thread_local! {
            static TEST_FORCE_LWC_BUILD_ERROR: Cell<bool> = const { Cell::new(false) };
            static TEST_FREEZE_PAGE_STATE_LOCKED_HOOK: RefCell<Option<FreezePageHook>> =
                const { RefCell::new(None) };
            static TEST_FROZEN_PAGE_SCAN_HOOK: RefCell<Option<FrozenPageScanHook>> =
                const { RefCell::new(None) };
            static TEST_FROZEN_PAGE_ROW_SCAN_HOOK: RefCell<Option<FrozenPageRowScanHook>> =
                const { RefCell::new(None) };
            static TEST_OPTIMISTIC_PAGE_PLAN_COMPARISON_HOOK:
                RefCell<Option<OptimisticPagePlanComparisonHook>> = const { RefCell::new(None) };
            static TEST_FROZEN_PAGES_READY_HOOK: RefCell<Option<FrozenPagePhaseHook>> =
                const { RefCell::new(None) };
            static TEST_STABLE_PAGE_PLANS_REFRESHED_HOOK: RefCell<Option<FrozenPagePhaseHook>> =
                const { RefCell::new(None) };
            static TEST_LOCKED_PAGE_PLAN_REBUILD_HOOK: RefCell<Option<TransitionPageHook>> =
                const { RefCell::new(None) };
            static TEST_TRANSITION_PAGE_PUBLISHED_HOOK: RefCell<Option<TransitionPageHook>> =
                const { RefCell::new(None) };
            static TEST_HOT_ROW_WRITE_BEFORE_STATE_LOCK_HOOK: RefCell<Option<HotRowWriteHook>> =
                const { RefCell::new(None) };
        }

        pub(crate) fn set_test_force_lwc_build_error(enabled: bool) {
            TEST_FORCE_LWC_BUILD_ERROR.with(|flag| flag.set(enabled));
        }

        pub(crate) struct ForceLwcBuildErrorGuard;

        impl ForceLwcBuildErrorGuard {
            pub(crate) fn new() -> Self {
                set_test_force_lwc_build_error(true);
                Self
            }
        }

        impl Drop for ForceLwcBuildErrorGuard {
            fn drop(&mut self) {
                set_test_force_lwc_build_error(false);
            }
        }

        pub(crate) fn maybe_force_lwc_build_error() -> RuntimeResult<()> {
            if TEST_FORCE_LWC_BUILD_ERROR.with(|flag| flag.get()) {
                return Err(Report::new(InternalError::LwcBuilderMisuse)
                    .attach("test LWC build failure")
                    .change_context(RuntimeError::TableAccess));
            }
            Ok(())
        }

        pub(crate) fn set_test_freeze_page_state_locked_hook<F>(hook: F)
        where
            F: FnOnce(PageID) + 'static,
        {
            TEST_FREEZE_PAGE_STATE_LOCKED_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "freeze page-state hook already installed");
            });
        }

        pub(crate) fn run_test_freeze_page_state_locked_hook(page_id: PageID) {
            let hook = TEST_FREEZE_PAGE_STATE_LOCKED_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook(page_id);
            }
        }

        pub(crate) fn set_test_locked_page_plan_rebuild_hook<F>(hook: F)
        where
            F: FnOnce(PageID) + 'static,
        {
            TEST_LOCKED_PAGE_PLAN_REBUILD_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "locked page-plan hook already installed");
            });
        }

        pub(crate) fn run_test_locked_page_plan_rebuild_hook(page_id: PageID) {
            let hook = TEST_LOCKED_PAGE_PLAN_REBUILD_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook(page_id);
            }
        }

        pub(crate) fn set_test_transition_page_published_hook<F>(hook: F)
        where
            F: FnOnce(PageID) + 'static,
        {
            TEST_TRANSITION_PAGE_PUBLISHED_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "transition page hook already installed");
            });
        }

        pub(crate) fn set_test_frozen_page_scan_hook<F>(hook: F)
        where
            F: FnMut(PageID) + 'static,
        {
            TEST_FROZEN_PAGE_SCAN_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "frozen-page scan hook already installed");
            });
        }

        pub(crate) fn run_test_frozen_page_scan_hook(page_id: PageID) {
            TEST_FROZEN_PAGE_SCAN_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().as_mut() {
                    hook(page_id);
                }
            });
        }

        pub(crate) fn set_test_frozen_page_row_scan_hook<F>(hook: F)
        where
            F: FnMut(PageID, usize) + 'static,
        {
            TEST_FROZEN_PAGE_ROW_SCAN_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "frozen-page row-scan hook already installed");
            });
        }

        pub(crate) fn run_test_frozen_page_row_scan_hook(page_id: PageID, row_idx: usize) {
            TEST_FROZEN_PAGE_ROW_SCAN_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().as_mut() {
                    hook(page_id, row_idx);
                }
            });
        }

        pub(crate) fn set_test_optimistic_page_plan_comparison_hook<F>(hook: F)
        where
            F: FnMut(PageID, u64, u64, bool) + 'static,
        {
            TEST_OPTIMISTIC_PAGE_PLAN_COMPARISON_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(
                    old.is_none(),
                    "optimistic page-plan comparison hook already installed"
                );
            });
        }

        pub(crate) fn run_test_optimistic_page_plan_comparison_hook(
            page_id: PageID,
            version_before: u64,
            version_after: u64,
            retained: bool,
        ) {
            TEST_OPTIMISTIC_PAGE_PLAN_COMPARISON_HOOK.with(|slot| {
                if let Some(hook) = slot.borrow_mut().as_mut() {
                    hook(page_id, version_before, version_after, retained);
                }
            });
        }

        pub(crate) fn set_test_frozen_pages_ready_hook<F>(hook: F)
        where
            F: FnOnce() + 'static,
        {
            TEST_FROZEN_PAGES_READY_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "frozen-pages-ready hook already installed");
            });
        }

        pub(crate) fn run_test_frozen_pages_ready_hook() {
            let hook = TEST_FROZEN_PAGES_READY_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }

        pub(crate) fn set_test_stable_page_plans_refreshed_hook<F>(hook: F)
        where
            F: FnOnce() + 'static,
        {
            TEST_STABLE_PAGE_PLANS_REFRESHED_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "stable page-plans hook already installed");
            });
        }

        pub(crate) fn run_test_stable_page_plans_refreshed_hook() {
            let hook = TEST_STABLE_PAGE_PLANS_REFRESHED_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }

        pub(crate) fn run_test_transition_page_published_hook(page_id: PageID) {
            let hook = TEST_TRANSITION_PAGE_PUBLISHED_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook(page_id);
            }
        }

        pub(crate) fn set_test_hot_row_write_before_state_lock_hook<F>(hook: F)
        where
            F: FnOnce() + 'static,
        {
            TEST_HOT_ROW_WRITE_BEFORE_STATE_LOCK_HOOK.with(|slot| {
                let old = slot.borrow_mut().replace(Box::new(hook));
                assert!(old.is_none(), "hot-row state-lock hook already installed");
            });
        }

        pub(crate) fn run_test_hot_row_write_before_state_lock_hook() {
            let hook =
                TEST_HOT_ROW_WRITE_BEFORE_STATE_LOCK_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }
    }

    pub(crate) const LIGHTWEIGHT_TEST_BUFFER_BYTES: usize = 16 * 1024 * 1024;
    pub(crate) const LIGHTWEIGHT_TEST_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
    pub(crate) const LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES: usize = 32 * 1024 * 1024;

    #[inline]
    pub(crate) fn test_user_table_id(offset: u64) -> TableID {
        USER_TABLE_ID_START
            .checked_add(offset)
            .expect("test user table id offset overflow")
    }

    pub(crate) fn begin_checkpoint_publish_for_test(
        table: &Table,
    ) -> (
        TableCheckpointRootMutationLease<'_>,
        CheckpointPublishLease<'_>,
    ) {
        let root_lease = table.try_begin_checkpoint_root_mutation().unwrap();
        let publish_lease = table.lifecycle.try_begin_checkpoint_publish().unwrap();
        (root_lease, publish_lease)
    }

    pub(crate) fn assert_checkpoint_workflow_closed(table: &Table) {
        table.checkpoint_workflow.assert_closed();
    }

    pub(crate) fn assert_freeze_created(outcome: FreezeOutcome) -> FrozenPageBatchInfo {
        let FreezeOutcome::Frozen { batch } = outcome else {
            panic!("freeze should create a new frozen batch, got {outcome:?}");
        };
        batch
    }

    pub(crate) fn assert_table_data_integrity(
        err: Error,
        block_kind: &str,
        block_id: BlockID,
        expected: DataIntegrityError,
    ) {
        let report = format!("{err:?}");
        assert_eq!(
            err.report().downcast_ref::<DataIntegrityError>().copied(),
            Some(expected),
            "{report}"
        );
        assert!(!report.contains("propagate from other threads"), "{report}");
        assert!(
            err.report()
                .downcast_ref::<CompletionErrorBridge>()
                .is_none(),
            "{report}"
        );
        assert!(report.contains("table_file"), "{report}");
        assert!(report.contains(block_kind), "{report}");
        assert!(report.contains(&format!("block_id={block_id}")), "{report}");
    }

    pub(crate) fn assert_checkpoint_write_poisoned(err: &Error, engine: &Engine) {
        assert_eq!(
            err.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::CheckpointWrite)
        );
        assert!(
            engine
                .inner()
                .poisoner
                .poison_error()
                .as_ref()
                .is_some_and(|err| *err.current_context() == FatalError::CheckpointWrite)
        );
    }

    pub(crate) async fn stmt_insert_row_by_id(
        stmt: &mut Statement<'_>,
        table_id: TableID,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        stmt.table_insert_mvcc(table_id, cols).await
    }

    pub(crate) async fn stmt_delete_row_by_id(
        stmt: &mut Statement<'_>,
        table_id: TableID,
        key: &SelectKey,
    ) -> Result<DeleteMvcc> {
        stmt.table_delete_unique_mvcc(table_id, key.index_no, &key.vals, false)
            .await
    }

    pub(crate) async fn stmt_update_row_by_id(
        stmt: &mut Statement<'_>,
        table_id: TableID,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        stmt.table_update_unique_mvcc(table_id, key.index_no, &key.vals, update)
            .await
    }

    pub(crate) async fn stmt_select_row_mvcc_by_id(
        stmt: &mut Statement<'_>,
        table_id: TableID,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        stmt.table_lookup_unique_mvcc(table_id, key.index_no, &key.vals, user_read_set)
            .await
    }

    pub(crate) async fn trx_insert_row_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        trx.exec(async |stmt| stmt_insert_row_by_id(stmt, table_id, cols).await)
            .await
    }

    pub(crate) async fn trx_delete_row_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        key: &SelectKey,
    ) -> Result<DeleteMvcc> {
        trx.exec(async |stmt| stmt_delete_row_by_id(stmt, table_id, key).await)
            .await
    }

    pub(crate) async fn trx_update_row_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        trx.exec(async |stmt| stmt_update_row_by_id(stmt, table_id, key, update).await)
            .await
    }

    pub(crate) async fn trx_select_row_mvcc_by_id(
        trx: &mut Transaction,
        table_id: TableID,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        trx.exec(async |stmt| stmt_select_row_mvcc_by_id(stmt, table_id, key, user_read_set).await)
            .await
    }

    pub(crate) struct FailingPageReadHook {
        file: StorageBackendFileIdentity,
        offset: usize,
        errno: i32,
        calls: AtomicUsize,
    }

    impl FailingPageReadHook {
        #[inline]
        pub(crate) fn for_page(
            file: StorageBackendFileIdentity,
            page_id: PageID,
            errno: i32,
        ) -> Self {
            Self {
                file,
                offset: usize::from(page_id) * PAGE_SIZE,
                errno,
                calls: AtomicUsize::new(0),
            }
        }

        #[inline]
        pub(crate) fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        #[inline]
        pub(crate) fn matches(&self, op: StorageBackendOp) -> bool {
            op.kind() == IOKind::Read
                && op.matches_file_identity(self.file)
                && op.offset() == self.offset
        }
    }

    impl StorageBackendTestHook for FailingPageReadHook {
        fn on_submit(&self, op: StorageBackendOp) {
            if self.matches(op) {
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
            if self.matches(op) {
                *res = Err(IoError::from_raw_os_error(self.errno));
            }
        }
    }

    pub(crate) struct FailingFirstWriteHook {
        file_path: PathBuf,
        calls: AtomicUsize,
    }

    impl FailingFirstWriteHook {
        #[inline]
        pub(crate) fn new(file_path: impl Into<PathBuf>) -> Self {
            Self {
                file_path: file_path.into(),
                calls: AtomicUsize::new(0),
            }
        }

        #[inline]
        pub(crate) fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        #[inline]
        pub(crate) fn matches(&self, op: StorageBackendOp) -> bool {
            if op.kind() != IOKind::Write {
                return false;
            }
            StorageBackendFileIdentity::from_path(&self.file_path)
                .is_ok_and(|expected| op.matches_file_identity(expected))
        }
    }

    impl StorageBackendTestHook for FailingFirstWriteHook {
        fn on_complete(&self, op: StorageBackendOp, res: &mut StdIoResult<usize>) {
            if self.matches(op) && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                *res = Err(IoError::from_raw_os_error(libc::EIO));
            }
        }
    }

    pub(crate) struct ColumnBlockIndexSnapshot {
        pub(crate) active_root: ActiveRoot,
        pub(crate) sparse_file: Arc<SparseFile>,
        pub(crate) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    }

    impl ColumnBlockIndexSnapshot {
        #[inline]
        pub(crate) fn index<'a>(&'a self, disk_pool_guard: &'a PoolGuard) -> ColumnBlockIndex<'a> {
            ColumnBlockIndex::new(
                self.active_root.column_block_index_root,
                self.active_root.pivot_row_id,
                FileKind::TableFile,
                &self.sparse_file,
                &self.disk_pool,
                disk_pool_guard,
            )
        }
    }

    #[inline]
    pub(crate) async fn evictable_test_engine(
        temp_dir: &TempDir,
        max_mem_size: u64,
        log_file_stem: &str,
    ) -> Engine {
        EngineConfig::default()
            .storage_root(temp_dir.path().to_path_buf())
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(max_mem_size)
                    .max_file_size(128u64 * 1024 * 1024),
            )
            .trx(TrxSysConfig::default().log_file_stem(log_file_stem))
            .file(
                FileSystemConfig::default()
                    .io_depth(16)
                    .readonly_buffer_size(128 * 1024 * 1024)
                    .data_dir("."),
            )
            .build()
            .await
            .unwrap()
    }

    #[inline]
    pub(crate) async fn lightweight_test_engine(temp_dir: &TempDir, log_file_stem: &str) -> Engine {
        lightweight_test_engine_config(temp_dir.path().to_path_buf(), log_file_stem)
            .build()
            .await
            .unwrap()
    }

    #[inline]
    pub(crate) async fn create_table2_for_test(engine: &Engine) -> TableID {
        table2(engine).await
    }

    #[inline]
    pub(crate) async fn create_non_unique_name_table_for_test(engine: &Engine) -> TableID {
        let mut ddl_session = engine.new_session().unwrap();
        let table_id = ddl_session
            .create_table(
                TableSpec::new(vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                ]),
                vec![
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ],
            )
            .await
            .unwrap();
        drop(ddl_session);
        table_id
    }

    #[inline]
    pub(crate) async fn create_nullable_name_table_for_test(engine: &Engine) -> TableID {
        let mut ddl_session = engine.new_session().unwrap();
        let table_id = ddl_session
            .create_table(
                TableSpec::new(vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::NULLABLE),
                ]),
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)],
            )
            .await
            .unwrap();
        drop(ddl_session);
        table_id
    }

    fn assert_invalid_dml_input(err: Error) {
        assert_eq!(
            err.report().downcast_ref::<OperationError>().copied(),
            Some(OperationError::InvalidDmlInput)
        );
        assert!(
            err.report().downcast_ref::<DmlValidationError>().is_some(),
            "invalid DML input must retain its validation source: {err:?}"
        );
    }

    #[test]
    #[should_panic(expected = "block-index column route requires disk pool guard")]
    fn test_block_index_column_route_panics_without_disk_pool_guard() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                lightweight_test_engine(&temp_dir, "block_index_disk_guard_invariant").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            table
                .mem
                .blk_idx
                .update_column_root(RowID::new(1), BlockID::new(77))
                .await;
            let _ = table
                .mem
                .blk_idx
                .find_row(
                    guards.meta_guard(),
                    None,
                    RowID::new(0),
                    Some(&table.storage),
                )
                .await;
        });
    }

    #[test]
    fn test_statement_insert_dml_validation_default_on() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "stmt_insert_dml_validation").await;
            let table_id = create_nullable_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::from(1i32)])
                        .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(
                        table_id,
                        vec![Val::from("wrong-id-type"), Val::from("name")],
                    )
                    .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::Null, Val::from("name")])
                        .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.table_insert_mvcc(table_id, vec![Val::from(1i32), Val::Null])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_statement_dml_validation_opt_out_is_statement_local() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "stmt_dml_validation_opt_out").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.disable_dml_validation()
                    .table_insert_mvcc(table_id, vec![Val::from(1i32), Val::from("name")])
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::from(2i32)])
                        .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_statement_unique_dml_validation_default_on() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "stmt_unique_dml_validation").await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("old")],
            )
            .await;

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        1,
                        vec![Val::from(2i32), Val::from("new")],
                    )
                    .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(0, vec![Val::from(1i32)]);
                    stmt.table_update_unique_mvcc(
                        table_id,
                        key.index_no,
                        &key.vals,
                        vec![
                            UpdateCol {
                                idx: 1,
                                val: Val::from("new"),
                            },
                            UpdateCol {
                                idx: 1,
                                val: Val::from("duplicate"),
                            },
                        ],
                    )
                    .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(1, vec![Val::from("old")]);
                    stmt.table_delete_unique_mvcc(table_id, key.index_no, &key.vals, false)
                        .await?;
                    Ok(())
                })
                .await
                .unwrap_err();
            assert_invalid_dml_input(err);
            trx.rollback().await.unwrap();
        });
    }

    pub(crate) fn lightweight_test_engine_config(
        main_dir: impl Into<PathBuf>,
        log_file_stem: &str,
    ) -> EngineConfig {
        EngineConfig::default()
            .storage_root(main_dir)
            .meta_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
            .index_buffer(LIGHTWEIGHT_TEST_BUFFER_BYTES)
            .index_max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES)
            .data_buffer(
                EvictableBufferPoolConfig::default()
                    .role(PoolRole::Mem)
                    .max_mem_size(LIGHTWEIGHT_TEST_BUFFER_BYTES)
                    .max_file_size(LIGHTWEIGHT_TEST_MAX_FILE_BYTES),
            )
            .trx(
                TrxSysConfig::default()
                    .log_write_io_depth(1)
                    .recovery_io_depth(1)
                    .catalog_checkpoint_scan_io_depth(1)
                    .log_file_stem(log_file_stem)
                    .purge_threads(1),
            )
            .file(
                FileSystemConfig::default()
                    .io_depth(1)
                    .readonly_buffer_size(LIGHTWEIGHT_TEST_READONLY_BUFFER_BYTES)
                    .data_dir("."),
            )
    }

    #[inline]
    pub(crate) fn table_for_internal_assertion(engine: &Engine, table_id: TableID) -> Arc<Table> {
        engine
            .catalog()
            .get_table_now(table_id)
            .expect("test table should exist")
    }

    #[inline]
    pub(crate) fn column_block_index_snapshot(
        engine: &Engine,
        table_id: TableID,
    ) -> ColumnBlockIndexSnapshot {
        let table = table_for_internal_assertion(engine, table_id);
        ColumnBlockIndexSnapshot {
            active_root: table.file().active_root_unchecked().clone(),
            sparse_file: Arc::clone(table.file().sparse_file()),
            disk_pool: table.disk_pool().clone(),
        }
    }

    #[inline]
    pub(crate) async fn expect_insert_committed(
        table_id: TableID,
        session: &mut Session,
        insert: Vec<Val>,
    ) {
        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_insert(table_id, trx, insert).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    pub(crate) async fn expect_trx_insert(
        table_id: TableID,
        mut trx: Transaction,
        insert: Vec<Val>,
    ) -> Transaction {
        let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
        if res.is_err() {
            panic!("res={:?}", res);
        }
        trx
    }

    #[inline]
    pub(crate) async fn expect_delete_committed(
        table_id: TableID,
        session: &mut Session,
        key: &SelectKey,
    ) {
        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_delete(table_id, trx, key).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    pub(crate) async fn expect_trx_delete(
        table_id: TableID,
        mut trx: Transaction,
        key: &SelectKey,
    ) -> Transaction {
        let res = trx_delete_row_by_id(&mut trx, table_id, key).await;
        if !matches!(res, Ok(DeleteMvcc::Deleted)) {
            panic!("res={:?}", res);
        }
        trx
    }

    #[inline]
    pub(crate) async fn expect_update_committed(
        table_id: TableID,
        session: &mut Session,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) {
        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_update(table_id, trx, key, update).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    pub(crate) async fn expect_trx_update(
        table_id: TableID,
        mut trx: Transaction,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Transaction {
        let res = trx_update_row_by_id(&mut trx, table_id, key, update).await;
        if !matches!(res, Ok(UpdateMvcc::Updated(_))) {
            panic!("res={:?}", res);
        }
        trx
    }

    #[inline]
    pub(crate) async fn expect_select_committed<F: FnOnce(Vec<Val>)>(
        table_id: TableID,
        session: &mut Session,
        key: &SelectKey,
        action: F,
    ) {
        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_select(table_id, trx, key, action).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    pub(crate) async fn expect_select_not_found_committed(
        table_id: TableID,
        session: &mut Session,
        key: &SelectKey,
    ) {
        let mut trx = session.begin_trx().unwrap();
        trx = expect_trx_select_not_found(table_id, trx, key).await;
        trx.commit().await.unwrap();
    }

    #[inline]
    pub(crate) async fn expect_trx_select_not_found(
        table_id: TableID,
        mut trx: Transaction,
        key: &SelectKey,
    ) -> Transaction {
        let res = trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1]).await;
        assert!(matches!(res, Ok(SelectMvcc::NotFound)));
        trx
    }

    #[inline]
    pub(crate) async fn expect_trx_select<F: FnOnce(Vec<Val>)>(
        table_id: TableID,
        mut trx: Transaction,
        key: &SelectKey,
        action: F,
    ) -> Transaction {
        let res = trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1]).await;
        if !matches!(res, Ok(SelectMvcc::Found(_))) {
            panic!("res={:?}", res);
        }
        action(res.unwrap().unwrap_found());
        trx
    }

    pub(crate) async fn insert_one_row(
        table_id: TableID,
        session: &mut Session,
        values: Vec<Val>,
    ) -> RowID {
        let mut trx = session.begin_trx().unwrap();
        let insert = trx_insert_row_by_id(&mut trx, table_id, values).await;
        let Ok(row_id) = insert else {
            panic!("insert should succeed: {insert:?}");
        };
        trx.commit().await.unwrap();
        row_id
    }

    pub(crate) fn single_key<V: Into<Val>>(value: V) -> SelectKey {
        SelectKey {
            index_no: 0,
            vals: vec![value.into()],
        }
    }

    pub(crate) async fn scan_table_i32s(trx: &mut Transaction, table_id: TableID) -> Vec<i32> {
        let mut rows = Vec::new();
        trx.exec(async |stmt| {
            stmt.table_scan_mvcc(table_id, &[0], |vals| {
                rows.push(vals[0].as_i32().unwrap());
                true
            })
            .await?;
            Ok(())
        })
        .await
        .unwrap();
        rows.sort_unstable();
        rows
    }

    pub(crate) async fn scan_table_pairs(
        trx: &mut Transaction,
        table_id: TableID,
    ) -> Vec<(i32, String)> {
        let mut rows = Vec::new();
        trx.exec(async |stmt| {
            stmt.table_scan_mvcc(table_id, &[0, 1], |vals| {
                rows.push((
                    vals[0].as_i32().unwrap(),
                    vals[1].as_str().unwrap().to_string(),
                ));
                true
            })
            .await?;
            Ok(())
        })
        .await
        .unwrap();
        rows.sort_unstable();
        rows
    }

    pub(crate) fn drop_table_test_spec() -> (TableSpec, Vec<IndexSpec>) {
        (
            TableSpec::new(vec![
                ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
            ]),
            vec![
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
            ],
        )
    }

    pub(crate) fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .filter(|entry| entry.owner == owner)
            .count()
    }

    pub(crate) fn has_lock_entry(
        engine: &Engine,
        owner: LockOwner,
        resource: LockResource,
        mode: LockMode,
        state: LockDebugEntryState,
    ) -> bool {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .any(|entry| {
                entry.owner == owner
                    && entry.resource == resource
                    && entry.mode == mode
                    && entry.state == state
            })
    }

    pub(crate) fn has_lock_resource(
        engine: &Engine,
        owner: LockOwner,
        resource: LockResource,
    ) -> bool {
        debug_snapshot(engine.lock_manager())
            .entries
            .iter()
            .any(|entry| entry.owner == owner && entry.resource == resource)
    }

    pub(crate) async fn wait_for_no_lock_resource(
        engine: &Engine,
        owner: LockOwner,
        resource: LockResource,
    ) {
        // Timer audit: lock-manager diagnostic state inspection.
        for _ in 0..100 {
            if !has_lock_resource(engine, owner, resource) {
                return;
            }
            Timer::after(Duration::from_millis(1)).await;
        }
        panic!("lock resource still present: owner={owner:?}, resource={resource:?}");
    }

    pub(crate) async fn wait_for_lock_entry(
        engine: &Engine,
        owner: LockOwner,
        resource: LockResource,
        mode: LockMode,
        state: LockDebugEntryState,
    ) {
        // Timer audit: lock-manager diagnostic state inspection.
        for _ in 0..100 {
            if has_lock_entry(engine, owner, resource, mode, state) {
                return;
            }
            Timer::after(Duration::from_millis(1)).await;
        }
        panic!(
            "lock entry not observed: owner={owner:?}, resource={resource:?}, mode={mode:?}, state={state:?}"
        );
    }

    pub(crate) fn name_key(value: &str) -> SelectKey {
        SelectKey {
            index_no: 1,
            vals: vec![Val::from(value)],
        }
    }

    pub(crate) fn unwrap_insert_result(res: Result<RowID>) -> RowID {
        match res {
            Ok(row_id) => row_id,
            res => panic!("unexpected insert result: {res:?}"),
        }
    }

    pub(crate) fn active_secondary_root(table: &Table, index_no: usize) -> BlockID {
        table.file().active_root_unchecked().secondary_index_roots[index_no]
    }

    pub(crate) struct BoundUniqueIndexCandidateStream<'a> {
        layout: Arc<TableRuntimeLayout>,
        guards: &'a PoolGuards,
        index_no: usize,
        root: BlockID,
        range: &'a KeyRange,
        ts: TrxID,
        done: bool,
    }

    impl IndexBatchStream<IndexLookupCandidate> for BoundUniqueIndexCandidateStream<'_> {
        #[inline]
        async fn next_batch(&mut self) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
            if self.done {
                return Ok(None);
            }
            self.done = true;
            let index = self.layout.secondary_index(self.index_no)?;
            let bound = index.bind_unique(self.guards, self.root)?;
            let mut stream = bound.index_scan_candidates(self.range, self.ts)?;
            let mut candidates = Vec::new();
            while let Some(batch) = stream.next_batch().await? {
                candidates.extend(batch);
            }
            Ok((!candidates.is_empty()).then_some(candidates))
        }
    }

    pub(crate) struct BoundNonUniqueIndexCandidateStream<'a> {
        layout: Arc<TableRuntimeLayout>,
        guards: &'a PoolGuards,
        index_no: usize,
        root: BlockID,
        range: &'a KeyRange,
        ts: TrxID,
        done: bool,
    }

    impl IndexBatchStream<IndexLookupCandidate> for BoundNonUniqueIndexCandidateStream<'_> {
        #[inline]
        async fn next_batch(&mut self) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
            if self.done {
                return Ok(None);
            }
            self.done = true;
            let index = self.layout.secondary_index(self.index_no)?;
            let bound = index.bind_non_unique(self.guards, self.root)?;
            let mut stream = bound.index_scan_candidates(self.range, self.ts)?;
            let mut candidates = Vec::new();
            while let Some(batch) = stream.next_batch().await? {
                candidates.extend(batch);
            }
            Ok((!candidates.is_empty()).then_some(candidates))
        }
    }

    pub(crate) struct BoundUniqueIndex<'a> {
        layout: Arc<TableRuntimeLayout>,
        guards: &'a PoolGuards,
        index_no: usize,
        root: BlockID,
    }

    impl UniqueIndex for BoundUniqueIndex<'_> {
        type LookupCandidateStream<'a>
            = BoundUniqueIndexCandidateStream<'a>
        where
            Self: 'a;

        #[inline]
        async fn lookup(&self, key: &[Val], ts: TrxID) -> RuntimeResult<Option<(RowID, bool)>> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.guards, self.root)?
                .lookup(key, ts)
                .await
        }

        #[inline]
        async fn insert_if_not_exists(
            &self,
            key: &[Val],
            row_id: RowID,
            merge_if_match_deleted: bool,
            ts: TrxID,
        ) -> RuntimeResult<IndexInsert> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.guards, self.root)?
                .insert_if_not_exists(key, row_id, merge_if_match_deleted, ts)
                .await
        }

        #[inline]
        async fn compare_delete(
            &self,
            key: &[Val],
            old_row_id: RowID,
            ignore_del_mask: bool,
            ts: TrxID,
        ) -> RuntimeResult<bool> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.guards, self.root)?
                .compare_delete(key, old_row_id, ignore_del_mask, ts)
                .await
        }

        #[inline]
        async fn compare_exchange(
            &self,
            key: &[Val],
            old_row_id: RowID,
            new_row_id: RowID,
            ts: TrxID,
        ) -> RuntimeResult<IndexCompareExchange> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.guards, self.root)?
                .compare_exchange(key, old_row_id, new_row_id, ts)
                .await
        }

        #[inline]
        fn index_scan_candidates<'a>(
            &'a self,
            range: &'a KeyRange,
            ts: TrxID,
        ) -> RuntimeResult<Self::LookupCandidateStream<'a>> {
            Ok(BoundUniqueIndexCandidateStream {
                layout: Arc::clone(&self.layout),
                guards: self.guards,
                index_no: self.index_no,
                root: self.root,
                range,
                ts,
                done: false,
            })
        }

        #[inline]
        async fn scan_values(&self, values: &mut Vec<RowID>, ts: TrxID) -> RuntimeResult<()> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_unique(self.guards, self.root)?
                .scan_values(values, ts)
                .await
        }
    }

    pub(crate) struct BoundNonUniqueIndexNo<'a> {
        layout: Arc<TableRuntimeLayout>,
        guards: &'a PoolGuards,
        index_no: usize,
        root: BlockID,
    }

    impl NonUniqueIndex for BoundNonUniqueIndexNo<'_> {
        type LookupCandidateStream<'a>
            = BoundNonUniqueIndexCandidateStream<'a>
        where
            Self: 'a;

        #[inline]
        async fn lookup(&self, key: &[Val], res: &mut Vec<RowID>, ts: TrxID) -> RuntimeResult<()> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .lookup(key, res, ts)
                .await
        }

        #[inline]
        async fn lookup_unique(
            &self,
            key: &[Val],
            row_id: RowID,
            ts: TrxID,
        ) -> RuntimeResult<Option<bool>> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .lookup_unique(key, row_id, ts)
                .await
        }

        #[inline]
        async fn insert_if_not_exists(
            &self,
            key: &[Val],
            row_id: RowID,
            merge_if_match_deleted: bool,
            ts: TrxID,
        ) -> RuntimeResult<IndexInsert> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .insert_if_not_exists(key, row_id, merge_if_match_deleted, ts)
                .await
        }

        #[inline]
        async fn mask_as_deleted(
            &self,
            key: &[Val],
            row_id: RowID,
            ts: TrxID,
        ) -> RuntimeResult<bool> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .mask_as_deleted(key, row_id, ts)
                .await
        }

        #[inline]
        async fn mask_as_active(
            &self,
            key: &[Val],
            row_id: RowID,
            ts: TrxID,
        ) -> RuntimeResult<bool> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .mask_as_active(key, row_id, ts)
                .await
        }

        #[inline]
        async fn compare_delete(
            &self,
            key: &[Val],
            row_id: RowID,
            ignore_del_mask: bool,
            ts: TrxID,
        ) -> RuntimeResult<bool> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .compare_delete(key, row_id, ignore_del_mask, ts)
                .await
        }

        #[inline]
        fn index_scan_candidates<'a>(
            &'a self,
            range: &'a KeyRange,
            ts: TrxID,
        ) -> RuntimeResult<Self::LookupCandidateStream<'a>> {
            Ok(BoundNonUniqueIndexCandidateStream {
                layout: Arc::clone(&self.layout),
                guards: self.guards,
                index_no: self.index_no,
                root: self.root,
                range,
                ts,
                done: false,
            })
        }

        #[inline]
        fn equal_scan_candidates<'a>(
            &'a self,
            range: &'a KeyRange,
            ts: TrxID,
        ) -> RuntimeResult<Self::LookupCandidateStream<'a>> {
            Ok(BoundNonUniqueIndexCandidateStream {
                layout: Arc::clone(&self.layout),
                guards: self.guards,
                index_no: self.index_no,
                root: self.root,
                range,
                ts,
                done: false,
            })
        }

        #[inline]
        async fn scan_values(&self, values: &mut Vec<RowID>, ts: TrxID) -> RuntimeResult<()> {
            let index = self.layout.secondary_index(self.index_no)?;
            index
                .bind_non_unique(self.guards, self.root)?
                .scan_values(values, ts)
                .await
        }
    }

    pub(crate) fn bound_unique_index<'a>(
        table: &Table,
        guards: &'a PoolGuards,
        index_no: usize,
    ) -> BoundUniqueIndex<'a> {
        let layout = table.layout_snapshot();
        BoundUniqueIndex {
            layout,
            guards,
            index_no,
            root: active_secondary_root(table, index_no),
        }
    }

    pub(crate) fn bound_non_unique_index_no<'a>(
        table: &Table,
        guards: &'a PoolGuards,
        index_no: usize,
    ) -> BoundNonUniqueIndexNo<'a> {
        let layout = table.layout_snapshot();
        BoundNonUniqueIndexNo {
            layout,
            guards,
            index_no,
            root: active_secondary_root(table, index_no),
        }
    }

    pub(crate) async fn assert_row_in_lwc(
        table: &Table,
        guards: &PoolGuards,
        key: &SelectKey,
        sts: TrxID,
    ) -> RowID {
        let index = bound_unique_index(table, guards, key.index_no);
        let Some((row_id, _)) = index
            .lookup(&key.vals, sts)
            .await
            .expect("index lookup should succeed")
        else {
            panic!("row should exist");
        };
        match table.find_row(guards, row_id).await.unwrap() {
            RowLocation::LwcBlock { .. } => row_id,
            RowLocation::RowPage(..) => panic!("row should be in lwc"),
            RowLocation::NotFound => panic!("row should exist"),
        }
    }

    pub(crate) async fn unique_disk_tree_lookup(
        table: &Table,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Option<RowID> {
        let root = active_secondary_root(table, key.index_no);
        let layout = table.layout_snapshot();
        let tree = layout
            .secondary_index(key.index_no)
            .unwrap()
            .disk_runtime()
            .open_unique_at(root, guards.disk_guard())
            .unwrap();
        tree.lookup(&key.vals).await.unwrap()
    }

    pub(crate) async fn non_unique_disk_tree_prefix_scan(
        table: &Table,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Vec<RowID> {
        let root = active_secondary_root(table, key.index_no);
        let layout = table.layout_snapshot();
        let tree = layout
            .secondary_index(key.index_no)
            .unwrap()
            .disk_runtime()
            .open_non_unique_at(root, guards.disk_guard())
            .unwrap();
        tree.prefix_scan_entries(&key.vals)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, row_id)| row_id)
            .collect()
    }

    pub(crate) async fn assert_unique_index_entry(
        table: &Table,
        guards: &PoolGuards,
        key: &SelectKey,
        sts: TrxID,
        expected_row_id: RowID,
        expected_deleted: bool,
    ) {
        let index = bound_unique_index(table, guards, key.index_no);
        let Some((row_id, deleted)) = index
            .lookup(&key.vals, sts)
            .await
            .expect("index lookup should succeed")
        else {
            panic!("index entry should exist");
        };
        assert_eq!(row_id, expected_row_id);
        assert_eq!(deleted, expected_deleted);
    }

    pub(crate) fn delete_marker_ts(marker: DeleteMarker) -> TrxID {
        match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => status.ts(),
        }
    }

    pub(crate) async fn wait_path_exists(path: &str, expected: bool) {
        // Timer audit: filesystem unlink/create side-effect observation.
        for _ in 0..250 {
            if Path::new(path).exists() == expected {
                return;
            }
            Timer::after(Duration::from_millis(50)).await;
        }
        panic!("path existence did not become {expected}: {path}");
    }

    pub(crate) fn assert_root_metadata_unchanged(before: &ActiveRoot, table: &Table) {
        let after = table.file().active_root_unchecked();
        assert_eq!(after.root_ts, before.root_ts);
        assert_eq!(after.meta_block_id, before.meta_block_id);
        assert_eq!(after.pivot_row_id, before.pivot_row_id);
        assert_eq!(after.heap_redo_start_ts, before.heap_redo_start_ts);
        assert_eq!(after.deletion_cutoff_ts, before.deletion_cutoff_ts);
        assert_eq!(
            after.column_block_index_root,
            before.column_block_index_root
        );
        assert_eq!(after.secondary_index_roots, before.secondary_index_roots);
    }

    pub(crate) fn corrupt_page_checksum(path: impl AsRef<Path>, page_id: impl Into<u64>) {
        let page_id = page_id.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64 + (COW_FILE_PAGE_SIZE as u64 - 1);
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xFF;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&byte).unwrap();
        file.flush().unwrap();
    }

    pub(crate) fn rewrite_page_with_checksum(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        rewrite: impl FnOnce(&mut [u8]),
    ) {
        let page_id = page_id.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = page_id * COW_FILE_PAGE_SIZE as u64;
        let mut page = vec![0u8; COW_FILE_PAGE_SIZE];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut page).unwrap();
        rewrite(&mut page);
        write_block_checksum(&mut page);
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&page).unwrap();
        file.flush().unwrap();
    }

    pub(crate) fn corrupt_leaf_delete_codec(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx) + 35;
            page[byte_offset] = 0xFF;
        });
    }

    pub(crate) fn corrupt_leaf_row_codec(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx) + 32;
            page[byte_offset] = 0;
        });
    }

    pub(crate) fn corrupt_leaf_block_id(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx);
            page[byte_offset..byte_offset + 8].copy_from_slice(&SUPER_BLOCK_ID.to_le_bytes());
        });
    }

    pub(crate) fn corrupt_leaf_short_delete_section_header(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
        prefix_idx: usize,
    ) {
        const LEAF_ENTRY_ENTRY_LEN_OFFSET: usize = 28;
        const LEAF_ENTRY_ROW_SECTION_LEN_OFFSET: usize = 30;
        const LEAF_ENTRY_HEADER_SIZE: usize = 32;
        const TRUNCATED_DELETE_SECTION_LEN: usize = 4;

        rewrite_page_with_checksum(path, page_id, |page| {
            let byte_offset = leaf_entry_payload_offset(page, prefix_idx);
            let row_section_len = u16::from_le_bytes(
                page[byte_offset + LEAF_ENTRY_ROW_SECTION_LEN_OFFSET
                    ..byte_offset + LEAF_ENTRY_ROW_SECTION_LEN_OFFSET + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let truncated_entry_len =
                (LEAF_ENTRY_HEADER_SIZE + row_section_len + TRUNCATED_DELETE_SECTION_LEN) as u16;
            page[byte_offset + LEAF_ENTRY_ENTRY_LEN_OFFSET
                ..byte_offset + LEAF_ENTRY_ENTRY_LEN_OFFSET + 2]
                .copy_from_slice(&truncated_entry_len.to_le_bytes());
        });
    }

    pub(crate) fn leaf_entry_payload_offset(page: &[u8], prefix_idx: usize) -> usize {
        const SEARCH_TYPE_PLAIN: u8 = 1;
        const SEARCH_TYPE_DELTA_U32: u8 = 2;
        const SEARCH_TYPE_DELTA_U16: u8 = 3;

        let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
        let search_type = page[payload_start + COLUMN_BLOCK_HEADER_SIZE];
        let (prefix_size, entry_offset_offset) = match search_type {
            SEARCH_TYPE_PLAIN => (10usize, 8usize),
            SEARCH_TYPE_DELTA_U32 => (6usize, 4usize),
            SEARCH_TYPE_DELTA_U16 => (4usize, 2usize),
            _ => panic!("invalid leaf search type {search_type}"),
        };
        let prefix_offset =
            payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + prefix_idx * prefix_size;
        let entry_offset = u16::from_le_bytes(
            page[prefix_offset + entry_offset_offset..prefix_offset + entry_offset_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        payload_start + COLUMN_BLOCK_LEAF_HEADER_SIZE + entry_offset
    }

    pub(crate) fn corrupt_lwc_row_shape_fingerprint(
        path: impl AsRef<Path>,
        page_id: impl Into<u64>,
    ) {
        rewrite_page_with_checksum(path, page_id, |page| {
            let payload_start = BLOCK_INTEGRITY_HEADER_SIZE;
            page[payload_start] ^= 0xFF;
        });
    }

    pub(crate) async fn insert_rows(
        table_id: TableID,
        session: &mut Session,
        start: i32,
        count: i32,
        name: &str,
    ) {
        let mut trx = session.begin_trx().unwrap();
        for i in 0..count {
            let insert = vec![Val::from(start + i), Val::from(name)];
            trx = expect_trx_insert(table_id, trx, insert).await;
        }
        trx.commit().await.unwrap();
    }

    pub(crate) async fn insert_rows_direct(
        table_id: TableID,
        session: &mut Session,
        start: i32,
        count: i32,
        name: &str,
    ) {
        let mut trx = session.begin_trx().unwrap();
        for i in 0..count {
            let insert = vec![Val::from(start + i), Val::from(name)];
            let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
            assert!(res.is_ok());
        }
        trx.commit().await.unwrap();
    }

    pub(crate) async fn delete_key_range_and_wait_gc_cutoff(
        table_id: TableID,
        session: &mut Session,
        start: i32,
        count: i32,
    ) {
        let mut max_delete_cts = TrxID::new(0);
        for i in 0..count {
            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_delete(table_id, trx, &single_key(start + i)).await;
            let cts = trx.commit().await.unwrap();
            max_delete_cts = max_delete_cts.max(cts);
        }
        session
            .wait_for_gc_horizon_after(max_delete_cts)
            .await
            .unwrap();
    }
}
