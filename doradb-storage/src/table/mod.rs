mod access;
mod deletion_buffer;
mod gc;
mod layout;
mod lifecycle;
mod mem_table;
mod persistence;
mod recover;
mod rollback;
mod storage;
#[cfg(test)]
mod tests;

pub(crate) use access::*;
pub(crate) use deletion_buffer::*;
pub use gc::{SecondaryMemIndexCleanupIndexStats, SecondaryMemIndexCleanupStats};
pub(crate) use layout::{RetiredSecondaryIndex, TableRuntimeLayout};
pub use lifecycle::CheckpointCancelReason;
#[cfg(test)]
pub(crate) use lifecycle::TableLifecycleState;
pub(crate) use lifecycle::{
    CheckpointPublishLease, TableCheckpointRootMutationLease, TableLifecycle,
    TableMetadataChangeLease,
};
pub(crate) use mem_table::MemTable;
pub use persistence::*;
pub(crate) use rollback::IndexRollback;
pub(crate) use storage::ColumnStorage;
#[cfg(test)]
pub(crate) use tests::test_user_table_id;

use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::{EvictableBufferPool, PoolGuard, PoolGuards, PoolRole, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{DataIntegrityError, Error, InternalError, Result};
use crate::file::table_file::{ActiveRoot, LwcBlockPersist, TableFile};
use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
use crate::index::{
    BlockIndex, ColumnBlockEntryShape, NonUniqueMemIndex, RowLocation, SecondaryDiskTreeRuntime,
    SecondaryIndex, UniqueMemIndex,
};
use crate::lwc::LwcBuilder;
use crate::quiescent::QuiescentGuard;
use crate::row::ops::{SelectKey, UpdateCol};
use crate::row::{RowPage, RowRead, var_len_for_insert};
use crate::trx::row::RowReadAccess;
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::{IndexBranch, RowUndoKind, UndoStatus};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MAX_SNAPSHOT_TS, TrxContext, TrxReadProof, trx_is_committed};
use crate::value::{PAGE_VAR_LEN_INLINE, Val};
use error_stack::Report;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

/// Runtime handle for a user table, combining in-memory and persisted storage.
pub(crate) struct Table {
    pub(crate) mem: MemTable<EvictableBufferPool, EvictableBufferPool>,
    pub(crate) storage: ColumnStorage,
    layout: Mutex<Arc<TableRuntimeLayout>>,
    retired_secondary_indexes: Mutex<Vec<RetiredSecondaryIndex>>,
    pub(crate) lifecycle: TableLifecycle,
}

impl Table {
    /// Create a new table.
    #[inline]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        index_pool_guard: &crate::buffer::PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        file: Arc<TableFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
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
        .await?;
        let mem = MemTable {
            table_id,
            metadata: Arc::clone(&metadata),
            mem_pool: mem_pool.clone(),
            row_pool_role: mem_pool.row_pool_role(),
            index_pool_role: PoolRole::Index,
            blk_idx,
            sec_idx: Box::new([]),
        };
        let storage = ColumnStorage::new(file, disk_pool)?;
        debug_assert_eq!(
            storage.secondary_index_runtimes().len(),
            secondary_index_count
        );
        debug_assert_eq!(sec_idx.len(), secondary_index_count);
        let layout = TableRuntimeLayout::new(0, Arc::clone(&metadata), sec_idx)?;
        Ok(Table {
            mem,
            storage,
            layout: Mutex::new(Arc::new(layout)),
            retired_secondary_indexes: Mutex::new(Vec::new()),
            lifecycle: TableLifecycle::new(),
        })
    }

    /// Returns the logical table id of this user-table runtime.
    #[inline]
    pub fn table_id(&self) -> TableID {
        self.mem.table_id()
    }

    /// Ensures a foreground operation may access this table after logical locks.
    #[inline]
    pub(crate) fn check_foreground_live(&self, operation: &'static str) -> Result<()> {
        self.lifecycle
            .check_foreground_live(self.table_id(), operation)
    }

    /// Attempts to enter the checkpoint no-cancel publish section.
    #[inline]
    pub(crate) fn try_begin_checkpoint_publish(
        &self,
    ) -> std::result::Result<CheckpointPublishLease<'_>, CheckpointCancelReason> {
        self.lifecycle.try_begin_checkpoint_publish()
    }

    /// Acquires the reversible metadata-change gate for future index DDL.
    #[inline]
    pub(crate) async fn begin_metadata_change(&self) -> Result<TableMetadataChangeLease<'_>> {
        self.lifecycle.begin_metadata_change(self.table_id()).await
    }

    /// Attempts to enter the checkpoint table-root mutation section.
    #[inline]
    pub(crate) fn try_begin_checkpoint_root_mutation(
        &self,
    ) -> std::result::Result<TableCheckpointRootMutationLease<'_>, CheckpointCancelReason> {
        self.lifecycle.try_begin_checkpoint_root_mutation()
    }

    /// Starts the irreversible drop lifecycle gate for this table.
    #[inline]
    pub(crate) async fn begin_drop_lifecycle(&self) -> Result<()> {
        self.lifecycle.begin_drop(self.table_id()).await
    }

    /// Marks this table lifecycle as fully dropped.
    #[inline]
    pub(crate) fn mark_dropped_lifecycle(&self) -> Result<()> {
        self.lifecycle.mark_dropped(self.table_id())
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
    pub(crate) async fn destroy_dropped_runtime(self, guards: &PoolGuards) -> Result<()> {
        let Table {
            mem,
            storage: _storage,
            layout,
            retired_secondary_indexes,
            lifecycle: _lifecycle,
        } = self;
        let index_pool_guard = guards.index_guard();
        for retired in retired_secondary_indexes.into_inner() {
            let _retired_identity = (retired.index_no, retired.retired_generation);
            let index = Arc::try_unwrap(retired.index).map_err(|index| {
                Report::new(InternalError::Generic)
                    .attach(format!(
                        "retired secondary index still referenced during runtime destroy: index_no={}, strong_count={}",
                        index.index_no(),
                        Arc::strong_count(&index)
                    ))
            })?;
            index.destroy(index_pool_guard).await?;
        }
        let layout = Arc::try_unwrap(layout.into_inner()).map_err(|layout| {
            Report::new(InternalError::Generic)
                .attach(format!(
                    "table runtime layout still referenced during runtime destroy: generation={}, strong_count={}",
                    layout.generation(),
                    Arc::strong_count(&layout)
                ))
        })?;
        let indexes = layout.into_secondary_indexes();
        for index in indexes.iter().flatten() {
            if Arc::strong_count(index) != 1 {
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "secondary index still referenced during runtime destroy: index_no={}, strong_count={}",
                        index.index_no(),
                        Arc::strong_count(index)
                    ))
                    .into());
            }
        }
        for index in indexes.into_iter().flatten() {
            let index = Arc::try_unwrap(index).map_err(|index| {
                Report::new(InternalError::Generic)
                    .attach(format!(
                        "secondary index still referenced during runtime destroy: index_no={}, strong_count={}",
                        index.index_no(),
                        Arc::strong_count(&index)
                    ))
            })?;
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
    ) -> Result<TableRootSnapshot<'ctx>> {
        Ok(self.with_active_root(proof, |root| {
            TableRootSnapshot::from_active_root(root, proof)
        }))
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
    ) -> Result<Arc<TableRuntimeLayout>> {
        new_layout.validate()?;
        if new_layout.generation() <= expected_generation {
            return Err(Report::new(InternalError::Generic)
                .attach(format!(
                    "new layout generation must advance: expected_generation={}, new_generation={}",
                    expected_generation,
                    new_layout.generation()
                ))
                .into());
        }

        let new_layout = Arc::new(new_layout);
        let old_layout = {
            let mut guard = self.layout.lock();
            if guard.generation() != expected_generation {
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "layout generation mismatch: expected={}, actual={}",
                        expected_generation,
                        guard.generation()
                    ))
                    .into());
            }
            if new_layout.index_slot_count() < guard.index_slot_count() {
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "new layout must not shrink sparse index slots: old_slots={}, new_slots={}",
                        guard.index_slot_count(),
                        new_layout.index_slot_count()
                    ))
                    .into());
            }
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
        Ok(new_layout)
    }

    /// Destroys retired secondary-index runtimes whose old layout snapshots drained.
    pub(crate) async fn cleanup_retired_secondary_indexes(
        &self,
        guards: &PoolGuards,
    ) -> Result<usize> {
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

    #[inline]
    pub(crate) async fn find_row(&self, guards: &PoolGuards, row_id: RowID) -> Result<RowLocation> {
        self.mem
            .blk_idx
            .find_row(
                guards.meta_guard(),
                Some(guards.disk_guard()),
                row_id,
                Some(&self.storage),
            )
            .await
    }

    #[inline]
    fn missing_pool_guard(&self, operation: &'static str, role: &'static str) -> Error {
        Report::new(InternalError::Generic)
            .attach(format!(
                "operation={operation}, table_id={}, missing {role} pool guard",
                self.table_id()
            ))
            .into()
    }

    #[inline]
    fn stale_block_index_leaf(&self, operation: &'static str) -> Error {
        Report::new(InternalError::Generic)
            .attach(format!(
                "operation={operation}, table_id={}, stale block-index leaf lock",
                self.table_id()
            ))
            .into()
    }

    #[inline]
    fn meta_pool_guard<'a>(
        &self,
        guards: &'a PoolGuards,
        operation: &'static str,
    ) -> Result<&'a PoolGuard> {
        guards
            .try_guard(PoolRole::Meta)
            .ok_or_else(|| self.missing_pool_guard(operation, "meta"))
    }

    async fn collect_frozen_pages(
        &self,
        guards: &PoolGuards,
    ) -> Result<(Vec<FrozenPage>, Option<TrxID>)> {
        let mut frozen_pages = Vec::new();
        let pivot_row_id = self.mem.pivot_row_id();
        let mut expected_row_id = pivot_row_id;
        let mut heap_redo_start_ts = None;
        let mut seen_first_page = false;
        self.mem_scan(guards, |page_guard| {
            let page = page_guard.page();
            if !seen_first_page {
                seen_first_page = true;
                debug_assert_eq!(
                    page.header.start_row_id, pivot_row_id,
                    "first in-memory row page must start from pivot_row_id"
                );
            }
            if page.header.start_row_id != expected_row_id {
                return false;
            }
            let (ctx, _) = page_guard.ctx_and_page();
            let row_ver = ctx.row_ver().unwrap();
            if row_ver.state() != RowPageState::Frozen {
                if row_ver.state() == RowPageState::Active {
                    // heap redo start ts is creation cts of first remaining active page.
                    heap_redo_start_ts = Some(row_ver.create_cts());
                }
                return false;
            }
            let end_row_id = page.header.start_row_id + page.header.max_row_count as u64;
            frozen_pages.push(FrozenPage {
                page_id: page_guard.page_id(),
                start_row_id: page.header.start_row_id,
                end_row_id,
            });
            expected_row_id = end_row_id;
            true
        })
        .await?;
        Ok((frozen_pages, heap_redo_start_ts))
    }

    async fn wait_for_frozen_pages_stable(
        &self,
        guards: &PoolGuards,
        trx_sys: &TransactionSystem,
        frozen_pages: &[FrozenPage],
    ) -> Result<()> {
        loop {
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let mut stabilized = true;
            for page_info in frozen_pages {
                // A potential optimization is to check row version map without loading
                // row page back. This requires interface change of buffer pool.
                let page_guard = self
                    .mem
                    .must_get_row_page_shared(guards, page_info.page_id)
                    .await?;
                let (ctx, _) = page_guard.ctx_and_page();
                let row_ver = ctx.row_ver().unwrap();
                // Check whether all insert and updates on this page are committed.
                // This may be blocked by a long-running irrelevant transaction
                // but we accept it now.
                if row_ver.max_ins_sts() >= min_active_sts {
                    stabilized = false;
                    break;
                }
            }
            if stabilized {
                break;
            }
            smol::Timer::after(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    async fn set_frozen_pages_to_transition(
        &self,
        guards: &PoolGuards,
        frozen_pages: &[FrozenPage],
        cutoff_ts: TrxID,
    ) -> Result<()> {
        for page_info in frozen_pages {
            let page_guard = self
                .mem
                .must_get_row_page_shared(guards, page_info.page_id)
                .await?;
            let (ctx, page) = page_guard.ctx_and_page();
            ctx.row_ver().unwrap().set_transition();
            self.capture_delete_markers_for_transition(page, ctx, cutoff_ts);
        }
        Ok(())
    }

    async fn build_lwc_blocks(
        &self,
        metadata: &TableMetadata,
        guards: &PoolGuards,
        cutoff_ts: TrxID,
        frozen_pages: &[FrozenPage],
        mut collect_visible_row: Option<VisibleRowCollector<'_>>,
    ) -> Result<Vec<LwcBlockPersist>> {
        #[cfg(test)]
        {
            if tests::test_force_lwc_build_error_enabled() {
                return Err(Report::new(InternalError::InjectedTestFailure).into());
            }
        }
        let mut lwc_blocks = Vec::new();
        if !frozen_pages.is_empty() {
            let mut builder = LwcBuilder::new(metadata.col.as_ref());
            let mut current_start = RowID::new(0);
            let mut current_end = RowID::new(0);
            for page_info in frozen_pages {
                let page_guard = self
                    .mem
                    .must_get_row_page_shared(guards, page_info.page_id)
                    .await?;
                let (ctx, page) = page_guard.ctx_and_page();
                let view = page.vector_view_in_transition(
                    metadata.col.as_ref(),
                    ctx,
                    cutoff_ts,
                    cutoff_ts,
                );
                if view.rows_non_deleted() == 0 {
                    continue;
                }
                if let Some(collect_visible_row) = collect_visible_row.as_mut() {
                    for (start_idx, end_idx) in view.range_non_deleted() {
                        for row_idx in start_idx..end_idx {
                            collect_visible_row(page, row_idx, page.row_id(row_idx))?;
                        }
                    }
                }
                if builder.is_empty() {
                    current_start = page_info.start_row_id;
                    current_end = page_info.end_row_id;
                }
                if !builder.append_view(page, view)? {
                    let shape = ColumnBlockEntryShape::new(
                        current_start,
                        current_end,
                        builder.row_ids().to_vec(),
                        Vec::new(),
                    )?;
                    let buf = builder.build(shape.row_shape_fingerprint())?;
                    lwc_blocks.push(LwcBlockPersist { shape, buf });
                    builder = LwcBuilder::new(metadata.col.as_ref());
                    current_start = page_info.start_row_id;
                    current_end = page_info.end_row_id;
                    let view = page.vector_view_in_transition(
                        metadata.col.as_ref(),
                        ctx,
                        cutoff_ts,
                        cutoff_ts,
                    );
                    if !builder.append_view(page, view)? {
                        return Err(Report::new(InternalError::LwcBuilderMisuse)
                            .attach(format!(
                                "single row page does not fit in LWC block: page_id={}",
                                page_info.page_id
                            ))
                            .into());
                    }
                } else {
                    current_end = page_info.end_row_id;
                }
            }
            if !builder.is_empty() {
                let shape = ColumnBlockEntryShape::new(
                    current_start,
                    current_end,
                    builder.row_ids().to_vec(),
                    Vec::new(),
                )?;
                let buf = builder.build(shape.row_shape_fingerprint())?;
                lwc_blocks.push(LwcBlockPersist { shape, buf });
            }
        }
        Ok(lwc_blocks)
    }

    fn capture_delete_markers_for_transition(
        &self,
        page: &RowPage,
        ctx: &crate::buffer::frame::FrameContext,
        cutoff_ts: TrxID,
    ) {
        let Some(map) = ctx.row_ver() else {
            return;
        };
        let row_count = page.header.row_count();
        for row_idx in 0..row_count {
            let undo_guard = map.read_latch(row_idx);
            let Some(head) = undo_guard.as_ref() else {
                continue;
            };
            let mut ts = head.next.main.status.ts();
            let mut status = match &head.next.main.status {
                UndoStatus::Ref(status) => Some(status.clone()),
                UndoStatus::Committed(_) => None,
            };
            let mut entry = head.next.main.entry.clone();
            loop {
                let row_id = page.row_id(row_idx);
                match entry.as_ref().kind {
                    RowUndoKind::Lock => {
                        if let Some(trx_status) = status.as_ref()
                            && !trx_is_committed(trx_status.ts())
                        {
                            let _ = self.deletion_buffer().put_ref(
                                row_id,
                                trx_status.clone(),
                                MAX_SNAPSHOT_TS,
                            );
                        }
                    }
                    RowUndoKind::Delete => {
                        match status.as_ref() {
                            Some(trx_status) => {
                                let status_ts = trx_status.ts();
                                if trx_is_committed(status_ts) {
                                    if status_ts >= cutoff_ts {
                                        let _ =
                                            self.deletion_buffer().put_committed(row_id, status_ts);
                                    }
                                } else {
                                    let _ = self.deletion_buffer().put_ref(
                                        row_id,
                                        trx_status.clone(),
                                        MAX_SNAPSHOT_TS,
                                    );
                                }
                            }
                            None => {
                                if ts >= cutoff_ts {
                                    let _ = self.deletion_buffer().put_committed(row_id, ts);
                                }
                            }
                        };
                        break;
                    }
                    RowUndoKind::Insert | RowUndoKind::Update(_) => {
                        break;
                    }
                }
                let next = entry.as_ref().next.as_ref().map(|next| {
                    let ts = next.main.status.ts();
                    let status = match &next.main.status {
                        UndoStatus::Ref(status) => Some(status.clone()),
                        UndoStatus::Committed(_) => None,
                    };
                    (ts, status, next.main.entry.clone())
                });
                let Some((next_ts, next_status, next_entry)) = next else {
                    break;
                };
                ts = next_ts;
                status = next_status;
                entry = next_entry;
            }
        }
    }

    /// Returns total number of row pages.
    #[inline]
    pub(crate) async fn total_row_pages(&self, guards: &PoolGuards) -> usize {
        let mut res = 0usize;
        let pivot_row_id = self.mem.pivot_row_id();
        let meta_pool_guard = guards.meta_guard();
        let mut cursor = self.mem.blk_idx().mem_cursor(meta_pool_guard);
        cursor.seek(pivot_row_id).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.lock_shared_async().await.unwrap();
            debug_assert!(g.page().is_leaf());
            res += g
                .page()
                .leaf_entries()
                .iter()
                .filter(|entry| entry.row_id >= pivot_row_id)
                .count();
        }
        res
    }

    async fn mem_scan<F>(&self, guards: &PoolGuards, mut page_action: F) -> Result<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        // With cursor, we lock two pages in block index and one row page
        // when scanning rows.
        let meta_pool_guard = self.meta_pool_guard(guards, "table mem scan")?;
        let pivot_row_id = self.mem.pivot_row_id();
        let mut cursor = self.mem.blk_idx().mem_cursor(meta_pool_guard);
        cursor.seek(pivot_row_id).await;
        while let Some(leaf) = cursor.next().await {
            let Some(g) = leaf.lock_shared_async().await else {
                return Err(self.stale_block_index_leaf("table mem scan"));
            };
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
    ) -> Result<()> {
        let page_id = page_guard.page_id();
        let (ctx, page) = page_guard.ctx_and_page_mut();
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
        if !ctx
            .recover()
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
        page.update_count_to_include_row_id(row_id);
        // insert CTS.
        ctx.recover_mut()
            .ok_or_else(|| {
                recovery_page_invariant_error("insert", page_id, row_id, cts, "missing recover map")
            })?
            .insert_at(row_idx, cts);
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
    ) -> Result<()> {
        let page_id = page_guard.page_id();
        let (ctx, page) = page_guard.ctx_and_page_mut();
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
        let var_len = page.var_len_for_update(row_idx, cols);
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
        if ctx
            .recover()
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
        ctx.recover_mut()
            .ok_or_else(|| {
                recovery_page_invariant_error("update", page_id, row_id, cts, "missing recover map")
            })?
            .update_at(row_idx, cts);
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
    ) -> Result<()> {
        let page_id = page_guard.page_id();
        let (ctx, page) = page_guard.ctx_and_page_mut();
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
        if ctx
            .recover()
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
        ctx.recover_mut()
            .ok_or_else(|| {
                recovery_page_invariant_error("delete", page_id, row_id, cts, "missing recover map")
            })?
            .update_at(row_idx, cts);
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
    pub(crate) fn secondary_index_root(&self, index_no: usize) -> Result<BlockID> {
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

struct FrozenPage {
    page_id: PageID,
    start_row_id: RowID,
    end_row_id: RowID,
}

type VisibleRowCollector<'a> = &'a mut dyn FnMut(&RowPage, usize, RowID) -> Result<()>;

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
        for index in std::mem::take(&mut self.staged).into_iter().rev().flatten() {
            // Keep the original construction error as the function result.
            let _ = index.destroy(pool_guard).await;
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
) -> Result<Box<[Option<Arc<SecondaryIndex<EvictableBufferPool>>>]>> {
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
) -> Error {
    Report::new(DataIntegrityError::InvalidRootInvariant)
        .attach(format!(
            "recover row {op}: page_id={page_id}, row_id={row_id}, cts={cts}, reason={reason}"
        ))
        .into()
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

enum InsertRowIntoPage {
    Ok(RowID, PageSharedGuard<RowPage>),
    NoSpaceOrFrozen(Vec<Val>, RowUndoKind, Vec<IndexBranch>),
}

enum UpdateRowInplace {
    // We keep row page lock if there is any index change,
    // so we can read latest values from page.
    // The hash map stores the changed column number and its old value.
    // for other columns in the changed index, we can read value(old and new are same)
    // from current page.
    Ok(RowID, HashMap<usize, Val>, PageSharedGuard<RowPage>),
    RowNotFound,
    RowDeleted,
    WriteConflict,
    RetryInTransition,
    NoFreeSpace(
        RowID,
        Vec<(Val, Option<u16>)>,
        Vec<UpdateCol>,
        PageSharedGuard<RowPage>,
    ),
}

enum DeleteInternal {
    Ok(PageSharedGuard<RowPage>),
    NotFound,
    WriteConflict,
    RetryInTransition,
}

#[inline]
fn index_key_is_changed(index_spec: &IndexSpec, index_change_cols: &HashMap<usize, Val>) -> bool {
    index_spec
        .cols
        .iter()
        .any(|key| index_change_cols.contains_key(&(key.col_no as usize)))
}

#[inline]
fn index_key_replace(
    index_spec: &IndexSpec,
    key: &SelectKey,
    updates: &HashMap<usize, Val>,
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
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let val = access.row().val(metadata.col.as_ref(), key.col_no as usize);
        new_key.vals[pos] = val;
    }
    new_key
}

#[inline]
fn missing_secondary_index(index_no: usize, index_count: usize) -> Error {
    Report::new(InternalError::SecondaryIndexOutOfBounds)
        .attach(format!("index_no={index_no}, index_count={index_count}"))
        .into()
}

#[inline]
fn secondary_index_kind_mismatch(operation: &'static str, expected: &'static str) -> Error {
    Report::new(InternalError::SecondaryIndexKindMismatch)
        .attach(format!("operation={operation}, expected={expected}"))
        .into()
}
