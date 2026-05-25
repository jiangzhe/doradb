use super::{
    DeleteInternal, InsertRowIntoPage, missing_secondary_index, read_latest_index_key, row_len,
    secondary_index_kind_mismatch, validate_page_row_range,
};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::{PageID, VersionedPageID};
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard, PoolGuards, PoolRole, RowPoolRole};
use crate::catalog::{TableColumnLayout, TableID, TableMetadata};
use crate::error::{Error, InternalError, OperationError, Result};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{
    BlockIndex, InMemorySecondaryIndex, IndexCompareExchange, IndexInsert, NonUniqueIndex,
    RowLocation, UniqueIndex,
};
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use crate::row::ops::{DeleteMvcc, InsertIndex, LinkForUniqueIndex, SelectKey};
use crate::row::{Row, RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::trx::TrxContext;
use crate::trx::TrxID;
use crate::trx::redo::{RowRedo, RowRedoKind};
use crate::trx::row::{
    FindOldVersion, LockRowForWrite, LockUndo, ReadAllRows, RowReadAccess, RowWriteAccess,
};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, IndexBranchTarget, OwnedRowUndo, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::value::Val;
use error_stack::Report;
use std::sync::Arc;

/// Shared in-memory table core used by both catalog and user tables.
///
/// `MemTable` owns only hot row-store state: row pages in a buffer pool,
/// the row-id-to-page block index, and optional in-memory secondary indexes.
/// It intentionally has no column-store, table-file, disk-cache, or runtime
/// layout ownership. User tables embed it inside [`Table`] and layer persisted
/// column storage plus user-only runtime layout on top; catalog tables wrap it
/// with fixed buffer pools and expose its base access methods through `Deref`.
///
/// The essential composition is:
///
/// 1. `table_id` and `metadata` identify the logical table and immutable row
///    shape used by row pages and index keys.
///
/// 2. `mem_pool` plus `row_pool_role` locate and validate the row-page buffer
///    pool used for inserts, scans, and row lookup.
///
/// 3. `blk_idx` maps row-id ranges to hot row pages and tracks the pivot row id
///    separating hot rows from rows that user tables may have checkpointed into
///    column storage.
///
/// 4. `sec_idx` plus `index_pool_role` own the in-memory secondary-index slots
///    for indexes that currently participate in hot-row access.
pub(crate) struct MemTable<D: 'static, I: 'static> {
    pub(crate) table_id: TableID,
    pub(crate) metadata: Arc<TableMetadata>,
    pub(crate) mem_pool: QuiescentGuard<D>,
    pub(crate) row_pool_role: RowPoolRole,
    pub(crate) index_pool_role: PoolRole,
    pub(crate) blk_idx: BlockIndex,
    pub(crate) sec_idx: Box<[Option<InMemorySecondaryIndex<I>>]>,
}

/// Stages newly built secondary indexes until the caller publishes them.
///
/// This keeps the build flow linear: construct each index, then either publish
/// the whole batch on success or explicitly destroy already-built trees before
/// returning the original build error.
struct InMemorySecondaryIndexScopedBuilder<P: 'static> {
    staged: Vec<Option<InMemorySecondaryIndex<P>>>,
}

impl<P: BufferPool> InMemorySecondaryIndexScopedBuilder<P> {
    #[inline]
    fn new(capacity: usize) -> Self {
        let mut staged = Vec::with_capacity(capacity);
        staged.resize_with(capacity, || None);
        Self { staged }
    }

    #[inline]
    async fn push_or_rollback(
        &mut self,
        index_no: usize,
        built: Result<InMemorySecondaryIndex<P>>,
        pool_guard: &PoolGuard,
    ) -> Result<()> {
        match built {
            Ok(index) => {
                debug_assert!(self.staged[index_no].is_none());
                self.staged[index_no] = Some(index);
                Ok(())
            }
            Err(err) => {
                self.rollback(pool_guard).await;
                Err(err)
            }
        }
    }

    #[inline]
    async fn rollback(&mut self, pool_guard: &PoolGuard) {
        for index in std::mem::take(&mut self.staged).into_iter().rev().flatten() {
            // Keep the original construction error as the function result.
            let _ = index.destroy(pool_guard).await;
        }
    }

    #[inline]
    fn publish(self) -> Box<[Option<InMemorySecondaryIndex<P>>]> {
        self.staged.into_boxed_slice()
    }
}

#[inline]
pub(crate) async fn build_in_memory_secondary_indexes<I: BufferPool + 'static>(
    index_pool: QuiescentGuard<I>,
    index_pool_guard: &PoolGuard,
    metadata: &TableMetadata,
    index_ts: TrxID,
) -> Result<Box<[Option<InMemorySecondaryIndex<I>>]>> {
    let mut builder = InMemorySecondaryIndexScopedBuilder::new(metadata.idx.index_slot_count());
    for (index_no, index_spec) in metadata.idx.active_indexes() {
        let ty_infer = |col_no: usize| metadata.col.col_type(col_no);
        builder
            .push_or_rollback(
                index_no,
                InMemorySecondaryIndex::new(
                    index_pool.clone(),
                    index_pool_guard,
                    index_spec,
                    ty_infer,
                    index_ts,
                )
                .await,
                index_pool_guard,
            )
            .await?;
    }
    Ok(builder.publish())
}

impl<D: BufferPool, I: BufferPool> MemTable<D, I> {
    #[inline]
    #[expect(clippy::too_many_arguments, reason = "code style")]
    pub(crate) async fn new(
        mem_pool: QuiescentGuard<D>,
        row_pool_role: RowPoolRole,
        index_pool: QuiescentGuard<I>,
        index_pool_role: PoolRole,
        index_pool_guard: &PoolGuard,
        table_id: TableID,
        metadata: Arc<TableMetadata>,
        blk_idx: BlockIndex,
        index_ts: TrxID,
    ) -> Result<Self> {
        let sec_idx =
            build_in_memory_secondary_indexes(index_pool, index_pool_guard, &metadata, index_ts)
                .await?;
        Ok(MemTable {
            table_id,
            metadata: Arc::clone(&metadata),
            mem_pool,
            row_pool_role,
            index_pool_role,
            blk_idx,
            sec_idx,
        })
    }

    /// Returns the logical table id of this runtime.
    #[inline]
    pub(crate) fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the immutable metadata for this table.
    #[inline]
    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Returns the buffer pool used for in-memory row pages.
    #[inline]
    pub(crate) fn mem_pool(&self) -> &D {
        &self.mem_pool
    }

    /// Returns the row page index used by this table.
    #[inline]
    pub(crate) fn blk_idx(&self) -> &BlockIndex {
        &self.blk_idx
    }

    /// Returns the secondary-index array owned by this table.
    #[inline]
    pub(crate) fn sec_idx(&self) -> &[Option<InMemorySecondaryIndex<I>>] {
        &self.sec_idx
    }

    #[inline]
    pub(crate) fn require_sec_idx(&self, index_no: usize) -> Result<&InMemorySecondaryIndex<I>> {
        self.sec_idx
            .get(index_no)
            .and_then(Option::as_ref)
            .ok_or_else(|| missing_secondary_index(index_no, self.sec_idx.len()))
    }

    #[inline]
    fn sec_idx_len(&self) -> usize {
        self.sec_idx().len()
    }

    #[inline]
    fn sec_idx_is_active(&self, index_no: usize) -> bool {
        self.sec_idx().get(index_no).is_some_and(Option::is_some)
    }

    #[inline]
    fn sec_idx_is_unique(&self, index_no: usize) -> bool {
        self.require_sec_idx(index_no)
            .expect("active index slot")
            .is_unique()
    }

    #[inline]
    async fn unique_lookup(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        sts: TrxID,
    ) -> Result<Option<(RowID, bool)>> {
        self.require_sec_idx(key.index_no)?
            .unique()
            .ok_or_else(|| secondary_index_kind_mismatch("unique lookup", "unique"))?
            .lookup(self.index_pool_guard(guards)?, &key.vals, sts)
            .await
    }

    #[inline]
    async fn unique_insert_if_not_exists(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        merge_if_match_deleted: bool,
        sts: TrxID,
    ) -> Result<IndexInsert> {
        self.require_sec_idx(key.index_no)?
            .unique()
            .ok_or_else(|| secondary_index_kind_mismatch("unique insert", "unique"))?
            .insert_if_not_exists(
                self.index_pool_guard(guards)?,
                &key.vals,
                row_id,
                merge_if_match_deleted,
                sts,
            )
            .await
    }

    #[inline]
    async fn unique_compare_delete(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.require_sec_idx(key.index_no)?
            .unique()
            .ok_or_else(|| secondary_index_kind_mismatch("unique compare delete", "unique"))?
            .compare_delete(
                self.index_pool_guard(guards)?,
                &key.vals,
                old_row_id,
                ignore_del_mask,
                ts,
            )
            .await
    }

    #[inline]
    async fn unique_mask_as_deleted(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        sts: TrxID,
    ) -> Result<bool> {
        self.require_sec_idx(key.index_no)?
            .unique()
            .ok_or_else(|| secondary_index_kind_mismatch("unique mask deleted", "unique"))?
            .mask_as_deleted(self.index_pool_guard(guards)?, &key.vals, row_id, sts)
            .await
    }

    #[inline]
    async fn unique_compare_exchange(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        sts: TrxID,
    ) -> Result<IndexCompareExchange> {
        self.require_sec_idx(key.index_no)?
            .unique()
            .ok_or_else(|| secondary_index_kind_mismatch("unique compare exchange", "unique"))?
            .compare_exchange(
                self.index_pool_guard(guards)?,
                &key.vals,
                old_row_id,
                new_row_id,
                sts,
            )
            .await
    }

    #[inline]
    async fn non_unique_lookup_unique(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        sts: TrxID,
    ) -> Result<Option<bool>> {
        self.require_sec_idx(key.index_no)?
            .non_unique()
            .ok_or_else(|| secondary_index_kind_mismatch("non-unique lookup unique", "non-unique"))?
            .lookup_unique(self.index_pool_guard(guards)?, &key.vals, row_id, sts)
            .await
    }

    #[inline]
    async fn non_unique_insert_if_not_exists(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        merge_if_match_deleted: bool,
        sts: TrxID,
    ) -> Result<IndexInsert> {
        self.require_sec_idx(key.index_no)?
            .non_unique()
            .ok_or_else(|| secondary_index_kind_mismatch("non-unique insert", "non-unique"))?
            .insert_if_not_exists(
                self.index_pool_guard(guards)?,
                &key.vals,
                row_id,
                merge_if_match_deleted,
                sts,
            )
            .await
    }

    #[inline]
    async fn non_unique_mask_as_deleted(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        sts: TrxID,
    ) -> Result<bool> {
        self.require_sec_idx(key.index_no)?
            .non_unique()
            .ok_or_else(|| secondary_index_kind_mismatch("non-unique mask deleted", "non-unique"))?
            .mask_as_deleted(self.index_pool_guard(guards)?, &key.vals, row_id, sts)
            .await
    }

    #[inline]
    async fn non_unique_compare_delete(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.require_sec_idx(key.index_no)?
            .non_unique()
            .ok_or_else(|| {
                secondary_index_kind_mismatch("non-unique compare delete", "non-unique")
            })?
            .compare_delete(
                self.index_pool_guard(guards)?,
                &key.vals,
                row_id,
                ignore_del_mask,
                ts,
            )
            .await
    }

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.blk_idx.pivot_row_id()
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

    #[inline]
    fn row_pool_guard<'a>(
        &self,
        guards: &'a PoolGuards,
        operation: &'static str,
    ) -> Result<&'a PoolGuard> {
        guards
            .try_row_guard(self.row_pool_role)
            .ok_or_else(|| self.missing_pool_guard(operation, "row-page"))
    }

    #[inline]
    pub(crate) fn index_pool_guard<'a>(&self, guards: &'a PoolGuards) -> Result<&'a PoolGuard> {
        guards
            .try_guard(self.index_pool_role)
            .ok_or_else(|| self.missing_pool_guard("secondary index access", "secondary-index"))
    }

    /// Destroy all mutable memory structures owned by this table runtime.
    #[inline]
    pub(crate) async fn destroy(self, guards: &PoolGuards) -> Result<()> {
        let row_pool_guard = self.row_pool_guard(guards, "destroy mem table")?;
        let index_pool_guard = self.index_pool_guard(guards)?;
        let meta_pool_guard = self.meta_pool_guard(guards, "destroy mem table")?;
        let MemTable {
            mem_pool,
            blk_idx,
            sec_idx,
            ..
        } = self;
        for index in sec_idx.into_iter().flatten() {
            index.destroy(index_pool_guard).await?;
        }
        blk_idx
            .destroy(meta_pool_guard, &*mem_pool, row_pool_guard)
            .await
    }

    #[inline]
    pub(crate) async fn get_row_page_shared(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        Ok(self
            .mem_pool()
            .get_page::<RowPage>(
                self.row_pool_guard(guards, "get row page shared")?,
                page_id,
                LatchFallbackMode::Shared,
            )
            .await?
            .lock_shared_async()
            .await)
    }

    #[inline]
    pub(crate) async fn get_row_page_versioned_shared(
        &self,
        guards: &PoolGuards,
        page_id: VersionedPageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        let guard = self
            .mem_pool()
            .get_page_versioned::<RowPage>(
                self.row_pool_guard(guards, "get versioned row page shared")?,
                page_id,
                LatchFallbackMode::Shared,
            )
            .await?;
        Ok(match guard {
            Some(guard) => guard.lock_shared_async().await,
            None => None,
        })
    }

    #[inline]
    pub(crate) async fn rollback_row_undo<F>(
        &self,
        entry: OwnedRowUndo,
        guards: &PoolGuards,
        rollback_sts: TrxID,
        on_cold_row_rollback: F,
    ) -> Result<()>
    where
        F: FnOnce(RowID),
    {
        let Some(page_id) = entry.page_id else {
            on_cold_row_rollback(entry.row_id);
            return Ok(());
        };
        if entry.row_id < self.pivot_row_id() {
            on_cold_row_rollback(entry.row_id);
            return Ok(());
        }
        let Some(page_guard) = self.get_row_page_versioned_shared(guards, page_id).await? else {
            return Ok(());
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let metadata = self.metadata();
        // TODO: we should retry or wait for notification if rollback happens on a page
        // in transition state. This will be handled in a future task.
        let row_idx = page.row_idx(entry.row_id);
        let mut access = RowWriteAccess::new(page, ctx, row_idx, Some(rollback_sts), false);
        access.rollback_first_undo(metadata, entry);
        Ok(())
    }

    #[inline]
    pub(crate) async fn get_row_page_exclusive(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<Option<PageExclusiveGuard<RowPage>>> {
        Ok(self
            .mem_pool()
            .get_page::<RowPage>(
                self.row_pool_guard(guards, "get row page exclusive")?,
                page_id,
                LatchFallbackMode::Exclusive,
            )
            .await?
            .lock_exclusive_async()
            .await)
    }

    #[inline]
    pub(crate) async fn must_get_row_page_shared(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<PageSharedGuard<RowPage>> {
        self.get_row_page_shared(guards, page_id)
            .await
            .and_then(|guard| {
                guard.ok_or_else(|| {
                    Report::new(InternalError::RowPageMissing)
                        .attach(format!("page_id={page_id}"))
                        .into()
                })
            })
    }

    #[inline]
    pub(crate) async fn must_get_row_page_exclusive(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        self.get_row_page_exclusive(guards, page_id)
            .await
            .and_then(|guard| {
                guard.ok_or_else(|| {
                    Report::new(InternalError::RowPageMissing)
                        .attach(format!("page_id={page_id}"))
                        .into()
                })
            })
    }

    #[inline]
    pub(crate) async fn try_get_insert_page(
        &self,
        guards: &PoolGuards,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageSharedGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards, "get insert page")?;
        let row_pool_guard = self.row_pool_guard(guards, "get insert page")?;
        self.blk_idx
            .try_get_insert_page_with_redo(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata.col,
                count,
                redo_ctx,
            )
            .await
    }

    #[inline]
    pub(crate) async fn get_insert_page_exclusive(
        &self,
        guards: &PoolGuards,
        count: usize,
        redo_ctx: Option<RowPageCreateRedoCtx<'_>>,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards, "get exclusive insert page")?;
        let row_pool_guard = self.row_pool_guard(guards, "get exclusive insert page")?;
        self.blk_idx
            .get_insert_page_exclusive_with_redo(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata.col,
                count,
                redo_ctx,
            )
            .await
    }

    #[inline]
    pub(crate) async fn allocate_row_page_at(
        &self,
        guards: &PoolGuards,
        count: usize,
        page_id: PageID,
    ) -> Result<PageExclusiveGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards, "allocate row page")?;
        let row_pool_guard = self.row_pool_guard(guards, "allocate row page")?;
        self.blk_idx
            .allocate_row_page_at(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata.col,
                count,
                page_id,
            )
            .await
    }

    #[inline]
    pub(crate) fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        self.blk_idx.cache_exclusive_insert_page(guard)
    }

    /// Scans in-memory row pages at or above the current table pivot.
    ///
    /// The pivot must be an exact row-page start boundary, unless it equals
    /// the current row-page-index end and there are no pages left to scan.
    pub(crate) async fn mem_scan<F>(&self, guards: &PoolGuards, page_action: F) -> Result<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = self.meta_pool_guard(guards, "mem scan")?;
        let start_row_id = self.pivot_row_id();
        self.mem_scan_from_with_meta_guard(
            guards,
            meta_pool_guard,
            start_row_id,
            "mem scan",
            page_action,
        )
        .await
    }

    /// Scans in-memory row pages at or above an explicit row-page start boundary.
    ///
    /// This intentionally does not consult the current pivot. Callers use it
    /// when a previously captured table-root snapshot defines the hot-row
    /// boundary for the scan. The boundary must be an exact row-page start,
    /// unless it equals the current row-page-index end and there are no pages
    /// left to scan.
    pub(crate) async fn mem_scan_from<F>(
        &self,
        guards: &PoolGuards,
        start_row_id: RowID,
        page_action: F,
    ) -> Result<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = self.meta_pool_guard(guards, "mem scan from")?;
        self.mem_scan_from_with_meta_guard(
            guards,
            meta_pool_guard,
            start_row_id,
            "mem scan from",
            page_action,
        )
        .await
    }

    #[inline]
    fn invalid_mem_scan_start(&self, operation: &'static str, start_row_id: RowID) -> Error {
        Report::new(InternalError::Generic)
            .attach(format!(
                "operation={operation}, table_id={}, row-page scan start is not a row-page boundary: start_row_id={start_row_id}",
                self.table_id()
            ))
            .into()
    }

    async fn mem_scan_from_with_meta_guard<F>(
        &self,
        guards: &PoolGuards,
        meta_pool_guard: &PoolGuard,
        start_row_id: RowID,
        operation: &'static str,
        mut page_action: F,
    ) -> Result<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let mut cursor = self.blk_idx.mem_cursor(meta_pool_guard);
        cursor.seek(start_row_id).await;
        let mut first_leaf = true;
        while let Some(leaf) = cursor.next().await {
            let Some(g) = leaf.lock_shared_async().await else {
                return Err(self.stale_block_index_leaf(operation));
            };
            debug_assert!(g.page().is_leaf());
            let page = g.page();
            let entries = page.leaf_entries();
            let start_idx = if first_leaf {
                first_leaf = false;
                if entries.is_empty() {
                    if page.header.start_row_id == start_row_id {
                        return Ok(());
                    }
                    return Err(self.invalid_mem_scan_start(operation, start_row_id));
                }
                match entries.binary_search_by_key(&start_row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(_) if page.header.end_row_id == start_row_id => return Ok(()),
                    Err(_) => return Err(self.invalid_mem_scan_start(operation, start_row_id)),
                }
            } else {
                0
            };
            for page_entry in &entries[start_idx..] {
                let page_guard = self
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
    pub(crate) async fn find_row(&self, guards: &PoolGuards, row_id: RowID) -> Result<RowLocation> {
        let meta_pool_guard = self.meta_pool_guard(guards, "find row")?;
        self.blk_idx.find_mem_row(meta_pool_guard, row_id).await
    }
}

impl MemTable<FixedBufferPool, FixedBufferPool> {
    #[inline]
    fn catalog_lwc_error<T>(&self, operation: &'static str, row_id: RowID) -> Result<T> {
        Err(Report::new(InternalError::Generic)
            .attach(format!(
                "catalog table resolved persisted LWC row unexpectedly: operation={operation}, table_id={}, row_id={row_id}",
                self.table_id()
            ))
            .into())
    }

    #[inline]
    fn debug_assert_table_write_lock_held(&self, ctx: &TrxContext) {
        ctx.debug_assert_table_write_lock_held(self.table_id());
    }

    #[inline]
    fn push_insert_unique_index_undo(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(ctx);
        effects.push_insert_unique_index_undo(self.table_id(), row_id, key, merge_old_deleted);
    }

    #[inline]
    fn push_insert_non_unique_index_undo(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(ctx);
        effects.push_insert_non_unique_index_undo(self.table_id(), row_id, key, merge_old_deleted);
    }

    #[inline]
    fn push_delete_index_undo(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        unique: bool,
    ) {
        self.debug_assert_table_write_lock_held(ctx);
        effects.push_delete_index_undo(self.table_id(), row_id, key, unique);
    }

    #[inline]
    fn push_update_unique_index_undo(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        key: SelectKey,
        old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(ctx);
        effects.push_update_unique_index_undo(
            self.table_id(),
            old_row_id,
            new_row_id,
            key,
            old_deleted,
        );
    }

    #[inline]
    async fn insert_index_no_trx(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<()> {
        if self
            .metadata()
            .idx
            .require_index_spec(key.index_no)?
            .unique()
        {
            let res = self
                .unique_insert_if_not_exists(
                    guards,
                    &key,
                    row_id,
                    false,
                    crate::trx::MIN_SNAPSHOT_TS,
                )
                .await?;
            assert!(res.is_ok());
        } else {
            let res = self
                .non_unique_insert_if_not_exists(
                    guards,
                    &key,
                    row_id,
                    false,
                    crate::trx::MIN_SNAPSHOT_TS,
                )
                .await?;
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[inline]
    async fn delete_index_directly(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
    ) -> Result<bool> {
        let spec = self.metadata().idx.require_index_spec(key.index_no)?;
        if spec.unique() {
            self.unique_compare_delete(guards, key, row_id, true, crate::trx::MIN_SNAPSHOT_TS)
                .await
        } else {
            self.non_unique_compare_delete(guards, key, row_id, true, crate::trx::MIN_SNAPSHOT_TS)
                .await
        }
    }

    #[inline]
    async fn insert_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<InsertIndex> {
        if self
            .metadata()
            .idx
            .require_index_spec(key.index_no)?
            .unique()
        {
            self.insert_unique_index(ctx, effects, key, row_id, page_guard)
                .await
        } else {
            self.insert_non_unique_index(ctx, effects, key, row_id)
                .await
        }
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut index_branches: Vec<IndexBranch>,
    ) -> Result<(RowID, PageSharedGuard<RowPage>)> {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col.col_count());
        loop {
            let page_guard = self.get_insert_page(ctx, row_count).await?;
            match self.insert_row_to_page(
                ctx,
                effects,
                page_guard,
                insert,
                undo_kind,
                index_branches,
            ) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    ctx.save_active_insert_page(
                        self.table_id(),
                        page_guard.versioned_page_id(),
                        row_id,
                    );
                    return Ok((row_id, page_guard));
                }
                InsertRowIntoPage::NoSpaceOrFrozen(ins, uk, ib) => {
                    insert = ins;
                    undo_kind = uk;
                    index_branches = ib;
                }
            }
        }
    }

    #[inline]
    fn insert_row_to_page(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        cols: Vec<Val>,
        undo_kind: RowUndoKind,
        index_branches: Vec<IndexBranch>,
    ) -> InsertRowIntoPage {
        debug_assert!(matches!(undo_kind, RowUndoKind::Insert));
        let metadata = self.metadata();
        let page_id = page_guard.page_id();
        let versioned_page_id = page_guard.versioned_page_id();
        let (page_ctx, page) = page_guard.ctx_and_page();
        let ver_map = page_ctx.row_ver().unwrap();
        let state_guard = ver_map.read_state();
        if *state_guard != RowPageState::Active {
            return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
        }
        debug_assert!(metadata.col.col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);

        let var_len = var_len_for_insert(metadata.col.as_ref(), &cols);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
            };
        let row_id = page.header.start_row_id + row_idx as u64;
        let mut access = RowWriteAccess::new_with_state_guard(
            page,
            page_ctx,
            row_idx,
            Some(ctx.sts()),
            true,
            state_guard,
        );
        let res = access.lock_undo(
            ctx,
            effects,
            metadata,
            self.table_id(),
            versioned_page_id,
            row_id,
            None,
        );
        debug_assert!(res.is_ok());
        let mut new_row = page.new_row(row_idx, var_offset);
        for v in &cols {
            new_row.add_col(metadata.col.as_ref(), v);
        }
        let new_row_id = new_row.finish();
        debug_assert!(new_row_id == row_id);
        effects.update_last_row_undo(undo_kind);
        for branch in index_branches {
            match branch.target {
                IndexBranchTarget::Hot { cts, entry } => {
                    access.link_for_unique_index(branch.key, cts, entry, branch.undo_vals);
                }
                IndexBranchTarget::ColdTerminal { delete_cts } => {
                    access.link_for_unique_index_cold_terminal(
                        branch.key,
                        delete_cts,
                        branch.undo_vals,
                    );
                }
            }
        }
        drop(access);

        let redo_entry = RowRedo {
            page_id,
            row_id,
            kind: RowRedoKind::Insert(cols),
        };
        effects.insert_row_redo(self.table_id(), redo_entry);
        InsertRowIntoPage::Ok(row_id, page_guard)
    }

    #[inline]
    async fn get_insert_page(
        &self,
        ctx: &TrxContext,
        row_count: usize,
    ) -> Result<PageSharedGuard<RowPage>> {
        let guards = ctx.require_pool_guards("catalog get insert page")?;
        if let Some((page_id, row_id)) = ctx.load_active_insert_page(self.table_id()) {
            let page_guard = self.get_row_page_versioned_shared(guards, page_id).await?;
            if let Some(page_guard) = page_guard
                && validate_page_row_range(&page_guard, page_id.page_id, row_id)
            {
                return Ok(page_guard);
            }
        }
        self.try_get_insert_page(guards, row_count, None).await
    }

    #[expect(clippy::await_holding_lock, reason = "clippy false positive")]
    #[inline]
    async fn lock_row_for_write<'b>(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: &'b PageSharedGuard<RowPage>,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockRowForWrite<'b> {
        let (page_ctx, page) = page_guard.ctx_and_page();
        let ver_map = page_ctx.row_ver().unwrap();
        loop {
            let state_guard = ver_map.read_state();
            if *state_guard == RowPageState::Transition {
                return LockRowForWrite::RetryInTransition;
            }
            let mut access = RowWriteAccess::new_with_state_guard(
                page,
                page_ctx,
                page.row_idx(row_id),
                Some(ctx.sts()),
                false,
                state_guard,
            );
            let lock_undo = access.lock_undo(
                ctx,
                effects,
                self.metadata(),
                self.table_id(),
                page_guard.versioned_page_id(),
                row_id,
                key,
            );
            match lock_undo {
                LockUndo::Ok => return LockRowForWrite::Ok(Some(access)),
                LockUndo::InvalidIndex => return LockRowForWrite::InvalidIndex,
                LockUndo::WriteConflict => return LockRowForWrite::WriteConflict,
                LockUndo::Preparing(listener) => {
                    if let Some(listener) = listener {
                        drop(access);
                        listener.await;
                    }
                }
            }
        }
    }

    #[inline]
    async fn link_for_unique_index(
        &self,
        ctx: &TrxContext,
        old_id: RowID,
        key: &SelectKey,
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> Result<LinkForUniqueIndex> {
        debug_assert!(old_id != new_id);
        let guards = ctx.require_pool_guards("catalog link unique index")?;
        let (old_guard, old_id) = loop {
            match self.find_row(guards, old_id).await {
                Ok(RowLocation::NotFound) => return Ok(LinkForUniqueIndex::NotNeeded),
                Ok(RowLocation::LwcBlock { .. }) => {
                    return self.catalog_lwc_error("link unique index", old_id);
                }
                Ok(RowLocation::RowPage(page_id)) => {
                    let Some(old_guard) = self
                        .try_get_validated_row_page_shared_result(guards, page_id, old_id)
                        .await?
                    else {
                        continue;
                    };
                    break (old_guard, old_id);
                }
                Err(err) => return Err(err),
            }
        };
        let metadata = self.metadata();
        let (page_ctx, page) = old_guard.ctx_and_page();
        let old_access = RowReadAccess::new(page, page_ctx, page.row_idx(old_id));
        match old_access.find_old_version_for_unique_key(metadata, key, ctx) {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::DuplicateKey => Ok(LinkForUniqueIndex::DuplicateKey),
            FindOldVersion::WriteConflict => Ok(LinkForUniqueIndex::WriteConflict),
            FindOldVersion::Ok(old_row, cts, old_entry) => {
                let (page_ctx, page) = new_guard.ctx_and_page();
                let mut new_access = RowWriteAccess::new(
                    page,
                    page_ctx,
                    page.row_idx(new_id),
                    Some(ctx.sts()),
                    false,
                );
                let undo_vals = new_access.row().calc_delta(metadata.col.as_ref(), &old_row);
                new_access.link_for_unique_index(key.clone(), cts, old_entry, undo_vals);
                Ok(LinkForUniqueIndex::Linked)
            }
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<InsertIndex> {
        let sts = ctx.sts();
        let guards = ctx.require_pool_guards("catalog insert unique index")?;
        loop {
            match self
                .unique_insert_if_not_exists(guards, &key, row_id, false, sts)
                .await?
            {
                IndexInsert::Ok(merged) => {
                    self.push_insert_unique_index_undo(ctx, effects, row_id, key, merged);
                    return Ok(InsertIndex::Inserted);
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    debug_assert!(old_row_id != row_id);
                    if !deleted {
                        return Ok(InsertIndex::DuplicateKey);
                    }
                    match self
                        .link_for_unique_index(ctx, old_row_id, &key, row_id, page_guard)
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(InsertIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(InsertIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            let index_old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match self
                                .unique_compare_exchange(
                                    guards,
                                    &key,
                                    index_old_row_id,
                                    row_id,
                                    sts,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    self.push_update_unique_index_undo(
                                        ctx, effects, old_row_id, row_id, key, deleted,
                                    );
                                    return Ok(InsertIndex::Inserted);
                                }
                                IndexCompareExchange::NotExists => continue,
                                IndexCompareExchange::Mismatch => {
                                    return Ok(InsertIndex::WriteConflict);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn insert_non_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<InsertIndex> {
        let sts = ctx.sts();
        let guards = ctx.require_pool_guards("catalog insert non-unique index")?;
        match self
            .non_unique_insert_if_not_exists(guards, &key, row_id, false, sts)
            .await?
        {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(ctx, effects, row_id, key, merged);
                Ok(InsertIndex::Inserted)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn delete_row_internal(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
        log_by_key: bool,
    ) -> DeleteInternal {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
        if !page.row_id_in_valid_range(row_id) {
            return DeleteInternal::NotFound;
        }
        let mut lock_row = self
            .lock_row_for_write(ctx, effects, &page_guard, row_id, Some(key))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => DeleteInternal::NotFound,
            LockRowForWrite::WriteConflict => DeleteInternal::WriteConflict,
            LockRowForWrite::RetryInTransition => DeleteInternal::RetryInTransition,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return DeleteInternal::NotFound;
                }
                access.delete_row();
                effects.update_last_row_undo(RowUndoKind::Delete);
                drop(access);
                drop(lock_row);
                let redo_entry = RowRedo {
                    page_id,
                    row_id,
                    kind: if log_by_key {
                        RowRedoKind::DeleteByUniqueKey(key.clone())
                    } else {
                        RowRedoKind::Delete
                    },
                };
                effects.insert_row_redo(self.table_id(), redo_entry);
                DeleteInternal::Ok(page_guard)
            }
        }
    }

    #[inline]
    async fn defer_delete_indexes(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<()> {
        let metadata = self.metadata();
        let keys = metadata
            .idx
            .active_indexes()
            .map(|(index_no, _)| read_latest_index_key(metadata, index_no, page_guard, row_id))
            .collect();
        self.defer_delete_index_keys(ctx, effects, row_id, keys)
            .await
    }

    #[inline]
    async fn defer_delete_index_keys(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        keys: Vec<SelectKey>,
    ) -> Result<()> {
        for key in keys {
            let spec = self.metadata().idx.require_index_spec(key.index_no)?;
            debug_assert_eq!(self.sec_idx_is_unique(key.index_no), spec.unique());
            if spec.unique() {
                self.defer_delete_unique_index(ctx, effects, row_id, key)
                    .await?;
            } else {
                self.defer_delete_non_unique_index(ctx, effects, row_id, key)
                    .await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn defer_delete_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
    ) -> Result<()> {
        let sts = ctx.sts();
        let guards = ctx.require_pool_guards("catalog defer delete unique index")?;
        let res = self
            .unique_mask_as_deleted(guards, &key, row_id, sts)
            .await?;
        debug_assert!(res);
        self.push_delete_index_undo(ctx, effects, row_id, key, true);
        Ok(())
    }

    #[inline]
    async fn defer_delete_non_unique_index(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
    ) -> Result<()> {
        let sts = ctx.sts();
        let guards = ctx.require_pool_guards("catalog defer delete non-unique index")?;
        let res = self
            .non_unique_mask_as_deleted(guards, &key, row_id, sts)
            .await?;
        debug_assert!(res);
        self.push_delete_index_undo(ctx, effects, row_id, key, false);
        Ok(())
    }

    #[inline]
    async fn try_get_validated_row_page_shared_result(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        let Some(page_guard) = self.get_row_page_shared(guards, page_id).await? else {
            return Ok(None);
        };
        if validate_page_row_range(&page_guard, page_id, row_id) {
            Ok(Some(page_guard))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn index_purge_decision(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
    ) -> Result<Option<PageID>> {
        match self.find_row(guards, row_id).await? {
            RowLocation::NotFound => Ok(None),
            RowLocation::LwcBlock { .. } => self.catalog_lwc_error("index purge", row_id),
            RowLocation::RowPage(page_id) => Ok(Some(page_id)),
        }
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        _min_active_sts: TrxID,
    ) -> Result<bool> {
        let (page_guard, row_id) = loop {
            let sts = crate::trx::MIN_SNAPSHOT_TS;
            match self.unique_lookup(guards, key, sts).await? {
                None => return Ok(false),
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        return Ok(false);
                    }
                    let Some(page_id) = self.index_purge_decision(guards, row_id).await? else {
                        return self
                            .unique_compare_delete(
                                guards,
                                key,
                                row_id,
                                false,
                                crate::trx::MIN_SNAPSHOT_TS,
                            )
                            .await;
                    };
                    let Some(page_guard) = self
                        .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                        .await?
                    else {
                        continue;
                    };
                    break (page_guard, row_id);
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        if !access.any_version_matches_key(self.metadata(), key) {
            return self
                .unique_compare_delete(guards, key, row_id, false, crate::trx::MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        _min_active_sts: TrxID,
    ) -> Result<bool> {
        let (page_guard, row_id) = loop {
            let sts = crate::trx::MIN_SNAPSHOT_TS;
            match self
                .non_unique_lookup_unique(guards, key, row_id, sts)
                .await?
            {
                None => return Ok(false),
                Some(active) => {
                    if active {
                        return Ok(false);
                    }
                    let Some(page_id) = self.index_purge_decision(guards, row_id).await? else {
                        return self
                            .non_unique_compare_delete(
                                guards,
                                key,
                                row_id,
                                false,
                                crate::trx::MIN_SNAPSHOT_TS,
                            )
                            .await;
                    };
                    let Some(page_guard) = self
                        .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                        .await?
                    else {
                        continue;
                    };
                    break (page_guard, row_id);
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        if !access.any_version_matches_key(self.metadata(), key) {
            return self
                .non_unique_compare_delete(guards, key, row_id, false, crate::trx::MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    #[inline]
    pub(crate) async fn insert_no_trx(&self, guards: &PoolGuards, cols: &[Val]) -> Result<()> {
        debug_assert!(cols.len() == self.metadata().col.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col.col_type_match(idx, val))
        });
        let metadata = self.metadata();
        let keys = metadata.idx.keys_for_insert(cols);
        let row_len = row_len(metadata, cols);
        let row_count = estimate_max_row_count(row_len, metadata.col.col_count());
        loop {
            let mut page_guard = self
                .get_insert_page_exclusive(guards, row_count, None)
                .await?;
            let page = page_guard.page_mut();
            debug_assert!(metadata.col.col_count() == page.header.col_count as usize);
            debug_assert!(cols.len() == page.header.col_count as usize);
            let var_len = var_len_for_insert(metadata.col.as_ref(), cols);
            let (row_idx, var_offset) =
                if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                    (row_idx, var_offset)
                } else {
                    continue;
                };
            let row_id = page.header.start_row_id + row_idx as RowID;
            let mut row = page.row_mut_exclusive(row_idx, var_offset, var_offset + var_len);
            debug_assert!(row.is_deleted());
            for (col_idx, user_col) in cols.iter().enumerate() {
                row.update_col(metadata.col.as_ref(), col_idx, user_col, false);
            }
            for key in keys {
                self.insert_index_no_trx(guards, key, row_id).await?;
            }
            row.finish_insert();
            self.cache_exclusive_insert_page(page_guard);
            return Ok(());
        }
    }

    #[inline]
    pub(crate) async fn delete_unique_no_trx(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Result<()> {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(key.index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            key.index_no,
            &key.vals
        ));
        let sts = crate::trx::MIN_SNAPSHOT_TS;
        let (mut page_guard, row_id) = match self.unique_lookup(guards, key, sts).await? {
            None => unreachable!(),
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => unreachable!(),
                RowLocation::LwcBlock { .. } => {
                    return self.catalog_lwc_error("delete unique no-trx", row_id);
                }
                RowLocation::RowPage(page_id) => {
                    let page_guard = self
                        .get_row_page_exclusive(guards, page_id)
                        .await
                        .expect("delete_unique_no_trx should not ignore page-I/O failures")
                        .expect("failed to lock exclusive row page");
                    (page_guard, row_id)
                }
            },
        };
        let page = page_guard.page_mut();
        let row_idx = page.row_idx(row_id);
        debug_assert!(!page.is_deleted(row_idx));
        let row = page.row(row_idx);
        let keys = self
            .metadata()
            .idx
            .keys_for_delete(self.metadata().col.as_ref(), row);
        for key in keys {
            let res = self.delete_index_directly(guards, &key, row_id).await?;
            assert!(res);
        }
        page.set_deleted_exclusive(row_idx, true);
        Ok(())
    }

    /// Table scan including uncommitted versions.
    ///
    /// This method iterates raw latest row versions and includes rows marked
    /// as deleted. Callers should explicitly filter `row.is_deleted()` if they
    /// only need live rows.
    ///
    /// Note: this scans only in-memory row-store pages and does not include
    /// persisted column-store rows on disk.
    #[inline]
    pub(crate) async fn table_scan_uncommitted<F>(
        &self,
        guards: &PoolGuards,
        mut row_action: F,
    ) -> Result<()>
    where
        F: for<'m, 'p> FnMut(&'m TableColumnLayout, Row<'p>) -> bool,
    {
        self.mem_scan(guards, |page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let col_layout = ctx.row_ver().unwrap().column_layout.as_ref();
            for row_access in ReadAllRows::new(page, ctx) {
                if !row_action(col_layout, row_access.row()) {
                    return false;
                }
            }
            true
        })
        .await
    }

    /// Index lookup unique row including uncommitted version.
    #[inline]
    pub(crate) async fn index_lookup_unique_uncommitted<R, F>(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_action: F,
    ) -> Result<Option<R>>
    where
        for<'m, 'p> F: FnOnce(&'m TableColumnLayout, Row<'p>) -> R,
    {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(key.index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            key.index_no,
            &key.vals
        ));
        let sts = crate::trx::MIN_SNAPSHOT_TS;
        let (page_guard, row_id) = match self.unique_lookup(guards, key, sts).await? {
            None => return Ok(None),
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => return Ok(None),
                RowLocation::LwcBlock { .. } => {
                    return self.catalog_lwc_error("unique uncommitted lookup", row_id);
                }
                RowLocation::RowPage(page_id) => {
                    let page_guard = self.must_get_row_page_shared(guards, page_id).await?;
                    (page_guard, row_id)
                }
            },
        };
        let (ctx, page) = page_guard.ctx_and_page();
        if !page.row_id_in_valid_range(row_id) {
            return Ok(None);
        }
        let row_layout = ctx.row_ver().unwrap().column_layout.as_ref();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let row = access.row();
        if row.is_deleted() {
            return Ok(None);
        }
        let metadata = self.metadata();
        let Some(index_spec) = metadata.idx.index_spec(key.index_no) else {
            return Ok(None);
        };
        if row.is_key_different(row_layout, index_spec, key) {
            return Ok(None);
        }
        Ok(Some(row_action(row_layout, row)))
    }

    /// Insert row in transaction.
    #[inline]
    pub(crate) async fn insert_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        let metadata = self.metadata();
        debug_assert!(cols.len() == metadata.col.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col.col_type_match(idx, val))
        });
        let keys = self.metadata().idx.keys_for_insert(&cols);
        let (row_id, page_guard) = self
            .insert_row_internal(ctx, effects, cols, RowUndoKind::Insert, Vec::new())
            .await?;
        for key in keys {
            match self
                .insert_index(ctx, effects, key, row_id, &page_guard)
                .await?
            {
                InsertIndex::Inserted => (),
                InsertIndex::DuplicateKey => {
                    return Err(Report::new(OperationError::DuplicateKey)
                        .attach("catalog insert MVCC secondary index claim")
                        .into());
                }
                InsertIndex::WriteConflict => {
                    return Err(Report::new(OperationError::WriteConflict)
                        .attach("catalog insert MVCC secondary index claim")
                        .into());
                }
            }
        }
        page_guard.set_dirty();
        Ok(row_id)
    }

    /// Delete row in transaction by unique index lookup.
    ///
    /// If `log_by_key` is true, redo logs the unique key instead of row id.
    /// Catalog callers use this because catalog row locations may differ across
    /// restart/recovery cycles.
    #[inline]
    pub(crate) async fn delete_unique_mvcc(
        &self,
        ctx: &TrxContext,
        effects: &mut StmtEffects,
        key: &SelectKey,
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        debug_assert!(key.index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(key.index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            key.index_no,
            &key.vals
        ));
        let guards = ctx.require_pool_guards("catalog delete unique MVCC")?;
        loop {
            let lookup_sts = ctx.sts();
            let (page_guard, row_id) = match self.unique_lookup(guards, key, lookup_sts).await? {
                None => return Ok(DeleteMvcc::NotFound),
                Some((row_id, _)) => match self.find_row(guards, row_id).await {
                    Ok(RowLocation::NotFound) => return Ok(DeleteMvcc::NotFound),
                    Ok(RowLocation::LwcBlock { .. }) => {
                        return self.catalog_lwc_error("delete unique MVCC", row_id);
                    }
                    Ok(RowLocation::RowPage(page_id)) => {
                        let Some(page_guard) = self
                            .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                            .await?
                        else {
                            continue;
                        };
                        (page_guard, row_id)
                    }
                    Err(err) => return Err(err),
                },
            };
            match self
                .delete_row_internal(ctx, effects, page_guard, row_id, key, log_by_key)
                .await
            {
                DeleteInternal::NotFound => return Ok(DeleteMvcc::NotFound),
                DeleteInternal::WriteConflict => return Ok(DeleteMvcc::WriteConflict),
                DeleteInternal::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                DeleteInternal::Ok(page_guard) => {
                    self.defer_delete_indexes(ctx, effects, row_id, &page_guard)
                        .await?;
                    page_guard.set_dirty();
                    return Ok(DeleteMvcc::Deleted);
                }
            }
        }
    }

    /// Delete an obsolete secondary-index entry from a purge path.
    #[inline]
    pub(crate) async fn delete_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let Some(index_schema) = self.metadata().idx.index_spec(key.index_no) else {
            return Ok(false);
        };
        if !self.sec_idx_is_active(key.index_no) {
            return Ok(false);
        }
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            self.delete_unique_index(guards, key, row_id, min_active_sts)
                .await
        } else {
            self.delete_non_unique_index(guards, key, row_id, min_active_sts)
                .await
        }
    }
}
