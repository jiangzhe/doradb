use super::{
    DmlValidationDomain, DmlValidator, UpdateUniqueMvcc,
    hot::{DeleteInternal, HotRowMutator, InsertRowIntoPage, RowInserter, UpdateRowInplace},
    index_key_is_changed, index_key_replace, missing_secondary_index, read_latest_index_key,
    row_len, unique_key_from_full_row, validate_page_row_range,
};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::VersionedPageID;
use crate::buffer::{
    BufferPool, PoolGuard, PoolGuards, PoolRole, RowPoolRole, get_page_versioned_shared,
};
use crate::catalog::{IndexSpec, PrimaryKeyMatchError, TableColumnLayout, TableMetadata};
use crate::error::{
    DataIntegrityError, Error, InternalError, OperationError, OperationResult,
    RecoveryDuplicateKey, Result,
};
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{
    BlockIndex, GuardedNonUniqueMemIndex, GuardedUniqueMemIndex, InMemorySecondaryIndex,
    IndexCompareExchange, IndexInsert, NonUniqueIndex, RowLocation, UniqueIndex,
};
use crate::latch::LatchFallbackMode;
use crate::map::FastHashMap;
use crate::quiescent::QuiescentGuard;
use crate::row::ops::{
    DeleteMvcc, LinkForUniqueIndex, RowUpdateInput, RowUpdateView, SelectKey, UpdateCol,
    UpdateMvcc, UpsertMvcc,
};
use crate::row::{Row, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::trx::row::FindOldVersion;
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, OwnedRowUndo, RowUndoKind};
use crate::trx::{MIN_SNAPSHOT_TS, RetiredRowPageBatch, TrxRuntime};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::mem::take;
use std::result::Result as StdResult;
use std::sync::Arc;

struct NoTrxIndexRefresh {
    old_keys: Vec<SelectKey>,
    new_keys: Vec<SelectKey>,
}

/// Successful catalog primary-key upsert performed without transaction state.
pub(crate) enum NoTrxUpsertChange {
    /// A new logical row was inserted at the reported runtime location.
    Inserted {
        page_id: PageID,
        row_id: RowID,
        vals: Vec<Val>,
    },
    /// An existing logical row was updated at the reported final runtime location.
    Updated {
        page_id: PageID,
        row_id: RowID,
        key: SelectKey,
        cols: Vec<UpdateCol>,
    },
}

/// Snapshot descriptor for one original hot row page.
///
/// The descriptor contains only stable block-index identity and the reserved
/// RowID range. Callers reopen the page when they are ready to scan it, so no
/// block-index leaf latch or row-page guard survives the snapshot operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowPageDescriptor {
    /// Buffer-pool page identity recorded by the row-page index.
    pub(crate) page_id: PageID,
    /// Inclusive first RowID reserved for the page.
    pub(crate) start_row_id: RowID,
    /// Exclusive RowID reservation boundary for the page.
    pub(crate) end_row_id: RowID,
}

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
    /// Logical table id for this in-memory runtime.
    pub(crate) table_id: TableID,
    /// Immutable table metadata used for row and index interpretation.
    pub(crate) metadata: Arc<TableMetadata>,
    /// Buffer pool that owns in-memory row pages.
    pub(crate) mem_pool: QuiescentGuard<D>,
    /// Pool role used for row-page buffer access.
    pub(crate) row_pool_role: RowPoolRole,
    /// Pool role used for in-memory secondary indexes.
    pub(crate) index_pool_role: PoolRole,
    /// Hot row-id to row-page index.
    pub(crate) blk_idx: BlockIndex,
    /// Sparse secondary-index runtimes for active in-memory indexes.
    pub(crate) sec_idx: Box<[Option<InMemorySecondaryIndex<I>>]>,
}

impl<D: BufferPool, I: BufferPool> MemTable<D, I> {
    /// Create a MemTable with freshly built in-memory secondary indexes.
    #[expect(clippy::too_many_arguments, reason = "code style")]
    #[inline]
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

    /// Return an active secondary-index runtime by stable index number.
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

    /// Return a guarded unique MemIndex by stable index number.
    #[inline]
    pub(crate) fn require_unique_index<'g>(
        &self,
        guards: &'g PoolGuards,
        index_no: usize,
    ) -> Result<GuardedUniqueMemIndex<'_, 'g, I>> {
        match self.require_sec_idx(index_no)? {
            InMemorySecondaryIndex::Unique(index) => Ok(index.bind(self.index_pool_guard(guards)?)),
            InMemorySecondaryIndex::NonUnique(_) => {
                Err(Error::wrong_secondary_index_binding("unique", "non-unique"))
            }
        }
    }

    /// Return a guarded non-unique MemIndex by stable index number.
    #[inline]
    pub(crate) fn require_non_unique_index<'g>(
        &self,
        guards: &'g PoolGuards,
        index_no: usize,
    ) -> Result<GuardedNonUniqueMemIndex<'_, 'g, I>> {
        match self.require_sec_idx(index_no)? {
            InMemorySecondaryIndex::Unique(_) => {
                Err(Error::wrong_secondary_index_binding("non-unique", "unique"))
            }
            InMemorySecondaryIndex::NonUnique(index) => {
                Ok(index.bind(self.index_pool_guard(guards)?))
            }
        }
    }

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.blk_idx.pivot_row_id()
    }

    #[inline]
    fn missing_pool_guard(&self, operation: &'static str, role: &'static str) -> Error {
        Report::new(InternalError::PoolGuardMissing)
            .attach(format!(
                "operation={operation}, table_id={}, missing {role} pool guard",
                self.table_id()
            ))
            .into()
    }

    #[inline]
    fn stale_block_index_leaf(&self, operation: &'static str) -> Error {
        Report::new(InternalError::BlockIndexLeafStale)
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

    /// Return the pool guard used by in-memory secondary indexes.
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

    /// Unlinks one exact checkpoint-retired row-page prefix from the hot index.
    #[inline]
    pub(crate) async fn unlink_retired_row_pages(
        &self,
        guards: &PoolGuards,
        batch: &RetiredRowPageBatch,
    ) -> Result<Box<[PageID]>> {
        let result = self
            .blk_idx
            .prune_checkpoint_prefix(
                self.meta_pool_guard(guards, "unlink retired row pages")?,
                batch.start_row_id,
                batch.end_row_id,
                &batch.page_ids,
            )
            .await?;
        Ok(result.page_ids)
    }

    /// Physically deallocates row pages already unlinked from the hot index.
    #[inline]
    pub(crate) async fn deallocate_retired_row_pages(
        &self,
        guards: &PoolGuards,
        page_ids: &[PageID],
    ) -> Result<()> {
        let row_pool_guard = self.row_pool_guard(guards, "deallocate retired row pages")?;
        for page_id in page_ids {
            let page_guard = self
                .mem_pool
                .get_page::<RowPage>(row_pool_guard, *page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .ok_or_else(|| {
                    Report::new(InternalError::RowPageMissing)
                        .attach(format!("retired row page missing: page_id={page_id}"))
                })?;
            self.mem_pool.deallocate_page(page_guard);
        }
        Ok(())
    }

    /// Lock an in-memory row page for shared access if it is present.
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

    /// Lock a specific row-page version for shared access if it is present.
    #[inline]
    pub(crate) async fn get_row_page_versioned_shared(
        &self,
        guards: &PoolGuards,
        page_id: VersionedPageID,
    ) -> Result<Option<PageSharedGuard<RowPage>>> {
        get_page_versioned_shared::<RowPage, _>(
            self.mem_pool(),
            self.row_pool_guard(guards, "get versioned row page shared")?,
            page_id,
        )
        .await
    }

    /// Roll back one row undo record against hot row state or cold-row hooks.
    #[inline]
    pub(crate) async fn rollback_row_undo<F>(
        &self,
        entry: OwnedRowUndo,
        guards: &PoolGuards,
        on_cold_row_rollback: F,
    ) -> StdResult<(), (Error, OwnedRowUndo)>
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
        let page_guard = match self.get_row_page_versioned_shared(guards, page_id).await {
            Ok(page_guard) => page_guard,
            Err(err) => return Err((err, entry)),
        };
        let Some(page_guard) = page_guard else {
            return Ok(());
        };
        let metadata = self.metadata();
        // TODO: we should retry or wait for notification if rollback happens on a page
        // in transition state. This will be handled in a future task.
        let mut access = page_guard.write_row_by_id(entry.row_id);
        access.rollback_first_undo(metadata, entry);
        Ok(())
    }

    /// Lock an in-memory row page for exclusive access if it is present.
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

    /// Lock an existing in-memory row page for shared access.
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

    /// Lock an existing in-memory row page for exclusive access.
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

    /// Find or allocate a shared insert page with enough row capacity.
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

    /// Find or allocate an exclusive insert page with enough row capacity.
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

    /// Allocate and lock a row page at an exact page id.
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

    /// Cache an exclusive insert page for subsequent inserts.
    #[inline]
    pub(crate) fn cache_exclusive_insert_page(&self, guard: PageExclusiveGuard<RowPage>) {
        self.blk_idx.cache_exclusive_insert_page(guard)
    }

    /// Cache an insert-page version for subsequent inserts.
    #[inline]
    pub(crate) fn cache_insert_page_version(&self, page_id: VersionedPageID) {
        self.blk_idx.cache_insert_page_version(page_id)
    }

    /// Scans in-memory row pages at or above the current table pivot.
    ///
    /// The pivot must be an exact row-page start boundary, unless it equals
    /// the current row-page-index end and there are no pages left to scan.
    pub(crate) async fn scan<F>(&self, guards: &PoolGuards, page_action: F) -> Result<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = self.meta_pool_guard(guards, "mem scan")?;
        let start_row_id = self.pivot_row_id();
        self.scan_from_with_meta_guard(
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
    pub(crate) async fn scan_from<F>(
        &self,
        guards: &PoolGuards,
        start_row_id: RowID,
        page_action: F,
    ) -> Result<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = self.meta_pool_guard(guards, "mem scan from")?;
        self.scan_from_with_meta_guard(
            guards,
            meta_pool_guard,
            start_row_id,
            "mem scan from",
            page_action,
        )
        .await
    }

    /// Snapshot original row-page descriptors at or above an explicit boundary.
    ///
    /// The returned RowID is the exclusive row-page-index upper bound observed
    /// with the descriptor list. The start must be an exact page boundary, with
    /// the current index end accepted as an empty snapshot.
    pub(crate) async fn snapshot_original_row_pages_from(
        &self,
        guards: &PoolGuards,
        start_row_id: RowID,
    ) -> Result<(RowID, Vec<RowPageDescriptor>)> {
        let operation = "snapshot original row pages";
        let meta_pool_guard = self.meta_pool_guard(guards, operation)?;
        let mut cursor = self.blk_idx.mem_cursor(meta_pool_guard);
        cursor.seek(start_row_id).await;
        let mut entries = Vec::new();
        let mut upper_bound = start_row_id;
        let mut first_leaf = true;
        while let Some(leaf) = cursor.next().await {
            let Some(guard) = leaf.lock_shared_async().await else {
                return Err(self.stale_block_index_leaf(operation));
            };
            let page = guard.page();
            debug_assert!(page.is_leaf());
            let leaf_entries = page.leaf_entries();
            let start_idx = if first_leaf {
                first_leaf = false;
                if leaf_entries.is_empty() {
                    if page.header.start_row_id != start_row_id {
                        return Err(invalid_scan_start(self.table_id(), operation, start_row_id));
                    }
                    upper_bound = page.header.end_row_id;
                    continue;
                }
                match leaf_entries.binary_search_by_key(&start_row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(_) if page.header.end_row_id == start_row_id => {
                        upper_bound = start_row_id;
                        continue;
                    }
                    Err(_) => {
                        return Err(invalid_scan_start(self.table_id(), operation, start_row_id));
                    }
                }
            } else {
                0
            };
            entries.extend_from_slice(&leaf_entries[start_idx..]);
            upper_bound = page.header.end_row_id;
        }

        let mut pages = Vec::with_capacity(entries.len());
        for (idx, entry) in entries.iter().enumerate() {
            let end_row_id = entries
                .get(idx + 1)
                .map(|next| next.row_id)
                .unwrap_or(upper_bound);
            if entry.row_id >= end_row_id {
                return Err(Report::new(InternalError::Generic)
                    .attach(format!(
                        "operation={operation}, table_id={}, invalid original row-page range: start_row_id={}, end_row_id={end_row_id}",
                        self.table_id(),
                        entry.row_id
                    ))
                    .into());
            }
            pages.push(RowPageDescriptor {
                page_id: entry.page_id,
                start_row_id: entry.row_id,
                end_row_id,
            });
        }
        Ok((upper_bound, pages))
    }

    async fn scan_from_with_meta_guard<F>(
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
                    return Err(invalid_scan_start(self.table_id(), operation, start_row_id));
                }
                match entries.binary_search_by_key(&start_row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(_) if page.header.end_row_id == start_row_id => return Ok(()),
                    Err(_) => {
                        return Err(invalid_scan_start(self.table_id(), operation, start_row_id));
                    }
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

    /// Find the current hot-row location in the in-memory block index.
    #[inline]
    pub(crate) async fn find_row(&self, guards: &PoolGuards, row_id: RowID) -> Result<RowLocation> {
        let meta_pool_guard = self.meta_pool_guard(guards, "find row")?;
        self.blk_idx.find_mem_row(meta_pool_guard, row_id).await
    }

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
    fn debug_assert_table_write_lock_held(&self, rt: TrxRuntime<'_>) {
        rt.debug_assert_table_write_lock_held(self.table_id());
    }

    #[inline]
    fn push_insert_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_insert_unique_index_undo(self.table_id(), row_id, key, merge_old_deleted);
    }

    #[inline]
    fn push_insert_non_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_insert_non_unique_index_undo(self.table_id(), row_id, key, merge_old_deleted);
    }

    #[inline]
    fn push_delete_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        unique: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_delete_index_undo(self.table_id(), row_id, key, unique);
    }

    #[inline]
    fn push_update_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        key: SelectKey,
        old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
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
                .require_unique_index(guards, key.index_no)?
                .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await?;
            ensure_no_trx_index_insert(key.index_no, res)?;
        } else {
            let res = self
                .require_non_unique_index(guards, key.index_no)?
                .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await?;
            ensure_no_trx_index_insert(key.index_no, res)?;
        }
        Ok(())
    }

    #[inline]
    async fn delete_index_directly(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
    ) -> Result<bool> {
        let spec = self.metadata().idx.require_index_spec(index_no)?;
        if spec.unique() {
            self.require_unique_index(guards, index_no)?
                .compare_delete(key_vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
        } else {
            self.require_non_unique_index(guards, index_no)?
                .compare_delete(key_vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
        }
    }

    #[inline]
    async fn refresh_changed_indexes_no_trx(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
        old_keys: &[SelectKey],
        new_keys: &[SelectKey],
    ) -> Result<()> {
        if old_keys.len() != new_keys.len() {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx index refresh key count mismatch: old={}, new={}",
                old_keys.len(),
                new_keys.len()
            )));
        }
        for (old_key, new_key) in old_keys.iter().zip(new_keys) {
            if old_key.index_no != new_key.index_no {
                return Err(catalog_primary_key_payload_error(format!(
                    "update primary key no-trx index refresh key order mismatch: old_index_no={}, new_index_no={}",
                    old_key.index_no, new_key.index_no
                )));
            }
            if old_key == new_key {
                continue;
            }
            self.insert_index_no_trx(guards, new_key.clone(), row_id)
                .await?;
            if !self
                .delete_index_directly(guards, old_key.index_no, &old_key.vals, row_id)
                .await?
            {
                return Err(catalog_primary_key_payload_error(format!(
                    "update primary key no-trx index refresh missing old key: index_no={}, row_id={row_id}",
                    old_key.index_no
                )));
            }
        }
        Ok(())
    }

    #[inline]
    async fn insert_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationResult<()> {
        if self
            .metadata()
            .idx
            .require_index_spec(key.index_no)
            .change_context(OperationError::IndexMutation)?
            .unique()
        {
            self.insert_unique_index(rt, effects, key, row_id, page_guard)
                .await
        } else {
            self.insert_non_unique_index(rt, effects, key, row_id).await
        }
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut index_branches: Vec<IndexBranch>,
    ) -> Result<(RowID, PageSharedGuard<RowPage>)> {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col.col_count());
        let inserter = RowInserter::new(self.table_id(), metadata, rt);
        loop {
            let page_guard = self
                .try_get_insert_page(rt.pool_guards(), row_count, None)
                .await?;
            match inserter.insert_to_page(effects, page_guard, insert, undo_kind, index_branches) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    self.cache_insert_page_version(page_guard.versioned_page_id());
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
    async fn link_for_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        old_id: RowID,
        index_no: usize,
        key_vals: &[Val],
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> Result<LinkForUniqueIndex> {
        debug_assert!(old_id != new_id);
        let guards = rt.pool_guards();
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
        let old_access = old_guard.read_row_by_id(old_id);
        match old_access.find_old_version_for_unique_key(metadata, index_no, key_vals, rt.ctx()) {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::DuplicateKey => Ok(LinkForUniqueIndex::DuplicateKey),
            FindOldVersion::WriteConflict => Ok(LinkForUniqueIndex::WriteConflict),
            FindOldVersion::Ok(old_row, cts, old_entry) => {
                let mut new_access = new_guard.write_row_by_id(new_id);
                let undo_vals = new_access.row().calc_delta(metadata.col.as_ref(), &old_row);
                new_access.link_for_unique_index(
                    SelectKey::new(index_no, key_vals.to_vec()),
                    cts,
                    old_entry,
                    undo_vals,
                );
                Ok(LinkForUniqueIndex::Linked)
            }
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let index = self
            .require_unique_index(guards, key.index_no)
            .change_context(OperationError::IndexMutation)?;
        loop {
            match index
                .insert_if_not_exists(&key.vals, row_id, false, sts)
                .await
                .change_context(OperationError::IndexMutation)?
            {
                IndexInsert::Ok(merged) => {
                    self.push_insert_unique_index_undo(rt, effects, row_id, key, merged);
                    return Ok(());
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    debug_assert!(old_row_id != row_id);
                    if !deleted {
                        return Err(Report::new(OperationError::DuplicateKey));
                    }
                    match self
                        .link_for_unique_index(
                            rt,
                            old_row_id,
                            key.index_no,
                            &key.vals,
                            row_id,
                            page_guard,
                        )
                        .await
                        .change_context(OperationError::IndexMutation)?
                    {
                        LinkForUniqueIndex::DuplicateKey => {
                            return Err(Report::new(OperationError::DuplicateKey));
                        }
                        LinkForUniqueIndex::WriteConflict => {
                            return Err(Report::new(OperationError::WriteConflict));
                        }
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            let index_old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match index
                                .compare_exchange(&key.vals, index_old_row_id, row_id, sts)
                                .await
                                .change_context(OperationError::IndexMutation)?
                            {
                                IndexCompareExchange::Ok => {
                                    self.push_update_unique_index_undo(
                                        rt, effects, old_row_id, row_id, key, deleted,
                                    );
                                    return Ok(());
                                }
                                IndexCompareExchange::NotExists => {}
                                IndexCompareExchange::Mismatch => {
                                    return Err(Report::new(OperationError::WriteConflict));
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
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
    ) -> OperationResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        match self
            .require_non_unique_index(guards, key.index_no)
            .change_context(OperationError::IndexMutation)?
            .insert_if_not_exists(&key.vals, row_id, false, sts)
            .await
            .change_context(OperationError::IndexMutation)?
        {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(rt, effects, row_id, key, merged);
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn defer_delete_indexes(
        &self,
        rt: TrxRuntime<'_>,
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
        self.defer_delete_index_keys(rt, effects, row_id, keys)
            .await
    }

    #[inline]
    async fn defer_delete_index_keys(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        keys: Vec<SelectKey>,
    ) -> Result<()> {
        for key in keys {
            let spec = self.metadata().idx.require_index_spec(key.index_no)?;
            debug_assert_eq!(self.sec_idx_is_unique(key.index_no), spec.unique());
            if spec.unique() {
                self.defer_delete_unique_index(rt, effects, row_id, key)
                    .await?;
            } else {
                self.defer_delete_non_unique_index(rt, effects, row_id, key)
                    .await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn defer_delete_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
    ) -> Result<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let res = self
            .require_unique_index(guards, key.index_no)?
            .mask_as_deleted(&key.vals, row_id, sts)
            .await?;
        debug_assert!(res);
        self.push_delete_index_undo(rt, effects, row_id, key, true);
        Ok(())
    }

    #[inline]
    async fn defer_delete_non_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
    ) -> Result<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let res = self
            .require_non_unique_index(guards, key.index_no)?
            .mask_as_deleted(&key.vals, row_id, sts)
            .await?;
        debug_assert!(res);
        self.push_delete_index_undo(rt, effects, row_id, key, false);
        Ok(())
    }

    #[inline]
    pub(super) async fn try_get_validated_row_page_shared_result(
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
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        _min_active_sts: TrxID,
    ) -> Result<bool> {
        let index = self.require_unique_index(guards, index_no)?;
        let (page_guard, row_id) = loop {
            let sts = MIN_SNAPSHOT_TS;
            match index.lookup(key_vals, sts).await? {
                None => return Ok(false),
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        return Ok(false);
                    }
                    let Some(page_id) = self.index_purge_decision(guards, row_id).await? else {
                        return index
                            .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
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
        let access = page_guard.read_row_by_id(row_id);
        if !access.any_version_matches_key(self.metadata(), index_no, key_vals) {
            return index
                .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        _min_active_sts: TrxID,
    ) -> Result<bool> {
        let index = self.require_non_unique_index(guards, index_no)?;
        let (page_guard, row_id) = loop {
            let sts = MIN_SNAPSHOT_TS;
            match index.lookup_unique(key_vals, row_id, sts).await? {
                None => return Ok(false),
                Some(active) => {
                    if active {
                        return Ok(false);
                    }
                    let Some(page_id) = self.index_purge_decision(guards, row_id).await? else {
                        return index
                            .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
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
        let access = page_guard.read_row_by_id(row_id);
        if !access.any_version_matches_key(self.metadata(), index_no, key_vals) {
            return index
                .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    /// Insert one catalog row without transactional undo/redo state.
    #[inline]
    pub(crate) async fn insert_no_trx(
        &self,
        guards: &PoolGuards,
        cols: &[Val],
        disable_dml_validation: bool,
    ) -> Result<()> {
        self.insert_no_trx_location(guards, cols, disable_dml_validation)
            .await?;
        Ok(())
    }

    #[inline]
    async fn insert_no_trx_location(
        &self,
        guards: &PoolGuards,
        cols: &[Val],
        disable_dml_validation: bool,
    ) -> Result<(PageID, RowID)> {
        let metadata = self.metadata();
        if !disable_dml_validation {
            DmlValidator::new(
                metadata,
                self.table_id(),
                "insert_no_trx",
                DmlValidationDomain::Recovery,
            )
            .validate_full_row(cols)?;
        }
        debug_assert!(cols.len() == self.metadata().col.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col.col_type_match(idx, val))
        });
        let keys = metadata.idx.keys_for_insert(cols);
        let row_len = row_len(metadata, cols);
        let row_count = estimate_max_row_count(row_len, metadata.col.col_count());
        loop {
            let mut page_guard = self
                .get_insert_page_exclusive(guards, row_count, None)
                .await?;
            let page_id = page_guard.page_id();
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
            let row_id = page.header.start_row_id + row_idx as u64;
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
            return Ok((page_id, row_id));
        }
    }

    /// Upsert one catalog row by primary key without transaction state.
    ///
    /// The callback is the final infallible step after a successful mutation.
    /// It is not invoked when the current logical row already equals `cols` or
    /// when validation/access/mutation fails.
    #[inline]
    pub(crate) async fn upsert_primary_key_no_trx<F>(
        &self,
        guards: &PoolGuards,
        cols: Vec<Val>,
        disable_dml_validation: bool,
        on_change: F,
    ) -> Result<()>
    where
        F: FnOnce(NoTrxUpsertChange),
    {
        let metadata = self.metadata();
        let primary_key = metadata.primary_key().ok_or_else(|| {
            Error::from(
                Report::new(InternalError::CatalogPrimaryKeyMissing).attach(format!(
                    "catalog primary-key no-trx upsert requires primary key: table_id={}",
                    self.table_id()
                )),
            )
        })?;
        let primary_key_index_no = primary_key.index_no();
        if !disable_dml_validation {
            let validator = DmlValidator::new(
                metadata,
                self.table_id(),
                "upsert_primary_key_no_trx",
                DmlValidationDomain::Recovery,
            );
            validator.validate_full_row(&cols)?;
            validator.validate_unique_index(primary_key_index_no)?;
        }
        let key = unique_key_from_full_row(
            metadata,
            primary_key_index_no,
            &cols,
            "upsert primary key no-trx",
        )?;
        let current = self
            .index_lookup_unique_uncommitted(guards, key.index_no, &key.vals, |layout, row| {
                row.clone_vals(layout)
            })
            .await?;
        let Some(current) = current else {
            let (page_id, row_id) = self
                .insert_no_trx_location(guards, &cols, disable_dml_validation)
                .await?;
            on_change(NoTrxUpsertChange::Inserted {
                page_id,
                row_id,
                vals: cols,
            });
            return Ok(());
        };
        let update: Vec<UpdateCol> = current
            .iter()
            .zip(&cols)
            .enumerate()
            .filter(|(_, (old, new))| old != new)
            .map(|(idx, (_, val))| UpdateCol {
                idx,
                val: val.clone(),
            })
            .collect();
        if update.is_empty() {
            return Ok(());
        }
        let (page_id, row_id) = self
            .update_primary_key_no_trx_location(
                guards,
                key.index_no,
                &key.vals,
                &update,
                disable_dml_validation,
            )
            .await?;
        on_change(NoTrxUpsertChange::Updated {
            page_id,
            row_id,
            key,
            cols: update,
        });
        Ok(())
    }

    /// Delete one catalog row through its primary key without transaction state.
    #[inline]
    pub(crate) async fn delete_primary_key_no_trx(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        disable_dml_validation: bool,
    ) -> Result<()> {
        let metadata = self.metadata();
        let index_spec = if disable_dml_validation {
            metadata.idx.require_index_spec(index_no)?
        } else {
            validate_primary_key_no_trx_key(
                metadata,
                index_no,
                key_vals,
                "delete primary key no-trx",
            )?
        };
        let index = self.require_unique_index(guards, index_no)?;
        let sts = MIN_SNAPSHOT_TS;
        let (mut page_guard, row_id) = match index.lookup(key_vals, sts).await? {
            None => {
                return Err(catalog_primary_key_payload_error(format!(
                    "delete primary key no-trx missing catalog row: index_no={}, key_vals={:?}",
                    index_no, key_vals
                )));
            }
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => {
                    return Err(catalog_primary_key_payload_error(format!(
                        "delete primary key no-trx row location missing: row_id={row_id}"
                    )));
                }
                RowLocation::LwcBlock { .. } => {
                    return self.catalog_lwc_error("delete primary key no-trx", row_id);
                }
                RowLocation::RowPage(page_id) => {
                    let page_guard = self.must_get_row_page_exclusive(guards, page_id).await?;
                    (page_guard, row_id)
                }
            },
        };
        let page = page_guard.page_mut();
        let row_idx = page.row_idx(row_id);
        if page.is_deleted(row_idx) {
            return Err(catalog_primary_key_payload_error(format!(
                "delete primary key no-trx row is deleted: row_id={row_id}"
            )));
        }
        let row = page.row(row_idx);
        if row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
            return Err(catalog_primary_key_payload_error(format!(
                "delete primary key no-trx row key mismatch: row_id={row_id}, index_no={}",
                index_no
            )));
        }
        let keys = self
            .metadata()
            .idx
            .keys_for_delete(self.metadata().col.as_ref(), row);
        for key in keys {
            let res = self
                .delete_index_directly(guards, key.index_no, &key.vals, row_id)
                .await?;
            assert!(res);
        }
        page.set_deleted_exclusive(row_idx, true);
        Ok(())
    }

    /// Update one catalog row through its primary key without transaction state.
    ///
    /// This is catalog logical recovery, not physical row-page recovery. The
    /// input key identifies the catalog row, but the recovered row may live at
    /// any row id. Fixed-size updates, such as silent-watermark replay, usually
    /// fit in place. Variable-length non-indexed updates can outgrow the current
    /// row page; in that case this helper relocates the row with delete+insert.
    ///
    /// This helper does not provide local undo. Recovery callers propagate any
    /// error and abort engine startup before the partially rebuilt in-memory
    /// catalog can be exposed.
    #[inline]
    pub(crate) async fn update_primary_key_no_trx(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        update: &[UpdateCol],
        disable_dml_validation: bool,
    ) -> Result<()> {
        self.update_primary_key_no_trx_location(
            guards,
            index_no,
            key_vals,
            update,
            disable_dml_validation,
        )
        .await?;
        Ok(())
    }

    #[inline]
    async fn update_primary_key_no_trx_location(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        update: &[UpdateCol],
        disable_dml_validation: bool,
    ) -> Result<(PageID, RowID)> {
        let metadata = self.metadata();
        let index_spec = if disable_dml_validation {
            metadata.idx.require_index_spec(index_no)?
        } else {
            validate_primary_key_no_trx_key(
                metadata,
                index_no,
                key_vals,
                "update primary key no-trx",
            )?
        };
        // Validation opt-out is an unchecked/prevalidated recovery path. When
        // validation is enabled, keep primary-key column changes rejected
        // because this helper addresses rows by primary key.
        if !disable_dml_validation {
            validate_update_primary_key_no_trx_cols(metadata, update)?;
        }

        let index = self.require_unique_index(guards, index_no)?;
        let sts = MIN_SNAPSHOT_TS;
        let (mut page_guard, row_id) = match index.lookup(key_vals, sts).await? {
            None => {
                return Err(catalog_primary_key_payload_error(format!(
                    "update primary key no-trx missing catalog row: index_no={}, key_vals={:?}",
                    index_no, key_vals
                )));
            }
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => {
                    return Err(catalog_primary_key_payload_error(format!(
                        "update primary key no-trx row location missing: row_id={row_id}"
                    )));
                }
                RowLocation::LwcBlock { .. } => {
                    return self.catalog_lwc_error("update primary key no-trx", row_id);
                }
                RowLocation::RowPage(page_id) => {
                    let page_guard = self.must_get_row_page_exclusive(guards, page_id).await?;
                    (page_guard, row_id)
                }
            },
        };
        let page_id = page_guard.page_id();
        let page = page_guard.page_mut();
        if !page.row_id_in_valid_range(row_id) {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx row id out of page range: row_id={row_id}"
            )));
        }
        let row_idx = page.row_idx(row_id);
        let row = page.row(row_idx);
        if row.is_deleted() {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx row is deleted: row_id={row_id}"
            )));
        }
        if row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx row key mismatch: row_id={row_id}, index_no={}",
                index_no
            )));
        }
        let var_len = page.var_len_for_update(row_idx, RowUpdateView::Sparse(update));
        match page.request_free_space(var_len) {
            Some(var_offset) => {
                let index_refresh =
                    prepare_update_primary_key_no_trx_index_refresh(metadata, row.clone(), update);
                {
                    let mut row = page.row_mut_exclusive(row_idx, var_offset, var_offset + var_len);
                    for update_col in update {
                        row.update_col(
                            metadata.col.as_ref(),
                            update_col.idx,
                            &update_col.val,
                            true,
                        );
                    }
                    row.finish_update();
                }
                if let Some(index_refresh) = index_refresh {
                    self.refresh_changed_indexes_no_trx(
                        guards,
                        row_id,
                        &index_refresh.old_keys,
                        &index_refresh.new_keys,
                    )
                    .await?;
                }
                Ok((page_id, row_id))
            }
            None => {
                // Catalog redo is logical by primary key. When variable-length
                // values do not fit the current in-memory row page, rebuild the
                // row at a new row id and refresh indexes through delete+insert.
                // If the replacement insert fails after the delete, recovery
                // fails immediately; the partially rebuilt engine is discarded
                // instead of trying to undo no-trx state.
                let mut row_vals = row.clone_vals(metadata.col.as_ref());
                for update_col in update {
                    row_vals[update_col.idx] = update_col.val.clone();
                }
                drop(page_guard);
                self.delete_primary_key_no_trx(guards, index_no, key_vals, disable_dml_validation)
                    .await?;
                self.insert_no_trx_location(guards, &row_vals, disable_dml_validation)
                    .await
            }
        }
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
        self.scan(guards, |page_guard| {
            let col_layout = page_guard.unwrap_vmap().column_layout.as_ref();
            for row_access in page_guard.read_all_rows() {
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
        index_no: usize,
        key_vals: &[Val],
        row_action: F,
    ) -> Result<Option<R>>
    where
        for<'m, 'p> F: FnOnce(&'m TableColumnLayout, Row<'p>) -> R,
    {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        let index = self.require_unique_index(guards, index_no)?;
        let sts = MIN_SNAPSHOT_TS;
        let (page_guard, row_id) = match index.lookup(key_vals, sts).await? {
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
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return Ok(None);
        }
        let row_layout = page_guard.unwrap_vmap().column_layout.as_ref();
        let access = page_guard.read_row_by_id(row_id);
        let row = access.row();
        if row.is_deleted() {
            return Ok(None);
        }
        let metadata = self.metadata();
        let Some(index_spec) = metadata.idx.index_spec(index_no) else {
            return Ok(None);
        };
        if row.is_key_different(row_layout, index_spec, key_vals) {
            return Ok(None);
        }
        Ok(Some(row_action(row_layout, row)))
    }

    /// Insert row in transaction.
    #[inline]
    pub(crate) async fn insert_mvcc(
        &self,
        rt: TrxRuntime<'_>,
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
            .insert_row_internal(rt, effects, cols, RowUndoKind::Insert, Vec::new())
            .await?;
        for key in keys {
            self.insert_index(rt, effects, key, row_id, &page_guard)
                .await
                .attach("catalog insert MVCC secondary index claim")?;
        }
        Ok(row_id)
    }

    /// Insert or replace one MVCC row selected by a unique key derived from the row.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reserved for future memory-only user tables")
    )]
    #[inline]
    pub(crate) async fn upsert_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        unique_index_no: usize,
        cols: Vec<Val>,
        log_by_key: bool,
    ) -> Result<UpsertMvcc> {
        let key = unique_key_from_full_row(
            self.metadata(),
            unique_index_no,
            &cols,
            "upsert unique MVCC",
        )?;
        let input = RowUpdateInput::FullRow(cols);
        match self
            .update_unique_mvcc_input(rt, effects, key.index_no, &key.vals, input, log_by_key)
            .await?
        {
            UpdateUniqueMvcc::Updated(row_id) => Ok(UpsertMvcc::Updated(row_id)),
            UpdateUniqueMvcc::NotFound(input) => {
                let cols = input
                    .into_full_row()
                    .expect("upsert update input must preserve the full row");
                self.insert_mvcc(rt, effects, cols)
                    .await
                    .map(UpsertMvcc::Inserted)
            }
        }
    }

    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reserved for future memory-only user tables")
    )]
    async fn update_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
        log_by_key: bool,
    ) -> Result<UpdateMvcc> {
        let input = RowUpdateInput::Sparse(update);
        match self
            .update_unique_mvcc_input(rt, effects, index_no, key_vals, input, log_by_key)
            .await?
        {
            UpdateUniqueMvcc::Updated(row_id) => Ok(UpdateMvcc::Updated(row_id)),
            UpdateUniqueMvcc::NotFound(_) => Ok(UpdateMvcc::NotFound),
        }
    }

    #[inline]
    async fn update_unique_mvcc_input(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        input: RowUpdateInput,
        log_by_key: bool,
    ) -> Result<UpdateUniqueMvcc> {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        debug_assert!(
            input.as_view().is_valid_for(self.metadata().col.as_ref()),
            "row update values must be ordered, in range, and type-compatible"
        );
        let guards = rt.pool_guards();
        let index = self.require_unique_index(guards, index_no)?;
        loop {
            let lookup_sts = rt.sts();
            let (page_guard, row_id) = match index.lookup(key_vals, lookup_sts).await? {
                None => return Ok(UpdateUniqueMvcc::NotFound(input)),
                Some((row_id, _)) => match self.find_row(guards, row_id).await {
                    Ok(RowLocation::NotFound) => {
                        return Ok(UpdateUniqueMvcc::NotFound(input));
                    }
                    Ok(RowLocation::LwcBlock { .. }) => {
                        return self.catalog_lwc_error("update unique MVCC", row_id);
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
            let res = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
                .update_inplace(effects, index_no, key_vals, input, log_by_key)
                .await?;
            match res {
                UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                    debug_assert!(row_id == new_row_id);
                    if !index_change_cols.is_empty() {
                        let result = self
                            .update_indexes_only_key_change(
                                rt,
                                effects,
                                row_id,
                                &page_guard,
                                &index_change_cols,
                            )
                            .await;
                        result.attach("update MVCC key-change index update")?;
                        return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                    }
                    return Ok(UpdateUniqueMvcc::Updated(row_id));
                }
                UpdateRowInplace::RowDeleted(input) | UpdateRowInplace::RowNotFound(input) => {
                    return Ok(UpdateUniqueMvcc::NotFound(input));
                }
                UpdateRowInplace::RetryInTransition(returned_input) => {
                    let _ = returned_input;
                    // Standalone/catalog MemTable owns hot row-store state
                    // only. Without user-table column storage and checkpoint
                    // route publication, TRANSITION is not a valid state here.
                    return Err(Report::new(InternalError::Generic)
                        .attach("standalone MemTable update observed TRANSITION row page")
                        .into());
                }
                UpdateRowInplace::NoFreeSpaceOrFrozen(old_row_id, old_row, returned_input) => {
                    let (new_row_id, index_change_cols, new_guard) = self
                        .move_update_for_space(
                            rt,
                            effects,
                            old_row,
                            returned_input,
                            old_row_id,
                            page_guard,
                        )
                        .await?;
                    if !index_change_cols.is_empty() {
                        let result = self
                            .update_indexes_may_both_change(
                                rt,
                                effects,
                                old_row_id,
                                new_row_id,
                                &index_change_cols,
                                &new_guard,
                            )
                            .await;
                        result.attach("update MVCC moved-row index update")?;
                        return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                    }
                    let result = self
                        .update_indexes_only_row_id_change(
                            rt, effects, old_row_id, new_row_id, &new_guard,
                        )
                        .await;
                    result.attach("update MVCC moved-row index update")?;
                    return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                }
            }
        }
    }

    #[inline]
    async fn move_update_for_space(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row: Vec<Val>,
        update: RowUpdateInput,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> Result<(RowID, FastHashMap<usize, Val>, PageSharedGuard<RowPage>)> {
        let prepared = HotRowMutator::new(self.table_id(), self.metadata(), rt, &old_guard, old_id)
            .prepare_move_update(old_row, update);
        // Release the old row page before awaiting replacement-row insertion.
        drop(old_guard);
        let (new_row_id, new_guard) = self
            .insert_row_internal(
                rt,
                effects,
                prepared.row,
                RowUndoKind::Insert,
                prepared.index_branches,
            )
            .await?;
        Ok((new_row_id, prepared.index_change_cols, new_guard))
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &FastHashMap<usize, Val>,
    ) -> OperationResult<()> {
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            if index_key_is_changed(index_schema, index_change_cols) {
                let new_key = read_latest_index_key(metadata, index_no, page_guard, row_id);
                let old_key = index_key_replace(index_schema, &new_key, index_change_cols);
                if index_schema.unique() {
                    self.update_unique_index_only_key_change(
                        rt, effects, old_key, new_key, row_id, page_guard,
                    )
                    .await?;
                } else {
                    self.update_non_unique_index_only_key_change(
                        rt, effects, old_key, new_key, row_id,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn update_indexes_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            let key = read_latest_index_key(metadata, index_no, page_guard, new_row_id);
            if index_schema.unique() {
                self.update_unique_index_only_row_id_change(
                    rt, effects, key, old_row_id, new_row_id,
                )
                .await?;
            } else {
                self.update_non_unique_index_only_row_id_change(
                    rt, effects, key, old_row_id, new_row_id,
                )
                .await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        index_change_cols: &FastHashMap<usize, Val>,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            let key = read_latest_index_key(metadata, index_no, page_guard, new_row_id);
            if index_key_is_changed(index_schema, index_change_cols) {
                let old_key = index_key_replace(index_schema, &key, index_change_cols);
                if index_schema.unique() {
                    self.update_unique_index_key_and_row_id_change(
                        rt, effects, old_key, key, old_row_id, new_row_id, page_guard,
                    )
                    .await?;
                } else {
                    self.update_non_unique_index_key_and_row_id_change(
                        rt, effects, old_key, key, old_row_id, new_row_id,
                    )
                    .await?;
                }
            } else if index_schema.unique() {
                self.update_unique_index_only_row_id_change(
                    rt, effects, key, old_row_id, new_row_id,
                )
                .await?;
            } else {
                self.update_non_unique_index_only_row_id_change(
                    rt, effects, key, old_row_id, new_row_id,
                )
                .await?;
            }
        }
        Ok(())
    }

    #[inline]
    #[expect(clippy::too_many_arguments, reason = "code style")]
    async fn update_unique_index_key_and_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> OperationResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let index = self
            .require_unique_index(guards, new_key.index_no)
            .change_context(OperationError::IndexMutation)?;
        loop {
            match index
                .insert_if_not_exists(&new_key.vals, new_row_id, false, sts)
                .await
                .change_context(OperationError::IndexMutation)?
            {
                IndexInsert::Ok(merged) => {
                    debug_assert!(!merged);
                    self.push_insert_unique_index_undo(rt, effects, new_row_id, new_key, false);
                    self.defer_delete_unique_index(rt, effects, old_row_id, old_key)
                        .await
                        .change_context(OperationError::IndexMutation)?;
                    return Ok(());
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    debug_assert!(index_row_id != new_row_id);
                    if !deleted {
                        return Err(Report::new(OperationError::DuplicateKey));
                    }
                    if index_row_id == old_row_id {
                        match index
                            .compare_exchange(&new_key.vals, old_row_id.deleted(), new_row_id, sts)
                            .await
                            .change_context(OperationError::IndexMutation)?
                        {
                            IndexCompareExchange::Ok => {
                                self.push_update_unique_index_undo(
                                    rt, effects, old_row_id, new_row_id, new_key, true,
                                );
                                self.defer_delete_unique_index(rt, effects, old_row_id, old_key)
                                    .await
                                    .change_context(OperationError::IndexMutation)?;
                                return Ok(());
                            }
                            IndexCompareExchange::Mismatch => unreachable!(),
                            IndexCompareExchange::NotExists => continue,
                        }
                    }
                    match self
                        .link_for_unique_index(
                            rt,
                            index_row_id,
                            new_key.index_no,
                            &new_key.vals,
                            new_row_id,
                            new_guard,
                        )
                        .await
                        .change_context(OperationError::IndexMutation)?
                    {
                        LinkForUniqueIndex::DuplicateKey => {
                            return Err(Report::new(OperationError::DuplicateKey));
                        }
                        LinkForUniqueIndex::WriteConflict => {
                            return Err(Report::new(OperationError::WriteConflict));
                        }
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            let index_old_row_id = index_row_id.deleted();
                            match index
                                .compare_exchange(&new_key.vals, index_old_row_id, new_row_id, sts)
                                .await
                                .change_context(OperationError::IndexMutation)?
                            {
                                IndexCompareExchange::Ok => {
                                    self.push_update_unique_index_undo(
                                        rt,
                                        effects,
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        true,
                                    );
                                    self.defer_delete_unique_index(
                                        rt, effects, old_row_id, old_key,
                                    )
                                    .await
                                    .change_context(OperationError::IndexMutation)?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Err(Report::new(OperationError::WriteConflict));
                                }
                                IndexCompareExchange::NotExists => {}
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_key_and_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> OperationResult<()> {
        debug_assert!(old_row_id != new_row_id);
        match self
            .require_non_unique_index(rt.pool_guards(), new_key.index_no)
            .change_context(OperationError::IndexMutation)?
            .insert_if_not_exists(&new_key.vals, new_row_id, false, rt.sts())
            .await
            .change_context(OperationError::IndexMutation)?
        {
            IndexInsert::Ok(merged) => {
                debug_assert!(!merged);
                self.push_insert_non_unique_index_undo(rt, effects, new_row_id, new_key, false);
                self.defer_delete_non_unique_index(rt, effects, old_row_id, old_key)
                    .await
                    .change_context(OperationError::IndexMutation)?;
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn update_unique_index_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> OperationResult<()> {
        debug_assert!(old_row_id != new_row_id);
        match self
            .require_unique_index(rt.pool_guards(), key.index_no)
            .change_context(OperationError::IndexMutation)?
            .compare_exchange(&key.vals, old_row_id, new_row_id, rt.sts())
            .await
            .change_context(OperationError::IndexMutation)?
        {
            IndexCompareExchange::Ok => {
                self.push_update_unique_index_undo(rt, effects, old_row_id, new_row_id, key, false);
                Ok(())
            }
            IndexCompareExchange::Mismatch | IndexCompareExchange::NotExists => unreachable!(),
        }
    }

    #[inline]
    async fn update_non_unique_index_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> OperationResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let res = self
            .require_non_unique_index(rt.pool_guards(), key.index_no)
            .change_context(OperationError::IndexMutation)?
            .insert_if_not_exists(&key.vals, new_row_id, false, rt.sts())
            .await
            .change_context(OperationError::IndexMutation)?;
        debug_assert!(res.is_ok());
        self.push_insert_non_unique_index_undo(rt, effects, new_row_id, key.clone(), false);
        self.defer_delete_non_unique_index(rt, effects, old_row_id, key)
            .await
            .change_context(OperationError::IndexMutation)?;
        Ok(())
    }

    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let index = self
            .require_unique_index(guards, new_key.index_no)
            .change_context(OperationError::IndexMutation)?;
        loop {
            match index
                .insert_if_not_exists(&new_key.vals, row_id, true, sts)
                .await
                .change_context(OperationError::IndexMutation)?
            {
                IndexInsert::Ok(merged) => {
                    self.push_insert_unique_index_undo(rt, effects, row_id, new_key, merged);
                    self.defer_delete_unique_index(rt, effects, row_id, old_key)
                        .await
                        .change_context(OperationError::IndexMutation)?;
                    return Ok(());
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    if !deleted {
                        return Err(Report::new(OperationError::DuplicateKey));
                    }
                    match self
                        .link_for_unique_index(
                            rt,
                            index_row_id,
                            new_key.index_no,
                            &new_key.vals,
                            row_id,
                            page_guard,
                        )
                        .await
                        .change_context(OperationError::IndexMutation)?
                    {
                        LinkForUniqueIndex::DuplicateKey => {
                            return Err(Report::new(OperationError::DuplicateKey));
                        }
                        LinkForUniqueIndex::WriteConflict => {
                            return Err(Report::new(OperationError::WriteConflict));
                        }
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            match index
                                .compare_exchange(
                                    &new_key.vals,
                                    index_row_id.deleted(),
                                    row_id,
                                    sts,
                                )
                                .await
                                .change_context(OperationError::IndexMutation)?
                            {
                                IndexCompareExchange::Ok => {
                                    self.push_update_unique_index_undo(
                                        rt,
                                        effects,
                                        index_row_id,
                                        row_id,
                                        new_key,
                                        true,
                                    );
                                    self.defer_delete_unique_index(rt, effects, row_id, old_key)
                                        .await
                                        .change_context(OperationError::IndexMutation)?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Err(Report::new(OperationError::WriteConflict));
                                }
                                IndexCompareExchange::NotExists => {}
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
    ) -> OperationResult<()> {
        match self
            .require_non_unique_index(rt.pool_guards(), new_key.index_no)
            .change_context(OperationError::IndexMutation)?
            .insert_if_not_exists(&new_key.vals, row_id, true, rt.sts())
            .await
            .change_context(OperationError::IndexMutation)?
        {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(rt, effects, row_id, new_key, merged);
                self.defer_delete_non_unique_index(rt, effects, row_id, old_key)
                    .await
                    .change_context(OperationError::IndexMutation)?;
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    /// Delete row in transaction by unique index lookup.
    ///
    /// If `log_by_key` is true, redo logs the unique key instead of row id.
    /// Catalog callers use this because catalog row locations may differ across
    /// restart/recovery cycles.
    #[inline]
    pub(crate) async fn delete_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        let guards = rt.pool_guards();
        let index = self.require_unique_index(guards, index_no)?;
        loop {
            let lookup_sts = rt.sts();
            let (page_guard, row_id) = match index.lookup(key_vals, lookup_sts).await? {
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
            let res = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
                .delete(effects, index_no, key_vals, log_by_key)
                .await?;
            match res {
                DeleteInternal::NotFound => return Ok(DeleteMvcc::NotFound),
                DeleteInternal::RetryInTransition => {
                    // Standalone/catalog MemTable owns hot row-store state
                    // only. Without user-table column storage and checkpoint
                    // route publication, TRANSITION is not a valid state here.
                    unreachable!("standalone MemTable delete observed TRANSITION row page");
                }
                DeleteInternal::Ok => {
                    self.defer_delete_indexes(rt, effects, row_id, &page_guard)
                        .await?;
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
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let Some(index_schema) = self.metadata().idx.index_spec(index_no) else {
            return Ok(false);
        };
        if !self.sec_idx_is_active(index_no) {
            return Ok(false);
        }
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            self.delete_unique_index(guards, index_no, key_vals, row_id, min_active_sts)
                .await
        } else {
            self.delete_non_unique_index(guards, index_no, key_vals, row_id, min_active_sts)
                .await
        }
    }
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
        for index in take(&mut self.staged).into_iter().rev().flatten() {
            // Keep the original construction error as the function result.
            let _ = index.destroy(pool_guard).await;
        }
    }

    #[inline]
    fn publish(self) -> Box<[Option<InMemorySecondaryIndex<P>>]> {
        self.staged.into_boxed_slice()
    }
}

/// Build in-memory secondary indexes for every active index in table metadata.
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

#[inline]
fn prepare_update_primary_key_no_trx_index_refresh(
    metadata: &TableMetadata,
    row: Row<'_>,
    update: &[UpdateCol],
) -> Option<NoTrxIndexRefresh> {
    if metadata.idx.active_index_count() <= 1 {
        return None;
    }

    let mut updated_index_vals = FastHashMap::default();
    for update_col in update {
        if metadata.idx.index_columns().contains(&update_col.idx) {
            updated_index_vals.insert(update_col.idx, update_col.val.clone());
        }
    }
    if updated_index_vals.is_empty() {
        return None;
    }

    let mut old_keys = Vec::new();
    let mut new_keys = Vec::new();
    for (index_no, index_spec) in metadata.idx.active_indexes() {
        if !index_key_is_changed(index_spec, &updated_index_vals) {
            continue;
        }
        let old_key_vals = index_spec
            .cols
            .iter()
            .map(|key| row.val(metadata.col.as_ref(), key.col_no as usize))
            .collect();
        let old_key = SelectKey::new(index_no, old_key_vals);
        let new_key = index_key_replace(index_spec, &old_key, &updated_index_vals);
        if old_key != new_key {
            old_keys.push(old_key);
            new_keys.push(new_key);
        }
    }
    if old_keys.is_empty() {
        return None;
    }
    Some(NoTrxIndexRefresh { old_keys, new_keys })
}

#[inline]
fn ensure_no_trx_index_insert(index_no: usize, res: IndexInsert) -> Result<()> {
    match res {
        IndexInsert::Ok(_) => Ok(()),
        IndexInsert::DuplicateKey(row_id, deleted) => Err(Report::new(
            DataIntegrityError::UnexpectedRecoveryDuplicateKey,
        )
        .attach(RecoveryDuplicateKey {
            index_no,
            row_id,
            deleted,
        })
        .into()),
    }
}

#[inline]
fn validate_update_primary_key_no_trx_cols(
    metadata: &TableMetadata,
    update: &[UpdateCol],
) -> Result<()> {
    let mut last_idx = None;
    for update_col in update {
        if update_col.idx >= metadata.col.col_count() {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx column out of range: column_no={}, column_count={}",
                update_col.idx,
                metadata.col.col_count()
            )));
        }
        if last_idx.is_some_and(|idx| update_col.idx <= idx) {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx columns not strictly ordered: column_no={}",
                update_col.idx
            )));
        }
        if !metadata.col.col_type_match(update_col.idx, &update_col.val) {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx column type mismatch: column_no={}",
                update_col.idx
            )));
        }
        last_idx = Some(update_col.idx);
    }
    validate_update_primary_key_no_trx_primary_key_cols(metadata, update)?;
    Ok(())
}

#[inline]
fn validate_update_primary_key_no_trx_primary_key_cols(
    metadata: &TableMetadata,
    update: &[UpdateCol],
) -> Result<()> {
    let Some(primary_key) = metadata.primary_key() else {
        return Ok(());
    };
    for update_col in update {
        if primary_key
            .spec()
            .cols
            .iter()
            .any(|key| usize::from(key.col_no) == update_col.idx)
        {
            return Err(catalog_primary_key_payload_error(format!(
                "update primary key no-trx cannot change primary key column: column_no={}",
                update_col.idx
            )));
        }
    }
    Ok(())
}

#[inline]
fn validate_primary_key_no_trx_key<'a>(
    metadata: &'a TableMetadata,
    index_no: usize,
    key_vals: &[Val],
    operation: &'static str,
) -> Result<&'a IndexSpec> {
    let Some(primary_key) = metadata.primary_key() else {
        return Err(catalog_primary_key_payload_error(format!(
            "{operation} primary key not found"
        )));
    };
    match primary_key.validate_key(index_no, key_vals) {
        Ok(()) => Ok(primary_key.spec()),
        Err(PrimaryKeyMatchError::IndexNo { actual, expected }) => {
            Err(catalog_primary_key_payload_error(format!(
                "{operation} key is not primary key: index_no={actual}, primary_key_index_no={expected}",
            )))
        }
        Err(PrimaryKeyMatchError::ValueCount { actual, expected }) => {
            Err(catalog_primary_key_payload_error(format!(
                "{operation} key value count {actual} does not match primary key column count {expected}",
            )))
        }
        Err(PrimaryKeyMatchError::Type { index_no }) => Err(catalog_primary_key_payload_error(
            format!("{operation} key type mismatch: index_no={index_no}"),
        )),
    }
}

#[inline]
fn catalog_primary_key_payload_error(message: impl Into<String>) -> Error {
    Report::new(DataIntegrityError::InvalidPayload)
        .attach(message.into())
        .into()
}

#[inline]
fn invalid_scan_start(table_id: TableID, operation: &'static str, start_row_id: RowID) -> Error {
    Report::new(InternalError::Generic)
        .attach(format!(
            "operation={operation}, table_id={table_id}, row-page scan start is not a row-page boundary: start_row_id={start_row_id}"
        ))
        .into()
}

#[cfg(test)]
mod tests {
    use super::{MemTable, NoTrxUpsertChange};
    use crate::buffer::guard::PageGuard;
    use crate::buffer::{BufferPool, EvictableBufferPool};
    use crate::buffer::{PoolGuards, PoolRole};
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
    };
    use crate::engine::Engine;
    use crate::error::{
        DataIntegrityError, ErrorKind, InternalError, OperationError, ResourceError,
    };
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::id::{RowID, TableID, TrxID};
    use crate::index::{BlockIndex, NonUniqueIndex, RowLocation, UniqueIndex};
    use crate::row::RowRead;
    use crate::row::ops::{DeleteMvcc, SelectKey, UpdateCol, UpdateMvcc, UpsertMvcc};
    use crate::session::{
        Session,
        tests::{SessionTestExt, assert_checkpoint_published},
    };
    use crate::table::tests::*;
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::ver_map::RowPageState;
    use crate::value::{Val, ValKind};
    use std::sync::Arc;
    use tempfile::TempDir;

    type TestMemTable = MemTable<EvictableBufferPool, EvictableBufferPool>;

    fn indexed_payload_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
                ],
                vec![
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                ],
            )
            .expect("valid indexed payload metadata"),
        )
    }

    fn unique_name_payload_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
                ],
                vec![
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK),
                ],
            )
            .expect("valid unique name payload metadata"),
        )
    }

    fn primary_key_payload_metadata() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![
                    ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                    ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
                ],
                vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK)],
            )
            .expect("valid primary-key payload metadata"),
        )
    }

    async fn test_mem_table(engine: &Engine, mem_table_id: TableID) -> TestMemTable {
        let source_table_id = create_table2_for_test(engine).await;
        let metadata = table_for_internal_assertion(engine, source_table_id).metadata();
        test_mem_table_with_metadata(engine, mem_table_id, metadata).await
    }

    async fn test_mem_table_with_metadata(
        engine: &Engine,
        mem_table_id: TableID,
        metadata: Arc<TableMetadata>,
    ) -> TestMemTable {
        let meta_guard = engine.inner().meta_pool.pool_guard();
        let index_guard = engine.inner().index_pool.pool_guard();
        let mem_pool = engine.inner().mem_pool.clone_inner();
        let blk_idx = BlockIndex::new(
            engine.inner().meta_pool.clone_inner(),
            &meta_guard,
            RowID::new(0),
            SUPER_BLOCK_ID,
        )
        .await
        .unwrap();
        MemTable::new(
            mem_pool.clone(),
            mem_pool.row_pool_role(),
            engine.inner().index_pool.clone_inner(),
            PoolRole::Index,
            &index_guard,
            mem_table_id,
            metadata,
            blk_idx,
            MIN_SNAPSHOT_TS,
        )
        .await
        .unwrap()
    }

    fn name_key(value: &str) -> SelectKey {
        SelectKey {
            index_no: 1,
            vals: vec![Val::from(value)],
        }
    }

    fn indexed_payload_row(id: i32, name: &str, payload: &[u8]) -> Vec<Val> {
        vec![Val::from(id), Val::from(name), Val::from(payload)]
    }

    async fn insert_mem_mvcc(
        session: &mut Session,
        table_id: TableID,
        mem_table: &TestMemTable,
        cols: Vec<Val>,
    ) -> RowID {
        let mut trx = session.begin_trx().unwrap();
        let row_id = trx
            .exec(async |stmt| {
                stmt.acquire_table_write_metadata_lock(table_id).await?;
                stmt.acquire_table_write_data_lock(table_id).await?;
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                mem_table.insert_mvcc(rt, effects, cols).await
            })
            .await
            .unwrap();
        trx.commit().await.unwrap();
        row_id
    }

    async fn update_mem_unique_mvcc(
        session: &mut Session,
        table_id: TableID,
        mem_table: &TestMemTable,
        key: SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        let mut trx = session.begin_trx().unwrap();
        let updated = trx
            .exec(async |stmt| {
                stmt.acquire_table_write_metadata_lock(table_id).await?;
                stmt.acquire_table_write_data_lock(table_id).await?;
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                mem_table
                    .update_unique_mvcc(rt, effects, key.index_no, &key.vals, update, false)
                    .await
            })
            .await
            .unwrap();
        trx.commit().await.unwrap();
        updated
    }

    async fn assert_unique_index_entry(
        mem_table: &TestMemTable,
        guards: &PoolGuards,
        key: SelectKey,
        expected: Option<(RowID, bool)>,
    ) {
        let entry = mem_table
            .require_unique_index(guards, key.index_no)
            .unwrap()
            .lookup(&key.vals, MIN_SNAPSHOT_TS)
            .await
            .unwrap();
        assert_eq!(entry, expected);
    }

    async fn assert_non_unique_index_entry(
        mem_table: &TestMemTable,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
        expected: Option<bool>,
    ) {
        let entry = mem_table
            .require_non_unique_index(guards, key.index_no)
            .unwrap()
            .lookup_unique(&key.vals, row_id, MIN_SNAPSHOT_TS)
            .await
            .unwrap();
        assert_eq!(entry, expected);
    }

    async fn assert_unique_row(
        mem_table: &TestMemTable,
        guards: &PoolGuards,
        key: SelectKey,
        expected: Option<Vec<Val>>,
    ) {
        let col_count = mem_table.metadata().col.col_count();
        let row = mem_table
            .index_lookup_unique_uncommitted(guards, key.index_no, &key.vals, |layout, row| {
                (0..col_count)
                    .map(|col_idx| row.val(layout, col_idx))
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap();
        assert_eq!(row, expected);
    }

    #[test]
    fn test_evict_pool_insert_full() {
        smol::block_on(async {
            const SIZE: i32 = 800;

            // in-mem ~1000 pages, on-disk 2000 pages.
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1000 rows
                let mut trx = session.begin_trx().unwrap();
                for i in 0..SIZE {
                    // make string 1KB long, so a page can only hold about 60 rows.
                    // if page is full, 17 pages are required.
                    // if page is half full, 35 pages are required.
                    let s: String = (0..1000).map(|_| 'a').collect();
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                let _ = trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_mem_table_upsert_unique_insert_and_update() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_000);
            let mem_table = test_mem_table(&engine, mem_table_id).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            let inserted = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    mem_table
                        .upsert_unique_mvcc(
                            rt,
                            effects,
                            0,
                            vec![Val::from(1i32), Val::from("hello")],
                            false,
                        )
                        .await
                })
                .await
                .unwrap();
            let inserted_row_id = match inserted {
                UpsertMvcc::Inserted(row_id) => row_id,
                UpsertMvcc::Updated(row_id) => panic!("unexpected update row_id={row_id}"),
            };
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let updated = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    mem_table
                        .upsert_unique_mvcc(
                            rt,
                            effects,
                            0,
                            vec![Val::from(1i32), Val::from("world")],
                            false,
                        )
                        .await
                })
                .await
                .unwrap();
            assert_eq!(updated, UpsertMvcc::Updated(inserted_row_id));
            trx.commit().await.unwrap();

            let key = single_key(1i32);
            let row = mem_table
                .index_lookup_unique_uncommitted(
                    &session.pool_guards(),
                    key.index_no,
                    &key.vals,
                    |layout, row| vec![row.val(layout, 0), row.val(layout, 1)],
                )
                .await
                .unwrap();
            assert_eq!(row, Some(vec![Val::from(1i32), Val::from("world")]));
        });
    }

    #[test]
    fn test_mem_table_upsert_unique_missing_key_write_conflict() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_001);
            let mem_table = test_mem_table(&engine, mem_table_id).await;
            let mut session1 = engine.new_session().unwrap();

            let mut trx1 = session1.begin_trx().unwrap();
            assert!(matches!(
                trx1.exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    mem_table
                        .upsert_unique_mvcc(
                            rt,
                            effects,
                            0,
                            vec![Val::from(2i32), Val::from("first")],
                            false,
                        )
                        .await
                })
                .await
                .unwrap(),
                UpsertMvcc::Inserted(_)
            ));

            let mut session2 = engine.new_session().unwrap();
            let mut trx2 = session2.begin_trx().unwrap();
            let err = trx2
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    mem_table
                        .upsert_unique_mvcc(
                            rt,
                            effects,
                            0,
                            vec![Val::from(2i32), Val::from("second")],
                            false,
                        )
                        .await
                })
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::WriteConflict));
            trx2.rollback().await.unwrap();
            trx1.commit().await.unwrap();
        });
    }

    #[test]
    fn test_mem_table_non_unique_no_trx_insert_and_delete() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_010);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();
            let payload = b"payload";

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "same", payload), false)
                .await
                .unwrap();
            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(2, "same", payload), false)
                .await
                .unwrap();

            let key1 = single_key(1i32);
            let (row1, row1_deleted) = mem_table
                .require_unique_index(&guards, key1.index_no)
                .unwrap()
                .lookup(&key1.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap()
                .expect("first unique index entry should exist");
            let key2 = single_key(2i32);
            let (row2, row2_deleted) = mem_table
                .require_unique_index(&guards, key2.index_no)
                .unwrap()
                .lookup(&key2.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap()
                .expect("second unique index entry should exist");
            assert!(!row1_deleted);
            assert!(!row2_deleted);
            assert_non_unique_index_entry(&mem_table, &guards, name_key("same"), row1, Some(true))
                .await;
            assert_non_unique_index_entry(&mem_table, &guards, name_key("same"), row2, Some(true))
                .await;

            mem_table
                .delete_primary_key_no_trx(&guards, key1.index_no, &key1.vals, false)
                .await
                .unwrap();

            assert_unique_index_entry(&mem_table, &guards, single_key(1i32), None).await;
            assert_non_unique_index_entry(&mem_table, &guards, name_key("same"), row1, None).await;
            assert_non_unique_index_entry(&mem_table, &guards, name_key("same"), row2, Some(true))
                .await;
        });
    }

    #[test]
    fn test_mem_table_primary_key_no_trx_upsert_reports_logical_change() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_no_trx_upsert").await;
            let mem_table_id = test_user_table_id(10_011);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, primary_key_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();
            let initial = indexed_payload_row(1, "name", b"old");

            let mut inserted = None;
            mem_table
                .upsert_primary_key_no_trx(&guards, initial.clone(), false, |change| {
                    inserted = Some(change);
                })
                .await
                .unwrap();
            let (insert_page_id, insert_row_id) = match inserted.unwrap() {
                NoTrxUpsertChange::Inserted {
                    page_id,
                    row_id,
                    vals,
                } => {
                    assert_eq!(vals, initial);
                    (page_id, row_id)
                }
                NoTrxUpsertChange::Updated { .. } => panic!("first upsert must insert"),
            };
            let mut no_op_called = false;
            mem_table
                .upsert_primary_key_no_trx(&guards, initial, false, |_| {
                    no_op_called = true;
                })
                .await
                .unwrap();
            assert!(!no_op_called);

            let mut updated = None;
            mem_table
                .upsert_primary_key_no_trx(
                    &guards,
                    indexed_payload_row(1, "name", b"new"),
                    false,
                    |change| updated = Some(change),
                )
                .await
                .unwrap();
            match updated.unwrap() {
                NoTrxUpsertChange::Updated {
                    page_id,
                    row_id,
                    key,
                    cols,
                } => {
                    assert_eq!(page_id, insert_page_id);
                    assert_eq!(row_id, insert_row_id);
                    assert_eq!(key, single_key(1i32));
                    assert_eq!(
                        cols,
                        vec![UpdateCol {
                            idx: 2,
                            val: Val::from(&b"new"[..]),
                        }]
                    );
                }
                NoTrxUpsertChange::Inserted { .. } => panic!("second changed upsert must update"),
            }
        });
    }

    #[test]
    fn test_mem_table_primary_key_no_trx_rejects_non_primary_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_015);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "same", b"payload"), false)
                .await
                .unwrap();
            let key = name_key("same");
            let err = mem_table
                .delete_primary_key_no_trx(&guards, key.index_no, &key.vals, false)
                .await
                .unwrap_err();

            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(report.contains("key is not primary key"), "{report}");
        });
    }

    #[test]
    fn test_mem_table_delete_primary_key_no_trx_opt_out_skips_primary_key_validation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_017);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, unique_name_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(
                    &guards,
                    &indexed_payload_row(1, "unique", b"payload"),
                    false,
                )
                .await
                .unwrap();

            let key = name_key("unique");
            let err = mem_table
                .delete_primary_key_no_trx(&guards, key.index_no, &key.vals, false)
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );

            mem_table
                .delete_primary_key_no_trx(&guards, key.index_no, &key.vals, true)
                .await
                .unwrap();

            assert_unique_index_entry(&mem_table, &guards, single_key(1i32), None).await;
            assert_unique_index_entry(&mem_table, &guards, name_key("unique"), None).await;
        });
    }

    #[test]
    fn test_mem_table_insert_no_trx_validates_full_row_by_default() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_016);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            let err = mem_table
                .insert_no_trx(&guards, &[Val::from(1i32), Val::from("short")], false)
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "same", b"payload"), true)
                .await
                .unwrap();
        });
    }

    #[test]
    fn test_mem_table_update_primary_key_no_trx_updates_non_indexed_columns() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_011);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "same", b"old"), false)
                .await
                .unwrap();
            let key = single_key(1i32);
            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 2,
                        val: Val::from(&b"new"[..]),
                    }],
                    false,
                )
                .await
                .unwrap();
            assert_unique_row(
                &mem_table,
                &guards,
                key.clone(),
                Some(indexed_payload_row(1, "same", b"new")),
            )
            .await;

            let err = mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 0,
                        val: Val::from(1i32),
                    }],
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
        });
    }

    #[test]
    fn test_mem_table_update_primary_key_no_trx_refreshes_non_unique_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_019);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "old", b"payload"), false)
                .await
                .unwrap();
            let key = single_key(1i32);
            let (row_id, deleted) = mem_table
                .require_unique_index(&guards, key.index_no)
                .unwrap()
                .lookup(&key.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap()
                .expect("inserted primary key should be indexed");
            assert!(!deleted);

            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 1,
                        val: Val::from("new"),
                    }],
                    false,
                )
                .await
                .unwrap();

            assert_non_unique_index_entry(&mem_table, &guards, name_key("old"), row_id, None).await;
            assert_non_unique_index_entry(&mem_table, &guards, name_key("new"), row_id, Some(true))
                .await;
            assert_unique_row(
                &mem_table,
                &guards,
                key,
                Some(indexed_payload_row(1, "new", b"payload")),
            )
            .await;
        });
    }

    #[test]
    fn test_mem_table_update_primary_key_no_trx_single_primary_key_updates_without_refresh() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_020);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, primary_key_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "old", b"payload"), false)
                .await
                .unwrap();
            let key = single_key(1i32);
            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 1,
                        val: Val::from("new"),
                    }],
                    false,
                )
                .await
                .unwrap();

            assert_unique_row(
                &mem_table,
                &guards,
                single_key(1i32),
                Some(indexed_payload_row(1, "new", b"payload")),
            )
            .await;
        });
    }

    #[test]
    fn test_mem_table_update_primary_key_no_trx_opt_out_skips_primary_key_validation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_018);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, unique_name_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "unique", b"old"), false)
                .await
                .unwrap();
            let key = name_key("unique");

            let err = mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 2,
                        val: Val::from(&b"new"[..]),
                    }],
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );

            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 2,
                        val: Val::from(&b"new"[..]),
                    }],
                    true,
                )
                .await
                .unwrap();
            assert_unique_row(
                &mem_table,
                &guards,
                single_key(1i32),
                Some(indexed_payload_row(1, "unique", b"new")),
            )
            .await;

            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 1,
                        val: Val::from("changed"),
                    }],
                    true,
                )
                .await
                .unwrap();
            assert_unique_row(
                &mem_table,
                &guards,
                single_key(1i32),
                Some(indexed_payload_row(1, "changed", b"new")),
            )
            .await;
            assert_unique_row(&mem_table, &guards, name_key("unique"), None).await;
            assert_unique_row(
                &mem_table,
                &guards,
                name_key("changed"),
                Some(indexed_payload_row(1, "changed", b"new")),
            )
            .await;
        });
    }

    #[test]
    fn test_mem_table_update_primary_key_no_trx_rejects_duplicate_unique_index_refresh() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_021);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, unique_name_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();

            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(1, "one", b"payload"), false)
                .await
                .unwrap();
            mem_table
                .insert_no_trx(&guards, &indexed_payload_row(2, "two", b"payload"), false)
                .await
                .unwrap();
            let key = single_key(1i32);

            let err = mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 1,
                        val: Val::from("two"),
                    }],
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::UnexpectedRecoveryDuplicateKey)
            );
        });
    }

    #[test]
    fn test_mem_table_update_primary_key_no_trx_relocates_on_no_free_space() {
        smol::block_on(async {
            const ROWS: i32 = 60;
            const BASE_PAYLOAD_SIZE: usize = 1000;
            const LARGE_PAYLOAD_SIZE: usize = 50_000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_015);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let session = engine.new_session().unwrap();
            let guards = session.pool_guards();
            let base_payload = vec![b'a'; BASE_PAYLOAD_SIZE];
            let mut row_ids = Vec::new();

            for id in 0..ROWS {
                let name = format!("name{id}");
                mem_table
                    .insert_no_trx(
                        &guards,
                        &indexed_payload_row(id, &name, &base_payload),
                        false,
                    )
                    .await
                    .unwrap();
                let key = single_key(id);
                let (row_id, deleted) = mem_table
                    .require_unique_index(&guards, key.index_no)
                    .unwrap()
                    .lookup(&key.vals, MIN_SNAPSHOT_TS)
                    .await
                    .unwrap()
                    .expect("inserted primary key should be indexed");
                assert!(!deleted);
                row_ids.push(row_id);
            }

            let old_row0 = row_ids[0];
            let large_payload = vec![b'b'; LARGE_PAYLOAD_SIZE];
            let key = single_key(0i32);
            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_no,
                    &key.vals,
                    &[UpdateCol {
                        idx: 2,
                        val: Val::from(&large_payload[..]),
                    }],
                    false,
                )
                .await
                .unwrap();

            let (new_row0, deleted) = mem_table
                .require_unique_index(&guards, key.index_no)
                .unwrap()
                .lookup(&key.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap()
                .expect("relocated primary key should be indexed");
            assert!(!deleted);
            assert_ne!(new_row0, old_row0);
            assert_non_unique_index_entry(&mem_table, &guards, name_key("name0"), old_row0, None)
                .await;
            assert_non_unique_index_entry(
                &mem_table,
                &guards,
                name_key("name0"),
                new_row0,
                Some(true),
            )
            .await;
            assert_unique_row(
                &mem_table,
                &guards,
                single_key(0i32),
                Some(indexed_payload_row(0, "name0", &large_payload)),
            )
            .await;
        });
    }

    #[test]
    fn test_mem_table_delete_unique_mvcc_marks_non_unique_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_011);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let mut session = engine.new_session().unwrap();

            let row_id = insert_mem_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                indexed_payload_row(10, "delete", b"payload"),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("delete"),
                row_id,
                Some(true),
            )
            .await;

            let mut trx = session.begin_trx().unwrap();
            let deleted = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    let key = single_key(10i32);
                    mem_table
                        .delete_unique_mvcc(rt, effects, key.index_no, &key.vals, false)
                        .await
                })
                .await
                .unwrap();
            assert_eq!(deleted, DeleteMvcc::Deleted);
            trx.commit().await.unwrap();

            assert_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                single_key(10i32),
                Some((row_id, true)),
            )
            .await;
            assert_unique_row(&mem_table, &session.pool_guards(), single_key(10i32), None).await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("delete"),
                row_id,
                Some(false),
            )
            .await;

            let key = name_key("delete");
            let removed = mem_table
                .delete_index(
                    &session.pool_guards(),
                    key.index_no,
                    &key.vals,
                    row_id,
                    false,
                    MIN_SNAPSHOT_TS,
                )
                .await
                .unwrap();
            assert!(!removed);
        });
    }

    #[test]
    fn test_mem_table_update_key_change_updates_unique_and_non_unique_indexes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_012);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let mut session = engine.new_session().unwrap();

            let row_id = insert_mem_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                indexed_payload_row(1, "old", b"payload"),
            )
            .await;
            insert_mem_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                indexed_payload_row(20, "other", b"payload"),
            )
            .await;

            let updated = update_mem_unique_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                single_key(1i32),
                vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(10i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("new"),
                    },
                ],
            )
            .await;
            assert_eq!(updated, UpdateMvcc::Updated(row_id));
            assert_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                single_key(1i32),
                Some((row_id, true)),
            )
            .await;
            assert_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                single_key(10i32),
                Some((row_id, false)),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("old"),
                row_id,
                Some(false),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("new"),
                row_id,
                Some(true),
            )
            .await;
            assert_unique_row(
                &mem_table,
                &session.pool_guards(),
                single_key(10i32),
                Some(indexed_payload_row(10, "new", b"payload")),
            )
            .await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                stmt.acquire_table_write_data_lock(mem_table_id).await?;
                let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                let page_id = match mem_table.find_row(rt.pool_guards(), row_id).await? {
                    RowLocation::RowPage(page_id) => page_id,
                    RowLocation::NotFound => panic!("updated row should exist"),
                    RowLocation::LwcBlock { .. } => {
                        panic!("standalone MemTable should not use LWC")
                    }
                };
                let page_guard = mem_table
                    .must_get_row_page_shared(rt.pool_guards(), page_id)
                    .await?;
                let report = mem_table
                    .update_unique_index_only_key_change(
                        rt,
                        effects,
                        single_key(10i32),
                        single_key(20i32),
                        row_id,
                        &page_guard,
                    )
                    .await
                    .unwrap_err();
                assert_eq!(report.current_context(), &OperationError::DuplicateKey);
                Ok(())
            })
            .await
            .unwrap();
            trx.rollback().await.unwrap();

            assert_unique_row(
                &mem_table,
                &session.pool_guards(),
                single_key(10i32),
                Some(indexed_payload_row(10, "new", b"payload")),
            )
            .await;
        });
    }

    #[test]
    fn test_mem_table_moved_updates_refresh_unique_and_non_unique_indexes() {
        smol::block_on(async {
            const ROWS: i32 = 60;
            const BASE_PAYLOAD_SIZE: usize = 1000;
            const LARGE_PAYLOAD_SIZE: usize = 50_000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_013);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;
            let mut session = engine.new_session().unwrap();
            let base_payload = vec![b'a'; BASE_PAYLOAD_SIZE];
            let mut row_ids = Vec::new();
            for id in 0..ROWS {
                let name = format!("name{id}");
                let row_id = insert_mem_mvcc(
                    &mut session,
                    mem_table_id,
                    &mem_table,
                    indexed_payload_row(id, &name, &base_payload),
                )
                .await;
                row_ids.push(row_id);
            }

            let large_payload = vec![b'b'; LARGE_PAYLOAD_SIZE];
            let old_row0 = row_ids[0];
            let updated = update_mem_unique_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                single_key(0i32),
                vec![UpdateCol {
                    idx: 2,
                    val: Val::from(&large_payload[..]),
                }],
            )
            .await;
            let new_row0 = match updated {
                UpdateMvcc::Updated(row_id) => row_id,
                UpdateMvcc::NotFound => panic!("payload update should find row"),
            };
            assert_ne!(new_row0, old_row0);
            assert_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                single_key(0i32),
                Some((new_row0, false)),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("name0"),
                old_row0,
                Some(false),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("name0"),
                new_row0,
                Some(true),
            )
            .await;
            assert_unique_row(
                &mem_table,
                &session.pool_guards(),
                single_key(0i32),
                Some(indexed_payload_row(0, "name0", &large_payload)),
            )
            .await;

            let changed_payload = vec![b'c'; LARGE_PAYLOAD_SIZE];
            let old_row1 = row_ids[1];
            let updated = update_mem_unique_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                single_key(1i32),
                vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(101i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("moved"),
                    },
                    UpdateCol {
                        idx: 2,
                        val: Val::from(&changed_payload[..]),
                    },
                ],
            )
            .await;
            let new_row1 = match updated {
                UpdateMvcc::Updated(row_id) => row_id,
                UpdateMvcc::NotFound => panic!("key-changing update should find row"),
            };
            assert_ne!(new_row1, old_row1);
            assert_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                single_key(1i32),
                Some((old_row1, true)),
            )
            .await;
            assert_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                single_key(101i32),
                Some((new_row1, false)),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("name1"),
                old_row1,
                Some(false),
            )
            .await;
            assert_non_unique_index_entry(
                &mem_table,
                &session.pool_guards(),
                name_key("moved"),
                new_row1,
                Some(true),
            )
            .await;
            assert_unique_row(&mem_table, &session.pool_guards(), single_key(1i32), None).await;
            assert_unique_row(
                &mem_table,
                &session.pool_guards(),
                single_key(101i32),
                Some(indexed_payload_row(101, "moved", &changed_payload)),
            )
            .await;
        });
    }

    #[test]
    fn test_mem_table_internal_error_helpers_report_context() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_014);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;

            let err = mem_table.stale_block_index_leaf("test stale block-index");
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::BlockIndexLeafStale)
            );
            let report = format!("{err:?}");
            assert!(report.contains("test stale block-index"), "{report}");
            assert!(report.contains(&mem_table_id.to_string()), "{report}");

            let err = mem_table
                .catalog_lwc_error::<()>("test catalog lwc", RowID::new(42))
                .unwrap_err();
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::Generic)
            );
            let report = format!("{err:?}");
            assert!(report.contains("test catalog lwc"), "{report}");
            assert!(report.contains("row_id=42"), "{report}");
        });
    }

    #[test]
    fn test_mem_table_transition_retry_is_internal_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_015);
            let mem_table = test_mem_table(&engine, mem_table_id).await;
            let mut session = engine.new_session().unwrap();
            let row_id = insert_mem_mvcc(
                &mut session,
                mem_table_id,
                &mem_table,
                vec![Val::from(1i32), Val::from("transition")],
            )
            .await;
            let page_id = match mem_table
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::NotFound => panic!("inserted row should be found"),
                RowLocation::LwcBlock { .. } => panic!("standalone MemTable should not use LWC"),
            };
            let page_guard = mem_table
                .try_get_validated_row_page_shared_result(&session.pool_guards(), page_id, row_id)
                .await
                .unwrap()
                .expect("inserted row page should validate");
            let row_ver = page_guard.unwrap_vmap();
            *row_ver.write_state() = RowPageState::Transition;
            drop(page_guard);

            let key = single_key(1i32);
            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    mem_table
                        .update_unique_mvcc(
                            rt,
                            effects,
                            key.index_no,
                            &key.vals,
                            vec![UpdateCol {
                                idx: 1,
                                val: Val::from("updated"),
                            }],
                            false,
                        )
                        .await
                })
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Internal);
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(mem_table_id).await?;
                    stmt.acquire_table_write_data_lock(mem_table_id).await?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    mem_table
                        .delete_unique_mvcc(rt, effects, key.index_no, &key.vals, false)
                        .await
                })
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Internal);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_mem_scan_uncommitted() {
        smol::block_on(async {
            const SIZE: i32 = 10000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;

            let mut session = engine.new_session().unwrap();
            {
                let mut trx = session.begin_trx().unwrap();
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                _ = trx.commit().await.unwrap();
            }
            {
                let mut res_len = 0usize;
                let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
                table_for_internal_assertion(&engine, table_id)
                    .accessor_with_layout(&layout)
                    .mem_scan_uncommitted(&session.pool_guards(), |_metadata, _row| {
                        res_len += 1;
                        true
                    })
                    .await
                    .unwrap();
                println!("res.len()={}", res_len);
                assert!(res_len == SIZE as usize);
            }
        });
    }

    #[test]
    fn test_mem_scan_uncommitted_returns_error_without_meta_guard() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let empty_guards = PoolGuards::builder().build();

            let err = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .mem_scan_uncommitted(&empty_guards, |_metadata, _row| true)
                .await
                .unwrap_err();

            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::PoolGuardMissing)
            );
        });
    }

    #[test]
    fn test_scan_from_requires_row_page_boundary() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "first").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let captured_pivot = table.file().active_root_unchecked().pivot_row_id;

            insert_rows(table_id, &mut session, 100, 4, "second").await;
            let mut later_pivot = captured_pivot;
            let mut explicit_count = 0usize;
            table
                .mem
                .scan_from(&session.pool_guards(), captured_pivot, |page_guard| {
                    let page = page_guard.page();
                    explicit_count += page.header.approx_non_deleted();
                    later_pivot = page.header.start_row_id + u64::from(page.header.max_row_count);
                    true
                })
                .await
                .unwrap();
            assert_eq!(explicit_count, 4);
            assert!(later_pivot > captured_pivot);

            let interior_start = captured_pivot + 2;
            let err = table
                .mem
                .scan_from(&session.pool_guards(), interior_start, |_| true)
                .await
                .unwrap_err();
            assert!(err.kind() == ErrorKind::Internal);

            // Keep the second batch's row page allocated while advancing only the
            // memory-scan pivot to that page's exclusive row-id boundary. A real
            // later checkpoint may reclaim pages once no transaction root protects
            // them, which is outside this helper's direct contract.
            table
                .mem
                .blk_idx()
                .update_column_root(later_pivot, SUPER_BLOCK_ID)
                .await;
            assert_eq!(table.mem.pivot_row_id(), later_pivot);

            let mut current_hot_pages = 0usize;
            table
                .mem
                .scan(&session.pool_guards(), |_| {
                    current_hot_pages += 1;
                    true
                })
                .await
                .unwrap();
            assert_eq!(current_hot_pages, 0);
        });
    }

    #[test]
    fn test_build_in_memory_secondary_indexes_reclaims_staged_indexes_on_error() {
        smol::block_on(async {
            use super::build_in_memory_secondary_indexes;
            use crate::buffer::FixedBufferPool;
            use crate::buffer::frame::BufferFrame;
            use crate::buffer::page::Page;
            use crate::catalog::{
                ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableMetadata,
            };
            use crate::quiescent::QuiescentBox;
            use crate::value::ValKind;
            use std::mem::size_of;

            let pool_bytes = size_of::<BufferFrame>() + size_of::<Page>();
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, pool_bytes)
                    .expect("one-page fixed index pool should be constructible"),
            );
            let pool_guard = (*pool).pool_guard();
            let metadata = TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )],
                vec![
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty()),
                ],
            )
            .expect("valid table metadata");

            let err = match build_in_memory_secondary_indexes(
                pool.guard(),
                &pool_guard,
                &metadata,
                TrxID::new(100),
            )
            .await
            {
                Ok(_) => panic!("second secondary-index construction should fail in one-page pool"),
                Err(err) => err,
            };
            assert_eq!(err.resource_error(), Some(ResourceError::BufferPoolFull));
            assert_eq!(pool.allocated(), 0);
        });
    }
}
