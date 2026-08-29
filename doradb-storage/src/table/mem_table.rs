use super::{
    DmlValidator, TableKind, UpdateUniqueMvcc,
    hot::{DeleteInternal, HotRowMutator, InsertRowIntoPage, RowInserter, UpdateRowInplace},
    index_key_is_changed, index_key_replace, read_latest_index_key,
    read_physical_index_keys_for_delete, row_len, unique_key_from_full_row,
    validate_page_row_range,
};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::VersionedPageID;
use crate::buffer::{
    BufferPool, PoolGuard, PoolGuards, PoolRole, RowPoolRole, get_page_versioned_shared,
};
use crate::catalog::{
    CatalogSelectKey, IndexSlot, IndexSpec, PrimaryKeyMatchError, ResolvedIndexKey,
    TableColumnLayout, TableMetadata, resolve_catalog_key, user_key_from_active_slot,
};
use crate::error::{
    DataIntegrityError, InternalError, InternalResult, MultiDomainResultExt, OperationError,
    OperationOrRuntimeError, OperationOrRuntimeResult, QuadResult, RecoveryDuplicateKey,
    RuntimeError, RuntimeOrFatalResult, RuntimeOrFatalResultExt, RuntimeResult,
    SecondaryIndexBinding,
};
use crate::id::{PageID, RowID, TableID, TrxID};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{
    BlockIndex, GuardedNonUniqueMemIndex, GuardedUniqueMemIndex, InMemorySecondaryIndex,
    IndexCompareExchange, IndexInsert, RowLocation,
};
use crate::latch::LatchFallbackMode;
use crate::map::FastHashMap;
use crate::obs;
use crate::quiescent::QuiescentGuard;
use crate::row::ops::{
    DeleteMvcc, LinkForUniqueIndex, RowUpdateInput, RowUpdateView, SelectKey, UpdateCol,
    UpdateMvcc, UpsertMvcc,
};
use crate::row::{Row, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::trx::row::FindOldVersion;
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, OwnedRowUndo, RowUndoKind, RowUndoRollbackAttempt};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MIN_SNAPSHOT_TS, RetiredRowPageBatch, TrxRuntime};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::mem::take;
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
    /// An existing logical row was updated with its stable runtime row identity.
    Updated {
        row_id: RowID,
        key: CatalogSelectKey,
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
    ) -> RuntimeResult<Self> {
        let sec_idx =
            build_in_memory_secondary_indexes(index_pool, index_pool_guard, &metadata, index_ts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| format!("operation=create_mem_table, table_id={table_id}"))?;
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

    /// Return an active secondary-index runtime by physical slot.
    #[inline]
    pub(crate) fn require_sec_idx(
        &self,
        index_slot: IndexSlot,
    ) -> RuntimeResult<&InMemorySecondaryIndex<I>> {
        self.sec_idx
            .get(index_slot.as_usize())
            .and_then(Option::as_ref)
            .ok_or_else(|| Report::new(InternalError::SecondaryIndexOutOfBounds))
            .attach_with(|| {
                format!(
                    "index_slot={index_slot}, index_count={}",
                    self.sec_idx.len()
                )
            })
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=require_secondary_index, table_id={}, index_slot={index_slot}",
                    self.table_id()
                )
            })
    }

    #[inline]
    fn sec_idx_len(&self) -> usize {
        self.sec_idx().len()
    }

    #[inline]
    fn sec_idx_is_active(&self, index_slot: IndexSlot) -> bool {
        self.sec_idx()
            .get(index_slot.as_usize())
            .is_some_and(Option::is_some)
    }

    #[inline]
    fn sec_idx_is_unique(&self, index_slot: IndexSlot) -> bool {
        self.sec_idx()[index_slot.as_usize()]
            .as_ref()
            .expect("active index slot")
            .is_unique()
    }

    /// Return a guarded unique MemIndex by physical slot.
    #[inline]
    pub(crate) fn require_unique_index<'g>(
        &self,
        guards: &'g PoolGuards,
        index_slot: IndexSlot,
    ) -> RuntimeResult<GuardedUniqueMemIndex<'_, 'g, I>> {
        match self.require_sec_idx(index_slot)? {
            InMemorySecondaryIndex::Unique(index) => Ok(index.bind(self.index_pool_guard(guards))),
            InMemorySecondaryIndex::NonUnique(_) => {
                Err(wrong_secondary_index_binding("unique", "non-unique"))
                    .change_context(RuntimeError::IndexAccess)
                    .attach_with(|| {
                        format!(
                            "operation=require_unique_index, table_id={}, index_slot={index_slot}",
                            self.table_id()
                        )
                    })
            }
        }
    }

    /// Return a guarded non-unique MemIndex by physical slot.
    #[inline]
    pub(crate) fn require_non_unique_index<'g>(
        &self,
        guards: &'g PoolGuards,
        index_slot: IndexSlot,
    ) -> RuntimeResult<GuardedNonUniqueMemIndex<'_, 'g, I>> {
        match self.require_sec_idx(index_slot)? {
            InMemorySecondaryIndex::Unique(_) => Err(wrong_secondary_index_binding(
                "non-unique",
                "unique",
            ))
            .change_context(RuntimeError::IndexAccess)
            .attach_with(|| {
                format!(
                    "operation=require_non_unique_index, table_id={}, index_slot={index_slot}",
                    self.table_id()
                )
            }),
            InMemorySecondaryIndex::NonUnique(index) => {
                Ok(index.bind(self.index_pool_guard(guards)))
            }
        }
    }

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.blk_idx.pivot_row_id()
    }

    #[inline]
    fn meta_pool_guard<'a>(&self, guards: &'a PoolGuards) -> &'a PoolGuard {
        // Every table runtime owns a metadata index, so every admitted table
        // operation carries the metadata guard. Catalog-only bundles are
        // intentionally partial but still include this role.
        guards.meta_guard()
    }

    #[inline]
    fn row_pool_guard<'a>(&self, guards: &'a PoolGuards) -> &'a PoolGuard {
        // Catalog row pages use Meta; user-table row pages use Mem. Runtime
        // construction installs the guard matching this immutable role.
        guards.row_guard(self.row_pool_role)
    }

    /// Return the pool guard used by in-memory secondary indexes.
    #[inline]
    pub(crate) fn index_pool_guard<'a>(&self, guards: &'a PoolGuards) -> &'a PoolGuard {
        // Catalog indexes use Meta; user-table indexes use Index. The table
        // runtime layout fixes the role before any admitted access.
        guards.guard(self.index_pool_role)
    }

    /// Destroy all mutable memory structures owned by this table runtime.
    #[inline]
    pub(crate) async fn destroy(self, guards: &PoolGuards) -> RuntimeResult<()> {
        let row_pool_guard = self.row_pool_guard(guards);
        let index_pool_guard = self.index_pool_guard(guards);
        let meta_pool_guard = self.meta_pool_guard(guards);
        let table_id = self.table_id();
        let MemTable {
            mem_pool,
            blk_idx,
            sec_idx,
            ..
        } = self;
        for index in sec_idx.into_iter().flatten() {
            index
                .destroy(index_pool_guard)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!("operation=destroy_secondary_index, table_id={table_id}")
                })?;
        }
        blk_idx
            .destroy(meta_pool_guard, &*mem_pool, row_pool_guard)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| format!("operation=destroy_block_index, table_id={table_id}"))
    }

    /// Unlinks one exact checkpoint-retired row-page prefix from the hot index.
    #[inline]
    pub(crate) async fn unlink_retired_row_pages(
        &self,
        guards: &PoolGuards,
        batch: &RetiredRowPageBatch,
    ) -> RuntimeResult<Box<[PageID]>> {
        let result = self
            .blk_idx
            .prune_checkpoint_prefix(
                self.meta_pool_guard(guards),
                batch.start_row_id,
                batch.end_row_id,
                &batch.page_ids,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=unlink_retired_row_pages, table_id={}, start_row_id={}, end_row_id={}",
                    self.table_id(), batch.start_row_id, batch.end_row_id
                )
            })?;
        Ok(result.page_ids)
    }

    /// Physically deallocates row pages already unlinked from the hot index.
    #[inline]
    pub(crate) async fn deallocate_retired_row_pages(
        &self,
        guards: &PoolGuards,
        page_ids: &[PageID],
    ) -> RuntimeResult<()> {
        let row_pool_guard = self.row_pool_guard(guards);
        for page_id in page_ids {
            let page_guard = self
                .mem_pool
                .get_page::<RowPage>(row_pool_guard, *page_id, LatchFallbackMode::Exclusive)
                .await?
                .lock_exclusive_async()
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "unlinked retired row page could not be locked for deallocation: table_id={}, page_id={page_id}",
                        self.table_id()
                    )
                });
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
    ) -> RuntimeResult<Option<PageSharedGuard<RowPage>>> {
        Ok(self
            .mem_pool()
            .get_page::<RowPage>(
                self.row_pool_guard(guards),
                page_id,
                LatchFallbackMode::Shared,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=get_row_page_shared, table_id={}, page_id={page_id}",
                    self.table_id()
                )
            })?
            .lock_shared_async()
            .await)
    }

    /// Lock a specific row-page version for shared access if it is present.
    #[inline]
    pub(crate) async fn get_row_page_versioned_shared(
        &self,
        guards: &PoolGuards,
        page_id: VersionedPageID,
    ) -> RuntimeResult<Option<PageSharedGuard<RowPage>>> {
        get_page_versioned_shared::<RowPage, _>(
            self.mem_pool(),
            self.row_pool_guard(guards),
            page_id,
        )
        .await
    }

    /// Try to roll back one row undo record against its exact hot page.
    #[inline]
    pub(crate) async fn try_rollback_hot_row_undo(
        &self,
        entry: &mut OwnedRowUndo,
        guards: &PoolGuards,
    ) -> RuntimeResult<RowUndoRollbackAttempt> {
        let page_id = entry
            .page_id
            .expect("hot row-undo rollback requires an original page generation");
        let page_guard = self.get_row_page_versioned_shared(guards, page_id).await?;
        let Some(page_guard) = page_guard else {
            return Ok(RowUndoRollbackAttempt::PageMissing);
        };
        let page = page_guard.page();
        let state_guard = page_guard.unwrap_vmap().read_state();
        if *state_guard == RowPageState::Transition {
            return Ok(RowUndoRollbackAttempt::Transition);
        }
        let metadata = self.metadata();
        let mut access =
            page_guard.write_row_with_state_guard(page.row_idx(entry.row_id), state_guard);
        access.rollback_first_undo(metadata, entry);
        Ok(RowUndoRollbackAttempt::Applied)
    }

    /// Lock an in-memory row page for exclusive access if it is present.
    #[inline]
    pub(crate) async fn get_row_page_exclusive(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> RuntimeResult<Option<PageExclusiveGuard<RowPage>>> {
        Ok(self
            .mem_pool()
            .get_page::<RowPage>(
                self.row_pool_guard(guards),
                page_id,
                LatchFallbackMode::Exclusive,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=get_row_page_exclusive, table_id={}, page_id={page_id}",
                    self.table_id()
                )
            })?
            .lock_exclusive_async()
            .await)
    }

    /// Lock an existing in-memory row page for shared access.
    #[inline]
    pub(crate) async fn must_get_row_page_shared(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> RuntimeResult<PageSharedGuard<RowPage>> {
        let guard = self.get_row_page_shared(guards, page_id).await?;
        Ok(guard.unwrap_or_else(|| {
            panic!(
                "required published row page could not be locked shared: table_id={}, page_id={page_id}",
                self.table_id()
            )
        }))
    }

    /// Lock an existing in-memory row page for exclusive access.
    #[inline]
    pub(crate) async fn must_get_row_page_exclusive(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> RuntimeResult<PageExclusiveGuard<RowPage>> {
        let guard = self.get_row_page_exclusive(guards, page_id).await?;
        Ok(guard.unwrap_or_else(|| {
            panic!(
                "required published row page could not be locked exclusive: table_id={}, page_id={page_id}",
                self.table_id()
            )
        }))
    }

    /// Find or allocate a shared insert page with enough row capacity.
    #[inline]
    pub(crate) async fn try_get_insert_page(
        &self,
        guards: &PoolGuards,
        count: usize,
    ) -> RuntimeResult<PageSharedGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards);
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .try_get_insert_page(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata.col,
                count,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=get_insert_page, table_id={}, row_capacity={count}",
                    self.table_id()
                )
            })
    }

    /// Find or allocate a shared insert page and publish physical creation redo.
    #[inline]
    pub(crate) async fn try_get_insert_page_with_redo(
        &self,
        guards: &PoolGuards,
        count: usize,
        redo_ctx: RowPageCreateRedoCtx<'_>,
    ) -> RuntimeOrFatalResult<PageSharedGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards);
        let row_pool_guard = self.row_pool_guard(guards);
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
            .change_runtime_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=get_insert_page_with_redo, table_id={}, row_capacity={count}",
                    self.table_id()
                )
            })
    }

    /// Find or allocate an exclusive insert page with enough row capacity.
    #[inline]
    pub(crate) async fn get_insert_page_exclusive(
        &self,
        guards: &PoolGuards,
        count: usize,
    ) -> RuntimeResult<PageExclusiveGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards);
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .get_insert_page_exclusive(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.metadata.col,
                count,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=get_exclusive_insert_page, table_id={}, row_capacity={count}",
                    self.table_id()
                )
            })
    }

    /// Allocate and lock a row page at an exact page id.
    #[inline]
    pub(crate) async fn allocate_row_page_at(
        &self,
        guards: &PoolGuards,
        count: usize,
        page_id: PageID,
    ) -> RuntimeResult<PageExclusiveGuard<RowPage>> {
        let meta_pool_guard = self.meta_pool_guard(guards);
        let row_pool_guard = self.row_pool_guard(guards);
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
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=allocate_row_page, table_id={}, page_id={page_id}, row_capacity={count}",
                    self.table_id()
                )
            })
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
    pub(crate) async fn scan<F>(&self, guards: &PoolGuards, page_action: F) -> RuntimeResult<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = self.meta_pool_guard(guards);
        let start_row_id = self.pivot_row_id();
        self.scan_from_with_meta_guard(
            guards,
            meta_pool_guard,
            start_row_id,
            "mem_scan",
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
    ) -> RuntimeResult<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let meta_pool_guard = self.meta_pool_guard(guards);
        self.scan_from_with_meta_guard(
            guards,
            meta_pool_guard,
            start_row_id,
            "mem_scan_from",
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
    ) -> RuntimeResult<(RowID, Vec<RowPageDescriptor>)> {
        let operation = "snapshot_original_row_pages";
        let meta_pool_guard = self.meta_pool_guard(guards);
        let mut cursor = self.blk_idx.mem_cursor(meta_pool_guard);
        cursor
            .seek(start_row_id)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=seek_row_page_index, table_id={}, start_row_id={start_row_id}",
                    self.table_id()
                )
            })?;
        let mut entries = Vec::new();
        let mut upper_bound = start_row_id;
        let mut first_leaf = true;
        while let Some(leaf) = cursor
            .next()
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=read_row_page_index, table_id={}",
                    self.table_id()
                )
            })?
        {
            let guard = leaf.lock_shared_async().await.unwrap_or_else(|| {
                panic!(
                    "cursor-held row-page-index leaf could not be locked: operation={operation}, table_id={}, start_row_id={start_row_id}",
                    self.table_id()
                )
            });
            let page = guard.page();
            debug_assert!(page.is_leaf());
            let leaf_entries = page.leaf_entries();
            let start_idx = if first_leaf {
                first_leaf = false;
                if leaf_entries.is_empty() {
                    if page.header.start_row_id != start_row_id {
                        return invalid_scan_start(self.table_id(), start_row_id)
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!("operation={operation}, table_id={}", self.table_id())
                            });
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
                        return invalid_scan_start(self.table_id(), start_row_id)
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!("operation={operation}, table_id={}", self.table_id())
                            });
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
            assert!(
                entry.row_id < end_row_id,
                "block index must produce an increasing original row-page range: table_id={}, start_row_id={}, end_row_id={end_row_id}",
                self.table_id(),
                entry.row_id
            );
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
    ) -> RuntimeResult<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let mut cursor = self.blk_idx.mem_cursor(meta_pool_guard);
        cursor
            .seek(start_row_id)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=seek_row_page_index, table_id={}, start_row_id={start_row_id}",
                    self.table_id()
                )
            })?;
        let mut first_leaf = true;
        while let Some(leaf) = cursor
            .next()
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=read_row_page_index, table_id={}",
                    self.table_id()
                )
            })?
        {
            let g = leaf.lock_shared_async().await.unwrap_or_else(|| {
                panic!(
                    "cursor-held row-page-index leaf could not be locked: operation={operation}, table_id={}, start_row_id={start_row_id}",
                    self.table_id()
                )
            });
            debug_assert!(g.page().is_leaf());
            let page = g.page();
            let entries = page.leaf_entries();
            let start_idx = if first_leaf {
                first_leaf = false;
                if entries.is_empty() {
                    if page.header.start_row_id == start_row_id {
                        return Ok(());
                    }
                    return invalid_scan_start(self.table_id(), start_row_id)
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!("operation={operation}, table_id={}", self.table_id())
                        });
                }
                match entries.binary_search_by_key(&start_row_id, |entry| entry.row_id) {
                    Ok(idx) => idx,
                    Err(_) if page.header.end_row_id == start_row_id => return Ok(()),
                    Err(_) => {
                        return invalid_scan_start(self.table_id(), start_row_id)
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!("operation={operation}, table_id={}", self.table_id())
                            });
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
    pub(crate) async fn find_row(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
    ) -> RuntimeResult<RowLocation> {
        let meta_pool_guard = self.meta_pool_guard(guards);
        self.blk_idx
            .find_mem_row(meta_pool_guard, row_id)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=find_row, table_id={}, row_id={row_id}",
                    self.table_id()
                )
            })
    }

    #[inline]
    fn catalog_lwc_invariant(&self, operation: &'static str, row_id: RowID) -> ! {
        // Catalog tables live entirely in the fixed row store and never
        // publish LWC roots, so resolving one here indicates corrupted routing.
        panic!(
            "catalog table unexpectedly resolved a persisted LWC row: operation={operation}, table_id={}, row_id={row_id}",
            self.table_id()
        )
    }

    #[inline]
    fn debug_assert_table_write_lock_held(&self, rt: TrxRuntime<'_>) {
        rt.debug_assert_table_write_lock_held(self.table_id());
    }

    /// Qualifies one metadata-proven active slot for retained transaction state.
    #[inline]
    fn resolved_index_key(&self, key: SelectKey) -> ResolvedIndexKey {
        match self.table_id().kind() {
            TableKind::Catalog => resolve_catalog_key(key),
            TableKind::User => user_key_from_active_slot(key.index_slot, key.vals),
        }
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
        effects.push_insert_unique_index_undo(
            self.table_id(),
            row_id,
            self.resolved_index_key(key),
            merge_old_deleted,
        );
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
        effects.push_insert_non_unique_index_undo(
            self.table_id(),
            row_id,
            self.resolved_index_key(key),
            merge_old_deleted,
        );
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
        effects.push_delete_index_undo(
            self.table_id(),
            row_id,
            self.resolved_index_key(key),
            unique,
        );
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
            self.resolved_index_key(key),
            old_deleted,
        );
    }

    #[inline]
    async fn insert_index_slot_no_trx(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> RuntimeResult<()> {
        if self
            .metadata()
            .idx
            .require_index_spec(key.index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_index_slot_no_trx, table_id={}, index_slot={}",
                    self.table_id(),
                    key.index_slot
                )
            })?
            .unique()
        {
            let res = self
                .require_unique_index(guards, key.index_slot)?
                .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await?;
            ensure_no_trx_index_insert(key.index_slot, res)?;
        } else {
            let res = self
                .require_non_unique_index(guards, key.index_slot)?
                .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await?;
            ensure_no_trx_index_insert(key.index_slot, res)?;
        }
        Ok(())
    }

    #[inline]
    async fn delete_index_directly(
        &self,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key_vals: &[Val],
        row_id: RowID,
    ) -> RuntimeResult<bool> {
        let spec = self
            .metadata()
            .idx
            .require_index_spec(index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=delete_index_directly, table_id={}, index_slot={index_slot}, row_id={row_id}",
                    self.table_id()
                )
            })?;
        if spec.unique() {
            Ok(self
                .require_unique_index(guards, index_slot)?
                .compare_delete(key_vals, row_id, true, MIN_SNAPSHOT_TS)
                .await?)
        } else {
            Ok(self
                .require_non_unique_index(guards, index_slot)?
                .compare_delete(key_vals, row_id, true, MIN_SNAPSHOT_TS)
                .await?)
        }
    }

    #[inline]
    async fn refresh_changed_indexes_no_trx(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
        old_keys: &[SelectKey],
        new_keys: &[SelectKey],
    ) -> RuntimeResult<()> {
        if old_keys.len() != new_keys.len() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx index refresh key count mismatch: old={}, new={}",
                    old_keys.len(),
                    new_keys.len()
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        for (old_key, new_key) in old_keys.iter().zip(new_keys) {
            if old_key.index_slot != new_key.index_slot {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "update primary key no-trx index refresh key order mismatch: old_index_slot={}, new_index_slot={}",
                        old_key.index_slot, new_key.index_slot
                    ))
                    .change_context(RuntimeError::TableAccess)
                    .attach("operation=validate_catalog_primary_key_payload"));
            }
            if old_key == new_key {
                continue;
            }
            self.insert_index_slot_no_trx(guards, new_key.clone(), row_id)
                .await?;
            if !self
                .delete_index_directly(guards, old_key.index_slot, &old_key.vals, row_id)
                .await?
            {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "update primary key no-trx index refresh missing old key: index_slot={}, row_id={row_id}",
                        old_key.index_slot
                    ))
                    .change_context(RuntimeError::TableAccess)
                    .attach("operation=validate_catalog_primary_key_payload"));
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
    ) -> OperationOrRuntimeResult<()> {
        if self
            .metadata()
            .idx
            .require_index_spec(key.index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(),
                    key.index_slot
                )
            })?
            .unique()
        {
            self.insert_unique_index(rt, effects, key, row_id, page_guard)
                .await?;
        } else {
            self.insert_non_unique_index(rt, effects, key, row_id)
                .await?;
        }
        Ok(())
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut index_branches: Vec<IndexBranch>,
    ) -> RuntimeResult<(RowID, PageSharedGuard<RowPage>)> {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col.col_count());
        let inserter = RowInserter::new(self.table_id(), metadata, rt);
        loop {
            let page_guard = self
                .try_get_insert_page(rt.pool_guards(), row_count)
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> OperationOrRuntimeResult<LinkForUniqueIndex> {
        debug_assert!(old_id != new_id);
        let guards = rt.pool_guards();
        let (old_guard, old_id) = loop {
            match self.find_row(guards, old_id).await {
                Ok(RowLocation::NotFound) => return Ok(LinkForUniqueIndex::NotNeeded),
                Ok(RowLocation::LwcBlock(..)) => {
                    self.catalog_lwc_invariant("link_unique_index", old_id);
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
                Err(err) => return Err(err.into()),
            }
        };
        let metadata = self.metadata();
        let old_access = old_guard.read_row_by_id(old_id);
        match old_access
            .find_old_version_for_unique_key(metadata, index_slot, key_vals, rt.ctx())
            .attach_with(|| format!("operation=link_for_unique_index, index_slot={index_slot}"))?
        {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::Found(old_row, cts, old_entry) => {
                let mut new_access = new_guard.write_row_by_id(new_id);
                let undo_vals = new_access.row().calc_delta(metadata.col.as_ref(), &old_row);
                new_access.link_for_unique_index(
                    self.resolved_index_key(SelectKey::new(index_slot, key_vals.to_vec())),
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
    ) -> OperationOrRuntimeResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let index = self
            .require_unique_index(guards, key.index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(),
                    key.index_slot
                )
            })?;
        loop {
            match index
                .insert_if_not_exists(&key.vals, row_id, false, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=insert_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                        self.table_id(),
                        key.index_slot
                    )
                })? {
                IndexInsert::Ok(merged) => {
                    self.push_insert_unique_index_undo(rt, effects, row_id, key, merged);
                    return Ok(());
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    debug_assert!(old_row_id != row_id);
                    if !deleted {
                        return Err(OperationOrRuntimeError::from(
                            Report::new(OperationError::DuplicateKey).attach(format!(
                                "operation=insert_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                                self.table_id(), key.index_slot
                            )),
                        ));
                    }
                    match self
                        .link_for_unique_index(
                            rt,
                            old_row_id,
                            key.index_slot,
                            &key.vals,
                            row_id,
                            page_guard,
                        )
                        .await?
                    {
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            let index_old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match index
                                .compare_exchange(&key.vals, index_old_row_id, row_id, sts)
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=insert_unique_index, phase=replace_deleted_key, table_id={}, index_slot={}, row_id={row_id}",
                                        self.table_id(), key.index_slot
                                    )
                                })?
                            {
                                IndexCompareExchange::Ok => {
                                    self.push_update_unique_index_undo(
                                        rt, effects, old_row_id, row_id, key, deleted,
                                    );
                                    return Ok(());
                                }
                                IndexCompareExchange::NotExists => {}
                                IndexCompareExchange::Mismatch => {
                                    return Err(OperationOrRuntimeError::from(
                                        Report::new(OperationError::WriteConflict).attach(format!(
                                            "operation=insert_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                                            self.table_id(), key.index_slot
                                        )),
                                    ));
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
    ) -> RuntimeResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        match self
            .require_non_unique_index(guards, key.index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_non_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(),
                    key.index_slot
                )
            })?
            .insert_if_not_exists(&key.vals, row_id, false, sts)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_non_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(),
                    key.index_slot
                )
            })? {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(rt, effects, row_id, key, merged);
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn defer_delete_index_keys(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        keys: Vec<SelectKey>,
    ) -> RuntimeResult<()> {
        for key in keys {
            let index_slot = key.index_slot;
            let spec = self
                .metadata()
                .idx
                .require_index_spec(index_slot)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=defer_delete_indexes, table_id={}, index_slot={}, row_id={row_id}",
                        self.table_id(),
                        key.index_slot
                    )
                })?;
            debug_assert_eq!(self.sec_idx_is_unique(index_slot), spec.unique());
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
    ) -> RuntimeResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let res = self
            .require_unique_index(guards, key.index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=defer_delete_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(), key.index_slot
                )
            })?
            .mask_as_deleted(&key.vals, row_id, sts)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=defer_delete_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(), key.index_slot
                )
            })?;
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
    ) -> RuntimeResult<()> {
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let res = self
            .require_non_unique_index(guards, key.index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=defer_delete_non_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(), key.index_slot
                )
            })?
            .mask_as_deleted(&key.vals, row_id, sts)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=defer_delete_non_unique_index, table_id={}, index_slot={}, row_id={row_id}",
                    self.table_id(), key.index_slot
                )
            })?;
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
    ) -> RuntimeResult<Option<PageSharedGuard<RowPage>>> {
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
    ) -> RuntimeResult<Option<PageID>> {
        match self.find_row(guards, row_id).await? {
            RowLocation::NotFound => Ok(None),
            RowLocation::LwcBlock(..) => self.catalog_lwc_invariant("index_purge", row_id),
            RowLocation::RowPage(page_id) => Ok(Some(page_id)),
        }
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key_vals: &[Val],
        row_id: RowID,
        _min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self
            .require_unique_index(guards, index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=delete_unique_index, table_id={}, index_slot={index_slot}, row_id={row_id}",
                    self.table_id()
                )
            })?;
        let (page_guard, row_id) = loop {
            let sts = MIN_SNAPSHOT_TS;
            match index
                .lookup(key_vals, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=delete_unique_index, phase=lookup, table_id={}, index_slot={index_slot}, row_id={row_id}",
                        self.table_id()
                    )
                })?
            {
                None => return Ok(false),
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        return Ok(false);
                    }
                    let Some(page_id) = self.index_purge_decision(guards, row_id).await? else {
                        return index
                            .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                            .await
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!(
                                    "operation=delete_unique_index, phase=remove_unreachable_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                    self.table_id()
                                )
                            });
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
        if !access.any_version_matches_key(self.metadata(), index_slot, key_vals) {
            return index
                .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=delete_unique_index, phase=remove_unreferenced_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                        self.table_id()
                    )
                });
        }
        Ok(false)
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        guards: &PoolGuards,
        index_slot: IndexSlot,
        key_vals: &[Val],
        row_id: RowID,
        _min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self
            .require_non_unique_index(guards, index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=delete_non_unique_index, table_id={}, index_slot={index_slot}, row_id={row_id}",
                    self.table_id()
                )
            })?;
        let (page_guard, row_id) = loop {
            let sts = MIN_SNAPSHOT_TS;
            match index
                .lookup_unique(key_vals, row_id, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=delete_non_unique_index, phase=lookup, table_id={}, index_slot={index_slot}, row_id={row_id}",
                        self.table_id()
                    )
                })?
            {
                None => return Ok(false),
                Some(active) => {
                    if active {
                        return Ok(false);
                    }
                    let Some(page_id) = self.index_purge_decision(guards, row_id).await? else {
                        return index
                            .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                            .await
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!(
                                    "operation=delete_non_unique_index, phase=remove_unreachable_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                    self.table_id()
                                )
                            });
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
        if !access.any_version_matches_key(self.metadata(), index_slot, key_vals) {
            return index
                .compare_delete(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=delete_non_unique_index, phase=remove_unreferenced_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                        self.table_id()
                    )
                });
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
    ) -> RuntimeResult<()> {
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
    ) -> RuntimeResult<(PageID, RowID)> {
        let metadata = self.metadata();
        if !disable_dml_validation {
            DmlValidator::new(metadata)
                .validate_full_row(cols)
                .change_context(DataIntegrityError::InvalidPayload)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| format!("operation=insert_no_trx, table_id={}", self.table_id()))?;
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
            let mut page_guard = self.get_insert_page_exclusive(guards, row_count).await?;
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
                self.insert_index_slot_no_trx(guards, key, row_id).await?;
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
    ) -> RuntimeResult<()>
    where
        F: FnOnce(NoTrxUpsertChange),
    {
        let metadata = self.metadata();
        // Every catalog table schema is declared with a primary key, and this
        // no-transaction path is exposed only for those internal tables.
        let primary_key = metadata.primary_key().unwrap_or_else(|| {
            panic!(
                "catalog primary-key no-trx upsert requires primary key: table_id={}",
                self.table_id()
            )
        });
        let primary_key_index_slot = primary_key.index_slot();
        if !disable_dml_validation {
            let validator = DmlValidator::new(metadata);
            validator
                .validate_full_row(&cols)
                .change_context(DataIntegrityError::InvalidPayload)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=upsert_primary_key_no_trx, phase=validate_row, table_id={}",
                        self.table_id()
                    )
                })?;
            validator
                .validate_unique_index(primary_key_index_slot)
                .change_context(DataIntegrityError::InvalidPayload)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=upsert_primary_key_no_trx, phase=validate_primary_key, table_id={}, index_slot={primary_key_index_slot}",
                        self.table_id()
                    )
                })?;
        }
        let key = unique_key_from_full_row(
            metadata,
            primary_key_index_slot,
            &cols,
            "upsert_primary_key_no_trx",
        );
        let current = self
            .index_lookup_unique_uncommitted(guards, key.index_slot, &key.vals, |layout, row| {
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
        let (_, row_id) = self
            .update_primary_key_no_trx_location(
                guards,
                key.index_slot,
                &key.vals,
                &update,
                disable_dml_validation,
            )
            .await?;
        on_change(NoTrxUpsertChange::Updated {
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        disable_dml_validation: bool,
    ) -> RuntimeResult<()> {
        let metadata = self.metadata();
        let index_spec = if disable_dml_validation {
            metadata
                .idx
                .require_index_spec(index_slot)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=delete_primary_key_no_trx, table_id={}, index_slot={index_slot}",
                        self.table_id()
                    )
                })?
        } else {
            validate_primary_key_no_trx_key(
                metadata,
                index_slot,
                key_vals,
                "delete primary key no-trx",
            )?
        };
        let index = self.require_unique_index(guards, index_slot)?;
        let sts = MIN_SNAPSHOT_TS;
        let (mut page_guard, row_id) = match index.lookup(key_vals, sts).await? {
            None => {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "delete primary key no-trx missing catalog row: index_slot={}, key_vals={:?}",
                        index_slot, key_vals
                    ))
                    .change_context(RuntimeError::TableAccess)
                    .attach("operation=validate_catalog_primary_key_payload"));
            }
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!(
                            "delete primary key no-trx row location missing: row_id={row_id}"
                        ))
                        .change_context(RuntimeError::TableAccess)
                        .attach("operation=validate_catalog_primary_key_payload"));
                }
                RowLocation::LwcBlock(..) => {
                    self.catalog_lwc_invariant("delete_primary_key_no_trx", row_id);
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
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "delete primary key no-trx row is deleted: row_id={row_id}"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        let row = page.row(row_idx);
        if row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "delete primary key no-trx row key mismatch: row_id={row_id}, index_slot={index_slot}",
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        let keys = self
            .metadata()
            .idx
            .keys_for_delete(self.metadata().col.as_ref(), row);
        for key in keys {
            let res = self
                .delete_index_directly(guards, key.index_slot, &key.vals, row_id)
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        update: &[UpdateCol],
        disable_dml_validation: bool,
    ) -> RuntimeResult<()> {
        self.update_primary_key_no_trx_location(
            guards,
            index_slot,
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        update: &[UpdateCol],
        disable_dml_validation: bool,
    ) -> RuntimeResult<(PageID, RowID)> {
        let metadata = self.metadata();
        let index_spec = if disable_dml_validation {
            metadata
                .idx
                .require_index_spec(index_slot)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=update_primary_key_no_trx, table_id={}, index_slot={index_slot}",
                        self.table_id()
                    )
                })?
        } else {
            validate_primary_key_no_trx_key(
                metadata,
                index_slot,
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

        let index = self.require_unique_index(guards, index_slot)?;
        let sts = MIN_SNAPSHOT_TS;
        let (mut page_guard, row_id) = match index.lookup(key_vals, sts).await? {
            None => {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "update primary key no-trx missing catalog row: index_slot={}, key_vals={:?}",
                        index_slot, key_vals
                    ))
                    .change_context(RuntimeError::TableAccess)
                    .attach("operation=validate_catalog_primary_key_payload"));
            }
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .attach(format!(
                            "update primary key no-trx row location missing: row_id={row_id}"
                        ))
                        .change_context(RuntimeError::TableAccess)
                        .attach("operation=validate_catalog_primary_key_payload"));
                }
                RowLocation::LwcBlock(..) => {
                    self.catalog_lwc_invariant("update_primary_key_no_trx", row_id);
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
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx row id out of page range: row_id={row_id}"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        let row_idx = page.row_idx(row_id);
        let row = page.row(row_idx);
        if row.is_deleted() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx row is deleted: row_id={row_id}"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        if row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx row key mismatch: row_id={row_id}, index_slot={index_slot}",
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
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
                self.delete_primary_key_no_trx(
                    guards,
                    index_slot,
                    key_vals,
                    disable_dml_validation,
                )
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
    ) -> RuntimeResult<()>
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        row_action: F,
    ) -> RuntimeResult<Option<R>>
    where
        for<'m, 'p> F: FnOnce(&'m TableColumnLayout, Row<'p>) -> R,
    {
        debug_assert!(index_slot.as_usize() < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_slot)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_slot,
            key_vals
        ));
        let index = self.require_unique_index(guards, index_slot)?;
        let sts = MIN_SNAPSHOT_TS;
        let (page_guard, row_id) = match index.lookup(key_vals, sts).await? {
            None => return Ok(None),
            Some((row_id, _)) => match self.find_row(guards, row_id).await? {
                RowLocation::NotFound => return Ok(None),
                RowLocation::LwcBlock(..) => {
                    self.catalog_lwc_invariant("unique_uncommitted_lookup", row_id);
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
        let Some(index_spec) = metadata.idx.index_spec(index_slot) else {
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
    ) -> OperationOrRuntimeResult<RowID> {
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
        // Catalog row allocation can already contribute Runtime-or-Fatal;
        // preserve index Operation-or-Runtime until this existing mixed seam.
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
        unique_index_slot: IndexSlot,
        cols: Vec<Val>,
        log_by_key: bool,
    ) -> QuadResult<UpsertMvcc> {
        let key = unique_key_from_full_row(
            self.metadata(),
            unique_index_slot,
            &cols,
            "upsert_unique_mvcc",
        );
        let input = RowUpdateInput::FullRow(cols);
        match self
            .update_unique_mvcc_input(rt, effects, key.index_slot, &key.vals, input, log_by_key)
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
                    .map_err(Into::into)
            }
        }
    }

    /// Update one row through a unique index in a standalone memory table.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reserved for future memory-only user tables")
    )]
    pub(crate) async fn update_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_slot: IndexSlot,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
        log_by_key: bool,
    ) -> QuadResult<UpdateMvcc> {
        let input = RowUpdateInput::Sparse(update);
        match self
            .update_unique_mvcc_input(rt, effects, index_slot, key_vals, input, log_by_key)
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        input: RowUpdateInput,
        log_by_key: bool,
    ) -> QuadResult<UpdateUniqueMvcc> {
        debug_assert!(index_slot.as_usize() < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_slot)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_slot,
            key_vals
        ));
        debug_assert!(
            input.as_view().is_valid_for(self.metadata().col.as_ref()),
            "row update values must be ordered, in range, and type-compatible"
        );
        let guards = rt.pool_guards();
        let index = self.require_unique_index(guards, index_slot)?;
        loop {
            let lookup_sts = rt.sts();
            let (page_guard, row_id) = match index.lookup(key_vals, lookup_sts).await? {
                None => return Ok(UpdateUniqueMvcc::NotFound(input)),
                Some((row_id, _)) => match self.find_row(guards, row_id).await {
                    Ok(RowLocation::NotFound) => {
                        return Ok(UpdateUniqueMvcc::NotFound(input));
                    }
                    Ok(RowLocation::LwcBlock(..)) => {
                        self.catalog_lwc_invariant("update_unique_mvcc", row_id);
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
                    Err(err) => return Err(err.into()),
                },
            };
            let res = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
                .update_inplace(effects, index_slot, key_vals, input, log_by_key)
                .await?;
            match res {
                UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                    debug_assert!(row_id == new_row_id);
                    if !index_change_cols.is_empty() {
                        self.update_indexes_only_key_change(
                            rt,
                            effects,
                            row_id,
                            &page_guard,
                            &index_change_cols,
                        )
                        .await
                        .attach("update MVCC key-change index update")?;
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
                    unreachable!(
                        "standalone MemTable update observed TRANSITION row page: table_id={}, index_slot={index_slot}, row_id={row_id}",
                        self.table_id()
                    );
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
                        self.update_indexes_may_both_change(
                            rt,
                            effects,
                            old_row_id,
                            new_row_id,
                            &index_change_cols,
                            &new_guard,
                        )
                        .await
                        .attach("update MVCC moved-row index update")?;
                        return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                    }
                    self.update_indexes_only_row_id_change(
                        rt, effects, old_row_id, new_row_id, &new_guard,
                    )
                    .await
                    .attach("update MVCC moved-row index update")?;
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
    ) -> RuntimeResult<(RowID, FastHashMap<usize, Val>, PageSharedGuard<RowPage>)> {
        let mutator = HotRowMutator::new(self.table_id(), self.metadata(), rt, &old_guard, old_id);
        let prepared = mutator.prepare_move_update(old_row, update, |key, target, undo_vals| {
            IndexBranch::new(self.resolved_index_key(key), target, undo_vals)
        });
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
    ) -> OperationOrRuntimeResult<()> {
        let metadata = self.metadata();
        for (index_slot, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_slot), index_schema.unique());
            if index_key_is_changed(index_schema, index_change_cols) {
                let new_key = read_latest_index_key(metadata, index_slot, page_guard, row_id);
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
    ) -> RuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index_slot, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_slot), index_schema.unique());
            let key = read_latest_index_key(metadata, index_slot, page_guard, new_row_id);
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
    ) -> OperationOrRuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index_slot, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_slot), index_schema.unique());
            let key = read_latest_index_key(metadata, index_slot, page_guard, new_row_id);
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
    ) -> OperationOrRuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let operation = "update_unique_index_key_and_row_id_change";
        let index_slot = new_key.index_slot;
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let index = self
            .require_unique_index(guards, index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, table_id={}, index_slot={}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                    self.table_id(), index_slot
                )
            })?;
        loop {
            match index
                .insert_if_not_exists(&new_key.vals, new_row_id, false, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation={operation}, phase=insert_new_key, table_id={}, index_slot={}, new_row_id={new_row_id}",
                        self.table_id(), index_slot
                    )
                })?
            {
                IndexInsert::Ok(merged) => {
                    debug_assert!(!merged);
                    self.push_insert_unique_index_undo(rt, effects, new_row_id, new_key, false);
                    self.defer_delete_unique_index(rt, effects, old_row_id, old_key)
                        .await
                        .attach_with(|| {
                            format!(
                                "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={}, old_row_id={old_row_id}",
                                self.table_id(), index_slot
                            )
                        })?;
                    return Ok(());
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    debug_assert!(index_row_id != new_row_id);
                    if !deleted {
                        return Err(OperationOrRuntimeError::from(
                            Report::new(OperationError::DuplicateKey).attach(format!(
                                "operation={operation}, table_id={}, index_slot={}, new_row_id={new_row_id}",
                                self.table_id(), index_slot
                            )),
                        ));
                    }
                    if index_row_id == old_row_id {
                        match index
                            .compare_exchange(&new_key.vals, old_row_id.deleted(), new_row_id, sts)
                            .await
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!(
                                    "operation={operation}, phase=replace_old_deleted_key, table_id={}, index_slot={}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                                    self.table_id(), index_slot
                                )
                            })?
                        {
                            IndexCompareExchange::Ok => {
                                self.push_update_unique_index_undo(
                                    rt, effects, old_row_id, new_row_id, new_key, true,
                                );
                                self.defer_delete_unique_index(rt, effects, old_row_id, old_key)
                                    .await
                                    .attach_with(|| {
                                        format!(
                                            "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={}, old_row_id={old_row_id}",
                                            self.table_id(), index_slot
                                        )
                                    })?;
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
                            index_slot,
                            &new_key.vals,
                            new_row_id,
                            new_guard,
                        )
                        .await?
                    {
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            let index_old_row_id = index_row_id.deleted();
                            match index
                                .compare_exchange(&new_key.vals, index_old_row_id, new_row_id, sts)
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation={operation}, phase=replace_deleted_key, table_id={}, index_slot={}, new_row_id={new_row_id}",
                                        self.table_id(), index_slot
                                    )
                                })?
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
                                    .attach_with(|| {
                                        format!(
                                            "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={}, old_row_id={old_row_id}",
                                            self.table_id(), index_slot
                                        )
                                    })?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Err(OperationOrRuntimeError::from(
                                        Report::new(OperationError::WriteConflict).attach(format!(
                                            "operation={operation}, table_id={}, index_slot={}, new_row_id={new_row_id}",
                                            self.table_id(), index_slot
                                        )),
                                    ));
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
    ) -> RuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let operation = "update_non_unique_index_key_and_row_id_change";
        let index_slot = new_key.index_slot;
        match self
            .require_non_unique_index(rt.pool_guards(), index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, table_id={}, index_slot={index_slot}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                    self.table_id()
                )
            })?
            .insert_if_not_exists(&new_key.vals, new_row_id, false, rt.sts())
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=insert_new_key, table_id={}, index_slot={index_slot}, new_row_id={new_row_id}",
                    self.table_id()
                )
            })?
        {
            IndexInsert::Ok(merged) => {
                debug_assert!(!merged);
                self.push_insert_non_unique_index_undo(rt, effects, new_row_id, new_key, false);
                self.defer_delete_non_unique_index(rt, effects, old_row_id, old_key)
                    .await
                    .attach_with(|| {
                        format!(
                            "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={index_slot}, old_row_id={old_row_id}",
                            self.table_id()
                        )
                    })?;
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
    ) -> RuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let operation = "update_unique_index_only_row_id_change";
        let index_slot = key.index_slot;
        match self
            .require_unique_index(rt.pool_guards(), index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, table_id={}, index_slot={index_slot}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                    self.table_id()
                )
            })?
            .compare_exchange(&key.vals, old_row_id, new_row_id, rt.sts())
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=replace_row_id, table_id={}, index_slot={index_slot}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                    self.table_id()
                )
            })?
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
    ) -> RuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let operation = "update_non_unique_index_only_row_id_change";
        let index_slot = key.index_slot;
        let res = self
            .require_non_unique_index(rt.pool_guards(), index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, table_id={}, index_slot={index_slot}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                    self.table_id()
                )
            })?
            .insert_if_not_exists(&key.vals, new_row_id, false, rt.sts())
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=insert_new_row_id, table_id={}, index_slot={index_slot}, new_row_id={new_row_id}",
                    self.table_id()
                )
            })?;
        debug_assert!(res.is_ok());
        self.push_insert_non_unique_index_undo(rt, effects, new_row_id, key.clone(), false);
        self.defer_delete_non_unique_index(rt, effects, old_row_id, key)
            .await
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=defer_old_row_id_delete, table_id={}, index_slot={index_slot}, old_row_id={old_row_id}",
                    self.table_id()
                )
            })?;
        Ok(())
    }

    /// Move one unique-index key between row versions without changing row data.
    #[inline]
    pub(crate) async fn update_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationOrRuntimeResult<()> {
        let operation = "update_unique_index_only_key_change";
        let index_slot = new_key.index_slot;
        let sts = rt.sts();
        let guards = rt.pool_guards();
        let index = self
            .require_unique_index(guards, index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, table_id={}, index_slot={index_slot}, row_id={row_id}",
                    self.table_id()
                )
            })?;
        loop {
            match index
                .insert_if_not_exists(&new_key.vals, row_id, true, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation={operation}, phase=insert_new_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                        self.table_id()
                    )
                })?
            {
                IndexInsert::Ok(merged) => {
                    self.push_insert_unique_index_undo(rt, effects, row_id, new_key, merged);
                    self.defer_delete_unique_index(rt, effects, row_id, old_key)
                        .await
                        .attach_with(|| {
                            format!(
                                "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                self.table_id()
                            )
                        })?;
                    return Ok(());
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    if !deleted {
                        return Err(OperationOrRuntimeError::from(
                            Report::new(OperationError::DuplicateKey).attach(format!(
                                "operation={operation}, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                self.table_id()
                            )),
                        ));
                    }
                    match self
                        .link_for_unique_index(
                            rt,
                            index_row_id,
                            index_slot,
                            &new_key.vals,
                            row_id,
                            page_guard,
                        )
                        .await?
                    {
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            match index
                                .compare_exchange(
                                    &new_key.vals,
                                    index_row_id.deleted(),
                                    row_id,
                                    sts,
                                )
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation={operation}, phase=replace_deleted_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                        self.table_id()
                                    )
                                })?
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
                                        .attach_with(|| {
                                            format!(
                                                "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                                self.table_id()
                                            )
                                        })?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Err(OperationOrRuntimeError::from(
                                        Report::new(OperationError::WriteConflict).attach(format!(
                                            "operation={operation}, table_id={}, index_slot={index_slot}, row_id={row_id}",
                                            self.table_id()
                                        )),
                                    ));
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
    ) -> RuntimeResult<()> {
        let operation = "update_non_unique_index_only_key_change";
        let index_slot = new_key.index_slot;
        match self
            .require_non_unique_index(rt.pool_guards(), index_slot)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, table_id={}, index_slot={index_slot}, row_id={row_id}",
                    self.table_id()
                )
            })?
            .insert_if_not_exists(&new_key.vals, row_id, true, rt.sts())
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation={operation}, phase=insert_new_key, table_id={}, index_slot={index_slot}, row_id={row_id}",
                    self.table_id()
                )
            })?
        {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(rt, effects, row_id, new_key, merged);
                self.defer_delete_non_unique_index(rt, effects, row_id, old_key)
                    .await
                    .attach_with(|| {
                        format!(
                            "operation={operation}, phase=defer_old_key_delete, table_id={}, index_slot={index_slot}, row_id={row_id}",
                            self.table_id()
                        )
                    })?;
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> QuadResult<DeleteMvcc> {
        debug_assert!(index_slot.as_usize() < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_slot)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_slot,
            key_vals
        ));
        let guards = rt.pool_guards();
        let index = self.require_unique_index(guards, index_slot)?;
        loop {
            let lookup_sts = rt.sts();
            let (page_guard, row_id) = match index.lookup(key_vals, lookup_sts).await? {
                None => return Ok(DeleteMvcc::NotFound),
                Some((row_id, _)) => match self.find_row(guards, row_id).await {
                    Ok(RowLocation::NotFound) => return Ok(DeleteMvcc::NotFound),
                    Ok(RowLocation::LwcBlock(..)) => {
                        self.catalog_lwc_invariant("delete_unique_mvcc", row_id);
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
                    Err(err) => return Err(err.into()),
                },
            };
            let res = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
                .delete(effects, index_slot, key_vals, log_by_key)
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
                    // Successful row undo ownership excludes another writer,
                    // and deletion changed only the row bit. Copy every key
                    // with one read guard, then release the page latch and
                    // buffer pin before awaiting secondary-index masking.
                    let index_keys =
                        read_physical_index_keys_for_delete(self.metadata(), &page_guard, row_id);
                    drop(page_guard);
                    self.defer_delete_index_keys(rt, effects, row_id, index_keys)
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
        index_slot: IndexSlot,
        key_vals: &[Val],
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        let Some(index_schema) = self.metadata().idx.index_spec(index_slot) else {
            return Ok(false);
        };
        if !self.sec_idx_is_active(index_slot) {
            return Ok(false);
        }
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            self.delete_unique_index(guards, index_slot, key_vals, row_id, min_active_sts)
                .await
        } else {
            self.delete_non_unique_index(guards, index_slot, key_vals, row_id, min_active_sts)
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
        index_slot: IndexSlot,
        built: RuntimeResult<InMemorySecondaryIndex<P>>,
        pool_guard: &PoolGuard,
    ) -> RuntimeResult<()> {
        match built {
            Ok(index) => {
                debug_assert!(self.staged[index_slot.as_usize()].is_none());
                self.staged[index_slot.as_usize()] = Some(index);
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
        for (index_slot, index) in take(&mut self.staged).into_iter().enumerate().rev() {
            let Some(index) = index else {
                continue;
            };
            let index_slot = IndexSlot::try_from(index_slot).unwrap_or_else(|_| {
                panic!("validated runtime index slot exceeds u16: index_slot={index_slot}")
            });
            // Keep the original construction error as the function result,
            // but observe this terminal best-effort cleanup report first.
            if let Err(report) = index.destroy(pool_guard).await {
                let report = report.attach(format!(
                    "operation=rollback_in_memory_secondary_index_build, index_slot={index_slot}"
                ));
                obs::error!(
                    "event=secondary_index_cleanup component=mem_table action=destroy_staged result=error error={report:?}"
                );
            }
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
) -> RuntimeResult<Box<[Option<InMemorySecondaryIndex<I>>]>> {
    let mut builder = InMemorySecondaryIndexScopedBuilder::new(metadata.idx.index_slot_count());
    for (index_slot, index_spec) in metadata.idx.active_indexes() {
        let ty_infer = |col_no: usize| metadata.col.col_type(col_no);
        builder
            .push_or_rollback(
                index_slot,
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
fn wrong_secondary_index_binding(
    expected: &'static str,
    actual: &'static str,
) -> Report<InternalError> {
    Report::new(InternalError::SecondaryIndexBindingMismatch)
        .attach(SecondaryIndexBinding { expected, actual })
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
    for (index_slot, index_spec) in metadata.idx.active_indexes() {
        if !index_key_is_changed(index_spec, &updated_index_vals) {
            continue;
        }
        let old_key_vals = index_spec
            .cols
            .iter()
            .map(|key| row.val(metadata.col.as_ref(), key.col_no as usize))
            .collect();
        let old_key = SelectKey::new(index_slot, old_key_vals);
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

fn ensure_no_trx_index_insert(index_slot: IndexSlot, res: IndexInsert) -> RuntimeResult<()> {
    match res {
        IndexInsert::Ok(_) => Ok(()),
        IndexInsert::DuplicateKey(row_id, deleted) => Err(Report::new(
            DataIntegrityError::UnexpectedRecoveryDuplicateKey,
        )
        .attach(RecoveryDuplicateKey {
            index_slot: index_slot.as_usize(),
            row_id,
            deleted,
        }))
        .change_context(RuntimeError::TableAccess)
        .attach_with(|| {
            format!(
                "operation=insert_index_slot_no_trx, index_slot={index_slot}, row_id={row_id}, deleted={deleted}"
            )
        }),
    }
}

#[inline]
fn validate_update_primary_key_no_trx_cols(
    metadata: &TableMetadata,
    update: &[UpdateCol],
) -> RuntimeResult<()> {
    let mut last_idx = None;
    for update_col in update {
        if update_col.idx >= metadata.col.col_count() {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx column out of range: column_no={}, column_count={}",
                    update_col.idx,
                    metadata.col.col_count()
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        if last_idx.is_some_and(|idx| update_col.idx <= idx) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx columns not strictly ordered: column_no={}",
                    update_col.idx
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
        if !metadata.col.col_type_match(update_col.idx, &update_col.val) {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx column type mismatch: column_no={}",
                    update_col.idx
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
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
) -> RuntimeResult<()> {
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
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "update primary key no-trx cannot change primary key column: column_no={}",
                    update_col.idx
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"));
        }
    }
    Ok(())
}

#[inline]
fn validate_primary_key_no_trx_key<'a>(
    metadata: &'a TableMetadata,
    index_slot: IndexSlot,
    key_vals: &[Val],
    operation: &'static str,
) -> RuntimeResult<&'a IndexSpec> {
    let Some(primary_key) = metadata.primary_key() else {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!("{operation} primary key not found"))
            .change_context(RuntimeError::TableAccess)
            .attach("operation=validate_catalog_primary_key_payload"));
    };
    match primary_key.validate_key(index_slot, key_vals) {
        Ok(()) => Ok(primary_key.spec()),
        Err(PrimaryKeyMatchError::IndexSlot { actual, expected }) => Err(Report::new(
            DataIntegrityError::InvalidPayload,
        )
        .attach(format!(
                "{operation} key is not primary key: index_slot={actual}, primary_key_index_slot={expected}",
            ))
        .change_context(RuntimeError::TableAccess)
        .attach("operation=validate_catalog_primary_key_payload")),
        Err(PrimaryKeyMatchError::ValueCount { actual, expected }) => Err(Report::new(
            DataIntegrityError::InvalidPayload,
        )
        .attach(format!(
                "{operation} key value count {actual} does not match primary key column count {expected}",
            ))
        .change_context(RuntimeError::TableAccess)
        .attach("operation=validate_catalog_primary_key_payload")),
        Err(PrimaryKeyMatchError::Type { index_slot }) => {
            Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "{operation} key type mismatch: index_slot={index_slot}"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach("operation=validate_catalog_primary_key_payload"))
        }
    }
}

#[inline]
fn invalid_scan_start<T>(table_id: TableID, start_row_id: RowID) -> InternalResult<T> {
    Err(Report::new(InternalError::RowPageScanStartInvalid))
        .attach_with(|| {
            format!(
                "table_id={table_id}, start_row_id={start_row_id}, row-page scan start is not a row-page boundary"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{MemTable, NoTrxUpsertChange};
    use crate::buffer::guard::PageGuard;
    use crate::buffer::page::VersionedPageID;
    use crate::buffer::{BufferPool, EvictableBufferPool};
    use crate::buffer::{PoolGuards, PoolRole};
    use crate::catalog::catalog_key_from_active_ordinal;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKeySpec, IndexSlot, IndexSpec,
        TableMetadata,
    };
    use crate::engine::Engine;
    use crate::error::{
        DataIntegrityError, InternalError, LifecycleError, OperationError, ResourceError,
        RuntimeError,
    };
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::id::{RowID, TableID, TrxID};
    use crate::index::{BlockIndex, RowLocation};
    use crate::row::RowRead;
    use crate::row::ops::{DeleteMvcc, SelectKey, UpdateCol, UpdateMvcc, UpsertMvcc};
    use crate::session::{
        Session,
        tests::{SessionTestExt, assert_checkpoint_published, wait_for_session_idle},
    };
    use crate::table::tests::*;
    use crate::trx::tests::{
        mem_table_delete_unique_mvcc, mem_table_duplicate_index_key_change, mem_table_insert_mvcc,
        mem_table_update_unique_mvcc, mem_table_upsert_unique_mvcc, shared_trx_status,
    };
    use crate::trx::undo::{OwnedRowUndo, RowUndoHead, RowUndoKind, RowUndoRollbackAttempt};
    use crate::trx::ver_map::RowPageState;
    use crate::trx::{MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS, NON_FOREGROUND_STMT_NO};
    use crate::value::{Val, ValKind};
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;
    use std::ptr::addr_eq;
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
                    IndexSpec::new(vec![IndexKeySpec::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKeySpec::new(1)], IndexAttributes::empty()),
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
                    IndexSpec::new(vec![IndexKeySpec::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKeySpec::new(1)], IndexAttributes::UK),
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
                vec![IndexSpec::new(
                    vec![IndexKeySpec::new(0)],
                    IndexAttributes::PK,
                )],
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
        let meta_guard = engine.inner().pools.meta.create_base_guard();
        let index_guard = engine.inner().pools.index.create_base_guard();
        let mem_pool = engine.inner().pools.mem.clone();
        let blk_idx = BlockIndex::new(
            engine.inner().pools.meta.clone(),
            &meta_guard,
            RowID::new(0),
            SUPER_BLOCK_ID,
        )
        .await
        .unwrap();
        MemTable::new(
            mem_pool.clone(),
            mem_pool.row_pool_role(),
            engine.inner().pools.index.clone(),
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
            index_slot: IndexSlot::new(1),
            vals: vec![Val::from(value)],
        }
    }

    fn indexed_payload_row(id: i32, name: &str, payload: &[u8]) -> Vec<Val> {
        vec![Val::from(id), Val::from(name), Val::from(payload)]
    }

    async fn insert_mem_mvcc(
        session: &mut Session,
        mem_table: &TestMemTable,
        cols: Vec<Val>,
    ) -> RowID {
        let mut trx = session.begin_trx().unwrap();
        let row_id = mem_table_insert_mvcc(&mut trx, mem_table, cols)
            .await
            .unwrap();
        trx.commit().await.unwrap();
        row_id
    }

    async fn update_mem_unique_mvcc(
        session: &mut Session,
        mem_table: &TestMemTable,
        key: SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        let mut trx = session.begin_trx().unwrap();
        let updated = mem_table_update_unique_mvcc(&mut trx, mem_table, &key, update)
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
            .require_unique_index(guards, key.index_slot)
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
            .require_non_unique_index(guards, key.index_slot)
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
            .index_lookup_unique_uncommitted(guards, key.index_slot, &key.vals, |layout, row| {
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
            let inserted = mem_table_upsert_unique_mvcc(
                &mut trx,
                &mem_table,
                vec![Val::from(1i32), Val::from("hello")],
            )
            .await
            .unwrap();
            let inserted_row_id = match inserted {
                UpsertMvcc::Inserted(row_id) => row_id,
                UpsertMvcc::Updated(row_id) => panic!("unexpected update row_id={row_id}"),
            };
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let updated = mem_table_upsert_unique_mvcc(
                &mut trx,
                &mem_table,
                vec![Val::from(1i32), Val::from("world")],
            )
            .await
            .unwrap();
            assert_eq!(updated, UpsertMvcc::Updated(inserted_row_id));
            trx.commit().await.unwrap();

            let key = single_key(1i32);
            let row = mem_table
                .index_lookup_unique_uncommitted(
                    &session.pool_guards(),
                    key.index_slot,
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
                mem_table_upsert_unique_mvcc(
                    &mut trx1,
                    &mem_table,
                    vec![Val::from(2i32), Val::from("first")],
                )
                .await
                .unwrap(),
                UpsertMvcc::Inserted(_)
            ));

            let mut session2 = engine.new_session().unwrap();
            let mut trx2 = session2.begin_trx().unwrap();
            let err = mem_table_upsert_unique_mvcc(
                &mut trx2,
                &mem_table,
                vec![Val::from(2i32), Val::from("second")],
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::WriteConflict)
            );
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
                .require_unique_index(&guards, key1.index_slot)
                .unwrap()
                .lookup(&key1.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap()
                .expect("first unique index entry should exist");
            let key2 = single_key(2i32);
            let (row2, row2_deleted) = mem_table
                .require_unique_index(&guards, key2.index_slot)
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
                .delete_primary_key_no_trx(&guards, key1.index_slot, &key1.vals, false)
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
            let insert_row_id = match inserted.unwrap() {
                NoTrxUpsertChange::Inserted { row_id, vals, .. } => {
                    assert_eq!(vals, initial);
                    row_id
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
                NoTrxUpsertChange::Updated { row_id, key, cols } => {
                    assert_eq!(row_id, insert_row_id);
                    assert_eq!(
                        key,
                        catalog_key_from_active_ordinal(0, vec![Val::from(1i32)])
                    );
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
                .delete_primary_key_no_trx(&guards, key.index_slot, &key.vals, false)
                .await
                .unwrap_err();

            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
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
                .delete_primary_key_no_trx(&guards, key.index_slot, &key.vals, false)
                .await
                .unwrap_err();
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );

            mem_table
                .delete_primary_key_no_trx(&guards, key.index_slot, &key.vals, true)
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
            assert_eq!(err.current_context(), &RuntimeError::TableAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
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
                    key.index_slot,
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
                    key.index_slot,
                    &key.vals,
                    &[UpdateCol {
                        idx: 0,
                        val: Val::from(1i32),
                    }],
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(err.current_context(), &RuntimeError::TableAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
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
                .require_unique_index(&guards, key.index_slot)
                .unwrap()
                .lookup(&key.vals, MIN_SNAPSHOT_TS)
                .await
                .unwrap()
                .expect("inserted primary key should be indexed");
            assert!(!deleted);

            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_slot,
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
                    key.index_slot,
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
                    key.index_slot,
                    &key.vals,
                    &[UpdateCol {
                        idx: 2,
                        val: Val::from(&b"new"[..]),
                    }],
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(err.current_context(), &RuntimeError::TableAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidPayload)
            );

            mem_table
                .update_primary_key_no_trx(
                    &guards,
                    key.index_slot,
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
                    key.index_slot,
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
                    key.index_slot,
                    &key.vals,
                    &[UpdateCol {
                        idx: 1,
                        val: Val::from("two"),
                    }],
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(err.current_context(), &RuntimeError::TableAccess);
            assert_eq!(
                err.downcast_ref::<DataIntegrityError>().copied(),
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
                    .require_unique_index(&guards, key.index_slot)
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
                    key.index_slot,
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
                .require_unique_index(&guards, key.index_slot)
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
            let deleted = mem_table_delete_unique_mvcc(&mut trx, &mem_table, &single_key(10i32))
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
                    key.index_slot,
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
                &mem_table,
                indexed_payload_row(1, "old", b"payload"),
            )
            .await;
            insert_mem_mvcc(
                &mut session,
                &mem_table,
                indexed_payload_row(20, "other", b"payload"),
            )
            .await;

            let updated = update_mem_unique_mvcc(
                &mut session,
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

            let page_id = match mem_table
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::NotFound => panic!("updated row should exist"),
                RowLocation::LwcBlock(..) => panic!("standalone MemTable should not use LWC"),
            };
            let page_guard = mem_table
                .must_get_row_page_shared(&session.pool_guards(), page_id)
                .await
                .unwrap();
            let mut trx = session.begin_trx().unwrap();
            let err = mem_table_duplicate_index_key_change(
                &mut trx,
                &mem_table,
                page_guard,
                row_id,
                single_key(10i32),
                single_key(20i32),
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::DuplicateKey)
            );
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
    #[should_panic(expected = "catalog table unexpectedly resolved a persisted LWC row")]
    fn test_mem_table_catalog_lwc_invariant_panics() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_014);
            let mem_table =
                test_mem_table_with_metadata(&engine, mem_table_id, indexed_payload_metadata())
                    .await;

            mem_table.catalog_lwc_invariant("test_catalog_lwc", RowID::new(42));
        });
    }

    #[test]
    fn test_mem_table_transition_update_and_delete_panic() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let mem_table_id = test_user_table_id(10_015);
            let mem_table = test_mem_table(&engine, mem_table_id).await;
            let mut session = engine.new_session().unwrap();
            let row_id = insert_mem_mvcc(
                &mut session,
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
                RowLocation::LwcBlock(..) => panic!("standalone MemTable should not use LWC"),
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
            let panic = AssertUnwindSafe(mem_table_update_unique_mvcc(
                &mut trx,
                &mem_table,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("updated"),
                }],
            ))
            .catch_unwind()
            .await
            .expect_err("standalone MemTable update must reject a transition page");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            assert!(
                message.contains("standalone MemTable update observed TRANSITION row page"),
                "unexpected panic: {message}"
            );
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            wait_for_session_idle(&engine.inner().session_registry, session.id()).await;

            let mut trx = session.begin_trx().unwrap();
            let panic = AssertUnwindSafe(mem_table_delete_unique_mvcc(&mut trx, &mem_table, &key))
                .catch_unwind()
                .await
                .expect_err("standalone MemTable delete must reject a transition page");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            assert!(
                message.contains("standalone MemTable delete observed TRANSITION row page"),
                "unexpected panic: {message}"
            );
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            wait_for_session_idle(&engine.inner().session_registry, session.id()).await;
            engine.shutdown();
        });
    }

    #[test]
    fn test_hot_row_undo_attempt_classifies_page_state_and_generation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "row_undo_attempt").await;
            let table_id = test_user_table_id(10_016);
            let mem_table = test_mem_table(&engine, table_id).await;
            let mut session = engine.new_session().unwrap();
            let row_id = insert_mem_mvcc(
                &mut session,
                &mem_table,
                vec![Val::from(1i32), Val::from("rollback")],
            )
            .await;
            let page_id = match mem_table
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::NotFound => panic!("inserted rollback row should exist"),
                RowLocation::LwcBlock(..) => panic!("standalone MemTable should remain hot"),
            };

            for state in [RowPageState::Active, RowPageState::Frozen] {
                let page_guard = mem_table
                    .must_get_row_page_shared(&session.pool_guards(), page_id)
                    .await
                    .unwrap();
                *page_guard.unwrap_vmap().write_state() = state;
                let status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 100));
                let mut undo = OwnedRowUndo::new(
                    NON_FOREGROUND_STMT_NO,
                    table_id,
                    Some(page_guard.versioned_page_id()),
                    row_id,
                    RowUndoKind::Delete,
                );
                let row_idx = page_guard.page().row_idx(row_id);
                *page_guard.unwrap_vmap().write_latch(row_idx) =
                    Some(Box::new(RowUndoHead::new(status, undo.leak())));
                page_guard.write_row_by_id(row_id).delete_row();
                assert!(page_guard.page().is_deleted(row_idx));
                drop(page_guard);

                assert_eq!(
                    mem_table
                        .try_rollback_hot_row_undo(&mut undo, &session.pool_guards())
                        .await
                        .unwrap(),
                    RowUndoRollbackAttempt::Applied
                );
                let page_guard = mem_table
                    .must_get_row_page_shared(&session.pool_guards(), page_id)
                    .await
                    .unwrap();
                assert!(!page_guard.page().is_deleted(row_idx));
                assert!(page_guard.unwrap_vmap().read_latch(row_idx).is_none());
            }

            let page_guard = mem_table
                .must_get_row_page_shared(&session.pool_guards(), page_id)
                .await
                .unwrap();
            *page_guard.unwrap_vmap().write_state() = RowPageState::Frozen;
            let status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 101));
            let mut undo = OwnedRowUndo::new(
                NON_FOREGROUND_STMT_NO,
                table_id,
                Some(page_guard.versioned_page_id()),
                row_id,
                RowUndoKind::Delete,
            );
            let row_idx = page_guard.page().row_idx(row_id);
            *page_guard.unwrap_vmap().write_latch(row_idx) =
                Some(Box::new(RowUndoHead::new(status, undo.leak())));
            page_guard.write_row_by_id(row_id).delete_row();
            *page_guard.unwrap_vmap().write_state() = RowPageState::Transition;
            let row_before = page_guard
                .page()
                .row(row_idx)
                .clone_vals(mem_table.metadata().col.as_ref());
            let dirty_before = page_guard.bf().is_dirty();
            let frozen_version_before = page_guard.unwrap_vmap().frozen_mutation_version();
            drop(page_guard);

            assert_eq!(
                mem_table
                    .try_rollback_hot_row_undo(&mut undo, &session.pool_guards())
                    .await
                    .unwrap(),
                RowUndoRollbackAttempt::Transition
            );
            let page_guard = mem_table
                .must_get_row_page_shared(&session.pool_guards(), page_id)
                .await
                .unwrap();
            assert_eq!(
                page_guard.unwrap_vmap().inspect_state(),
                RowPageState::Transition
            );
            assert!(page_guard.page().is_deleted(row_idx));
            assert_eq!(
                page_guard
                    .page()
                    .row(row_idx)
                    .clone_vals(mem_table.metadata().col.as_ref()),
                row_before
            );
            let undo_guard = page_guard.unwrap_vmap().read_latch(row_idx);
            let undo_head = undo_guard
                .as_ref()
                .expect("transition attempt must retain the exact undo head");
            assert!(addr_eq(undo_head.next.main.entry.as_ref(), &*undo));
            drop(undo_guard);
            assert_eq!(page_guard.bf().is_dirty(), dirty_before);
            assert_eq!(
                page_guard.unwrap_vmap().frozen_mutation_version(),
                frozen_version_before
            );
            *page_guard.unwrap_vmap().write_state() = RowPageState::Frozen;
            drop(page_guard);
            assert_eq!(
                mem_table
                    .try_rollback_hot_row_undo(&mut undo, &session.pool_guards())
                    .await
                    .unwrap(),
                RowUndoRollbackAttempt::Applied
            );

            let page_guard = mem_table
                .must_get_row_page_shared(&session.pool_guards(), page_id)
                .await
                .unwrap();
            *page_guard.unwrap_vmap().write_state() = RowPageState::Active;
            let versioned_page_id = page_guard.versioned_page_id();
            drop(page_guard);
            let mut stale_undo = OwnedRowUndo::new(
                NON_FOREGROUND_STMT_NO,
                table_id,
                Some(VersionedPageID {
                    page_id: versioned_page_id.page_id,
                    generation: versioned_page_id.generation.saturating_add(1),
                }),
                row_id,
                RowUndoKind::Delete,
            );
            assert_eq!(
                mem_table
                    .try_rollback_hot_row_undo(&mut stale_undo, &session.pool_guards())
                    .await
                    .unwrap(),
                RowUndoRollbackAttempt::PageMissing
            );
        });
    }

    #[test]
    fn test_mem_scan_uncommitted_from_current_pivot() {
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
                let table = table_for_internal_assertion(&engine, table_id);
                let layout = table.layout_snapshot();
                let pivot_row_id = table.file().active_root_unchecked().pivot_row_id;
                table
                    .accessor_with_layout(&layout)
                    .mem_scan_uncommitted_from(
                        &session.pool_guards(),
                        pivot_row_id,
                        |_metadata, _row| {
                            res_len += 1;
                            true
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(res_len, SIZE as usize);
            }
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
            assert_eq!(err.current_context(), &RuntimeError::TableAccess);
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::RowPageScanStartInvalid)
            );

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
                ColumnAttributes, ColumnSpec, IndexAttributes, IndexKeySpec, IndexSpec,
                TableMetadata,
            };
            use crate::quiescent::QuiescentBox;
            use crate::value::ValKind;
            use std::mem::size_of;

            let pool_bytes = size_of::<BufferFrame>() + size_of::<Page>();
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, pool_bytes)
                    .expect("one-page fixed index pool should be constructible"),
            );
            let pool_guard = (*pool).create_base_guard();
            let metadata = TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "id",
                    ValKind::I32,
                    ColumnAttributes::empty(),
                )],
                vec![
                    IndexSpec::new(vec![IndexKeySpec::new(0)], IndexAttributes::PK),
                    IndexSpec::new(vec![IndexKeySpec::new(0)], IndexAttributes::empty()),
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
            assert_eq!(
                err.downcast_ref::<ResourceError>().copied(),
                Some(ResourceError::BufferPoolFull)
            );
            assert_eq!(pool.allocated(), 0);
        });
    }
}
