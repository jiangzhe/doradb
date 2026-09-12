use super::validate_page_row_range;
use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::VersionedPageID;
use crate::buffer::{BufferPool, PoolGuard, PoolGuards, RowPoolRole, get_page_versioned_shared};
use crate::catalog::TableColumnLayout;
use crate::error::{
    InternalError, InternalResult, MultiDomainResultExt, RuntimeError, RuntimeOrFatalResult,
    RuntimeOrFatalResultExt, RuntimeResult,
};
use crate::id::{PageID, RowID, TableID};
use crate::index::util::RowPageCreateRedoCtx;
use crate::index::{BlockIndex, RowLocation};
use crate::latch::LatchFallbackMode;
use crate::quiescent::QuiescentGuard;
use crate::row::RowPage;
use crate::trx::undo::{ForwardLinkUndo, HotForwardSource, OwnedRowUndo, RowUndoRollbackAttempt};
use crate::trx::ver_map::RowPageState;
use crate::trx::{RetiredRowPageBatch, TrxRuntime};
use error_stack::{Report, ResultExt};
use std::sync::Arc;

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

/// Physical hot row storage with a stable column layout and table identity.
/// Active index metadata and runtimes belong to the complete table's layout.
pub(crate) struct RowStore<D: 'static> {
    table_id: TableID,
    column_layout: Arc<TableColumnLayout>,
    /// Buffer pool owning this table's row-page frames.
    pub(crate) mem_pool: QuiescentGuard<D>,
    row_pool_role: RowPoolRole,
    /// Hot row-page routing and published cold-row boundary.
    pub(crate) blk_idx: BlockIndex,
}

impl<D: BufferPool> RowStore<D> {
    /// Binds physical resources to the owning table and its stable row shape.
    #[inline]
    pub(crate) fn new(
        table_id: TableID,
        column_layout: Arc<TableColumnLayout>,
        mem_pool: QuiescentGuard<D>,
        row_pool_role: RowPoolRole,
        blk_idx: BlockIndex,
    ) -> Self {
        Self {
            table_id,
            column_layout,
            mem_pool,
            row_pool_role,
            blk_idx,
        }
    }

    /// Returns the owning table's identity.
    #[inline]
    pub(crate) fn table_id(&self) -> TableID {
        self.table_id
    }

    /// Returns the stable column allocation used to interpret row bytes.
    #[inline]
    pub(crate) fn column_layout(&self) -> &Arc<TableColumnLayout> {
        &self.column_layout
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

    /// Returns the row-id boundary between persisted and in-memory rows.
    #[inline]
    pub(crate) fn pivot_row_id(&self) -> RowID {
        self.blk_idx.pivot_row_id()
    }

    #[inline]
    fn row_pool_guard<'a>(&self, guards: &'a PoolGuards) -> &'a PoolGuard {
        // Catalog row pages use Meta; user-table row pages use Mem. Runtime
        // construction installs the guard matching this immutable role.
        guards.row_guard(self.row_pool_role)
    }

    /// Destroys the hot block index and its row pages after index cleanup.
    #[inline]
    pub(crate) async fn destroy(self, guards: &PoolGuards) -> RuntimeResult<()> {
        let row_pool_guard = self.row_pool_guard(guards);
        let meta_pool_guard = guards.meta_guard();
        let table_id = self.table_id;
        self.blk_idx
            .destroy(meta_pool_guard, &*self.mem_pool, row_pool_guard)
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
                guards.meta_guard(),
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

    /// Locks a known allocated row page, returning `None` if its frame generation
    /// changes while acquiring shared access.
    #[inline]
    async fn get_row_page_shared(
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

    /// Reopens a captured hot row page and validates its descriptor's row range.
    pub(super) async fn get_captured_row_page_shared(
        &self,
        guards: &PoolGuards,
        descriptor: RowPageDescriptor,
    ) -> RuntimeResult<PageSharedGuard<RowPage>> {
        let page_guard = self
            .get_row_page_shared(guards, descriptor.page_id)
            .await
            .attach_with(|| {
                format!(
                    "operation=load_table_scan_hot_page, page_id={}",
                    descriptor.page_id
                )
            })?
            .ok_or_else(|| {
                Report::new(InternalError::CapturedRowPageUnavailable)
                    .attach(format!(
                        "captured page is missing: table_id={}, page_id={}, start_row_id={}, end_row_id={}",
                        self.table_id(),
                        descriptor.page_id,
                        descriptor.start_row_id,
                        descriptor.end_row_id
                    ))
                    .change_context(RuntimeError::TableAccess)
            })?;
        let page = page_guard.page();
        let row_end = page
            .header
            .start_row_id
            .checked_add(page.header.row_count() as u64);
        if page.header.start_row_id != descriptor.start_row_id
            || row_end.is_none_or(|row_end| row_end > descriptor.end_row_id)
        {
            return Err(Report::new(InternalError::CapturedRowPageUnavailable)
                .attach(format!(
                    "captured page identity changed: table_id={}, page_id={}, expected_start={}, expected_end={}, actual_start={}, actual_rows={}",
                    self.table_id(),
                    descriptor.page_id,
                    descriptor.start_row_id,
                    descriptor.end_row_id,
                    page.header.start_row_id,
                    page.header.row_count()
                ))
                .change_context(RuntimeError::TableAccess));
        }
        Ok(page_guard)
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

    /// Pins an optional exact source page before an index exchange can publish a hint.
    /// A reclaimed source must have a published cold route, which retains fallback.
    #[inline]
    pub(super) async fn pin_forward_source(
        &self,
        rt: TrxRuntime<'_>,
        source: Option<&HotForwardSource>,
    ) -> RuntimeResult<Option<PageSharedGuard<RowPage>>> {
        let Some(source) = source else {
            return Ok(None);
        };
        let page = self
            .get_row_page_versioned_shared(rt.pool_guards(), source.page_id)
            .await?;
        assert!(
            page.is_some()
                || (!self.table_id().is_catalog() && source.row_id < self.pivot_row_id()),
            "missing forward source requires published cold routing: table_id={}, row_id={}",
            self.table_id(),
            source.row_id
        );
        Ok(page)
    }

    /// Restores one exact source slot before rolling back its destination row.
    #[inline]
    pub(crate) async fn try_restore_forward_link(
        &self,
        undo: &ForwardLinkUndo,
        guards: &PoolGuards,
    ) -> RuntimeResult<RowUndoRollbackAttempt> {
        let Some(page) = self
            .get_row_page_versioned_shared(guards, undo.source.page_id)
            .await?
        else {
            return Ok(RowUndoRollbackAttempt::PageMissing);
        };
        let mut access = page.write_row_by_id(undo.source.row_id);
        if access.page_state() == RowPageState::Transition {
            return Ok(RowUndoRollbackAttempt::Transition);
        }
        let restored = access.with_forward_source(&undo.source, |links| {
            links.restore(undo.index, undo.previous);
        });
        assert!(
            restored.is_some(),
            "forward rollback requires its exact writer-owned source: table_id={}, row_id={}, index={}",
            self.table_id(),
            undo.source.row_id,
            undo.index
        );
        Ok(RowUndoRollbackAttempt::Applied)
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
        let column_layout = self.column_layout();
        let mut access =
            page_guard.write_row_with_state_guard(page.row_idx(entry.row_id), state_guard);
        access.rollback_first_undo(column_layout, entry);
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
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .try_get_insert_page(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.column_layout,
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
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .try_get_insert_page_with_redo(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.column_layout,
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
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .get_insert_page_exclusive(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.column_layout,
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
        let meta_pool_guard = guards.meta_guard();
        let row_pool_guard = self.row_pool_guard(guards);
        self.blk_idx
            .allocate_row_page_at(
                meta_pool_guard,
                self.mem_pool(),
                row_pool_guard,
                &self.column_layout,
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
        let meta_pool_guard = guards.meta_guard();
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
        let meta_pool_guard = guards.meta_guard();
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
        let meta_pool_guard = guards.meta_guard();
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
        let meta_pool_guard = guards.meta_guard();
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

    /// Locks a row page only when its reserved range contains the requested row.
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
