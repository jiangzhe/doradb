mod access;
mod deletion_buffer;
mod recover;
#[cfg(test)]
mod tests;

pub use access::*;
pub use deletion_buffer::*;
pub use recover::*;

use crate::buffer::guard::{PageExclusiveGuard, PageGuard, PageSharedGuard};
use crate::buffer::page::PageID;
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool};
use crate::catalog::TableMetadata;
use crate::catalog::{IndexSpec, TableID};
use crate::error::{Error, Result};
use crate::file::table_file::{LwcPagePersist, TableFile};
use crate::index::util::Maskable;
use crate::index::{
    BlockIndex, IndexCompareExchange, IndexInsert, NonUniqueBTreeIndex, NonUniqueIndex,
    RowLocation, SecondaryIndex, UniqueBTreeIndex, UniqueIndex,
};
use crate::latch::LatchFallbackMode;
use crate::lwc::{LwcBuilder, LwcPage};
use crate::row::ops::{
    InsertIndex, LinkForUniqueIndex, ReadRow, Recover, RecoverIndex, SelectKey, SelectMvcc,
    UndoCol, UpdateCol, UpdateIndex, UpdateRow,
};
use crate::row::{RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::stmt::Statement;
use crate::trx::redo::{RowRedo, RowRedoKind};
use crate::trx::row::{FindOldVersion, LockRowForWrite, LockUndo, RowReadAccess, RowWriteAccess};
use crate::trx::sys::TransactionSystem;
use crate::trx::undo::{IndexBranch, RowUndoKind, UndoStatus};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MIN_SNAPSHOT_TS, TrxID, trx_is_committed};
use crate::value::{PAGE_VAR_LEN_INLINE, Val};
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(test)]
static TEST_FORCE_LWC_BUILD_ERROR: AtomicBool = AtomicBool::new(false);

/// Table is a logical data set of rows.
/// It combines components such as row page, undo map, block index, secondary
/// index to provide full-featured CRUD and MVCC functionalities.
///
/// The basic flow is:
///
/// secondary index -> block index -> buffer pool -> row page -> undo map.
///
/// 1. secondary index stores mapping from key to row id.
///
/// 2. block index stores mapping from row id to page.
///
/// 3. Buffer pool takes care of creating and fetching pages.
///
/// 4. Row page stores latest version fo row data.
///
/// 5. Undo map stores old versions of row data.
///
/// We have a separate undo array associated to each row in row page.
///
/// The undo head also acts as *physical* row lock, so that threads need to
/// synchronize the row access.
///
/// Undo entry with uncommitted timestamp represents *logical* row lock and
/// only released once transaction commits.
///
/// Insert/update/delete operation will add one or more undo entry to the
/// chain linked to undo head.
///
/// Select operation will traverse undo chain to find visible version.
///
/// Additional key validation is performed if index lookup is used, because
/// index does not contain version information, and out-of-date index entry
/// should ignored if visible data version does not match index key.
pub struct Table {
    pub data_pool: &'static EvictableBufferPool,
    pub file: Arc<TableFile>,
    pub blk_idx: Arc<BlockIndex>,
    pub sec_idx: Arc<[SecondaryIndex]>,
    deletion_buffer: Arc<ColumnDeletionBuffer>,
}

struct FrozenPage {
    page_id: PageID,
    start_row_id: RowID,
    end_row_id: RowID,
}

impl Table {
    /// Create a new table.
    #[inline]
    pub async fn new(
        data_pool: &'static EvictableBufferPool,
        index_pool: &'static FixedBufferPool,
        blk_idx: BlockIndex,
        file: Arc<TableFile>,
    ) -> Self {
        let active_root = file.active_root();
        let mut sec_idx = Vec::with_capacity(active_root.metadata.index_specs.len());
        for (index_no, index_spec) in active_root.metadata.index_specs.iter().enumerate() {
            let ty_infer = |col_no: usize| active_root.metadata.col_type(col_no);
            let si = SecondaryIndex::new(
                index_pool,
                index_no,
                index_spec,
                ty_infer,
                active_root.trx_id,
            )
            .await;
            sec_idx.push(si);
        }
        Table {
            data_pool,
            file,
            blk_idx: Arc::new(blk_idx),
            sec_idx: Arc::from(sec_idx.into_boxed_slice()),
            deletion_buffer: Arc::new(ColumnDeletionBuffer::new()),
        }
    }

    /// Returns table id.
    #[inline]
    pub fn table_id(&self) -> TableID {
        self.blk_idx.table_id
    }

    /// Returns current pivot row id for row-store/column-store boundary.
    #[inline]
    pub fn pivot_row_id(&self) -> RowID {
        self.file.active_root().pivot_row_id
    }

    #[inline]
    pub fn deletion_buffer(&self) -> &ColumnDeletionBuffer {
        &self.deletion_buffer
    }

    async fn collect_frozen_pages(&self, pivot_row_id: RowID) -> (Vec<FrozenPage>, Option<TrxID>) {
        let mut frozen_pages = Vec::new();
        let mut expected_row_id = pivot_row_id;
        let mut heap_redo_start_ts = None;
        self.mem_scan(pivot_row_id, |page_guard| {
            let page = page_guard.page();
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
        .await;
        (frozen_pages, heap_redo_start_ts)
    }

    async fn wait_for_frozen_pages_stable(
        &self,
        trx_sys: &'static TransactionSystem,
        frozen_pages: &[FrozenPage],
    ) {
        loop {
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let mut stabilized = true;
            for page_info in frozen_pages {
                // A potential optimization is to check row version map without loading
                // row page back. This requires interface change of buffer pool.
                let page_guard = self
                    .data_pool
                    .get_page::<RowPage>(page_info.page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;
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
    }

    async fn set_frozen_pages_to_transition(&self, frozen_pages: &[FrozenPage]) {
        for page_info in frozen_pages {
            let page_guard = self
                .data_pool
                .get_page::<RowPage>(page_info.page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
            let (ctx, _) = page_guard.ctx_and_page();
            ctx.row_ver().unwrap().set_transition();
        }
    }

    async fn build_lwc_pages(
        &self,
        trx_sys: &'static TransactionSystem,
        sts: TrxID,
        frozen_pages: &[FrozenPage],
    ) -> Result<Vec<LwcPagePersist>> {
        #[cfg(test)]
        {
            if TEST_FORCE_LWC_BUILD_ERROR.load(Ordering::SeqCst) {
                return Err(crate::error::Error::InvalidState);
            }
        }
        let mut lwc_pages = Vec::new();
        if !frozen_pages.is_empty() {
            let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
            let metadata = self.metadata();
            let mut builder = LwcBuilder::new(metadata);
            let mut current_start: RowID = 0;
            let mut current_end: RowID = 0;
            for page_info in frozen_pages {
                let page_guard = self
                    .data_pool
                    .get_page::<RowPage>(page_info.page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;
                let (ctx, page) = page_guard.ctx_and_page();
                self.capture_uncommitted_deletes(page, ctx);
                let view = page.vector_view_in_transition(metadata, ctx, sts, min_active_sts);
                if view.rows_non_deleted() == 0 {
                    continue;
                }
                if builder.is_empty() {
                    current_start = page_info.start_row_id;
                    current_end = page_info.end_row_id;
                }
                if !builder.append_view(page, view)? {
                    let buf = builder.build()?;
                    lwc_pages.push(LwcPagePersist {
                        start_row_id: current_start,
                        end_row_id: current_end,
                        buf,
                    });
                    builder = LwcBuilder::new(metadata);
                    current_start = page_info.start_row_id;
                    current_end = page_info.end_row_id;
                    let view = page.vector_view_in_transition(metadata, ctx, sts, min_active_sts);
                    if !builder.append_view(page, view)? {
                        return Err(crate::error::Error::InvalidState);
                    }
                } else {
                    current_end = page_info.end_row_id;
                }
            }
            if !builder.is_empty() {
                let buf = builder.build()?;
                lwc_pages.push(LwcPagePersist {
                    start_row_id: current_start,
                    end_row_id: current_end,
                    buf,
                });
            }
        }
        Ok(lwc_pages)
    }

    fn capture_uncommitted_deletes(&self, page: &RowPage, ctx: &crate::buffer::frame::FrameContext) {
        let Some(map) = ctx.row_ver() else {
            return;
        };
        let row_count = page.header.row_count();
        for row_idx in 0..row_count {
            let undo_guard = map.read_latch(row_idx);
            let Some(head) = undo_guard.as_ref() else {
                continue;
            };
            let mut status = &head.next.main.status;
            let mut entry = head.next.main.entry.clone();
            loop {
                match entry.as_ref().kind {
                    RowUndoKind::Delete => {
                        if let UndoStatus::Ref(trx_status) = status {
                            if !trx_is_committed(trx_status.ts()) {
                                let row_id = page.row_id(row_idx);
                                let _ = self.deletion_buffer.put(row_id, trx_status.clone());
                            }
                        }
                        break;
                    }
                    RowUndoKind::Insert | RowUndoKind::Update(_) => {
                        break;
                    }
                    RowUndoKind::Lock => {}
                }
                let Some(next) = entry.as_ref().next.as_ref() else {
                    break;
                };
                status = &next.main.status;
                entry = next.main.entry.clone();
            }
        }
    }

    /// Returns total number of row pages.
    #[inline]
    pub async fn total_row_pages(&self) -> usize {
        let mut res = 0usize;
        let mut cursor = self.blk_idx.mem_cursor();
        cursor.seek(0).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.shared_async().await;
            debug_assert!(g.page().is_leaf());
            res += g.page().leaf_entries().len();
        }
        res
    }

    #[inline]
    async fn index_lookup_unique_row_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
        row_id: RowID,
    ) -> SelectMvcc {
        match self.blk_idx.find_row(row_id).await {
            RowLocation::NotFound => SelectMvcc::NotFound,
            RowLocation::LwcPage(page_id) => {
                if let Some(status) = self.deletion_buffer.get(row_id) {
                    let ts = status.ts();
                    if trx_is_committed(ts) {
                        if ts <= stmt.trx.sts {
                            return SelectMvcc::NotFound;
                        }
                    } else if Arc::ptr_eq(&status, &stmt.trx.status()) {
                        return SelectMvcc::NotFound;
                    }
                }
                match self.read_lwc_row(page_id, row_id, user_read_set).await {
                    Ok(Some(vals)) => SelectMvcc::Ok(vals),
                    Ok(None) => SelectMvcc::NotFound,
                    Err(err) => SelectMvcc::Err(err),
                }
            }
            RowLocation::RowPage(page_id) => {
                let page_guard = self
                    .data_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;
                let page = page_guard.page();
                if !page.row_id_in_valid_range(row_id) {
                    return SelectMvcc::NotFound;
                }
                let (ctx, page) = page_guard.ctx_and_page();
                let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
                match access.read_row_mvcc(&stmt.trx, self.metadata(), user_read_set, Some(key)) {
                    ReadRow::Ok(vals) => SelectMvcc::Ok(vals),
                    ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
                }
            }
        }
    }

    #[inline]
    async fn read_lwc_row(
        &self,
        page_id: PageID,
        row_id: RowID,
        read_set: &[usize],
    ) -> Result<Option<Vec<Val>>> {
        let buf = self.file.read_page(page_id).await?;
        let page = unsafe { &*(buf.data().as_ptr() as *const LwcPage) };
        let Some(row_idx) = page.row_idx(row_id) else {
            return Ok(None);
        };
        let metadata = self.metadata();
        let mut vals = Vec::with_capacity(read_set.len());
        for &col_idx in read_set {
            let column = page.column(metadata, col_idx)?;
            if column.is_null(row_idx) {
                vals.push(Val::Null);
                continue;
            }
            let data = column.data()?;
            let val = data.value(row_idx).ok_or(Error::InvalidCompressedData)?;
            vals.push(val);
        }
        Ok(Some(vals))
    }

    async fn mem_scan<F>(&self, start_row_id: RowID, mut page_action: F)
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        // With cursor, we lock two pages in block index and one row page
        // when scanning rows.
        let mut cursor = self.blk_idx.mem_cursor();
        cursor.seek(start_row_id).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.shared_async().await;
            debug_assert!(g.page().is_leaf());
            let entries = g.page().leaf_entries();
            for page_entry in entries {
                let page_guard: PageSharedGuard<RowPage> = self
                    .data_pool
                    .get_page(page_entry.page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;
                if !page_action(page_guard) {
                    return;
                }
            }
        }
    }

    #[inline]
    async fn insert_index_no_trx(&self, key: SelectKey, row_id: RowID) {
        if self.metadata().index_specs[key.index_no].unique() {
            let res = self.sec_idx[key.index_no]
                .unique()
                .unwrap()
                .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
            assert!(res.is_ok());
        } else {
            self.sec_idx[key.index_no]
                .non_unique()
                .unwrap()
                .insert_if_not_exists(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
    }

    #[inline]
    async fn insert_index(
        &self,
        stmt: &mut Statement,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> InsertIndex {
        if self.metadata().index_specs[key.index_no].unique() {
            self.insert_unique_index(stmt, key, row_id, page_guard)
                .await
        } else {
            self.insert_non_unique_index(stmt, key, row_id).await
        }
    }

    #[inline]
    fn recover_row_insert_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
    ) -> Recover {
        let (ctx, page) = page_guard.ctx_and_page_mut();
        debug_assert!(self.metadata().col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);
        let row_idx = page.row_idx(row_id);
        // Insert log should always be located to an empty slot.
        debug_assert!(ctx.recover().unwrap().is_vacant(row_idx));
        let var_len = var_len_for_insert(self.metadata(), cols);
        let (var_offset, var_end) = if let Some(var_offset) = page.request_free_space(var_len) {
            (var_offset, var_offset + var_len)
        } else {
            return Recover::NoSpace;
        };
        // update count field to include current row id.
        page.update_count_to_include_row_id(row_id);
        // insert CTS.
        ctx.recover_mut().unwrap().insert_at(row_idx, cts);
        let row_idx = page.row_idx(row_id);
        let mut row = page.row_mut_exclusive(row_idx, var_offset, var_end);
        debug_assert!(row.is_deleted()); // before recovery, this row should be initialized as deleted.
        let metadata = self.metadata();
        for (user_col_idx, user_col) in cols.iter().enumerate() {
            row.update_col(metadata, user_col_idx, user_col, false);
        }
        row.finish_insert()
    }

    #[inline]
    async fn recover_index_insert(
        &self,
        key: SelectKey,
        row_id: RowID,
        cts: TrxID,
    ) -> RecoverIndex {
        if self.metadata().index_specs[key.index_no].unique() {
            self.recover_unique_index_insert(key, row_id, cts).await
        } else {
            self.recover_non_unique_index_insert(key, row_id).await
        }
    }

    #[inline]
    async fn recover_index_delete(&self, key: SelectKey, row_id: RowID) -> RecoverIndex {
        if self.metadata().index_specs[key.index_no].unique() {
            self.recover_unique_index_delete(key, row_id).await
        } else {
            self.recover_non_unique_index_delete(key, row_id).await
        }
    }

    #[inline]
    fn recover_row_update_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cols: &[UpdateCol],
        cts: TrxID,
        index_change_cols: Option<&mut HashMap<usize, Val>>,
    ) -> Recover {
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
            return Recover::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        if page.row(row_idx).is_deleted() {
            return Recover::AlreadyDeleted;
        }
        let var_len = page.var_len_for_update(row_idx, cols);
        let (var_offset, var_end) = if let Some(var_offset) = page.request_free_space(var_len) {
            (var_offset, var_offset + var_len)
        } else {
            return Recover::NoSpace;
        };
        // update CTS.
        ctx.recover_mut().unwrap().update_at(row_idx, cts);
        let mut row = page.row_mut_exclusive(row_idx, var_offset, var_end);
        debug_assert_eq!(row_id, row.row_id());

        let metadata = self.metadata();
        let disable_index = index_change_cols.is_none();
        if disable_index {
            for uc in cols {
                row.update_col(metadata, uc.idx, &uc.val, true);
            }
            row.finish_update()
        } else {
            // collect index change columns.
            let index_change_cols = index_change_cols.unwrap();
            for uc in cols {
                if let Some((old_val, _)) = row.different(metadata, uc.idx, &uc.val) {
                    // we also check whether the value change is related to any index,
                    // so we can update index later.
                    if metadata.index_cols.contains(&uc.idx) {
                        index_change_cols.insert(uc.idx, old_val);
                    }
                    // actual update
                    row.update_col(metadata, uc.idx, &uc.val, true);
                }
            }
            row.finish_update()
        }
    }

    #[inline]
    fn recover_row_delete_to_page(
        &self,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        row_id: RowID,
        cts: TrxID,
        index_cols: Option<&mut HashMap<usize, Val>>,
    ) -> Recover {
        let (ctx, page) = page_guard.ctx_and_page_mut();
        if !page.row_id_in_valid_range(row_id) {
            return Recover::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        if page.row(row_idx).is_deleted() {
            return Recover::AlreadyDeleted;
        }
        ctx.recover_mut().unwrap().update_at(row_idx, cts);
        page.set_deleted_exclusive(row_idx, true);
        let metadata = self.metadata();
        if let Some(index_cols) = index_cols {
            // save index columns for index update.
            let row = page.row(row_idx);
            for idx_col_no in &metadata.index_cols {
                let val = row.val(metadata, *idx_col_no);
                index_cols.insert(*idx_col_no, val);
            }
        }
        Recover::Ok
    }

    /// Directly delete a index entry.
    /// It will ignore delete flag, and won't update page ts.
    #[inline]
    async fn delete_index_directly(&self, key: &SelectKey, row_id: RowID) -> bool {
        let index_schema = &self.metadata().index_specs[key.index_no];
        if index_schema.unique() {
            let index = self.sec_idx[key.index_no].unique().unwrap();
            index
                .compare_delete(&key.vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
        } else {
            let index = self.sec_idx[key.index_no].non_unique().unwrap();
            index
                .compare_delete(&key.vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
        }
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        index: &UniqueBTreeIndex,
        key: &SelectKey,
        row_id: RowID,
    ) -> bool {
        let (page_guard, row_id) = loop {
            match index.lookup(&key.vals, MIN_SNAPSHOT_TS).await {
                None => return false, // Another thread deleted this entry.
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return false;
                    }
                    match self.blk_idx.find_row(row_id).await {
                        RowLocation::NotFound => {
                            return index
                                .compare_delete(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                        }
                        RowLocation::LwcPage(..) => todo!("lwc page"),
                        RowLocation::RowPage(page_id) => {
                            let page_guard = self
                                .data_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await
                                .shared_async()
                                .await;
                            if validate_page_row_range(&page_guard, page_id, row_id) {
                                break (page_guard, row_id);
                            }
                        }
                    }
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain.
        if !access.any_version_matches_key(self.metadata(), key) {
            return index
                .compare_delete(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        false
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        index: &NonUniqueBTreeIndex,
        key: &SelectKey,
        row_id: RowID,
    ) -> bool {
        let (page_guard, row_id) = loop {
            match index
                .lookup_unique(&key.vals, row_id, MIN_SNAPSHOT_TS)
                .await
            {
                None => return false, // Another thread deleted this entry.
                Some(deleted) => {
                    if !deleted {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return false;
                    }
                    match self.blk_idx.find_row(row_id).await {
                        RowLocation::NotFound => {
                            return index
                                .compare_delete(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                        }
                        RowLocation::LwcPage(..) => todo!("lwc page"),
                        RowLocation::RowPage(page_id) => {
                            let page_guard = self
                                .data_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await
                                .shared_async()
                                .await;
                            if validate_page_row_range(&page_guard, page_id, row_id) {
                                break (page_guard, row_id);
                            }
                        }
                    }
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain.
        if !access.any_version_matches_key(self.metadata(), key) {
            return index
                .compare_delete(&key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        false
    }

    /// Move update is similar to a delete+insert.
    /// It's caused by no more space on current row page.
    #[inline]
    async fn move_update_for_space(
        &self,
        stmt: &mut Statement,
        old_row: Vec<(Val, Option<u16>)>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> (RowID, HashMap<usize, Val>, PageSharedGuard<RowPage>) {
        // calculate new row and index changes.
        let (new_row, old_vals, index_change_cols) = {
            let mut index_change_cols = HashMap::new();
            let mut row = Vec::with_capacity(old_row.len());
            let mut old_vals = Vec::with_capacity(old_row.len());
            for (v, _) in old_row {
                old_vals.push(v.clone());
                row.push(v);
            }
            let metadata = self.metadata();
            for mut uc in update {
                let old_val = &mut row[uc.idx];
                if old_val != &uc.val {
                    if metadata.index_cols.contains(&uc.idx) {
                        index_change_cols.insert(uc.idx, old_val.clone());
                    }
                    // swap old value and new value
                    mem::swap(&mut uc.val, old_val);
                }
            }
            (row, old_vals, index_change_cols)
        };
        let metadata = self.metadata();
        let undo_vals: Vec<UpdateCol> = new_row
            .iter()
            .enumerate()
            .filter_map(|(idx, val)| {
                if val != &old_vals[idx] {
                    Some(UpdateCol {
                        idx,
                        val: old_vals[idx].clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        let index_branches = {
            let (ctx, page) = old_guard.ctx_and_page();
            let old_access = RowReadAccess::new(page, ctx, page.row_idx(old_id));
            let undo_head = old_access.undo_head().expect("undo head");
            debug_assert!(stmt.trx.is_same_trx(undo_head));
            let old_entry = old_access.first_undo_entry().expect("old undo entry");
            debug_assert!(matches!(old_entry.as_ref().kind, RowUndoKind::Delete));
            metadata
                .index_specs
                .iter()
                .enumerate()
                .filter(|(_, index)| index.unique())
                .map(|(index_no, index)| {
                    let vals = index
                        .index_cols
                        .iter()
                        .map(|key| new_row[key.col_no as usize].clone())
                        .collect();
                    IndexBranch {
                        key: SelectKey::new(index_no, vals),
                        cts: undo_head.ts(),
                        entry: old_entry.clone(),
                        undo_vals: undo_vals.clone(),
                    }
                })
                .collect::<Vec<_>>()
        };
        old_guard.set_dirty(); // mark as dirty page.
        let (new_row_id, new_guard) = self
            .insert_row_internal(stmt, new_row, RowUndoKind::Insert, index_branches)
            .await;
        // do not unlock the page because we may need to update index
        (new_row_id, index_change_cols, new_guard)
    }

    /// Link old version for index.
    /// This is a special operation for unique index maintainance.
    /// It's triggered by duplicate key finding when updating index.
    ///
    /// There are scenarios as below:
    /// 1. The old row not found. Just skip it.
    /// 2. The old row is being modified. Just throw write conflict.
    /// 3. Then we search from row page through version chain,
    ///    try to find one version that is not deleted and matches
    ///    the index key.
    ///    a) we find it, then link it.
    ///    b) no version found, we skip this row.
    #[inline]
    async fn link_for_unique_index(
        &self,
        stmt: &Statement,
        old_id: RowID,
        key: &SelectKey,
        new_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> LinkForUniqueIndex {
        debug_assert!(old_id != new_id);
        let (old_guard, old_id) = loop {
            match self.blk_idx.find_row(old_id).await {
                RowLocation::NotFound => return LinkForUniqueIndex::None,
                RowLocation::LwcPage(..) => todo!("lwc page"),
                RowLocation::RowPage(page_id) => {
                    let old_guard = self
                        .data_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    if validate_page_row_range(&old_guard, page_id, old_id) {
                        break (old_guard, old_id);
                    }
                }
            }
        };
        // The link process is to find one version of the old row that matches
        // given key and then link new row to it.
        let metadata = self.metadata();
        let (ctx, page) = old_guard.ctx_and_page();
        let old_access = RowReadAccess::new(page, ctx, page.row_idx(old_id));
        match old_access.find_old_version_for_unique_key(metadata, key, &stmt.trx) {
            FindOldVersion::None => LinkForUniqueIndex::None,
            FindOldVersion::DuplicateKey => LinkForUniqueIndex::DuplicateKey,
            FindOldVersion::WriteConflict => LinkForUniqueIndex::WriteConflict,
            FindOldVersion::Ok(old_row, cts, old_entry) => {
                // row latch is enough, because row lock is already acquired.
                let (ctx, page) = new_guard.ctx_and_page();
                let mut new_access =
                    RowWriteAccess::new(page, ctx, page.row_idx(new_id), Some(stmt.trx.sts), false);
                let undo_vals = new_access.row().calc_delta(metadata, &old_row);
                new_access.link_for_unique_index(key.clone(), cts, old_entry, undo_vals);
                LinkForUniqueIndex::Ok
            }
        }
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        stmt: &mut Statement,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut index_branches: Vec<IndexBranch>,
    ) -> (RowID, PageSharedGuard<RowPage>) {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col_count());
        loop {
            let page_guard = self.get_insert_page(stmt, row_count).await;
            let page_id = page_guard.page_id();
            match self.insert_row_to_page(stmt, page_guard, insert, undo_kind, index_branches) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    stmt.save_active_insert_page(self.table_id(), page_id, row_id);
                    return (row_id, page_guard);
                }
                // this page cannot be inserted any more, just leave it and retry another page.
                InsertRowIntoPage::NoSpaceOrFrozen(ins, uk, ib) => {
                    insert = ins;
                    undo_kind = uk;
                    index_branches = ib;
                }
            }
        }
    }

    /// Insert row into given page.
    /// There might be move+update call this method, in such case, undo_kind will be
    /// set to UndoKind::Update.
    /// If row page is frozen, the insert will fail.
    #[inline]
    fn insert_row_to_page(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<RowPage>,
        cols: Vec<Val>,
        undo_kind: RowUndoKind,
        index_branches: Vec<IndexBranch>,
    ) -> InsertRowIntoPage {
        debug_assert!(matches!(undo_kind, RowUndoKind::Insert));
        let metadata = self.metadata();
        let page_id = page_guard.page_id();
        let (ctx, page) = page_guard.ctx_and_page();
        if ctx.row_ver().unwrap().is_frozen() {
            return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
        }
        debug_assert!(metadata.col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);

        let var_len = var_len_for_insert(metadata, &cols);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
            };
        // Before real insert, we need to lock the row.
        let row_id = page.header.start_row_id + row_idx as u64;
        let mut access = RowWriteAccess::new(page, ctx, row_idx, Some(stmt.trx.sts), true);
        let res = access.lock_undo(stmt, metadata, self.table_id(), page_id, row_id, None);
        debug_assert!(res.is_ok());
        // Apply insert
        let mut new_row = page.new_row(row_idx, var_offset);
        for v in &cols {
            new_row.add_col(metadata, v);
        }
        let new_row_id = new_row.finish();
        debug_assert!(new_row_id == row_id);
        stmt.update_last_row_undo(undo_kind);
        for branch in index_branches {
            access.link_for_unique_index(branch.key, branch.cts, branch.entry, branch.undo_vals);
        }
        drop(access);

        // Here we do not unlock the page because we need to verify validity of unique index update
        // according to this insert.
        // There might be scenario that a deleted row or old version of updated row shares the same
        // key with this insert.
        // Then we have to link insert's undo head to that version via *INDEX* branch.
        // Hold the page guard in order to re-lock the undo head fast.
        //
        // create redo log.
        let redo_entry = RowRedo {
            page_id,
            row_id,
            kind: RowRedoKind::Insert(cols),
        };
        // store redo log into transaction redo buffer.
        stmt.redo.insert_dml(self.table_id(), redo_entry);
        InsertRowIntoPage::Ok(row_id, page_guard)
    }

    #[inline]
    async fn update_row_inplace(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<RowPage>,
        key: &SelectKey,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let (ctx, page) = page_guard.ctx_and_page();
        // column indexes must be in range
        debug_assert!(
            {
                update
                    .iter()
                    .all(|uc| uc.idx < page_guard.page().header.col_count as usize)
            },
            "update column indexes must be in range"
        );
        // column indexes should be in order.
        debug_assert!(
            {
                update.is_empty()
                    || update
                        .iter()
                        .zip(update.iter().skip(1))
                        .all(|(l, r)| l.idx < r.idx)
            },
            "update columns should be in order"
        );
        if row_id < page.header.start_row_id
            || row_id >= page.header.start_row_id + page.header.max_row_count as u64
        {
            return UpdateRowInplace::RowNotFound;
        }
        let row_state = ctx.row_ver().unwrap().state();
        if row_state == RowPageState::Transition {
            return UpdateRowInplace::RetryInTransition;
        }
        let frozen = row_state == RowPageState::Frozen;
        let mut lock_row = self
            .lock_row_for_write(stmt, &page_guard, row_id, Some(key))
            .await;
        let metadata = self.metadata();
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => UpdateRowInplace::RowNotFound,
            LockRowForWrite::WriteConflict => UpdateRowInplace::WriteConflict,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                if ctx.row_ver().unwrap().is_transition() {
                    drop(access);
                    drop(lock_row);
                    return UpdateRowInplace::RetryInTransition;
                }
                if access.row().is_deleted() {
                    return UpdateRowInplace::RowDeleted;
                }
                match access.update_row(metadata, &update, frozen) {
                    UpdateRow::NoFreeSpaceOrFrozen(old_row) => {
                        // Page does not have enough space or has been frozen for update,
                        // we need to switch to out-of-place update mode, which will add
                        // a DELETE undo entry to end original row and perform an INSERT into
                        // new page, and link the two versions with index branches.
                        //
                        // Mark page data as deleted.
                        access.delete_row();
                        // Update LOCK entry to DELETE entry.
                        stmt.update_last_row_undo(RowUndoKind::Delete);
                        drop(access); // unlock row
                        drop(lock_row);
                        // Here we do not unlock page because we need to perform out-of-place
                        // update and link undo entries of two rows via index branches.
                        // The re-lock of current undo is required.
                        let redo_entry = RowRedo {
                            page_id,
                            row_id,
                            // use DELETE for redo is ok, no version chain should be maintained if recovering from redo.
                            kind: RowRedoKind::Delete,
                        };
                        stmt.redo.insert_dml(self.table_id(), redo_entry);
                        UpdateRowInplace::NoFreeSpace(row_id, old_row, update, page_guard)
                    }
                    UpdateRow::Ok(mut row) => {
                        // Index change columns contains the col_no and old value.
                        let mut index_change_cols = HashMap::new();
                        // perform in-place update.
                        let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                        for uc in &mut update {
                            if let Some((old_val, var_offset)) =
                                row.different(metadata, uc.idx, &uc.val)
                            {
                                let new_val = mem::take(&mut uc.val);
                                // we also check whether the value change is related to any index,
                                // so we can update index later.
                                if metadata.index_cols.contains(&uc.idx) {
                                    index_change_cols.insert(uc.idx, old_val.clone());
                                }
                                // actual update
                                row.update_col(metadata, uc.idx, &new_val);
                                // record undo and redo
                                undo_cols.push(UndoCol {
                                    idx: uc.idx,
                                    val: old_val,
                                    var_offset,
                                });
                                redo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    // new value no longer needed, so safe to take it here.
                                    val: new_val,
                                });
                            }
                        }
                        // Update LOCK entry to UPDATE entry.
                        stmt.update_last_row_undo(RowUndoKind::Update(undo_cols));
                        // Mark this access as update, so page-level max_ins_sts will be updated.
                        access.enable_ins_or_update();
                        drop(access); // unlock the row.
                        drop(lock_row);
                        // we may still need this page if we'd like to update index.
                        if !redo_cols.is_empty() {
                            // there might be nothing to update, so we do not need to add redo log.
                            // but undo is required because we need to properly lock the row.
                            let redo_entry = RowRedo {
                                page_id,
                                row_id,
                                kind: RowRedoKind::Update(redo_cols),
                            };
                            stmt.redo.insert_dml(self.table_id(), redo_entry);
                        }
                        UpdateRowInplace::Ok(row_id, index_change_cols, page_guard)
                    }
                }
            }
        }
    }

    #[inline]
    async fn delete_row_internal(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
        log_by_key: bool,
    ) -> DeleteInternal {
        let page_id = page_guard.page_id();
        let (ctx, page) = page_guard.ctx_and_page();
        if ctx.row_ver().unwrap().is_transition() {
            return DeleteInternal::RetryInTransition;
        }
        if !page.row_id_in_valid_range(row_id) {
            return DeleteInternal::NotFound;
        }
        let mut lock_row = self
            .lock_row_for_write(stmt, &page_guard, row_id, Some(key))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => DeleteInternal::NotFound,
            LockRowForWrite::WriteConflict => DeleteInternal::WriteConflict,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                if ctx.row_ver().unwrap().is_transition() {
                    drop(access);
                    drop(lock_row);
                    return DeleteInternal::RetryInTransition;
                }
                if access.row().is_deleted() {
                    return DeleteInternal::NotFound;
                }
                access.delete_row();
                // update LOCK entry to DELETE entry.
                stmt.update_last_row_undo(RowUndoKind::Delete);
                drop(access); // unlock row.
                drop(lock_row);
                // hold page lock in order to update index later.
                // create redo log.
                let redo_entry = RowRedo {
                    page_id,
                    row_id,
                    kind: if log_by_key {
                        RowRedoKind::DeleteByUniqueKey(key.clone())
                    } else {
                        RowRedoKind::Delete
                    },
                };
                stmt.redo.insert_dml(self.table_id(), redo_entry);
                DeleteInternal::Ok(page_guard)
            }
        }
    }

    #[inline]
    async fn get_insert_page(
        &self,
        stmt: &mut Statement,
        row_count: usize,
    ) -> PageSharedGuard<RowPage> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id()) {
            let page_guard = self
                .data_pool
                .get_page(page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
            // because we save last insert page in session and meanwhile other thread may access this page
            // and do some modification, even worse, buffer pool may evict it and reload other data into
            // this page. so here, we do not require that no change should happen, but if something change,
            // we validate that page id and row id range is still valid.
            if validate_page_row_range(&page_guard, page_id, row_id) {
                return page_guard;
            }
        }
        self.blk_idx
            .get_insert_page(self.data_pool, row_count)
            .await
    }

    // lock row will check write conflict on given row and lock it.
    #[inline]
    async fn lock_row_for_write<'a>(
        &self,
        stmt: &mut Statement,
        page_guard: &'a PageSharedGuard<RowPage>,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockRowForWrite<'a> {
        let (ctx, page) = page_guard.ctx_and_page();
        loop {
            let mut access =
                RowWriteAccess::new(page, ctx, page.row_idx(row_id), Some(stmt.trx.sts), false);
            let lock_undo = access.lock_undo(
                stmt,
                self.metadata(),
                self.table_id(),
                page_guard.page_id(),
                row_id,
                key,
            );
            match lock_undo {
                LockUndo::Ok => {
                    return LockRowForWrite::Ok(Some(access));
                }
                LockUndo::InvalidIndex => {
                    return LockRowForWrite::InvalidIndex;
                }
                LockUndo::WriteConflict => {
                    return LockRowForWrite::WriteConflict;
                }
                LockUndo::Preparing(listener) => {
                    if let Some(listener) = listener {
                        drop(access);

                        // Here we do not unlock the page, because the preparation time of commit is supposed
                        // to be short.
                        // And as active transaction is using this page, we don't want page evictor swap it onto
                        // disk.
                        // Other transactions can still access this page and modify other rows.

                        listener.await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    } // there might be progress on preparation, so recheck.
                }
            }
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        stmt: &mut Statement,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> InsertIndex {
        let index = self.sec_idx[key.index_no].unique().unwrap();
        loop {
            match index
                .insert_if_not_exists(&key.vals, row_id, false, stmt.trx.sts)
                .await
            {
                IndexInsert::Ok(merged) => {
                    // insert index success.
                    stmt.push_insert_unique_index_undo(self.table_id(), row_id, key, merged);
                    return InsertIndex::Ok;
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    // we found there is already one existing row with same key.
                    // so perform move+link.
                    debug_assert!(old_row_id != row_id);
                    if !deleted {
                        // As the key is not deleted, there must be an active row with same key.
                        // todo: change logic if switch to lock-based protocol.
                        return InsertIndex::DuplicateKey;
                    }
                    match self
                        .link_for_unique_index(stmt, old_row_id, &key, row_id, page_guard)
                        .await
                    {
                        LinkForUniqueIndex::DuplicateKey => return InsertIndex::DuplicateKey,
                        LinkForUniqueIndex::WriteConflict => {
                            return InsertIndex::WriteConflict;
                        }
                        LinkForUniqueIndex::None => {
                            // No old row found, so we can update index to point to self.
                            // This may happen because purge thread can remove row data,
                            // but leave index not purged.
                            // The purge thread may delete the key before we apply the update.
                            // so our update can fail.
                            match index
                                .compare_exchange(
                                    &key.vals,
                                    old_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    // If we rollback this transaction, we need to undo the index update.
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        old_row_id,
                                        row_id,
                                        key,
                                        deleted,
                                    );
                                    return InsertIndex::Ok;
                                }
                                IndexCompareExchange::NotExists => {
                                    // There is race condition when GC thread delete the index entry concurrently.
                                    // So try to insert index entry again.
                                    continue;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return InsertIndex::WriteConflict;
                                }
                            }
                        }
                        LinkForUniqueIndex::Ok => {
                            // There is scenario that two transactions update different rows to the same
                            // key. Because we only search the matched version of old row but not add
                            // logical lock on it, it's possible that both transactions are trying to
                            // update the index. Only one should succeed and the other will fail.
                            match index
                                .compare_exchange(
                                    &key.vals,
                                    old_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        old_row_id,
                                        row_id,
                                        key,
                                        deleted,
                                    );
                                    return InsertIndex::Ok;
                                }
                                IndexCompareExchange::NotExists => {
                                    // The purge thread may concurrently delete the index entry.
                                    // In this case, we need to retry the insertion of index.
                                    continue;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return InsertIndex::WriteConflict;
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
        stmt: &mut Statement,
        key: SelectKey,
        row_id: RowID,
    ) -> InsertIndex {
        let index = self.sec_idx[key.index_no].non_unique().unwrap();
        // For non-unique index, it's guaranteed to be success.
        match index
            .insert_if_not_exists(&key.vals, row_id, false, stmt.trx.sts)
            .await
        {
            IndexInsert::Ok(merged) => {
                // insert index success.
                stmt.push_insert_non_unique_index_undo(self.table_id(), row_id, key, merged);
                InsertIndex::Ok
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn recover_unique_index_insert(
        &self,
        key: SelectKey,
        row_id: RowID,
        cts: TrxID,
    ) -> RecoverIndex {
        let index = self.sec_idx[key.index_no].unique().unwrap();
        loop {
            match index
                .insert_if_not_exists(&key.vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
            {
                IndexInsert::Ok(_) => {
                    // insert index success.
                    return RecoverIndex::Ok;
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    debug_assert!(old_row_id != row_id);
                    // Find CTS of old row.
                    match self.find_recover_cts_for_row_id(old_row_id).await {
                        Some(old_cts) => {
                            if cts < old_cts {
                                // Current row has smaller CTS, that means this insert
                                // can be skipped, and probably there is a followed DELETE
                                // operation on it.
                                return RecoverIndex::InsertOutdated;
                            }
                            // Current row is newer, we should update the index entry.
                            let old_row_id = if deleted {
                                old_row_id.deleted()
                            } else {
                                old_row_id
                            };
                            match index
                                .compare_exchange(&key.vals, old_row_id, row_id, MIN_SNAPSHOT_TS)
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    return RecoverIndex::Ok;
                                }
                                // retry the insert.
                                IndexCompareExchange::Mismatch
                                | IndexCompareExchange::NotExists => {}
                            }
                        }
                        None => {
                            unreachable!()
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn recover_non_unique_index_insert(&self, key: SelectKey, row_id: RowID) -> RecoverIndex {
        let index = self.sec_idx[key.index_no].non_unique().unwrap();
        // The recovery should make sure no duplicate key.
        let res = index
            .insert_if_not_exists(&key.vals, row_id, true, MIN_SNAPSHOT_TS)
            .await;
        debug_assert!(matches!(res, IndexInsert::Ok(_)));
        RecoverIndex::Ok
    }

    #[inline]
    async fn recover_unique_index_delete(&self, key: SelectKey, row_id: RowID) -> RecoverIndex {
        let index = self.sec_idx[key.index_no].unique().unwrap();
        if !index
            .compare_delete(&key.vals, row_id, true, MIN_SNAPSHOT_TS)
            .await
        {
            // Another recover thread concurrently insert index entry with same key, probably with greater CTS.
            // We just skip this deletion.
            return RecoverIndex::DeleteOutdated;
        }
        RecoverIndex::Ok
    }

    #[inline]
    async fn recover_non_unique_index_delete(&self, key: SelectKey, row_id: RowID) -> RecoverIndex {
        let index = self.sec_idx[key.index_no].non_unique().unwrap();
        if !index
            .compare_delete(&key.vals, row_id, true, MIN_SNAPSHOT_TS)
            .await
        {
            return RecoverIndex::DeleteOutdated;
        }
        RecoverIndex::Ok
    }

    #[inline]
    async fn find_recover_cts_for_row_id(&self, row_id: RowID) -> Option<TrxID> {
        match self.blk_idx.find_row(row_id).await {
            RowLocation::NotFound => None,
            RowLocation::LwcPage(..) => todo!("lwc page"),
            RowLocation::RowPage(page_id) => {
                let page_guard = self
                    .data_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .shared_async()
                    .await;
                debug_assert!(validate_page_row_range(&page_guard, page_id, row_id));
                let (ctx, page) = page_guard.ctx_and_page();
                let row_idx = page.row_idx(row_id);
                let access = RowReadAccess::new(page, ctx, row_idx);
                access.ts()
            }
        }
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        stmt: &mut Statement,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &HashMap<usize, Val>,
    ) -> UpdateIndex {
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx.iter().zip(&metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            if index_key_is_changed(index_schema, index_change_cols) {
                let new_key = read_latest_index_key(metadata, index.index_no, page_guard, row_id);
                let old_key = index_key_replace(index_schema, &new_key, index_change_cols);
                // First we need to insert new entry to index due to key change.
                // There might be conflict we will try to fix (if old one is already deleted).
                // Once the insert is done, we also need to defer deletion of original key.
                if index_schema.unique() {
                    match self
                        .update_unique_index_only_key_change(
                            stmt,
                            index.unique().unwrap(),
                            old_key,
                            new_key,
                            row_id,
                            page_guard,
                        )
                        .await
                    {
                        UpdateIndex::Ok => (),
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                    }
                } else {
                    let res = self
                        .update_non_unique_index_only_key_change(
                            stmt,
                            index.non_unique().unwrap(),
                            old_key,
                            new_key,
                            row_id,
                        )
                        .await;
                    assert!(res.is_ok());
                }
            } // otherwise, in-place update do not change row id, so we do nothing
        }
        UpdateIndex::Ok
    }

    #[inline]
    async fn update_indexes_only_row_id_change(
        &self,
        stmt: &mut Statement,
        old_row_id: RowID,
        new_row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx.iter().zip(&metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            let key = read_latest_index_key(metadata, index.index_no, page_guard, new_row_id);
            if index_schema.unique() {
                let res = self
                    .update_unique_index_only_row_id_change(
                        stmt,
                        index.unique().unwrap(),
                        key,
                        old_row_id,
                        new_row_id,
                    )
                    .await;
                assert!(res.is_ok());
            } else {
                let res = self
                    .update_non_unique_index_only_row_id_change(
                        stmt,
                        index.non_unique().unwrap(),
                        key,
                        old_row_id,
                        new_row_id,
                    )
                    .await;
                assert!(res.is_ok());
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        stmt: &mut Statement,
        old_row_id: RowID,
        new_row_id: RowID,
        index_change_cols: &HashMap<usize, Val>,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx.iter().zip(&metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            let key = read_latest_index_key(metadata, index.index_no, page_guard, new_row_id);
            if index_key_is_changed(index_schema, index_change_cols) {
                let old_key = index_key_replace(index_schema, &key, index_change_cols);
                // key change and row id change.
                if index_schema.unique() {
                    match self
                        .update_unique_index_key_and_row_id_change(
                            stmt,
                            index.unique().unwrap(),
                            old_key,
                            key,
                            old_row_id,
                            new_row_id,
                            page_guard,
                        )
                        .await
                    {
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        UpdateIndex::Ok => (),
                    }
                } else {
                    let res = self
                        .update_non_unique_index_key_and_row_id_change(
                            stmt,
                            index.non_unique().unwrap(),
                            old_key,
                            key,
                            old_row_id,
                            new_row_id,
                        )
                        .await;
                    assert!(res.is_ok());
                }
            } else {
                // only row id change.
                if index_schema.unique() {
                    match self
                        .update_unique_index_only_row_id_change(
                            stmt,
                            index.unique().unwrap(),
                            key,
                            old_row_id,
                            new_row_id,
                        )
                        .await
                    {
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        UpdateIndex::Ok => (),
                    }
                } else {
                    let res = self
                        .update_non_unique_index_only_row_id_change(
                            stmt,
                            index.non_unique().unwrap(),
                            key,
                            old_row_id,
                            new_row_id,
                        )
                        .await;
                    assert!(res.is_ok());
                }
            }
        }
        UpdateIndex::Ok
    }

    /// Defer the actual deletion until GC happens, but mark the index as deleted.
    #[inline]
    async fn defer_delete_indexes(
        &self,
        stmt: &mut Statement,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) {
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx.iter().zip(&metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            let key = read_latest_index_key(metadata, index.index_no, page_guard, row_id);
            if index_schema.unique() {
                let index = index.unique().unwrap();
                self.defer_delete_unique_index(stmt, index, row_id, key)
                    .await;
            } else {
                let index = index.non_unique().unwrap();
                self.defer_delete_non_unique_index(stmt, index, row_id, key)
                    .await;
            }
        }
    }

    #[inline]
    async fn defer_delete_unique_index(
        &self,
        stmt: &mut Statement,
        index: &UniqueBTreeIndex,
        row_id: RowID,
        key: SelectKey,
    ) {
        let res = index.mask_as_deleted(&key.vals, row_id, stmt.trx.sts).await;
        debug_assert!(res); // should always succeed.
        stmt.push_delete_index_undo(self.table_id(), row_id, key, true);
    }

    #[inline]
    async fn defer_delete_non_unique_index(
        &self,
        stmt: &mut Statement,
        index: &NonUniqueBTreeIndex,
        row_id: RowID,
        key: SelectKey,
    ) {
        let res = index.mask_as_deleted(&key.vals, row_id, stmt.trx.sts).await;
        debug_assert!(res);
        stmt.push_delete_index_undo(self.table_id(), row_id, key, false);
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_unique_index_key_and_row_id_change(
        &self,
        stmt: &mut Statement,
        index: &UniqueBTreeIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        loop {
            // new_row_id is guaranteed to be not in the index.
            match index
                .insert_if_not_exists(&new_key.vals, new_row_id, false, stmt.trx.sts)
                .await
            {
                IndexInsert::Ok(merged) => {
                    debug_assert!(!merged);
                    // New key insert succeed.
                    stmt.push_insert_unique_index_undo(self.table_id(), new_row_id, new_key, false);
                    // mark index of old row as deleted and defer delete.
                    self.defer_delete_unique_index(stmt, index, old_row_id, old_key)
                        .await;
                    return UpdateIndex::Ok;
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    // new row id is the insert id so index value must not be the same.
                    debug_assert!(index_row_id != new_row_id);
                    if !deleted {
                        return UpdateIndex::DuplicateKey;
                    }
                    // todo: change the logic.
                    // If we treat move-update just as delete and insert,
                    // with an extra linking step. then, we don't need to
                    // care about if index_row_id equal to old_row_id.
                    if index_row_id == old_row_id {
                        // This is possible.
                        // For example, transaction update row(RowID=100) key=1 to key=2.
                        //
                        // Then index has following entries:
                        // key=1 -> RowID=100 (old version)
                        // key=2 -> RowID=100 (latest version)
                        //
                        // Then we update key=2 to key=1 again.
                        // And page does not have enough space, so move+update with RowID=200.
                        // Now we should have:
                        // key=1 -> RowID=200 (latest version)
                        // key=2 -> RowID=100 (old version)
                        //
                        // In this case, we can just update index to point to new version.
                        //
                        // There can be an optimization to combine the update into insert.
                        // e.g. add a new method BTree::insert_if_not_exists_or_merge_match_value().
                        // But I think the case is rare so keep as is.
                        match index
                            .compare_exchange(
                                &new_key.vals,
                                old_row_id.deleted(),
                                new_row_id,
                                stmt.trx.sts,
                            )
                            .await
                        {
                            IndexCompareExchange::Ok => {
                                // New key update succeed.
                                stmt.push_update_unique_index_undo(
                                    self.table_id(),
                                    old_row_id,
                                    new_row_id,
                                    new_key,
                                    deleted,
                                );
                                // mark index of old row as deleted and defer delete.
                                self.defer_delete_unique_index(stmt, index, old_row_id, old_key)
                                    .await;
                                return UpdateIndex::Ok;
                            }
                            IndexCompareExchange::Mismatch => {
                                unreachable!();
                            }
                            IndexCompareExchange::NotExists => {
                                // re-insert index entry.
                                continue;
                            }
                        }
                    }
                    // There is a conflict key pointing to another row.
                    // We have to check the status of the old row.
                    // See comments of method link_for_unique_index().
                    match self
                        .link_for_unique_index(stmt, index_row_id, &new_key, new_row_id, new_guard)
                        .await
                    {
                        LinkForUniqueIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        LinkForUniqueIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        LinkForUniqueIndex::None => {
                            // No old version found.
                            // so we can update index to point to self
                            match index
                                .compare_exchange(
                                    &new_key.vals,
                                    index_row_id,
                                    new_row_id,
                                    stmt.trx.sts,
                                )
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeed.
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        deleted,
                                    );
                                    // mark index of old row as deleted and defer delete.
                                    self.defer_delete_unique_index(
                                        stmt, index, old_row_id, old_key,
                                    )
                                    .await;
                                    return UpdateIndex::Ok;
                                }
                                IndexCompareExchange::Mismatch => {
                                    // This may happen when another transaction insert/update with same key.
                                    return UpdateIndex::WriteConflict;
                                }
                                IndexCompareExchange::NotExists => {
                                    // Purge thread may delete the index entry before we update,
                                    // we should re-insert.
                                    continue;
                                }
                            }
                        }
                        LinkForUniqueIndex::Ok => {
                            // Old version found and linked.
                            // Because in linking process, we checked the old row status.
                            // The on-page version of old row must be deleted or being
                            // modified by self transaction. That means no other transaction
                            // can modify the new index key concurrently.
                            // So below operation must succeed.
                            match index
                                .compare_exchange(
                                    &new_key.vals,
                                    index_row_id,
                                    new_row_id,
                                    stmt.trx.sts,
                                )
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeeds.
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        deleted,
                                    );
                                    // mark index of old row as deleted and defer delete.
                                    self.defer_delete_unique_index(
                                        stmt, index, old_row_id, old_key,
                                    )
                                    .await;
                                    return UpdateIndex::Ok;
                                }
                                IndexCompareExchange::Mismatch
                                | IndexCompareExchange::NotExists => {
                                    unreachable!()
                                }
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
        stmt: &mut Statement,
        index: &NonUniqueBTreeIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        // new_row_id is guaranteed to be not in the index.
        match index
            .insert_if_not_exists(&new_key.vals, new_row_id, false, stmt.trx.sts)
            .await
        {
            IndexInsert::Ok(merged) => {
                debug_assert!(!merged);
                // New key insert succeed.
                stmt.push_insert_non_unique_index_undo(self.table_id(), new_row_id, new_key, false);
                // mark index of old row as deleted and defer delete.
                self.defer_delete_non_unique_index(stmt, index, old_row_id, old_key)
                    .await;
                UpdateIndex::Ok
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn update_unique_index_only_row_id_change(
        &self,
        stmt: &mut Statement,
        index: &UniqueBTreeIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        match index
            .compare_exchange(&key.vals, old_row_id, new_row_id, stmt.trx.sts)
            .await
        {
            IndexCompareExchange::Ok => {
                stmt.push_update_unique_index_undo(
                    self.table_id(),
                    old_row_id,
                    new_row_id,
                    key,
                    false,
                );
                UpdateIndex::Ok
            }
            IndexCompareExchange::Mismatch | IndexCompareExchange::NotExists => {
                unreachable!()
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_only_row_id_change(
        &self,
        stmt: &mut Statement,
        index: &NonUniqueBTreeIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        // insert new entry.
        let res = index
            .insert_if_not_exists(&key.vals, new_row_id, false, stmt.trx.sts)
            .await;
        debug_assert!(res.is_ok());
        stmt.push_insert_non_unique_index_undo(self.table_id(), new_row_id, key.clone(), false);
        // defer delete old entry.
        self.defer_delete_non_unique_index(stmt, index, old_row_id, key)
            .await;
        UpdateIndex::Ok
    }

    /// Update unique index due to key change.
    /// In this scenario, we only need to insert pair of new key and row id
    /// into index. Keep old index entry as is.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        stmt: &mut Statement,
        index: &UniqueBTreeIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> UpdateIndex {
        loop {
            // This is case for one transaction or multiple transactions to update
            // key of the same row back and forth.
            // e.g. update k=1 to k=2, then update k=2 to k=1, ...
            //
            // Each update will mask old index entry as deleted, and try to insert a new
            // entry, with same row id(Because it's the same row).
            // And all old versions are also linked from the same row.
            // That mean we can just merge the new index entry into the deleted entry(flip
            // the delete flag) if key and row id all match.
            // So we set merge_if_match_deleted to true.
            match index
                .insert_if_not_exists(&new_key.vals, row_id, true, stmt.trx.sts)
                .await
            {
                IndexInsert::Ok(merged) => {
                    // Insert new key success.
                    stmt.push_insert_unique_index_undo(self.table_id(), row_id, new_key, merged);
                    // Defer delete old key.
                    self.defer_delete_unique_index(stmt, index, row_id, old_key)
                        .await;
                    return UpdateIndex::Ok;
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    // There is already a row with same new key.
                    // We have to check its status.
                    if !deleted {
                        // As the key is not deleted, there must be an active row with same key.
                        // this active row may be committed or uncommitted,
                        // but we do not really care.
                        // todo: if we change concurrency control to lock-based protocol,
                        // we need to check and wait if other is modifying.
                        return UpdateIndex::DuplicateKey;
                    }
                    // The assertion will always succeed because merge_if_match_deleted
                    // is set to true.
                    debug_assert!(index_row_id != row_id);
                    match self
                        .link_for_unique_index(stmt, index_row_id, &new_key, row_id, page_guard)
                        .await
                    {
                        LinkForUniqueIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        LinkForUniqueIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        LinkForUniqueIndex::None => {
                            // no old row found.
                            match index
                                .compare_exchange(
                                    &new_key.vals,
                                    index_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    // Update new key succeeds.
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        row_id,
                                        new_key,
                                        deleted,
                                    );
                                    // Defer delete old key.
                                    self.defer_delete_unique_index(stmt, index, row_id, old_key)
                                        .await;
                                    return UpdateIndex::Ok;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return UpdateIndex::WriteConflict;
                                }
                                IndexCompareExchange::NotExists => {
                                    // re-insert
                                    continue;
                                }
                            }
                        }
                        LinkForUniqueIndex::Ok => {
                            // Both old row(index points to) and new row are locked.
                            // we must succeed on updating index.
                            match index
                                .compare_exchange(
                                    &new_key.vals,
                                    index_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeeds.
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        index_row_id,
                                        row_id,
                                        new_key,
                                        deleted,
                                    );
                                    // Defer delete old key.
                                    self.defer_delete_unique_index(stmt, index, row_id, old_key)
                                        .await;
                                    return UpdateIndex::Ok;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return UpdateIndex::WriteConflict;
                                }
                                IndexCompareExchange::NotExists => {
                                    // re-insert
                                    continue;
                                }
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
        stmt: &mut Statement,
        index: &NonUniqueBTreeIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
    ) -> UpdateIndex {
        // This is case for one transaction or multiple transactions to update
        // key of the same row back and forth.
        // e.g. update k=1 to k=2, then update k=2 to k=1, ...
        //
        // Each update will mask old index entry as deleted, and try to insert a new
        // entry, with same row id(Because it's the same row).
        // And all old versions are also linked from the same row.
        // That mean we can just merge the new index entry into the deleted entry(flip
        // the delete flag) if key and row id all match.
        // So we set merge_if_match_deleted to true.
        match index
            .insert_if_not_exists(&new_key.vals, row_id, true, stmt.trx.sts)
            .await
        {
            IndexInsert::Ok(merged) => {
                stmt.push_insert_non_unique_index_undo(self.table_id(), row_id, new_key, merged);
                // Defer delete old key.
                self.defer_delete_non_unique_index(stmt, index, row_id, old_key)
                    .await;
                UpdateIndex::Ok
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    pub fn metadata(&self) -> &TableMetadata {
        &self.file.active_root().metadata
    }
}

impl Clone for Table {
    #[inline]
    fn clone(&self) -> Self {
        Table {
            data_pool: self.data_pool,
            file: Arc::clone(&self.file),
            blk_idx: Arc::clone(&self.blk_idx),
            sec_idx: Arc::clone(&self.sec_idx),
            deletion_buffer: Arc::clone(&self.deletion_buffer),
        }
    }
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
fn row_len(metadata: &TableMetadata, cols: &[Val]) -> usize {
    let var_len = metadata
        .var_cols
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
    metadata.fix_len + var_len
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
        .index_cols
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
        .index_cols
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
    let index_spec = &metadata.index_specs[index_no];
    let mut new_key = SelectKey::null(index_no, index_spec.index_cols.len());
    for (pos, key) in index_spec.index_cols.iter().enumerate() {
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let val = access.row().val(metadata, key.col_no as usize);
        new_key.vals[pos] = val;
    }
    new_key
}
