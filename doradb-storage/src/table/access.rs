use crate::buffer::ReadonlyBufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::{INVALID_PAGE_ID, PageID};
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool};
use crate::catalog::{CatalogTable, TableID, TableMetadata};
use crate::error::{Error, Result};
use crate::index::util::Maskable;
use crate::index::{
    BlockIndex, GenericNonUniqueBTreeIndex, GenericSecondaryIndex, GenericUniqueBTreeIndex,
    IndexCompareExchange, IndexInsert, NonUniqueIndex, RowLocation, UniqueIndex,
};
use crate::latch::LatchFallbackMode;
use crate::lwc::PersistedLwcPage;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, LinkForUniqueIndex, ReadRow, ScanMvcc, SelectKey,
    SelectMvcc, UndoCol, UpdateCol, UpdateIndex, UpdateMvcc, UpdateRow,
};
use crate::row::{Row, RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::stmt::Statement;
use crate::table::{
    ColumnDeletionBuffer, DeleteInternal, DeleteMarker, DeletionError, InsertRowIntoPage, Table,
    UpdateRowInplace, index_key_is_changed, index_key_replace, read_latest_index_key, row_len,
    validate_page_row_range,
};
use crate::trx::redo::{RowRedo, RowRedoKind};
use crate::trx::row::{
    FindOldVersion, LockRowForWrite, LockUndo, ReadAllRows, RowReadAccess, RowWriteAccess,
};
use crate::trx::undo::{IndexBranch, OwnedRowUndo, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MIN_SNAPSHOT_TS, trx_is_committed};
use crate::value::Val;
use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::sync::Arc;

pub trait TableAccess {
    /// Table scan including uncommitted versions.
    ///
    /// This method iterates raw latest row versions and includes rows marked
    /// as deleted. Callers should explicitly filter `row.is_deleted()` if they
    /// only need live rows.
    ///
    /// Note: this scans only in-memory row-store pages and does not include
    /// persisted column-store rows on disk.
    fn table_scan_uncommitted<F>(&self, row_action: F) -> impl Future<Output = ()>
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool;

    /// Table scan with MVCC.
    ///
    /// Note: this scans only in-memory row-store pages and does not include
    /// persisted column-store rows on disk.
    fn table_scan_mvcc<F>(
        &self,
        stmt: &Statement,
        read_set: &[usize],
        row_action: F,
    ) -> impl Future<Output = ()>
    where
        F: FnMut(Vec<Val>) -> bool;

    /// Index lookup unique row with MVCC.
    /// Result should be no more than one row.
    fn index_lookup_unique_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = SelectMvcc>;

    /// Index lookup unique row including uncommitted version.
    fn index_lookup_unique_uncommitted<R, F>(
        &self,
        key: &SelectKey,
        row_action: F,
    ) -> impl Future<Output = Option<R>>
    where
        for<'m, 'p> F: FnOnce(&'m TableMetadata, Row<'p>) -> R;

    /// Index scan with MVCC of given key.
    fn index_scan_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = ScanMvcc>;

    /// Insert row in transaction.
    fn insert_mvcc(&self, stmt: &mut Statement, cols: Vec<Val>)
    -> impl Future<Output = InsertMvcc>;

    /// Insert row in non-transactional way.
    fn insert_no_trx(&self, cols: &[Val]) -> impl Future<Output = ()>;

    /// Update row in transaction.
    /// This method is for update based on unique index lookup.
    /// It also takes care of index change.
    fn update_unique_mvcc(
        &self,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> impl Future<Output = UpdateMvcc>;

    /// Delete row in transaction.
    /// This method is for delete based on unique index lookup.
    ///
    /// If the parameter log_by_key is set to true, the delete operation
    /// is logged with (unique) key instead of row id.
    /// Such type of log is used for catalog tables, which will have
    /// inconsistent page_id/row_id among multiple restarts(recoveries)
    /// of database.
    fn delete_unique_mvcc(
        &self,
        stmt: &mut Statement,
        key: &SelectKey,
        log_by_key: bool,
    ) -> impl Future<Output = DeleteMvcc>;

    /// Delete row in non-transactional way.
    fn delete_unique_no_trx(&self, key: &SelectKey) -> impl Future<Output = ()>;

    /// Delete index by purge threads.
    /// This method will be only called by internal threads and don't maintain
    /// transaction properties.
    ///
    /// It checks whether the index entry still points to valid row, and if not,
    /// remove the entry.
    ///
    /// The validation is based on MVCC with minimum active STS. If the input
    /// key is not found on the path of undo chain, it means the index entry can be
    /// removed.
    ///
    /// todo: The look-back mechanism for each index entry is not performant. A
    /// potential optimization is similar to covering index, page ts can be checked
    /// to see if all delete-masked values in it can be remove directly.
    fn delete_index(
        &self,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
    ) -> impl Future<Output = bool>;
}

/// Thin operation wrapper that exposes `TableAccess` over a table reference.
///
/// `D` is the buffer-pool type for row-page (in-memory) operations.
/// `I` is the buffer-pool type used by secondary-index trees.
///
/// In runtime, this is usually bound via [`HybridTableAccessor`], while tests
/// or alternative runtimes can bind different pool implementations.
pub struct TableAccessor<'a, D: 'static, I: 'static> {
    mem_pool: &'static D,
    disk_pool: Option<&'a ReadonlyBufferPool>,
    metadata: &'a TableMetadata,
    blk_idx: &'a BlockIndex,
    sec_idx: &'a [GenericSecondaryIndex<I>],
    deletion_buffer: Option<&'a ColumnDeletionBuffer>,
}

impl<'a> From<&'a Table> for TableAccessor<'a, EvictableBufferPool, FixedBufferPool> {
    #[inline]
    fn from(table: &'a Table) -> Self {
        TableAccessor {
            mem_pool: table.mem_pool,
            disk_pool: Some(&table.disk_pool),
            metadata: &table.metadata,
            blk_idx: &table.blk_idx,
            sec_idx: &table.sec_idx,
            deletion_buffer: Some(&table.deletion_buffer),
        }
    }
}

impl<'a> From<&'a CatalogTable> for TableAccessor<'a, FixedBufferPool, FixedBufferPool> {
    #[inline]
    fn from(table: &'a CatalogTable) -> Self {
        TableAccessor {
            mem_pool: table.mem_pool,
            disk_pool: None,
            metadata: &table.metadata,
            blk_idx: &table.blk_idx,
            sec_idx: &table.sec_idx,
            deletion_buffer: None,
        }
    }
}

impl<'a, D: BufferPool, I: BufferPool> TableAccessor<'a, D, I> {
    #[inline]
    fn table_id(&self) -> TableID {
        self.blk_idx.table_id
    }

    #[inline]
    fn metadata(&self) -> &TableMetadata {
        self.metadata
    }

    #[inline]
    async fn mem_scan<F>(&self, page_action: F)
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        let mut page_action = page_action;
        // With cursor, we lock two pages in block index and one row page
        // when scanning rows.
        let mut cursor = self.blk_idx.mem_cursor();
        cursor.seek(0).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.lock_shared_async().await.unwrap();
            debug_assert!(g.page().is_leaf());
            let entries = g.page().leaf_entries();
            for page_entry in entries {
                let page_guard: PageSharedGuard<RowPage> = self
                    .mem_pool
                    .get_page(page_entry.page_id, LatchFallbackMode::Shared)
                    .await
                    .lock_shared_async()
                    .await
                    .unwrap();
                if !page_action(page_guard) {
                    return;
                }
            }
        }
    }

    #[inline]
    async fn index_lookup_unique_row_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
        row_id: RowID,
    ) -> SelectMvcc {
        let location = match self.blk_idx.try_find_row(row_id).await {
            Ok(location) => location,
            Err(err) => return SelectMvcc::Err(err),
        };
        match location {
            RowLocation::NotFound => SelectMvcc::NotFound,
            RowLocation::LwcPage(page_id) => {
                let Some(deletion_buffer) = self.deletion_buffer else {
                    return SelectMvcc::NotFound;
                };
                if let Some(marker) = deletion_buffer.get(row_id) {
                    match marker {
                        DeleteMarker::Committed(ts) => {
                            if ts <= stmt.trx.sts {
                                return SelectMvcc::NotFound;
                            }
                        }
                        DeleteMarker::Ref(status) => {
                            let ts = status.ts();
                            if trx_is_committed(ts) {
                                if ts <= stmt.trx.sts {
                                    return SelectMvcc::NotFound;
                                }
                            } else if Arc::ptr_eq(&status, &stmt.trx.status()) {
                                return SelectMvcc::NotFound;
                            }
                        }
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
                    .mem_pool
                    .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                    .await
                    .lock_shared_async()
                    .await
                    .unwrap();
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
        let Some(disk_pool) = self.disk_pool else {
            return Err(Error::InvalidState);
        };
        let page = PersistedLwcPage::load(disk_pool, page_id).await?;
        let Some(row_idx) = page.find_row_idx(row_id)? else {
            return Ok(None);
        };
        page.decode_row_values(self.metadata(), row_idx, read_set)
            .map(Some)
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
    pub(super) fn insert_row_to_page(
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
        let versioned_page_id = page_guard.bf().versioned_page_id();
        let (ctx, page) = page_guard.ctx_and_page();
        let ver_map = ctx.row_ver().unwrap();
        let state_guard = ver_map.read_state();
        if *state_guard != RowPageState::Active {
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
        let mut access = RowWriteAccess::new_with_state_guard(
            page,
            ctx,
            row_idx,
            Some(stmt.trx.sts),
            true,
            state_guard,
        );
        let res = access.lock_undo(
            stmt,
            metadata,
            self.table_id(),
            versioned_page_id,
            row_id,
            None,
        );
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
    pub(super) async fn update_row_inplace(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<RowPage>,
        key: &SelectKey,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
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
        let mut lock_row = self
            .lock_row_for_write(stmt, &page_guard, row_id, Some(key))
            .await;
        let metadata = self.metadata();
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => UpdateRowInplace::RowNotFound,
            LockRowForWrite::WriteConflict => UpdateRowInplace::WriteConflict,
            LockRowForWrite::RetryInTransition => UpdateRowInplace::RetryInTransition,
            LockRowForWrite::Ok(access) => {
                let mut access = access.take().unwrap();
                let frozen = access.page_state() == RowPageState::Frozen;
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

    #[inline]
    pub(super) async fn delete_row_internal(
        &self,
        stmt: &mut Statement,
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
            .lock_row_for_write(stmt, &page_guard, row_id, Some(key))
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
        index: &GenericUniqueBTreeIndex<I>,
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
                                .mem_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await
                                .lock_shared_async()
                                .await
                                .unwrap();
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
        index: &GenericNonUniqueBTreeIndex<I>,
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
                                .mem_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await
                                .lock_shared_async()
                                .await
                                .unwrap();
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
    async fn get_insert_page(
        &self,
        stmt: &mut Statement,
        row_count: usize,
    ) -> PageSharedGuard<RowPage> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id()) {
            let page_guard = self
                .mem_pool
                .get_page(page_id, LatchFallbackMode::Shared)
                .await
                .lock_shared_async()
                .await
                .unwrap();
            // because we save last insert page in session and meanwhile other thread may access this page
            // and do some modification, even worse, buffer pool may evict it and reload other data into
            // this page. so here, we do not require that no change should happen, but if something change,
            // we validate that page id and row id range is still valid.
            if validate_page_row_range(&page_guard, page_id, row_id) {
                return page_guard;
            }
        }
        self.blk_idx.get_insert_page(self.mem_pool, row_count).await
    }

    // lock row will check write conflict on given row and lock it.
    // clippy can not find the guard is actually dropped before await point.
    #[allow(clippy::await_holding_lock)]
    #[inline]
    pub(super) async fn lock_row_for_write<'b>(
        &self,
        stmt: &mut Statement,
        page_guard: &'b PageSharedGuard<RowPage>,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockRowForWrite<'b> {
        let (ctx, page) = page_guard.ctx_and_page();
        let ver_map = ctx.row_ver().unwrap();
        loop {
            let state_guard = ver_map.read_state();
            if *state_guard == RowPageState::Transition {
                return LockRowForWrite::RetryInTransition;
            }
            let mut access = RowWriteAccess::new_with_state_guard(
                page,
                ctx,
                page.row_idx(row_id),
                Some(stmt.trx.sts),
                false,
                state_guard,
            );
            let lock_undo = access.lock_undo(
                stmt,
                self.metadata(),
                self.table_id(),
                page_guard.bf().versioned_page_id(),
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
                        .mem_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .lock_shared_async()
                        .await
                        .unwrap();
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
    async fn defer_delete_unique_index(
        &self,
        stmt: &mut Statement,
        index: &GenericUniqueBTreeIndex<I>,
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
        index: &GenericNonUniqueBTreeIndex<I>,
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
        index: &GenericUniqueBTreeIndex<I>,
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

    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_non_unique_index_key_and_row_id_change(
        &self,
        stmt: &mut Statement,
        index: &GenericNonUniqueBTreeIndex<I>,
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
        index: &GenericUniqueBTreeIndex<I>,
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
        index: &GenericNonUniqueBTreeIndex<I>,
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
        index: &GenericUniqueBTreeIndex<I>,
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
                        // duplicate key.
                        return UpdateIndex::DuplicateKey;
                    }
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
        index: &GenericNonUniqueBTreeIndex<I>,
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
}

/// Runtime accessor type binding for user tables:
/// - `D = EvictableBufferPool` for row pages
/// - `I = FixedBufferPool` for secondary indexes
pub type HybridTableAccessor<'a> = TableAccessor<'a, EvictableBufferPool, FixedBufferPool>;

/// Runtime accessor type binding for in-memory catalog tables:
/// - `D = FixedBufferPool` for row pages
/// - `I = FixedBufferPool` for secondary indexes
pub type MemTableAccessor<'a> = TableAccessor<'a, FixedBufferPool, FixedBufferPool>;

impl<D: BufferPool, I: BufferPool> TableAccess for TableAccessor<'_, D, I> {
    async fn table_scan_uncommitted<F>(&self, mut row_action: F)
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool,
    {
        self.mem_scan(|page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let metadata = &*ctx.row_ver().unwrap().metadata;
            for row_access in ReadAllRows::new(page, ctx) {
                if !row_action(metadata, row_access.row()) {
                    return false;
                }
            }
            true
        })
        .await;
    }

    async fn table_scan_mvcc<F>(&self, stmt: &Statement, read_set: &[usize], mut row_action: F)
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        self.mem_scan(|page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let metadata = &*ctx.row_ver().unwrap().metadata;
            for row_access in ReadAllRows::new(page, ctx) {
                match row_access.read_row_mvcc(&stmt.trx, metadata, read_set, None) {
                    ReadRow::InvalidIndex => unreachable!(),
                    ReadRow::NotFound => (),
                    ReadRow::Ok(vals) => {
                        if !row_action(vals) {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .await;
    }

    async fn index_lookup_unique_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        match self.sec_idx[key.index_no]
            .unique()
            .unwrap()
            .lookup(&key.vals, stmt.trx.sts)
            .await
        {
            None => SelectMvcc::NotFound,
            Some((row_id, _)) => {
                self.index_lookup_unique_row_mvcc(stmt, key, user_read_set, row_id)
                    .await
            }
        }
    }

    async fn index_lookup_unique_uncommitted<R, F>(
        &self,
        key: &SelectKey,
        row_action: F,
    ) -> Option<R>
    where
        for<'m, 'p> F: FnOnce(&'m TableMetadata, Row<'p>) -> R,
    {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let (page_guard, row_id) = match self.sec_idx[key.index_no]
            .unique()
            .unwrap()
            .lookup(&key.vals, MIN_SNAPSHOT_TS)
            .await
        {
            None => return None,
            Some((row_id, _)) => match self.blk_idx.find_row(row_id).await {
                RowLocation::NotFound => return None,
                RowLocation::LwcPage(..) => todo!("lwc page"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = self
                        .mem_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .lock_shared_async()
                        .await
                        .unwrap();
                    (page_guard, row_id)
                }
            },
        };
        let (ctx, page) = page_guard.ctx_and_page();
        if !page.row_id_in_valid_range(row_id) {
            return None;
        }
        let metadata = &*ctx.row_ver().unwrap().metadata;
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let row = access.row();
        // latest version in row page.
        if row.is_deleted() {
            return None;
        }
        if row.is_key_different(self.metadata(), key) {
            return None;
        }
        Some(row_action(metadata, row))
    }

    async fn index_scan_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> ScanMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        // Index scan should be applied to non-unique index.
        // todo: support partial key scan on unique index.
        debug_assert!(!self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        // todo: support batching, streaming and sorting.
        let mut row_ids = vec![];
        self.sec_idx[key.index_no]
            .non_unique()
            .unwrap()
            .lookup(&key.vals, &mut row_ids, stmt.trx.sts)
            .await;
        let mut res = vec![];
        for row_id in row_ids {
            match self
                .index_lookup_unique_row_mvcc(stmt, key, user_read_set, row_id)
                .await
            {
                SelectMvcc::NotFound => (),
                SelectMvcc::Ok(vals) => {
                    res.push(vals);
                }
                SelectMvcc::Err(err) => return ScanMvcc::Err(err),
            }
        }
        ScanMvcc::Ok(res)
    }

    async fn insert_mvcc(&self, stmt: &mut Statement, cols: Vec<Val>) -> InsertMvcc {
        let metadata = self.metadata();
        debug_assert!(cols.len() == metadata.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col_type_match(idx, val))
        });
        let keys = self.metadata().keys_for_insert(&cols);
        // insert row into page with undo log linked.
        let (row_id, page_guard) = self
            .insert_row_internal(stmt, cols, RowUndoKind::Insert, Vec::new())
            .await;
        // insert index
        for key in keys {
            match self.insert_index(stmt, key, row_id, &page_guard).await {
                InsertIndex::Ok => (),
                InsertIndex::DuplicateKey => {
                    return InsertMvcc::DuplicateKey;
                }
                InsertIndex::WriteConflict => {
                    return InsertMvcc::WriteConflict;
                }
            }
        }
        page_guard.set_dirty(); // mark as dirty page.
        InsertMvcc::Ok(row_id)
    }

    async fn insert_no_trx(&self, cols: &[Val]) {
        debug_assert!(cols.len() == self.metadata().col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col_type_match(idx, val))
        });
        let metadata = self.metadata();
        // prepare index keys.
        let keys = metadata.keys_for_insert(cols);
        // calculate row length.
        let row_len = row_len(metadata, cols);
        // estimate max row count for insert page.
        let row_count = estimate_max_row_count(row_len, metadata.col_count());
        loop {
            // acquire insert page from block index.
            let mut page_guard = self
                .blk_idx
                .get_insert_page_exclusive(self.mem_pool, row_count)
                .await;
            let page = page_guard.page_mut();
            debug_assert!(metadata.col_count() == page.header.col_count as usize);
            debug_assert!(cols.len() == page.header.col_count as usize);
            let var_len = var_len_for_insert(metadata, cols);
            let (row_idx, var_offset) =
                if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                    (row_idx, var_offset)
                } else {
                    // we just ignore this page and retry.
                    continue;
                };
            let row_id = page.header.start_row_id + row_idx as RowID;
            let mut row = page.row_mut_exclusive(row_idx, var_offset, var_offset + var_len);
            debug_assert!(row.is_deleted());
            for (col_idx, user_col) in cols.iter().enumerate() {
                row.update_col(metadata, col_idx, user_col, false);
            }
            // update index
            for key in keys {
                self.insert_index_no_trx(key, row_id).await;
            }
            row.finish_insert();
            // Cache insert page.
            self.blk_idx.cache_exclusive_insert_page(page_guard);
            return;
        }
    }

    async fn update_unique_mvcc(
        &self,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        loop {
            let (page_guard, row_id) = match index.lookup(&key.vals, stmt.trx.sts).await {
                None => return UpdateMvcc::NotFound,
                Some((row_id, _)) => match self.blk_idx.find_row(row_id).await {
                    RowLocation::NotFound => return UpdateMvcc::NotFound,
                    RowLocation::LwcPage(..) => todo!("lwc page"),
                    RowLocation::RowPage(page_id) => {
                        let page_guard = self
                            .mem_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .lock_shared_async()
                            .await
                            .unwrap();
                        (page_guard, row_id)
                    }
                },
            };
            let res = self
                .update_row_inplace(stmt, page_guard, key, row_id, update.clone())
                .await;
            match res {
                UpdateRowInplace::Ok(new_row_id, index_change_cols, page_guard) => {
                    debug_assert!(row_id == new_row_id);
                    if !index_change_cols.is_empty() {
                        // Index may change, we should check whether each index key change and update correspondingly.
                        let res = self
                            .update_indexes_only_key_change(
                                stmt,
                                row_id,
                                &page_guard,
                                &index_change_cols,
                            )
                            .await;
                        page_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                            UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                            UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                        };
                    } // otherwise, do nothing
                    page_guard.set_dirty(); // mark as dirty page.
                    return UpdateMvcc::Ok(row_id);
                }
                UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => {
                    return UpdateMvcc::NotFound;
                }
                UpdateRowInplace::WriteConflict => return UpdateMvcc::WriteConflict,
                UpdateRowInplace::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                UpdateRowInplace::NoFreeSpace(old_row_id, old_row, update, old_guard) => {
                    // in-place update failed, we transfer update into
                    // move+update.
                    let (new_row_id, index_change_cols, new_guard) = self
                        .move_update_for_space(stmt, old_row, update, old_row_id, old_guard)
                        .await;
                    if !index_change_cols.is_empty() {
                        let res = self
                            .update_indexes_may_both_change(
                                stmt,
                                old_row_id,
                                new_row_id,
                                &index_change_cols,
                                &new_guard,
                            )
                            .await;
                        // old guard is already marked inside.
                        new_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                            UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                            UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                        };
                    } else {
                        let res = self
                            .update_indexes_only_row_id_change(
                                stmt, old_row_id, new_row_id, &new_guard,
                            )
                            .await;
                        new_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                            UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                            UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                        };
                    }
                }
            }
        }
    }

    async fn delete_unique_mvcc(
        &self,
        stmt: &mut Statement,
        key: &SelectKey,
        log_by_key: bool,
    ) -> DeleteMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        loop {
            let (page_guard, row_id) = match index.lookup(&key.vals, stmt.trx.sts).await {
                None => return DeleteMvcc::NotFound,
                Some((row_id, _)) => match self.blk_idx.find_row(row_id).await {
                    RowLocation::NotFound => return DeleteMvcc::NotFound,
                    RowLocation::LwcPage(..) => {
                        let deletion_buffer = self
                            .deletion_buffer
                            .expect("catalog table should never have lwc page rows");
                        match deletion_buffer.put_ref(row_id, stmt.trx.status()) {
                            Ok(()) => {
                                let undo = OwnedRowUndo::new(
                                    self.table_id(),
                                    None,
                                    row_id,
                                    RowUndoKind::Delete,
                                );
                                stmt.row_undo.push(undo);
                                let redo_kind = if log_by_key {
                                    RowRedoKind::DeleteByUniqueKey(key.clone())
                                } else {
                                    RowRedoKind::Delete
                                };
                                let redo = RowRedo {
                                    page_id: INVALID_PAGE_ID,
                                    row_id,
                                    kind: redo_kind,
                                };
                                stmt.redo.insert_dml(self.table_id(), redo);
                                return DeleteMvcc::Ok;
                            }
                            Err(DeletionError::WriteConflict) => {
                                return DeleteMvcc::WriteConflict;
                            }
                            Err(DeletionError::AlreadyDeleted) => {
                                return DeleteMvcc::NotFound;
                            }
                        }
                    }
                    RowLocation::RowPage(page_id) => {
                        let page_guard = self
                            .mem_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .lock_shared_async()
                            .await
                            .unwrap();
                        (page_guard, row_id)
                    }
                },
            };
            match self
                .delete_row_internal(stmt, page_guard, row_id, key, log_by_key)
                .await
            {
                DeleteInternal::NotFound => return DeleteMvcc::NotFound,
                DeleteInternal::WriteConflict => return DeleteMvcc::WriteConflict,
                DeleteInternal::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                DeleteInternal::Ok(page_guard) => {
                    // defer index deletion with index undo log.
                    self.defer_delete_indexes(stmt, row_id, &page_guard).await;
                    page_guard.set_dirty(); // mark as dirty.
                    return DeleteMvcc::Ok;
                }
            }
        }
    }

    async fn delete_unique_no_trx(&self, key: &SelectKey) {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        let (mut page_guard, row_id) = match index.lookup(&key.vals, MIN_SNAPSHOT_TS).await {
            None => unreachable!(),
            Some((row_id, _)) => match self.blk_idx.find_row(row_id).await {
                RowLocation::NotFound => unreachable!(),
                RowLocation::LwcPage(..) => todo!("lwc page"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = self
                        .mem_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
                        .await
                        .lock_exclusive_async()
                        .await
                        .unwrap();
                    (page_guard, row_id)
                }
            },
        };
        let page = page_guard.page_mut();
        let row_idx = page.row_idx(row_id);
        debug_assert!(!page.is_deleted(row_idx));
        let row = page.row(row_idx);
        let keys = self.metadata().keys_for_delete(row);
        // delete index immediately.
        for key in keys {
            let res = self.delete_index_directly(&key, row_id).await;
            assert!(res);
        }
        page.set_deleted_exclusive(row_idx, true);
    }

    async fn delete_index(&self, key: &SelectKey, row_id: RowID, unique: bool) -> bool {
        // todo: consider index drop.
        let index_schema = &self.metadata().index_specs[key.index_no];
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            let index = self.sec_idx[key.index_no].unique().unwrap();
            self.delete_unique_index(index, key, row_id).await
        } else {
            let index = self.sec_idx[key.index_no].non_unique().unwrap();
            self.delete_non_unique_index(index, key, row_id).await
        }
    }
}
