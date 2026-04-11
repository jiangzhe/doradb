use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::INVALID_PAGE_ID;
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool, PageID, PoolGuards};
use crate::catalog::{CatalogTable, TableMetadata};
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::file::BlockID;
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{IndexCompareExchange, IndexInsert, NonUniqueIndex, RowLocation, UniqueIndex};
use crate::lwc::PersistedLwcBlock;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, LinkForUniqueIndex, ReadRow, ScanMvcc, SelectKey,
    SelectMvcc, UndoCol, UpdateCol, UpdateIndex, UpdateMvcc, UpdateRow,
};
use crate::row::{Row, RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::stmt::Statement;
use crate::table::{
    ColumnDeletionBuffer, ColumnStorage, DeleteInternal, DeleteMarker, DeletionError,
    GenericMemTable, InsertRowIntoPage, Table, UpdateRowInplace, index_key_is_changed,
    index_key_replace, read_latest_index_key, row_len, validate_page_row_range,
};
use crate::trx::redo::{RowRedo, RowRedoKind};
use crate::trx::row::{
    FindOldVersion, LockRowForWrite, LockUndo, ReadAllRows, RowReadAccess, RowWriteAccess,
};
use crate::trx::undo::{IndexBranch, OwnedRowUndo, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::trx::{MIN_SNAPSHOT_TS, TrxID, trx_is_committed};
use crate::value::Val;
use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::ops::Deref;
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
    fn table_scan_uncommitted<F>(
        &self,
        guards: &PoolGuards,
        row_action: F,
    ) -> impl Future<Output = ()>
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
    ) -> impl Future<Output = Result<SelectMvcc>>;

    /// Index lookup unique row including uncommitted version.
    fn index_lookup_unique_uncommitted<R, F>(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_action: F,
    ) -> impl Future<Output = Result<Option<R>>>
    where
        for<'m, 'p> F: FnOnce(&'m TableMetadata, Row<'p>) -> R;

    /// Index scan with MVCC of given key.
    fn index_scan_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = Result<ScanMvcc>>;

    /// Insert row in transaction.
    fn insert_mvcc(
        &self,
        stmt: &mut Statement,
        cols: Vec<Val>,
    ) -> impl Future<Output = Result<InsertMvcc>>;

    /// Update row in transaction.
    /// This method is for update based on unique index lookup.
    /// It also takes care of index change.
    fn update_unique_mvcc(
        &self,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> impl Future<Output = Result<UpdateMvcc>>;

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
    ) -> impl Future<Output = Result<DeleteMvcc>>;

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
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> impl Future<Output = Result<bool>>;
}

/// Thin operation wrapper that exposes `TableAccess` over a table reference.
///
/// `D` is the row-page pool type and `I` is the secondary-index pool type.
/// Runtime aliases bind user tables to evictable row/index pools and catalog
/// tables to the fixed metadata pool for both roles.
pub struct TableAccessor<'a, D: 'static, I: 'static> {
    mem: &'a GenericMemTable<D, I>,
    storage: Option<&'a ColumnStorage>,
}

enum IndexPurgeDecision {
    Delete,
    Keep,
    RowPage(PageID),
}

impl<'a> From<&'a Table> for TableAccessor<'a, EvictableBufferPool, EvictableBufferPool> {
    #[inline]
    fn from(table: &'a Table) -> Self {
        TableAccessor {
            mem: table,
            storage: Some(&table.storage),
        }
    }
}

impl<'a> From<&'a CatalogTable> for TableAccessor<'a, FixedBufferPool, FixedBufferPool> {
    #[inline]
    fn from(table: &'a CatalogTable) -> Self {
        TableAccessor {
            mem: table,
            storage: None,
        }
    }
}

impl<'a, D: BufferPool, I: BufferPool> TableAccessor<'a, D, I> {
    #[inline]
    fn deletion_buffer(&self) -> Option<&ColumnDeletionBuffer> {
        self.storage.map(ColumnStorage::deletion_buffer)
    }

    #[inline]
    async fn index_lookup_unique_row_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
        row_id: RowID,
    ) -> Result<SelectMvcc> {
        loop {
            let location = self
                .try_find_row(stmt.pool_guards(), row_id, self.storage)
                .await?;
            match location {
                RowLocation::NotFound => return Ok(SelectMvcc::NotFound),
                RowLocation::LwcBlock {
                    block_id,
                    row_idx,
                    row_shape_fingerprint,
                } => {
                    let Some(deletion_buffer) = self.deletion_buffer() else {
                        return Ok(SelectMvcc::NotFound);
                    };
                    if let Some(marker) = deletion_buffer.get(row_id) {
                        match marker {
                            DeleteMarker::Committed(ts) => {
                                if ts <= stmt.trx.sts {
                                    return Ok(SelectMvcc::NotFound);
                                }
                            }
                            DeleteMarker::Ref(status) => {
                                let ts = status.ts();
                                if trx_is_committed(ts) {
                                    if ts <= stmt.trx.sts {
                                        return Ok(SelectMvcc::NotFound);
                                    }
                                } else if Arc::ptr_eq(&status, &stmt.trx.status()) {
                                    return Ok(SelectMvcc::NotFound);
                                }
                            }
                        }
                    }
                    let vals = self
                        .read_lwc_row(
                            stmt.pool_guards(),
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                            user_read_set,
                        )
                        .await?;
                    return Ok(SelectMvcc::Found(vals));
                }
                RowLocation::RowPage(page_id) => {
                    let page_guard =
                        match self.get_row_page_shared(stmt.pool_guards(), page_id).await {
                            Ok(Some(page_guard)) => page_guard,
                            Ok(None) => continue,
                            Err(err) => return Err(err),
                        };
                    let page = page_guard.page();
                    if !page.row_id_in_valid_range(row_id) {
                        continue;
                    }
                    let (ctx, page) = page_guard.ctx_and_page();
                    let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
                    return match access.read_row_mvcc(
                        &stmt.trx,
                        self.metadata(),
                        user_read_set,
                        Some(key),
                    ) {
                        ReadRow::Ok(vals) => Ok(SelectMvcc::Found(vals)),
                        ReadRow::InvalidIndex | ReadRow::NotFound => Ok(SelectMvcc::NotFound),
                    };
                }
            }
        }
    }

    #[inline]
    async fn read_lwc_row(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
        read_set: &[usize],
    ) -> Result<Vec<Val>> {
        let Some(storage) = self.storage else {
            return Err(Error::InvalidState);
        };
        let block = PersistedLwcBlock::load(
            storage.file().file_kind(),
            storage.file().sparse_file(),
            storage.disk_pool(),
            guards.disk_guard(),
            block_id,
        )
        .await?;
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(Error::block_corrupted(
                FileKind::TableFile,
                BlockKind::LwcBlock,
                block_id,
                BlockCorruptionCause::InvalidPayload,
            ));
        }
        block.decode_row_values(self.metadata(), row_idx, read_set)
    }

    #[inline]
    fn cold_delete_marker_is_globally_purgeable(
        &self,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> bool {
        let Some(deletion_buffer) = self.deletion_buffer() else {
            return false;
        };
        let Some(marker) = deletion_buffer.get(row_id) else {
            return false;
        };
        let delete_cts = match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => {
                let ts = status.ts();
                if !trx_is_committed(ts) {
                    return false;
                }
                ts
            }
        };
        delete_cts < min_active_sts
    }

    #[inline]
    async fn persisted_lwc_key_differs(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<bool> {
        let read_set = self.metadata().index_specs[key.index_no]
            .index_cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let vals = self
            .read_lwc_row(guards, block_id, row_idx, row_shape_fingerprint, &read_set)
            .await?;
        Ok(vals.as_slice() != key.vals.as_slice())
    }

    #[inline]
    async fn index_purge_decision(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> Result<IndexPurgeDecision> {
        // This path is physical GC cleanup for a previously delete-marked
        // secondary-index entry. A cold delete marker proves that every key for
        // the row is unreachable only after its transaction is committed and
        // older than the current purge horizon.
        if self.cold_delete_marker_is_globally_purgeable(row_id, min_active_sts) {
            return Ok(IndexPurgeDecision::Delete);
        }

        match self.try_find_row(guards, row_id, self.storage).await? {
            RowLocation::NotFound => Ok(IndexPurgeDecision::Delete),
            RowLocation::LwcBlock {
                block_id,
                row_idx,
                row_shape_fingerprint,
            } => {
                // LWC rows are immutable persisted images. If no globally
                // purgeable marker proves the whole row invisible, decode only
                // the indexed columns and delete the purge key only when it no
                // longer matches the persisted current key.
                if self
                    .persisted_lwc_key_differs(
                        guards,
                        key,
                        block_id,
                        row_idx,
                        row_shape_fingerprint,
                    )
                    .await?
                {
                    Ok(IndexPurgeDecision::Delete)
                } else {
                    Ok(IndexPurgeDecision::Keep)
                }
            }
            RowLocation::RowPage(page_id) => Ok(IndexPurgeDecision::RowPage(page_id)),
        }
    }

    #[inline]
    async fn insert_index_no_trx(
        &self,
        guards: &PoolGuards,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<()> {
        let index_pool_guard = self.index_pool_guard(guards);
        if self.metadata().index_specs[key.index_no].unique() {
            let res = self.sec_idx()[key.index_no]
                .unique()
                .unwrap()
                .insert_if_not_exists(index_pool_guard, &key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await?;
            assert!(res.is_ok());
        } else {
            let res = self.sec_idx()[key.index_no]
                .non_unique()
                .unwrap()
                .insert_if_not_exists(index_pool_guard, &key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await?;
            assert!(res.is_ok());
        }
        Ok(())
    }

    #[inline]
    async fn insert_index(
        &self,
        stmt: &mut Statement,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<InsertIndex> {
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
    ) -> Result<(RowID, PageSharedGuard<RowPage>)> {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col_count());
        loop {
            let page_guard = self.get_insert_page(stmt, row_count).await?;
            match self.insert_row_to_page(stmt, page_guard, insert, undo_kind, index_branches) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    stmt.save_active_insert_page(
                        self.table_id(),
                        page_guard.versioned_page_id(),
                        row_id,
                    );
                    return Ok((row_id, page_guard));
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

    #[inline]
    async fn insert_no_trx_inner(&self, guards: &PoolGuards, cols: &[Val]) -> Result<()> {
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
            let mut page_guard =
                GenericMemTable::get_insert_page_exclusive(self, guards, row_count, None).await?;
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
                self.insert_index_no_trx(guards, key, row_id).await?;
            }
            row.finish_insert();
            // Cache insert page.
            GenericMemTable::cache_exclusive_insert_page(self, page_guard);
            return Ok(());
        }
    }

    #[inline]
    async fn delete_unique_no_trx_inner(&self, guards: &PoolGuards, key: &SelectKey) -> Result<()> {
        debug_assert!(key.index_no < self.sec_idx().len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx()[key.index_no].unique().unwrap();
        let index_pool_guard = self.index_pool_guard(guards);
        let (mut page_guard, row_id) = match index
            .lookup(index_pool_guard, &key.vals, MIN_SNAPSHOT_TS)
            .await?
        {
            None => unreachable!(),
            Some((row_id, _)) => match self.find_row(guards, row_id, self.storage).await {
                RowLocation::NotFound => unreachable!(),
                RowLocation::LwcBlock { .. } => todo!("lwc block"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = self
                        .get_row_page_exclusive(guards, page_id)
                        .await
                        .expect("delete_unique_no_trx_inner should not ignore page-I/O failures")
                        .expect("failed to lock exclusive row page");
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
            let res = self.delete_index_directly(guards, &key, row_id).await?;
            assert!(res);
        }
        page.set_deleted_exclusive(row_idx, true);
        Ok(())
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
        let versioned_page_id = page_guard.versioned_page_id();
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
    ) -> Result<(RowID, HashMap<usize, Val>, PageSharedGuard<RowPage>)> {
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
            .await?;
        // do not unlock the page because we may need to update index
        Ok((new_row_id, index_change_cols, new_guard))
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        stmt: &mut Statement,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &HashMap<usize, Val>,
    ) -> Result<UpdateIndex> {
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx().iter().zip(&metadata.index_specs) {
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
                        .await?
                    {
                        UpdateIndex::Updated => (),
                        UpdateIndex::WriteConflict => return Ok(UpdateIndex::WriteConflict),
                        UpdateIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
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
                        .await?;
                    debug_assert!(res.is_updated());
                }
            } // otherwise, in-place update do not change row id, so we do nothing
        }
        Ok(UpdateIndex::Updated)
    }

    #[inline]
    async fn update_indexes_only_row_id_change(
        &self,
        stmt: &mut Statement,
        old_row_id: RowID,
        new_row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx().iter().zip(&metadata.index_specs) {
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
                    .await?;
                debug_assert!(res.is_updated());
            } else {
                let res = self
                    .update_non_unique_index_only_row_id_change(
                        stmt,
                        index.non_unique().unwrap(),
                        key,
                        old_row_id,
                        new_row_id,
                    )
                    .await?;
                debug_assert!(res.is_updated());
            }
        }
        Ok(UpdateIndex::Updated)
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        stmt: &mut Statement,
        old_row_id: RowID,
        new_row_id: RowID,
        index_change_cols: &HashMap<usize, Val>,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx().iter().zip(&metadata.index_specs) {
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
                        .await?
                    {
                        UpdateIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        UpdateIndex::WriteConflict => return Ok(UpdateIndex::WriteConflict),
                        UpdateIndex::Updated => (),
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
                        .await?;
                    debug_assert!(res.is_updated());
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
                        .await?
                    {
                        UpdateIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        UpdateIndex::WriteConflict => return Ok(UpdateIndex::WriteConflict),
                        UpdateIndex::Updated => (),
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
                        .await?;
                    debug_assert!(res.is_updated());
                }
            }
        }
        Ok(UpdateIndex::Updated)
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
    ) -> Result<()> {
        let metadata = self.metadata();
        for (index, index_schema) in self.sec_idx().iter().zip(&metadata.index_specs) {
            debug_assert!(index.is_unique() == index_schema.unique());
            let key = read_latest_index_key(metadata, index.index_no, page_guard, row_id);
            if index_schema.unique() {
                let index = index.unique().unwrap();
                self.defer_delete_unique_index(stmt, index, row_id, key)
                    .await?;
            } else {
                let index = index.non_unique().unwrap();
                self.defer_delete_non_unique_index(stmt, index, row_id, key)
                    .await?;
            }
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
        let index_schema = &self.metadata().index_specs[key.index_no];
        let index_pool_guard = self.index_pool_guard(guards);
        if index_schema.unique() {
            let index = self.sec_idx()[key.index_no].unique().unwrap();
            index
                .compare_delete(index_pool_guard, &key.vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
        } else {
            let index = self.sec_idx()[key.index_no].non_unique().unwrap();
            index
                .compare_delete(index_pool_guard, &key.vals, row_id, true, MIN_SNAPSHOT_TS)
                .await
        }
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        guards: &PoolGuards,
        index: &impl UniqueIndex,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let index_pool_guard = self.index_pool_guard(guards);
        let (page_guard, row_id) = loop {
            match index
                .lookup(index_pool_guard, &key.vals, MIN_SNAPSHOT_TS)
                .await?
            {
                None => return Ok(false), // Another thread deleted this entry.
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return Ok(false);
                    }
                    match self
                        .index_purge_decision(guards, key, row_id, min_active_sts)
                        .await?
                    {
                        IndexPurgeDecision::Delete => {
                            return index
                                .compare_delete(
                                    index_pool_guard,
                                    &key.vals,
                                    row_id,
                                    false,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await;
                        }
                        IndexPurgeDecision::Keep => return Ok(false),
                        IndexPurgeDecision::RowPage(page_id) => {
                            let Some(page_guard) = self
                                .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                                .await?
                            else {
                                continue;
                            };
                            break (page_guard, row_id);
                        }
                    }
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain. Hot row pages still have undo chains, unlike
        // LWC rows whose persisted image is the only current key material.
        if !access.any_version_matches_key(self.metadata(), key) {
            return index
                .compare_delete(index_pool_guard, &key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        guards: &PoolGuards,
        index: &impl NonUniqueIndex,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        let index_pool_guard = self.index_pool_guard(guards);
        let (page_guard, row_id) = loop {
            match index
                .lookup_unique(index_pool_guard, &key.vals, row_id, MIN_SNAPSHOT_TS)
                .await?
            {
                None => return Ok(false), // Another thread deleted this entry.
                Some(active) => {
                    if active {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return Ok(false);
                    }
                    match self
                        .index_purge_decision(guards, key, row_id, min_active_sts)
                        .await?
                    {
                        IndexPurgeDecision::Delete => {
                            return index
                                .compare_delete(
                                    index_pool_guard,
                                    &key.vals,
                                    row_id,
                                    false,
                                    MIN_SNAPSHOT_TS,
                                )
                                .await;
                        }
                        IndexPurgeDecision::Keep => return Ok(false),
                        IndexPurgeDecision::RowPage(page_id) => {
                            let Some(page_guard) = self
                                .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                                .await?
                            else {
                                continue;
                            };
                            break (page_guard, row_id);
                        }
                    }
                }
            }
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain. Hot row pages still have undo chains, unlike
        // LWC rows whose persisted image is the only current key material.
        if !access.any_version_matches_key(self.metadata(), key) {
            return index
                .compare_delete(index_pool_guard, &key.vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
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
    async fn get_insert_page(
        &self,
        stmt: &mut Statement,
        row_count: usize,
    ) -> Result<PageSharedGuard<RowPage>> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id()) {
            let page_guard = self
                .get_row_page_versioned_shared(stmt.pool_guards(), page_id)
                .await?;
            if let Some(page_guard) = page_guard {
                // because we save last insert page in session and meanwhile other thread may access this page
                // and do some modification, even worse, buffer pool may evict it and reload other data into
                // this page. so here, we do not require that no change should happen, but if something change,
                // we validate that page id and row id range is still valid.
                if validate_page_row_range(&page_guard, page_id.page_id, row_id) {
                    return Ok(page_guard);
                }
            }
        }
        let redo_ctx = self.row_page_create_redo_ctx(stmt);
        GenericMemTable::try_get_insert_page(self, stmt.pool_guards(), row_count, redo_ctx).await
    }

    #[inline]
    fn row_page_create_redo_ctx<'b>(
        &self,
        stmt: &'b Statement,
    ) -> Option<RowPageCreateRedoCtx<'b>> {
        self.storage?;
        let engine = stmt
            .trx
            .engine()
            .expect("user-table insert requires an attached engine");
        Some(RowPageCreateRedoCtx::new(&engine.trx_sys, self.table_id()))
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
                page_guard.versioned_page_id(),
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
    ) -> Result<LinkForUniqueIndex> {
        debug_assert!(old_id != new_id);
        let (old_guard, old_id) = loop {
            match self
                .try_find_row(stmt.pool_guards(), old_id, self.storage)
                .await
            {
                Ok(RowLocation::NotFound) => return Ok(LinkForUniqueIndex::NotNeeded),
                Ok(RowLocation::LwcBlock { .. }) => todo!("lwc block"),
                Ok(RowLocation::RowPage(page_id)) => {
                    let Some(old_guard) = self
                        .try_get_validated_row_page_shared_result(
                            stmt.pool_guards(),
                            page_id,
                            old_id,
                        )
                        .await?
                    else {
                        continue;
                    };
                    break (old_guard, old_id);
                }
                Err(err) => return Err(err),
            }
        };
        // The link process is to find one version of the old row that matches
        // given key and then link new row to it.
        let metadata = self.metadata();
        let (ctx, page) = old_guard.ctx_and_page();
        let old_access = RowReadAccess::new(page, ctx, page.row_idx(old_id));
        match old_access.find_old_version_for_unique_key(metadata, key, &stmt.trx) {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::DuplicateKey => Ok(LinkForUniqueIndex::DuplicateKey),
            FindOldVersion::WriteConflict => Ok(LinkForUniqueIndex::WriteConflict),
            FindOldVersion::Ok(old_row, cts, old_entry) => {
                // row latch is enough, because row lock is already acquired.
                let (ctx, page) = new_guard.ctx_and_page();
                let mut new_access =
                    RowWriteAccess::new(page, ctx, page.row_idx(new_id), Some(stmt.trx.sts), false);
                let undo_vals = new_access.row().calc_delta(metadata, &old_row);
                new_access.link_for_unique_index(key.clone(), cts, old_entry, undo_vals);
                Ok(LinkForUniqueIndex::Linked)
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
    ) -> Result<InsertIndex> {
        let index = self.sec_idx()[key.index_no].unique().unwrap();
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        loop {
            match index
                .insert_if_not_exists(index_pool_guard, &key.vals, row_id, false, stmt.trx.sts)
                .await?
            {
                IndexInsert::Ok(merged) => {
                    // insert index success.
                    stmt.push_insert_unique_index_undo(self.table_id(), row_id, key, merged);
                    return Ok(InsertIndex::Inserted);
                }
                IndexInsert::DuplicateKey(old_row_id, deleted) => {
                    // we found there is already one existing row with same key.
                    // so perform move+link.
                    debug_assert!(old_row_id != row_id);
                    if !deleted {
                        // As the key is not deleted, there must be an active row with same key.
                        // todo: change logic if switch to lock-based protocol.
                        return Ok(InsertIndex::DuplicateKey);
                    }
                    match self
                        .link_for_unique_index(stmt, old_row_id, &key, row_id, page_guard)
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(InsertIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(InsertIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded => {
                            // No old row found, so we can update index to point to self.
                            // This may happen because purge thread can remove row data,
                            // but leave index not purged.
                            // The purge thread may delete the key before we apply the update.
                            // so our update can fail.
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &key.vals,
                                    old_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await?
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
                                    return Ok(InsertIndex::Inserted);
                                }
                                IndexCompareExchange::NotExists => {
                                    // There is race condition when GC thread delete the index entry concurrently.
                                    // So try to insert index entry again.
                                    continue;
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(InsertIndex::WriteConflict);
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // There is scenario that two transactions update different rows to the same
                            // key. Because we only search the matched version of old row but not add
                            // logical lock on it, it's possible that both transactions are trying to
                            // update the index. Only one should succeed and the other will fail.
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &key.vals,
                                    old_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await?
                            {
                                IndexCompareExchange::Ok => {
                                    stmt.push_update_unique_index_undo(
                                        self.table_id(),
                                        old_row_id,
                                        row_id,
                                        key,
                                        deleted,
                                    );
                                    return Ok(InsertIndex::Inserted);
                                }
                                IndexCompareExchange::NotExists => {
                                    // The purge thread may concurrently delete the index entry.
                                    // In this case, we need to retry the insertion of index.
                                    continue;
                                }
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
        stmt: &mut Statement,
        key: SelectKey,
        row_id: RowID,
    ) -> Result<InsertIndex> {
        let index = self.sec_idx()[key.index_no].non_unique().unwrap();
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        // For non-unique index, it's guaranteed to be success.
        match index
            .insert_if_not_exists(index_pool_guard, &key.vals, row_id, false, stmt.trx.sts)
            .await?
        {
            IndexInsert::Ok(merged) => {
                // insert index success.
                stmt.push_insert_non_unique_index_undo(self.table_id(), row_id, key, merged);
                Ok(InsertIndex::Inserted)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn defer_delete_unique_index(
        &self,
        stmt: &mut Statement,
        index: &impl UniqueIndex,
        row_id: RowID,
        key: SelectKey,
    ) -> Result<()> {
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        let res = index
            .mask_as_deleted(index_pool_guard, &key.vals, row_id, stmt.trx.sts)
            .await?;
        debug_assert!(res); // should always succeed.
        stmt.push_delete_index_undo(self.table_id(), row_id, key, true);
        Ok(())
    }

    #[inline]
    async fn defer_delete_non_unique_index(
        &self,
        stmt: &mut Statement,
        index: &impl NonUniqueIndex,
        row_id: RowID,
        key: SelectKey,
    ) -> Result<()> {
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        let res = index
            .mask_as_deleted(index_pool_guard, &key.vals, row_id, stmt.trx.sts)
            .await?;
        debug_assert!(res);
        stmt.push_delete_index_undo(self.table_id(), row_id, key, false);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_unique_index_key_and_row_id_change(
        &self,
        stmt: &mut Statement,
        index: &impl UniqueIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        loop {
            // new_row_id is guaranteed to be not in the index.
            match index
                .insert_if_not_exists(
                    index_pool_guard,
                    &new_key.vals,
                    new_row_id,
                    false,
                    stmt.trx.sts,
                )
                .await?
            {
                IndexInsert::Ok(merged) => {
                    debug_assert!(!merged);
                    // New key insert succeed.
                    stmt.push_insert_unique_index_undo(self.table_id(), new_row_id, new_key, false);
                    // mark index of old row as deleted and defer delete.
                    self.defer_delete_unique_index(stmt, index, old_row_id, old_key)
                        .await?;
                    return Ok(UpdateIndex::Updated);
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    // new row id is the insert id so index value must not be the same.
                    debug_assert!(index_row_id != new_row_id);
                    if !deleted {
                        return Ok(UpdateIndex::DuplicateKey);
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
                                index_pool_guard,
                                &new_key.vals,
                                old_row_id.deleted(),
                                new_row_id,
                                stmt.trx.sts,
                            )
                            .await?
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
                                    .await?;
                                return Ok(UpdateIndex::Updated);
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
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(UpdateIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded => {
                            // No old version found.
                            // so we can update index to point to self
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &new_key.vals,
                                    index_row_id,
                                    new_row_id,
                                    stmt.trx.sts,
                                )
                                .await?
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
                                    .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch => {
                                    // This may happen when another transaction insert/update with same key.
                                    return Ok(UpdateIndex::WriteConflict);
                                }
                                IndexCompareExchange::NotExists => {
                                    // Purge thread may delete the index entry before we update,
                                    // we should re-insert.
                                    continue;
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // Old version found and linked.
                            // Because in linking process, we checked the old row status.
                            // The on-page version of old row must be deleted or being
                            // modified by self transaction. That means no other transaction
                            // can modify the new index key concurrently.
                            // So below operation must succeed.
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &new_key.vals,
                                    index_row_id,
                                    new_row_id,
                                    stmt.trx.sts,
                                )
                                .await?
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
                                    .await?;
                                    return Ok(UpdateIndex::Updated);
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
        index: &impl NonUniqueIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        // new_row_id is guaranteed to be not in the index.
        match index
            .insert_if_not_exists(
                index_pool_guard,
                &new_key.vals,
                new_row_id,
                false,
                stmt.trx.sts,
            )
            .await?
        {
            IndexInsert::Ok(merged) => {
                debug_assert!(!merged);
                // New key insert succeed.
                stmt.push_insert_non_unique_index_undo(self.table_id(), new_row_id, new_key, false);
                // mark index of old row as deleted and defer delete.
                self.defer_delete_non_unique_index(stmt, index, old_row_id, old_key)
                    .await?;
                Ok(UpdateIndex::Updated)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn update_unique_index_only_row_id_change(
        &self,
        stmt: &mut Statement,
        index: &impl UniqueIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        match index
            .compare_exchange(
                index_pool_guard,
                &key.vals,
                old_row_id,
                new_row_id,
                stmt.trx.sts,
            )
            .await?
        {
            IndexCompareExchange::Ok => {
                stmt.push_update_unique_index_undo(
                    self.table_id(),
                    old_row_id,
                    new_row_id,
                    key,
                    false,
                );
                Ok(UpdateIndex::Updated)
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
        index: &impl NonUniqueIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> Result<UpdateIndex> {
        debug_assert!(old_row_id != new_row_id);
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
        // insert new entry.
        let res = index
            .insert_if_not_exists(index_pool_guard, &key.vals, new_row_id, false, stmt.trx.sts)
            .await?;
        debug_assert!(res.is_ok());
        stmt.push_insert_non_unique_index_undo(self.table_id(), new_row_id, key.clone(), false);
        // defer delete old entry.
        self.defer_delete_non_unique_index(stmt, index, old_row_id, key)
            .await?;
        Ok(UpdateIndex::Updated)
    }

    /// Update unique index due to key change.
    /// In this scenario, we only need to insert pair of new key and row id
    /// into index. Keep old index entry as is.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        stmt: &mut Statement,
        index: &impl UniqueIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> Result<UpdateIndex> {
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
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
                .insert_if_not_exists(index_pool_guard, &new_key.vals, row_id, true, stmt.trx.sts)
                .await?
            {
                IndexInsert::Ok(merged) => {
                    // Insert new key success.
                    stmt.push_insert_unique_index_undo(self.table_id(), row_id, new_key, merged);
                    // Defer delete old key.
                    self.defer_delete_unique_index(stmt, index, row_id, old_key)
                        .await?;
                    return Ok(UpdateIndex::Updated);
                }
                IndexInsert::DuplicateKey(index_row_id, deleted) => {
                    // There is already a row with same new key.
                    // We have to check its status.
                    if !deleted {
                        // duplicate key.
                        return Ok(UpdateIndex::DuplicateKey);
                    }
                    match self
                        .link_for_unique_index(stmt, index_row_id, &new_key, row_id, page_guard)
                        .await?
                    {
                        LinkForUniqueIndex::DuplicateKey => return Ok(UpdateIndex::DuplicateKey),
                        LinkForUniqueIndex::WriteConflict => {
                            return Ok(UpdateIndex::WriteConflict);
                        }
                        LinkForUniqueIndex::NotNeeded => {
                            // no old row found.
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &new_key.vals,
                                    index_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await?
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
                                        .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(UpdateIndex::WriteConflict);
                                }
                                IndexCompareExchange::NotExists => {
                                    // re-insert
                                    continue;
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // Both old row(index points to) and new row are locked.
                            // we must succeed on updating index.
                            match index
                                .compare_exchange(
                                    index_pool_guard,
                                    &new_key.vals,
                                    index_row_id.deleted(),
                                    row_id,
                                    stmt.trx.sts,
                                )
                                .await?
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
                                        .await?;
                                    return Ok(UpdateIndex::Updated);
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Ok(UpdateIndex::WriteConflict);
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
        index: &impl NonUniqueIndex,
        old_key: SelectKey,
        new_key: SelectKey,
        row_id: RowID,
    ) -> Result<UpdateIndex> {
        let index_pool_guard = self.index_pool_guard(stmt.pool_guards());
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
            .insert_if_not_exists(index_pool_guard, &new_key.vals, row_id, true, stmt.trx.sts)
            .await?
        {
            IndexInsert::Ok(merged) => {
                stmt.push_insert_non_unique_index_undo(self.table_id(), row_id, new_key, merged);
                // Defer delete old key.
                self.defer_delete_non_unique_index(stmt, index, row_id, old_key)
                    .await?;
                Ok(UpdateIndex::Updated)
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }
}

/// Runtime accessor type binding for user tables:
/// - `D = EvictableBufferPool` for row pages
/// - `I = EvictableBufferPool` for secondary indexes
pub type HybridTableAccessor<'a> = TableAccessor<'a, EvictableBufferPool, EvictableBufferPool>;

/// Runtime accessor type binding for in-memory catalog tables:
/// - `D = FixedBufferPool` for row pages
/// - `I = FixedBufferPool` for secondary indexes
pub type MemTableAccessor<'a> = TableAccessor<'a, FixedBufferPool, FixedBufferPool>;

impl TableAccessor<'_, FixedBufferPool, FixedBufferPool> {
    #[inline]
    pub(crate) async fn insert_catalog_no_trx(
        &self,
        guards: &PoolGuards,
        cols: &[Val],
    ) -> Result<()> {
        self.insert_no_trx_inner(guards, cols).await
    }

    #[inline]
    pub(crate) async fn delete_catalog_unique_no_trx(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Result<()> {
        self.delete_unique_no_trx_inner(guards, key).await
    }
}

impl<D: BufferPool, I: BufferPool> TableAccess for TableAccessor<'_, D, I> {
    async fn table_scan_uncommitted<F>(&self, guards: &PoolGuards, mut row_action: F)
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool,
    {
        self.mem_scan(guards, |page_guard| {
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
        self.mem_scan(stmt.pool_guards(), |page_guard| {
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
    ) -> Result<SelectMvcc> {
        debug_assert!(key.index_no < self.sec_idx().len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        match self.sec_idx()[key.index_no]
            .unique()
            .unwrap()
            .lookup(
                self.index_pool_guard(stmt.pool_guards()),
                &key.vals,
                stmt.trx.sts,
            )
            .await?
        {
            None => Ok(SelectMvcc::NotFound),
            Some((row_id, _)) => {
                self.index_lookup_unique_row_mvcc(stmt, key, user_read_set, row_id)
                    .await
            }
        }
    }

    async fn index_lookup_unique_uncommitted<R, F>(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_action: F,
    ) -> Result<Option<R>>
    where
        for<'m, 'p> F: FnOnce(&'m TableMetadata, Row<'p>) -> R,
    {
        debug_assert!(key.index_no < self.sec_idx().len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let (page_guard, row_id) = match self.sec_idx()[key.index_no]
            .unique()
            .unwrap()
            .lookup(self.index_pool_guard(guards), &key.vals, MIN_SNAPSHOT_TS)
            .await?
        {
            None => return Ok(None),
            Some((row_id, _)) => match self.find_row(guards, row_id, self.storage).await {
                RowLocation::NotFound => return Ok(None),
                RowLocation::LwcBlock { .. } => todo!("lwc block"),
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
        let metadata = &*ctx.row_ver().unwrap().metadata;
        let access = RowReadAccess::new(page, ctx, page.row_idx(row_id));
        let row = access.row();
        // latest version in row page.
        if row.is_deleted() {
            return Ok(None);
        }
        if row.is_key_different(self.metadata(), key) {
            return Ok(None);
        }
        Ok(Some(row_action(metadata, row)))
    }

    async fn index_scan_mvcc(
        &self,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        debug_assert!(key.index_no < self.sec_idx().len());
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
        self.sec_idx()[key.index_no]
            .non_unique()
            .unwrap()
            .lookup(
                self.index_pool_guard(stmt.pool_guards()),
                &key.vals,
                &mut row_ids,
                stmt.trx.sts,
            )
            .await?;
        let mut res = vec![];
        for row_id in row_ids {
            match self
                .index_lookup_unique_row_mvcc(stmt, key, user_read_set, row_id)
                .await?
            {
                SelectMvcc::NotFound => (),
                SelectMvcc::Found(vals) => {
                    res.push(vals);
                }
            }
        }
        Ok(ScanMvcc::Rows(res))
    }

    async fn insert_mvcc(&self, stmt: &mut Statement, cols: Vec<Val>) -> Result<InsertMvcc> {
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
            .await?;
        // insert index
        for key in keys {
            match self.insert_index(stmt, key, row_id, &page_guard).await? {
                InsertIndex::Inserted => (),
                InsertIndex::DuplicateKey => {
                    return Ok(InsertMvcc::DuplicateKey);
                }
                InsertIndex::WriteConflict => {
                    return Ok(InsertMvcc::WriteConflict);
                }
            }
        }
        page_guard.set_dirty(); // mark as dirty page.
        Ok(InsertMvcc::Inserted(row_id))
    }

    async fn update_unique_mvcc(
        &self,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        debug_assert!(key.index_no < self.sec_idx().len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx()[key.index_no].unique().unwrap();
        loop {
            let (page_guard, row_id) = match index
                .lookup(
                    self.index_pool_guard(stmt.pool_guards()),
                    &key.vals,
                    stmt.trx.sts,
                )
                .await?
            {
                None => return Ok(UpdateMvcc::NotFound),
                Some((row_id, _)) => match self
                    .try_find_row(stmt.pool_guards(), row_id, self.storage)
                    .await
                {
                    Ok(RowLocation::NotFound) => return Ok(UpdateMvcc::NotFound),
                    Ok(RowLocation::LwcBlock { .. }) => todo!("lwc block"),
                    Ok(RowLocation::RowPage(page_id)) => {
                        let Some(page_guard) = self
                            .try_get_validated_row_page_shared_result(
                                stmt.pool_guards(),
                                page_id,
                                row_id,
                            )
                            .await?
                        else {
                            continue;
                        };
                        (page_guard, row_id)
                    }
                    Err(err) => return Err(err),
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
                            .await?;
                        page_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Updated => Ok(UpdateMvcc::Updated(new_row_id)),
                            UpdateIndex::DuplicateKey => Ok(UpdateMvcc::DuplicateKey),
                            UpdateIndex::WriteConflict => Ok(UpdateMvcc::WriteConflict),
                        };
                    } // otherwise, do nothing
                    page_guard.set_dirty(); // mark as dirty page.
                    return Ok(UpdateMvcc::Updated(row_id));
                }
                UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => {
                    return Ok(UpdateMvcc::NotFound);
                }
                UpdateRowInplace::WriteConflict => return Ok(UpdateMvcc::WriteConflict),
                UpdateRowInplace::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                UpdateRowInplace::NoFreeSpace(old_row_id, old_row, update, old_guard) => {
                    // in-place update failed, we transfer update into
                    // move+update.
                    let (new_row_id, index_change_cols, new_guard) = self
                        .move_update_for_space(stmt, old_row, update, old_row_id, old_guard)
                        .await?;
                    if !index_change_cols.is_empty() {
                        let res = self
                            .update_indexes_may_both_change(
                                stmt,
                                old_row_id,
                                new_row_id,
                                &index_change_cols,
                                &new_guard,
                            )
                            .await?;
                        // old guard is already marked inside.
                        new_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Updated => Ok(UpdateMvcc::Updated(new_row_id)),
                            UpdateIndex::DuplicateKey => Ok(UpdateMvcc::DuplicateKey),
                            UpdateIndex::WriteConflict => Ok(UpdateMvcc::WriteConflict),
                        };
                    } else {
                        let res = self
                            .update_indexes_only_row_id_change(
                                stmt, old_row_id, new_row_id, &new_guard,
                            )
                            .await?;
                        new_guard.set_dirty(); // mark as dirty page.
                        return match res {
                            UpdateIndex::Updated => Ok(UpdateMvcc::Updated(new_row_id)),
                            UpdateIndex::DuplicateKey => Ok(UpdateMvcc::DuplicateKey),
                            UpdateIndex::WriteConflict => Ok(UpdateMvcc::WriteConflict),
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
    ) -> Result<DeleteMvcc> {
        debug_assert!(key.index_no < self.sec_idx().len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx()[key.index_no].unique().unwrap();
        loop {
            let (page_guard, row_id) = match index
                .lookup(
                    self.index_pool_guard(stmt.pool_guards()),
                    &key.vals,
                    stmt.trx.sts,
                )
                .await?
            {
                None => return Ok(DeleteMvcc::NotFound),
                Some((row_id, _)) => match self
                    .try_find_row(stmt.pool_guards(), row_id, self.storage)
                    .await
                {
                    Ok(RowLocation::NotFound) => return Ok(DeleteMvcc::NotFound),
                    Ok(RowLocation::LwcBlock { .. }) => {
                        let deletion_buffer = self
                            .deletion_buffer()
                            .expect("catalog table should never have lwc block rows");
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
                                return Ok(DeleteMvcc::Deleted);
                            }
                            Err(DeletionError::WriteConflict) => {
                                return Ok(DeleteMvcc::WriteConflict);
                            }
                            Err(DeletionError::AlreadyDeleted) => {
                                return Ok(DeleteMvcc::NotFound);
                            }
                        }
                    }
                    Ok(RowLocation::RowPage(page_id)) => {
                        let Some(page_guard) = self
                            .try_get_validated_row_page_shared_result(
                                stmt.pool_guards(),
                                page_id,
                                row_id,
                            )
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
                .delete_row_internal(stmt, page_guard, row_id, key, log_by_key)
                .await
            {
                DeleteInternal::NotFound => return Ok(DeleteMvcc::NotFound),
                DeleteInternal::WriteConflict => return Ok(DeleteMvcc::WriteConflict),
                DeleteInternal::RetryInTransition => {
                    smol::Timer::after(std::time::Duration::from_millis(1)).await;
                    continue;
                }
                DeleteInternal::Ok(page_guard) => {
                    // defer index deletion with index undo log.
                    self.defer_delete_indexes(stmt, row_id, &page_guard).await?;
                    page_guard.set_dirty(); // mark as dirty.
                    return Ok(DeleteMvcc::Deleted);
                }
            }
        }
    }

    async fn delete_index(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> Result<bool> {
        // todo: consider index drop.
        let index_schema = &self.metadata().index_specs[key.index_no];
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            let index = self.sec_idx()[key.index_no].unique().unwrap();
            self.delete_unique_index(guards, index, key, row_id, min_active_sts)
                .await
        } else {
            let index = self.sec_idx()[key.index_no].non_unique().unwrap();
            self.delete_non_unique_index(guards, index, key, row_id, min_active_sts)
                .await
        }
    }
}

impl<D: BufferPool, I: BufferPool> Deref for TableAccessor<'_, D, I> {
    type Target = GenericMemTable<D, I>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.mem
    }
}
