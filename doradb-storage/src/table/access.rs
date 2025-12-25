use crate::buffer::BufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::index::{NonUniqueIndex, RowLocation, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, ScanMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateIndex,
    UpdateMvcc,
};
use crate::row::{Row, RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::stmt::Statement;
use crate::table::{DeleteInternal, Table, UpdateRowInplace, row_len};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::undo::RowUndoKind;
use crate::value::Val;
use std::future::Future;

pub trait TableAccess {
    /// Table scan including uncommitted versions.
    fn table_scan_uncommitted<P: BufferPool, F>(
        &self,
        data_pool: &'static P,
        start_row_id: RowID,
        row_action: F,
    ) -> impl Future<Output = ()>
    where
        F: for<'a> FnMut(Row<'a>) -> bool;

    // /// Table scan with MVCC.
    // fn table_scan_mvcc<P: BufferPool, F>(
    //     &self,
    //     data_pool: &'static P,
    //     stmt: &Statement,
    //     start_row_id: RowID,
    //     row_action: F,
    // ) -> impl Future<Output = ()>
    // where
    //     F: for<'a> FnMut(Row<'a>) -> bool;

    /// Index lookup unique row with MVCC.
    /// Result should be no more than one row.
    fn index_lookup_unique_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = SelectMvcc>;

    /// Index lookup unique row including uncommitted version.
    fn index_lookup_unique_uncommitted<P: BufferPool, R, F>(
        &self,
        data_pool: &'static P,
        key: &SelectKey,
        row_action: F,
    ) -> impl Future<Output = Option<R>>
    where
        for<'a> F: FnOnce(Row<'a>) -> R;

    /// Index scan with MVCC of given key.
    fn index_scan_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> impl Future<Output = ScanMvcc>;

    /// Insert row in transaction.
    fn insert_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &mut Statement,
        cols: Vec<Val>,
    ) -> impl Future<Output = InsertMvcc>;

    /// Insert row in non-transactional way.
    fn insert_no_trx<P: BufferPool>(
        &self,
        data_pool: &'static P,
        cols: &[Val],
    ) -> impl Future<Output = ()>;

    /// Update row in transaction.
    /// This method is for update based on unique index lookup.
    /// It also takes care of index change.
    ///
    /// If parameter disable_inplace is set to true, update will be
    /// converted to delete+insert.
    fn update_unique_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
        disable_inplace: bool,
    ) -> impl Future<Output = UpdateMvcc>;

    /// Delete row in transaction.
    /// This method is for delete based on unique index lookup.
    ///
    /// If the parameter log_by_key is set to true, the delete operation
    /// is logged with (unique) key instead of row id.
    /// Such type of log is used for catalog tables, which will have
    /// inconsistent page_id/row_id among multiple restarts(recoveries)
    /// of database.
    fn delete_unique_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &mut Statement,
        key: &SelectKey,
        log_by_key: bool,
    ) -> impl Future<Output = DeleteMvcc>;

    /// Delete row in non-transactional way.
    fn delete_unique_no_trx<P: BufferPool>(
        &self,
        data_pool: &'static P,
        key: &SelectKey,
    ) -> impl Future<Output = ()>;

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
    fn delete_index<P: BufferPool>(
        &self,
        data_pool: &'static P,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
    ) -> impl Future<Output = bool>;
}

impl TableAccess for Table {
    async fn table_scan_uncommitted<P: BufferPool, F>(
        &self,
        data_pool: &'static P,
        start_row_id: RowID,
        mut row_action: F,
    ) where
        F: for<'a> FnMut(Row<'a>) -> bool,
    {
        // With cursor, we lock two pages in block index and one row page
        // when scanning rows.
        let mut cursor = self.blk_idx.cursor();
        cursor.seek(start_row_id).await;
        while let Some(leaf) = cursor.next().await {
            let g = leaf.shared_async().await;
            debug_assert!(g.page().is_leaf());
            let blocks = g.page().leaf_blocks();
            for block in blocks {
                for page_entry in block.row_page_entries() {
                    let row_page: PageSharedGuard<RowPage> = data_pool
                        .get_page(page_entry.page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    for row_access in row_page.read_all_rows() {
                        if !row_action(row_access.row()) {
                            return;
                        }
                    }
                }
            }
        }
    }

    // async fn table_vector_scan_mvcc<P: BufferPool, B, F>(
    //     &self,
    //     data_pool: &'static P,
    //     stmt: &Statement,
    //     start_row_id: RowID,
    //     column_action: F,
    // ) -> ()
    // where
    //     B: Bitmap,
    //     F: for<'a> FnMut(&B, usize, ) -> bool,
    // {
    // }

    async fn index_lookup_unique_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
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
                self.index_lookup_unique_row_mvcc(data_pool, stmt, key, user_read_set, row_id)
                    .await
            }
        }
    }

    async fn index_lookup_unique_uncommitted<P: BufferPool, R, F>(
        &self,
        data_pool: &'static P,
        key: &SelectKey,
        row_action: F,
    ) -> Option<R>
    where
        for<'a> F: FnOnce(Row<'a>) -> R,
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
                    let page_guard = data_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    (page_guard, row_id)
                }
            },
        };
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return None;
        }
        let access = page_guard.read_row_by_id(row_id);
        let row = access.row();
        // latest version in row page.
        if row.is_deleted() {
            return None;
        }
        if row.is_key_different(self.metadata(), key) {
            return None;
        }
        Some(row_action(row))
    }

    async fn index_scan_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
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
                .index_lookup_unique_row_mvcc(data_pool, stmt, key, user_read_set, row_id)
                .await
            {
                SelectMvcc::NotFound => (),
                SelectMvcc::Ok(vals) => {
                    res.push(vals);
                }
            }
        }
        res
    }

    async fn insert_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &mut Statement,
        cols: Vec<Val>,
    ) -> InsertMvcc {
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
            .insert_row_internal(data_pool, stmt, cols, RowUndoKind::Insert, None)
            .await;
        // insert index
        for key in keys {
            match self
                .insert_index(data_pool, stmt, key, row_id, &page_guard)
                .await
            {
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

    async fn insert_no_trx<P: BufferPool>(&self, data_pool: &'static P, cols: &[Val]) {
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
                .get_insert_page_exclusive(data_pool, row_count)
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
                row.update_col(col_idx, user_col, false);
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

    async fn update_unique_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
        disable_inplace: bool,
    ) -> UpdateMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        let (page_guard, row_id) = match index.lookup(&key.vals, stmt.trx.sts).await {
            None => return UpdateMvcc::NotFound,
            Some((row_id, _)) => match self.blk_idx.find_row(row_id).await {
                RowLocation::NotFound => return UpdateMvcc::NotFound,
                RowLocation::LwcPage(..) => todo!("lwc page"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = data_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    (page_guard, row_id)
                }
            },
        };
        if disable_inplace {
            todo!()
        }
        let res = self
            .update_row_inplace(stmt, page_guard, key, row_id, update)
            .await;
        match res {
            UpdateRowInplace::Ok(new_row_id, index_change_cols, page_guard) => {
                debug_assert!(row_id == new_row_id);
                if !index_change_cols.is_empty() {
                    // Index may change, we should check whether each index key change and update correspondingly.
                    let res = self
                        .update_indexes_only_key_change(
                            data_pool,
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
                UpdateMvcc::Ok(row_id)
            }
            UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => UpdateMvcc::NotFound,
            UpdateRowInplace::WriteConflict => UpdateMvcc::WriteConflict,
            UpdateRowInplace::NoFreeSpace(old_row_id, old_row, update, old_guard) => {
                // in-place update failed, we transfer update into
                // move+update.
                let (new_row_id, index_change_cols, new_guard) = self
                    .move_update_for_space(data_pool, stmt, old_row, update, old_row_id, old_guard)
                    .await;
                if !index_change_cols.is_empty() {
                    let res = self
                        .update_indexes_may_both_change(
                            data_pool,
                            stmt,
                            old_row_id,
                            new_row_id,
                            &index_change_cols,
                            &new_guard,
                        )
                        .await;
                    // old guard is already marked inside.
                    new_guard.set_dirty(); // mark as dirty page.
                    match res {
                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                    }
                } else {
                    let res = self
                        .update_indexes_only_row_id_change(stmt, old_row_id, new_row_id, &new_guard)
                        .await;
                    new_guard.set_dirty(); // mark as dirty page.
                    match res {
                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                    }
                }
            }
        }
    }

    async fn delete_unique_mvcc<P: BufferPool>(
        &self,
        data_pool: &'static P,
        stmt: &mut Statement,
        key: &SelectKey,
        log_by_key: bool,
    ) -> DeleteMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.metadata().index_specs[key.index_no].unique());
        debug_assert!(self.metadata().index_type_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        let (page_guard, row_id) = match index.lookup(&key.vals, stmt.trx.sts).await {
            None => return DeleteMvcc::NotFound,
            Some((row_id, _)) => match self.blk_idx.find_row(row_id).await {
                RowLocation::NotFound => return DeleteMvcc::NotFound,
                RowLocation::LwcPage(..) => todo!("lwc page"),
                RowLocation::RowPage(page_id) => {
                    let page_guard = data_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    (page_guard, row_id)
                }
            },
        };
        match self
            .delete_row_internal(stmt, page_guard, row_id, key, log_by_key)
            .await
        {
            DeleteInternal::NotFound => DeleteMvcc::NotFound,
            DeleteInternal::WriteConflict => DeleteMvcc::WriteConflict,
            DeleteInternal::Ok(page_guard) => {
                // defer index deletion with index undo log.
                self.defer_delete_indexes(stmt, row_id, &page_guard).await;
                page_guard.set_dirty(); // mark as dirty.
                DeleteMvcc::Ok
            }
        }
    }

    async fn delete_unique_no_trx<P: BufferPool>(&self, data_pool: &'static P, key: &SelectKey) {
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
                    let page_guard = data_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Exclusive)
                        .await
                        .exclusive_async()
                        .await;
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

    async fn delete_index<P: BufferPool>(
        &self,
        data_pool: &'static P,
        key: &SelectKey,
        row_id: RowID,
        unique: bool,
    ) -> bool {
        // todo: consider index drop.
        let index_schema = &self.metadata().index_specs[key.index_no];
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            let index = self.sec_idx[key.index_no].unique().unwrap();
            self.delete_unique_index(data_pool, index, key, row_id)
                .await
        } else {
            let index = self.sec_idx[key.index_no].non_unique().unwrap();
            self.delete_non_unique_index(data_pool, index, key, row_id)
                .await
        }
    }
}
