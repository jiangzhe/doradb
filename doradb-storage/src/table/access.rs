use crate::buffer::BufferPool;
use crate::buffer::page::PageID;
use crate::catalog::TableMetadata;
use crate::index::{NonUniqueIndex, RowLocation, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::lwc::LwcBuilder;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, ReadRow, ScanMvcc, SelectKey, SelectMvcc, UpdateCol,
    UpdateIndex, UpdateMvcc,
};
use crate::row::{Row, RowID, RowPage, RowRead, estimate_max_row_count, var_len_for_insert};
use crate::stmt::Statement;
use crate::table::{DeleteInternal, Table, UpdateRowInplace, row_len};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::row::{ReadAllRows, RowReadAccess};
use crate::trx::undo::RowUndoKind;
use crate::trx::ver_map::RowPageState;
use crate::trx::sys::TransactionSystem;
use crate::error::Result;
use crate::file::table_file::{LwcPagePersist, MutableTableFile};
use crate::value::Val;
use std::future::Future;
use std::time::Duration;

pub trait TableAccess {
    /// Table scan including uncommitted versions.
    fn table_scan_uncommitted<F>(
        &self,
        start_row_id: RowID,
        row_action: F,
    ) -> impl Future<Output = ()>
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool;

    /// Table scan with MVCC.
    fn table_scan_mvcc<F>(
        &self,
        stmt: &Statement,
        start_row_id: RowID,
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

    /// Freeze row pages.
    /// Returns number of pages that are frozen.
    fn freeze(&self, max_rows: usize) -> impl Future<Output = usize>;

    /// Convert frozen row pages to LWC pages and persist to table file.
    fn data_checkpoint(
        &self,
        trx_sys: &'static TransactionSystem,
    ) -> impl Future<Output = Result<()>>;
}

impl TableAccess for Table {
    async fn table_scan_uncommitted<F>(&self, start_row_id: RowID, mut row_action: F)
    where
        F: for<'m, 'p> FnMut(&'m TableMetadata, Row<'p>) -> bool,
    {
        self.mem_scan(start_row_id, |page_guard| {
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

    async fn table_scan_mvcc<F>(
        &self,
        stmt: &Statement,
        start_row_id: RowID,
        read_set: &[usize],
        mut row_action: F,
    ) where
        F: FnMut(Vec<Val>) -> bool,
    {
        self.mem_scan(start_row_id, |page_guard| {
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
                        .data_pool
                        .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
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
            }
        }
        res
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
                .get_insert_page_exclusive(self.data_pool, row_count)
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
                            .data_pool
                            .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                            .await
                            .shared_async()
                            .await;
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
                    RowLocation::LwcPage(..) => todo!("lwc page"),
                    RowLocation::RowPage(page_id) => {
                        let page_guard = self
                            .data_pool
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
                        .data_pool
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

    async fn freeze(&self, max_rows: usize) -> usize {
        let mut rows = 0usize;
        self.mem_scan(0, |page_guard| {
            let (ctx, page) = page_guard.ctx_and_page();
            let vmap = ctx.row_ver().unwrap();
            rows += page.header.approx_non_deleted();
            if vmap.is_frozen() {
                return rows < max_rows;
            }
            // set frozen to true.
            vmap.set_frozen();
            rows < max_rows
        })
        .await;
        rows
    }

    async fn data_checkpoint(&self, trx_sys: &'static TransactionSystem) -> Result<()> {
        struct FrozenPage {
            page_id: PageID,
            start_row_id: RowID,
            end_row_id: RowID,
        }

        let pivot_row_id = self.file.active_root().pivot_row_id;
        let mut frozen_pages = Vec::new();
        let mut expected_row_id = pivot_row_id;
        let mut found_frozen = true;
        self.mem_scan(pivot_row_id, |page_guard| {
            if !found_frozen {
                return false;
            }
            let page = page_guard.page();
            if page.header.start_row_id != expected_row_id {
                found_frozen = false;
                return false;
            }
            let (ctx, _) = page_guard.ctx_and_page();
            let row_ver = ctx.row_ver().unwrap();
            if row_ver.state() != RowPageState::Frozen {
                found_frozen = false;
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

        if !frozen_pages.is_empty() {
            loop {
                let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
                let mut stabilized = true;
                for page_info in &frozen_pages {
                    let page_guard = self
                        .data_pool
                        .get_page::<RowPage>(page_info.page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    let (ctx, _) = page_guard.ctx_and_page();
                    let row_ver = ctx.row_ver().unwrap();
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

            for page_info in &frozen_pages {
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

        let mut trx = trx_sys.begin_checkpoint_trx();
        let sts = trx.sts;
        let new_pivot_row_id = frozen_pages
            .last()
            .map(|page| page.end_row_id)
            .unwrap_or(pivot_row_id);
        let build_result = async {
            let mut lwc_pages = Vec::new();
            if !frozen_pages.is_empty() {
                let min_active_sts = trx_sys.calc_min_active_sts_for_gc();
                let metadata = self.metadata();
                let mut builder = LwcBuilder::new(metadata);
                let mut current_start: Option<RowID> = None;
                let mut current_end: RowID = 0;
                for page_info in &frozen_pages {
                    let page_guard = self
                        .data_pool
                        .get_page::<RowPage>(page_info.page_id, LatchFallbackMode::Shared)
                        .await
                        .shared_async()
                        .await;
                    let (ctx, page) = page_guard.ctx_and_page();
                    let view = page.vector_view_in_transition(metadata, ctx, sts, min_active_sts);
                    if view.rows_non_deleted() == 0 {
                        continue;
                    }
                    if builder.is_empty() {
                        current_start = Some(page_info.start_row_id);
                        current_end = page_info.end_row_id;
                    }
                    if !builder.append_view(page, view)? {
                        let buf = builder.build()?;
                        lwc_pages.push(LwcPagePersist {
                            start_row_id: current_start.unwrap(),
                            end_row_id: current_end,
                            buf,
                        });
                        builder = LwcBuilder::new(metadata);
                        current_start = Some(page_info.start_row_id);
                        current_end = page_info.end_row_id;
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
                        start_row_id: current_start.unwrap(),
                        end_row_id: current_end,
                        buf,
                    });
                }
            }

            let mut heap_redo_start_cts = None;
            self.mem_scan(new_pivot_row_id, |page_guard| {
                let (ctx, _) = page_guard.ctx_and_page();
                let row_ver = ctx.row_ver().unwrap();
                if row_ver.state() == RowPageState::Active {
                    let cts = row_ver.create_cts();
                    heap_redo_start_cts = Some(match heap_redo_start_cts {
                        Some(prev) => prev.min(cts),
                        None => cts,
                    });
                }
                true
            })
            .await;
            let heap_redo_start_cts = heap_redo_start_cts.unwrap_or(sts);
            Ok((lwc_pages, heap_redo_start_cts))
        }
        .await;
        let (lwc_pages, heap_redo_start_cts) = match build_result {
            Ok(payload) => payload,
            Err(err) => {
                trx_sys.rollback(trx, self.data_pool).await;
                return Err(err);
            }
        };
        let mut lwc_pages = lwc_pages;
        if let Some(last) = lwc_pages.last_mut() {
            if last.end_row_id < new_pivot_row_id {
                last.end_row_id = new_pivot_row_id;
            }
        }
        let gc_pages: Vec<PageID> = frozen_pages.iter().map(|page| page.page_id).collect();
        trx.extend_gc_row_pages(gc_pages);

        let cts = match trx_sys.commit(trx, self.data_pool).await {
            Ok(cts) => cts,
            Err(err) => return Err(err),
        };

        let table_file = MutableTableFile::fork(&self.file);
        let (table_file, old_root) = if !lwc_pages.is_empty() {
            table_file
                .persist_lwc_pages(lwc_pages, heap_redo_start_cts, cts)
                .await?
        } else {
            table_file
                .update_checkpoint(new_pivot_row_id, heap_redo_start_cts, cts)
                .await?
        };
        self.blk_idx.update_file_root(table_file.active_root()).await;
        drop(old_root);
        Ok(())
    }
}
