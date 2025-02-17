// pub mod schema;
#[cfg(test)]
mod tests;

use crate::buffer::guard::PageSharedGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::{IndexSchema, TableSchema};
use crate::index::{BlockIndex, RowLocation, SecondaryIndex, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, InsertRow, MoveLinkForIndex, ReadRow, SelectKey,
    SelectMvcc, SelectResult, SelectUncommitted, UndoCol, UpdateCol, UpdateIndex, UpdateMvcc,
    UpdateRow,
};
use crate::row::Row;
use crate::row::{estimate_max_row_count, RowID, RowPage, RowRead};
use crate::stmt::Statement;
use crate::trx::redo::{RedoEntry, RedoKind};
use crate::trx::row::{RowLatestStatus, RowReadAccess, RowWriteAccess};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, NextRowUndo, NextRowUndoStatus, NextTrxCTS, OwnedRowUndo,
    RowUndoBranch, RowUndoHead, RowUndoKind, RowUndoRef,
};
use crate::trx::{trx_is_committed, ActiveTrx, TrxID};
use crate::value::{Val, PAGE_VAR_LEN_INLINE};
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;

// todo: integrate with doradb_catalog::TableID.
pub type TableID = u64;

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
/// 3. Buffer pool take care of creating and fetching pages.
///
/// 4. Row page stores latest version fo row data.
///
/// 5. Undo map stores old versions of row data.
///
/// We have a separate undo array associated to each row in row page.
/// The undo head also acts as the (logical) row lock, so that transactions
/// can abort/wait if write conflict is found on acquire lock of undo head.
///
/// Insert/update/delete operation will add one or more undo entry to the
/// chain linked to undo head.
///
/// Select operation will traverse undo chain to find visible version.
///
/// Additional key validation is performed if index lookup is used, because
/// index does not contain version information, and out-of-date index entry
/// should ignored if visible data version does not match index key.
pub struct Table<P: BufferPool> {
    pub table_id: TableID,
    pub schema: Arc<TableSchema>,
    pub blk_idx: Arc<BlockIndex<P>>,
    // todo: secondary indexes.
    pub sec_idx: Arc<[SecondaryIndex]>,
}

impl<P: BufferPool> Table<P> {
    /// Create a new table.
    #[inline]
    pub async fn new(buf_pool: P, table_id: TableID, schema: TableSchema) -> Self {
        let blk_idx = BlockIndex::new(buf_pool).await.unwrap();
        let sec_idx: Vec<_> = schema
            .indexes
            .iter()
            .enumerate()
            .map(|(index_no, index_schema)| {
                SecondaryIndex::new(index_no, index_schema, schema.user_types())
            })
            .collect();
        Table {
            table_id,
            schema: Arc::new(schema),
            blk_idx: Arc::new(blk_idx),
            sec_idx: Arc::from(sec_idx.into_boxed_slice()),
        }
    }

    #[inline]
    pub async fn scan_rows(&self) {
        todo!()
    }

    /// Select row with unique index with MVCC.
    /// Result should be no more than one row.
    #[inline]
    pub async fn select_row_mvcc(
        &self,
        buf_pool: P,
        stmt: &Statement,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.schema.indexes[key.index_no].unique);
        debug_assert!(self.schema.index_layout_match(key.index_no, &key.vals));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        loop {
            match self.sec_idx[key.index_no]
                .unique()
                .unwrap()
                .lookup(&key.vals)
            {
                None => return SelectMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return SelectMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page_guard =
                            buf_pool.get_page(page_id, LatchFallbackMode::Shared).await;
                        let page_guard = page_guard.block_until_shared();
                        if !validate_page_id(&page_guard, page_id) {
                            continue;
                        }
                        let page = page_guard.page();
                        if !page.row_id_in_valid_range(row_id) {
                            return SelectMvcc::NotFound;
                        }
                        let row_idx = page.row_idx(row_id);
                        let access = self
                            .lock_row_for_read(&stmt.trx, &page_guard, row_idx)
                            .await;
                        return match access.read_row_mvcc(
                            &stmt.trx,
                            &self.schema,
                            user_read_set,
                            key,
                        ) {
                            ReadRow::Ok(vals) => SelectMvcc::Ok(vals),
                            ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
                        };
                    }
                },
            }
        }
    }

    /// Select row with unique index in uncommitted mode.
    #[inline]
    pub async fn select_row_uncommitted<R, F>(
        &self,
        buf_pool: P,
        key: &SelectKey,
        row_action: F,
    ) -> Option<R>
    where
        F: FnOnce(Row) -> R,
    {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.schema.indexes[key.index_no].unique);
        debug_assert!(self.schema.index_layout_match(key.index_no, &key.vals));
        loop {
            match self.sec_idx[key.index_no]
                .unique()
                .unwrap()
                .lookup(&key.vals)
            {
                None => return None,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return None,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page_guard =
                            buf_pool.get_page(page_id, LatchFallbackMode::Shared).await;
                        let page_guard = page_guard.block_until_shared();
                        if !validate_page_id(&page_guard, page_id) {
                            continue;
                        }
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
                        if row.is_key_different(&self.schema, key) {
                            return None;
                        }
                        return Some(row_action(row));
                    }
                },
            }
        }
    }

    /// Insert row with MVCC.
    /// This method will also take care of index update.
    #[inline]
    pub async fn insert_row(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        cols: Vec<Val>,
    ) -> InsertMvcc {
        debug_assert!(cols.len() + 1 == self.schema.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.schema.user_col_type_match(idx, val))
        });
        // let key = cols[self.schema.user_key_idx()].clone();
        let keys = self.schema.keys_for_insert(&cols);
        // insert row into page with undo log linked.
        let (row_id, page_guard) = self
            .insert_row_internal(buf_pool, stmt, cols, RowUndoKind::Insert, None)
            .await;
        // insert index
        for key in keys {
            if self.schema.indexes[key.index_no].unique {
                match self
                    .insert_unique_index(buf_pool, stmt, key, row_id, &page_guard)
                    .await
                {
                    InsertIndex::Ok => (),
                    InsertIndex::DuplicateKey => return InsertMvcc::DuplicateKey,
                    InsertIndex::WriteConflict => return InsertMvcc::WriteConflict,
                }
            } else {
                todo!()
            }
        }
        InsertMvcc::Ok(row_id)
    }

    /// Update row with MVCC.
    /// This method is for update based on unique index lookup.
    /// It also takes care of index change.
    #[inline]
    pub async fn update_row(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.schema.indexes[key.index_no].unique);
        debug_assert!(self.schema.index_layout_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        loop {
            match index.lookup(&key.vals) {
                None => return UpdateMvcc::NotFound,
                Some(row_id) => {
                    match self.blk_idx.find_row_id(buf_pool, row_id).await {
                        RowLocation::NotFound => return UpdateMvcc::NotFound,
                        RowLocation::ColSegment(..) => todo!(),
                        RowLocation::RowPage(page_id) => {
                            let page = buf_pool.get_page(page_id, LatchFallbackMode::Shared).await;
                            let page_guard = page.block_until_shared();
                            if !validate_page_id(&page_guard, page_id) {
                                continue;
                            }
                            let res = self
                                .update_row_inplace(stmt, page_guard, key, row_id, update)
                                .await;
                            return match res {
                                UpdateRowInplace::Ok(new_row_id, index_change_cols, page_guard) => {
                                    debug_assert!(row_id == new_row_id);
                                    if !index_change_cols.is_empty() {
                                        // index may change, we should check whether each index key change and update correspondingly.
                                        return match self
                                            .update_indexes_only_key_change(
                                                buf_pool,
                                                stmt,
                                                row_id,
                                                &page_guard,
                                                &index_change_cols,
                                            )
                                            .await
                                        {
                                            UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                                            UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                                            UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                                        };
                                    } // otherwise, do nothing
                                    UpdateMvcc::Ok(row_id)
                                }
                                UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => {
                                    UpdateMvcc::NotFound
                                }
                                UpdateRowInplace::WriteConflict => UpdateMvcc::WriteConflict,
                                UpdateRowInplace::NoFreeSpace(
                                    old_row_id,
                                    old_row,
                                    update,
                                    old_guard,
                                ) => {
                                    // in-place update failed, we transfer update into
                                    // move+update.
                                    let (new_row_id, index_change_cols, new_guard) = self
                                        .move_update_for_space(
                                            buf_pool, stmt, old_row, update, old_row_id, old_guard,
                                        )
                                        .await;
                                    if !index_change_cols.is_empty() {
                                        match self
                                            .update_indexes_may_both_change(
                                                buf_pool,
                                                stmt,
                                                old_row_id,
                                                new_row_id,
                                                &index_change_cols,
                                                &new_guard,
                                            )
                                            .await
                                        {
                                            UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                                            UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                                            UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                                        }
                                    } else {
                                        match self.update_indexes_only_row_id_change(
                                            stmt, old_row_id, new_row_id, &new_guard,
                                        ) {
                                            UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                                            UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                                            UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                                        }
                                    }
                                }
                            };
                        }
                    }
                }
            }
        }
    }

    /// Delete row with MVCC.
    /// This method is for delete based on unique index lookup.
    #[inline]
    pub async fn delete_row(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        key: &SelectKey,
    ) -> DeleteMvcc {
        debug_assert!(key.index_no < self.sec_idx.len());
        debug_assert!(self.schema.indexes[key.index_no].unique);
        debug_assert!(self.schema.index_layout_match(key.index_no, &key.vals));
        let index = self.sec_idx[key.index_no].unique().unwrap();
        loop {
            match index.lookup(&key.vals) {
                None => return DeleteMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id).await {
                    RowLocation::NotFound => return DeleteMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page = buf_pool.get_page(page_id, LatchFallbackMode::Shared).await;
                        let page_guard = page.block_until_shared();
                        if !validate_page_id(&page_guard, page_id) {
                            continue;
                        }
                        return match self
                            .delete_row_internal(stmt, page_guard, row_id, key)
                            .await
                        {
                            DeleteInternal::NotFound => DeleteMvcc::NotFound,
                            DeleteInternal::WriteConflict => DeleteMvcc::WriteConflict,
                            DeleteInternal::Ok(page_guard) => {
                                // defer index deletion with index undo log.
                                self.defer_delete_indexes(stmt, row_id, page_guard);
                                DeleteMvcc::Ok
                            }
                        };
                    }
                },
            }
        }
    }

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
    #[inline]
    pub async fn delete_index(
        &self,
        buf_pool: P,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> bool {
        // todo: consider index drop.
        let index_schema = &self.schema.indexes[key.index_no];
        if index_schema.unique {
            let index = self.sec_idx[key.index_no].unique().unwrap();
            return self
                .delete_unique_index(buf_pool, index, key, row_id, min_active_sts)
                .await;
        }
        todo!()
    }

    #[inline]
    async fn delete_unique_index(
        &self,
        buf_pool: P,
        index: &dyn UniqueIndex,
        key: &SelectKey,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> bool {
        loop {
            match index.lookup(&key.vals) {
                None => return false, // Another thread deleted this entry.
                Some(index_row_id) => {
                    if index_row_id != row_id {
                        // Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return false;
                    }
                    match self.blk_idx.find_row_id(buf_pool, row_id).await {
                        RowLocation::NotFound => {
                            return index.compare_delete(&key.vals, row_id);
                        }
                        RowLocation::ColSegment(..) => todo!(),
                        RowLocation::RowPage(page_id) => {
                            let page_guard = buf_pool
                                .get_page(page_id, LatchFallbackMode::Shared)
                                .await
                                .block_until_shared();
                            if !validate_page_row_range(&page_guard, page_id, row_id) {
                                continue;
                            }
                            let access = page_guard.read_row_by_id(row_id);
                            // check if row is invisible
                            match access.latest_status() {
                                RowLatestStatus::NotFound => {
                                    return index.compare_delete(&key.vals, row_id);
                                }
                                RowLatestStatus::Uncommitted => {
                                    // traverse version chain to see if any visible version matches
                                    // the input key.
                                    todo!()
                                }
                                RowLatestStatus::Committed(cts, deleted) => {
                                    if cts < min_active_sts && deleted {
                                        if deleted {
                                            return index.compare_delete(&key.vals, row_id);
                                        }
                                    }
                                    // traverse version chain to see if any visible version matches
                                    // the input key.
                                    todo!()
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Move update is similar to a delete+insert.
    /// It's caused by no more space on current row page.
    #[inline]
    async fn move_update_for_space(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        old_row: Vec<(Val, Option<u16>)>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<'static, RowPage>,
    ) -> (RowID, HashSet<usize>, PageSharedGuard<'static, RowPage>) {
        // calculate new row and undo entry.
        let (new_row, undo_kind, index_change_cols) = {
            let mut index_change_cols = HashSet::new();
            let mut row = Vec::with_capacity(old_row.len());
            let mut var_offsets = Vec::with_capacity(old_row.len());
            for (v, var_offset) in old_row {
                row.push(v);
                var_offsets.push(var_offset);
            }
            let mut undo_cols = vec![];
            for mut uc in update {
                let old_val = &mut row[uc.idx];
                if old_val != &uc.val {
                    if self.schema.user_index_cols.contains(&uc.idx) {
                        index_change_cols.insert(uc.idx);
                    }
                    // swap old value and new value, then put into undo columns
                    mem::swap(&mut uc.val, old_val);
                    undo_cols.push(UndoCol {
                        idx: uc.idx,
                        val: uc.val,
                        var_offset: var_offsets[uc.idx],
                    });
                }
            }
            (row, RowUndoKind::Update(undo_cols), index_change_cols)
        };
        let (new_row_id, new_guard) = self
            .insert_row_internal(
                buf_pool,
                stmt,
                new_row,
                undo_kind,
                Some((old_id, old_guard)),
            )
            .await;
        // do not unlock the page because we may need to update index
        (new_row_id, index_change_cols, new_guard)
    }

    /// Move insert is similar to a delete+insert.
    /// But it triggered by duplicate key finding when updating index.
    /// The insert is already done and we additionally add a move entry to the
    /// already deleted version.
    /// Note: key of old row and new row must be identical, so we can directly
    /// use new key to build reborn branch in undo chain.
    #[inline]
    async fn move_link_for_index(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        row_id: RowID,
        key: &SelectKey,
        new_id: RowID,
        new_guard: &PageSharedGuard<'static, RowPage>,
    ) -> MoveLinkForIndex {
        loop {
            match self.blk_idx.find_row_id(buf_pool, row_id).await {
                RowLocation::NotFound => return MoveLinkForIndex::None,
                RowLocation::ColSegment(..) => todo!(),
                RowLocation::RowPage(page_id) => {
                    let page_guard = buf_pool
                        .get_page(page_id, LatchFallbackMode::Shared)
                        .await
                        .block_until_shared();
                    if !validate_page_row_range(&page_guard, page_id, row_id) {
                        continue;
                    }
                    let row_idx = page_guard.page().row_idx(row_id);
                    let mut lock_row = self
                        .lock_row_for_write(&stmt.trx, &page_guard, row_idx, &key, false)
                        .await;
                    match &mut lock_row {
                        LockRowForWrite::InvalidIndex => {
                            unreachable!("move link operation does not validate index");
                        }
                        LockRowForWrite::WriteConflict => return MoveLinkForIndex::WriteConflict,
                        LockRowForWrite::Ok(access, old_cts) => {
                            let mut access = access.take().unwrap();
                            // Move for index requires old row has been deleted.
                            if !access.row().is_deleted() {
                                return MoveLinkForIndex::DuplicateKey;
                            }
                            debug_assert!(!access.row().is_key_different(&self.schema, key));
                            let old_cts = mem::take(old_cts);
                            let mut move_entry = OwnedRowUndo::new(
                                self.table_id,
                                page_id,
                                row_id,
                                RowUndoKind::Move(true),
                            );
                            access.build_undo_chain(&stmt.trx, &mut move_entry, old_cts);
                            drop(access); // unlock the row.
                            drop(lock_row);
                            drop(page_guard); // unlock the page.

                            // Here we re-lock new row and link new entry to move entry.
                            // In this way, we can make sure no other thread can access new entry pointer
                            // so the update of next pointer is safe.
                            //
                            // key validation is unneccessary because they must match.
                            let new_idx = new_guard.page().row_idx(new_id);
                            let lock_new = self
                                .lock_row_for_write(&stmt.trx, &new_guard, new_idx, &key, false)
                                .await;
                            let (new_access, _) =
                                lock_new.ok().expect("lock new row to link move entry");
                            debug_assert!(new_access.is_some());
                            let mut new_entry = stmt.row_undo.pop().expect("new entry to link");
                            link_move_entry(&mut new_entry, move_entry.leak(), Some(key.clone()));

                            drop(new_access); // unlock new row

                            stmt.row_undo.push(move_entry);
                            stmt.row_undo.push(new_entry);
                            // no redo required, because no change on row data.
                            return MoveLinkForIndex::Ok;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn insert_row_internal(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut move_entry: Option<(RowID, PageSharedGuard<'static, RowPage>)>,
    ) -> (RowID, PageSharedGuard<'static, RowPage>) {
        let row_len = row_len(&self.schema, &insert);
        let row_count = estimate_max_row_count(row_len, self.schema.col_count());
        loop {
            let page_guard = self.get_insert_page(buf_pool, stmt, row_count).await;
            let page_id = page_guard.page_id();
            match self.insert_row_to_page(stmt, page_guard, insert, undo_kind, move_entry) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    stmt.save_active_insert_page(self.table_id, page_id, row_id);
                    return (row_id, page_guard);
                }
                // this page cannot be inserted any more, just leave it and retry another page.
                InsertRowIntoPage::NoSpaceOrRowID(ins, uk, me) => {
                    insert = ins;
                    undo_kind = uk;
                    move_entry = me;
                }
            }
        }
    }

    /// Insert row into given page.
    /// There might be move+update call this method, in such case, undo_kind will be
    /// set to UndoKind::Update.
    #[inline]
    fn insert_row_to_page<'a>(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<'a, RowPage>,
        insert: Vec<Val>,
        undo_kind: RowUndoKind,
        move_entry: Option<(RowID, PageSharedGuard<'a, RowPage>)>,
    ) -> InsertRowIntoPage<'a> {
        debug_assert!({
            (matches!(undo_kind, RowUndoKind::Insert) && move_entry.is_none())
                || (matches!(undo_kind, RowUndoKind::Update(_)) && move_entry.is_some())
        });

        let page_id = page_guard.page_id();
        match page_guard.page().insert(&self.schema, &insert) {
            InsertRow::Ok(row_id) => {
                let row_idx = page_guard.page().row_idx(row_id);
                let mut access = page_guard.write_row(row_idx);
                // create undo log.
                let mut new_entry = OwnedRowUndo::new(self.table_id, page_id, row_id, undo_kind);
                // The MOVE undo entry is for MOVE+UPDATE.
                // Once update in-place fails, we convert the update operation to insert.
                // and link them together.
                if let Some((old_id, old_guard)) = move_entry {
                    let old_row_idx = old_guard.page().row_idx(old_id);
                    // Here we actually lock both new row and old row,
                    // not very sure if this will cause dead-lock.
                    //
                    let access = old_guard.write_row(old_row_idx);
                    debug_assert!({
                        access.undo_head().is_some()
                            && stmt
                                .trx
                                .is_same_trx(&access.undo_head().as_ref().unwrap().status)
                    });

                    // re-lock moved row and link new entry to it.
                    let move_entry = access.first_undo_entry().unwrap();
                    link_move_entry(&mut new_entry, move_entry, None);
                }

                debug_assert!(access.undo_head().is_none());
                access.build_undo_chain(&stmt.trx, &mut new_entry, NextTrxCTS::None);
                drop(access);
                // Here we do not unlock the page because we need to verify validity of unique index update
                // according to this insert.
                // There might be scenario that a deleted row shares the same key with this insert.
                // Then we have to mark it as MOVE and point insert undo's next version to it.
                // So hold the page guard in order to re-lock the insert undo fast.
                stmt.row_undo.push(new_entry);
                // create redo log.
                // even if the operation is move+update, we still treat it as insert redo log.
                // because redo is only useful when recovering and no version chain is required
                // during recovery.
                let redo_entry = RedoEntry {
                    page_id,
                    row_id,
                    kind: RedoKind::Insert(insert),
                };
                // store redo log into transaction redo buffer.
                stmt.redo.push(redo_entry);
                InsertRowIntoPage::Ok(row_id, page_guard)
            }
            InsertRow::NoFreeSpaceOrRowID => {
                InsertRowIntoPage::NoSpaceOrRowID(insert, undo_kind, move_entry)
            }
        }
    }

    #[inline]
    async fn update_row_inplace(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<'static, RowPage>,
        key: &SelectKey,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
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
        let row_idx = (row_id - page.header.start_row_id) as usize;
        let mut lock_row = self
            .lock_row_for_write(&stmt.trx, &page_guard, row_idx, key, true)
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => return UpdateRowInplace::RowNotFound,
            LockRowForWrite::WriteConflict => return UpdateRowInplace::WriteConflict,
            LockRowForWrite::Ok(access, old_cts) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return UpdateRowInplace::RowDeleted;
                }
                let old_cts = mem::take(old_cts);
                match access.update_row(&self.schema, &update) {
                    UpdateRow::NoFreeSpace(old_row) => {
                        // page does not have enough space for update, we need to switch
                        // to out-of-place update mode, which will add a MOVE undo entry
                        // to end original row and perform a INSERT into new page, and
                        // link the two versions.
                        let mut new_entry = OwnedRowUndo::new(
                            self.table_id,
                            page_id,
                            row_id,
                            RowUndoKind::Move(false),
                        );
                        access.build_undo_chain(&stmt.trx, &mut new_entry, old_cts);
                        drop(access); // unlock row
                        drop(lock_row);
                        // Here we do not unlock page because we need to perform MOVE+UPDATE
                        // and link undo entries of two rows.
                        // The re-lock of current undo is required.
                        stmt.row_undo.push(new_entry);
                        let redo_entry = RedoEntry {
                            page_id,
                            row_id,
                            // use DELETE for redo is ok, no version chain should be maintained if recovering from redo.
                            kind: RedoKind::Delete,
                        };
                        stmt.redo.push(redo_entry);
                        UpdateRowInplace::NoFreeSpace(row_id, old_row, update, page_guard)
                    }
                    UpdateRow::Ok(mut row) => {
                        let mut index_change_cols = HashSet::new();
                        // perform in-place update.
                        let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                        for uc in &mut update {
                            if let Some((old, var_offset)) =
                                row.user_different(&self.schema, uc.idx, &uc.val)
                            {
                                let old_val = Val::from(old);
                                let new_val = mem::take(&mut uc.val);
                                // we also check whether the value change is related to any index,
                                // so we can update index later.
                                if self.schema.user_index_cols.contains(&uc.idx) {
                                    index_change_cols.insert(uc.idx);
                                }
                                // actual update
                                row.update_user_col(uc.idx, &new_val);
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
                        let mut new_entry = OwnedRowUndo::new(
                            self.table_id,
                            page_id,
                            row_id,
                            RowUndoKind::Update(undo_cols),
                        );
                        access.build_undo_chain(&stmt.trx, &mut new_entry, old_cts);
                        drop(access); // unlock the row.
                        drop(lock_row);
                        // we may still need this page if we'd like to update index.
                        stmt.row_undo.push(new_entry);
                        if !redo_cols.is_empty() {
                            // there might be nothing to update, so we do not need to add redo log.
                            // but undo is required because we need to properly lock the row.
                            let redo_entry = RedoEntry {
                                page_id,
                                row_id,
                                kind: RedoKind::Update(redo_cols),
                            };
                            stmt.redo.push(redo_entry);
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
        page_guard: PageSharedGuard<'static, RowPage>,
        row_id: RowID,
        key: &SelectKey,
    ) -> DeleteInternal {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return DeleteInternal::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        let mut lock_row = self
            .lock_row_for_write(&stmt.trx, &page_guard, row_idx, key, true)
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => return DeleteInternal::NotFound,
            LockRowForWrite::WriteConflict => return DeleteInternal::WriteConflict,
            LockRowForWrite::Ok(access, old_cts) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return DeleteInternal::NotFound;
                }
                access.delete_row();
                let mut new_entry =
                    OwnedRowUndo::new(self.table_id, page_id, row_id, RowUndoKind::Delete);
                access.build_undo_chain(&stmt.trx, &mut new_entry, mem::take(old_cts));
                drop(access); // unlock row
                drop(lock_row);
                // hold page lock in order to update index later.
                stmt.row_undo.push(new_entry);
                // create redo log
                let redo_entry = RedoEntry {
                    page_id,
                    row_id,
                    kind: RedoKind::Delete,
                };
                stmt.redo.push(redo_entry);
                DeleteInternal::Ok(page_guard)
            }
        }
    }

    #[inline]
    async fn get_insert_page(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        row_count: usize,
    ) -> PageSharedGuard<'static, RowPage> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id) {
            let g = buf_pool.get_page(page_id, LatchFallbackMode::Shared).await;
            // because we save last insert page in session and meanwhile other thread may access this page
            // and do some modification, even worse, buffer pool may evict it and reload other data into
            // this page. so here, we do not require that no change should happen, but if something change,
            // we validate that page id and row id range is still valid.
            let g = g.block_until_shared();
            if validate_page_row_range(&g, page_id, row_id) {
                return g;
            }
        }
        self.blk_idx
            .get_insert_page(buf_pool, row_count, &self.schema)
            .await
    }

    // lock row will check write conflict on given row and lock it.
    #[inline]
    async fn lock_row_for_write<'a>(
        &self,
        trx: &ActiveTrx,
        page_guard: &'a PageSharedGuard<'static, RowPage>,
        row_idx: usize,
        key: &SelectKey,
        validate_key: bool,
    ) -> LockRowForWrite<'a> {
        loop {
            let mut access = page_guard.write_row(row_idx);
            let (row, undo_head) = access.row_and_undo_mut();
            match undo_head {
                None => {
                    let head = RowUndoHead {
                        status: trx.status(),
                        entry: None, // currently we don't have undo entry to insert.
                    };
                    *undo_head = Some(head); // lock the row.
                    return LockRowForWrite::Ok(Some(access), NextTrxCTS::None);
                }
                Some(head) => {
                    if trx.is_same_trx(head.status.as_ref()) {
                        // Locked by itself
                        return LockRowForWrite::Ok(Some(access), NextTrxCTS::Myself);
                    }
                    let ts = head.status.ts();
                    if trx_is_committed(ts) {
                        // This row is committed, no lock conflict.
                        // Check whether the row is valid through index lookup.
                        // There might be case an out-of-date index entry pointing to the
                        // latest version of the row which has different key other than index.
                        //
                        // For example, assume:
                        //
                        // 1. one row with row_id=100, k=200 is inserted.
                        //    Then index has entry k(200) -> row_id(100).
                        //
                        // 2. update row set k=300.
                        //    If in-place update is available, we will reuse row_id=100, and
                        //    just update its key to 300.
                        //    So in index, we have two entries: k(200) -> row_id(100),
                        //    k(300) -> row_id(100).
                        //    The first entry is supposed to be linked to the old version, and
                        //    second entry to new version.
                        //    But in our design, both of them point to latest version and
                        //    we need to traverse the version chain to find correct(visible)
                        //    version.
                        //
                        // 3. insert one row with row_id=101, k=200.
                        //    Now we need to identify k=200 is actually out-of-date index entry,
                        //    and just skip it.
                        //
                        // argument validate_key indicates whether we should perform the validation.
                        // When we chain deleted row and new row with same key, we may need to
                        // skip the validation.
                        //
                        // For example:
                        //
                        // 1. One row[row_id=100, k=1] inserted.
                        //
                        // 2. Update k to 2. so row becomes [row_id=100, k=2].
                        //
                        // 3. Delete it. [row_id=100, k=2, deleted].
                        //
                        // 4. Insert k=1 again. We will find index entry k=1 already
                        //    pointed to deleted row [row_id=100, k=2].
                        //    Now we should not validate the key.
                        //
                        // todo: A further optimization for this scenario is to traverse through
                        // undo chain and check whether the same key exists in any old versions.
                        // If not exists, we do not need to build the version chain.
                        if validate_key && row.is_key_different(&self.schema, key) {
                            return LockRowForWrite::InvalidIndex;
                        }
                        head.status = trx.status(); // lock the row.
                        return LockRowForWrite::Ok(Some(access), NextTrxCTS::Value(ts));
                    }
                    if !head.status.preparing() {
                        // uncommitted, write-write conflict.
                        return LockRowForWrite::WriteConflict;
                    }
                    if let Some(notify) = head.status.prepare_notify() {
                        // unlock row(but logical row lock is still held)
                        drop(access);

                        // Here we do not unlock the page, because the preparation time of commit is supposed
                        // to be short.
                        // And as active transaction is using this page, we don't want page evictor swap it onto
                        // disk.
                        // Other transactions can still access this page and modify other rows.

                        let _ = notify.wait_async().await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    } // there might be progress on preparation, so recheck.
                }
            }
        }
    }

    // perform non-locking read on row.
    #[inline]
    async fn lock_row_for_read<'a>(
        &self,
        trx: &ActiveTrx,
        page_guard: &'a PageSharedGuard<'a, RowPage>,
        row_idx: usize,
    ) -> RowReadAccess<'a> {
        loop {
            let access = page_guard.read_row(row_idx);
            match access.undo() {
                None => return access,
                Some(head) => {
                    if trx.is_same_trx(head.status.as_ref()) {
                        // Locked by itself
                        return access;
                    }
                    let ts = head.status.ts();
                    if trx_is_committed(ts) {
                        // Because MVCC will backtrace to visible version, we do not need to check if index lookup is out-of-date here.
                        return access;
                    }
                    if !head.status.preparing() {
                        // uncommitted, write-write conflict.
                        return access;
                    }
                    if let Some(notify) = head.status.prepare_notify() {
                        // unlock row
                        drop(access);
                        // Even if it's non-locking read, we still need to wait for the preparation to avoid partial read.
                        // For example:
                        // Suppose transaction T1 is committing with CTS 100,
                        // Transaction T2 starts with STS 101 and reads rows that are modified by T1.
                        // If we do not block on waiting for T1, we may read one row of old version, and another
                        // row with new version. This breaks ACID properties.

                        let _ = notify.wait_async().await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    } // there might be progress on preparation, so recheck.
                }
            }
        }
    }

    #[inline]
    fn index_undo(&self, row_id: RowID, kind: IndexUndoKind) -> IndexUndo {
        IndexUndo {
            table_id: self.table_id,
            row_id,
            kind,
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<'static, RowPage>,
    ) -> InsertIndex {
        let index = self.sec_idx[key.index_no].unique().unwrap();
        match index.insert_if_not_exists(&key.vals, row_id) {
            None => {
                // insert index success.
                let index_undo = self.index_undo(row_id, IndexUndoKind::InsertUnique(key));
                stmt.index_undo.push(index_undo);
                InsertIndex::Ok
            }
            Some(old_row_id) => {
                // we found there is already one existing row with same key.
                // so perform move+link.
                debug_assert!(old_row_id != row_id);
                return match self
                    .move_link_for_index(buf_pool, stmt, old_row_id, &key, row_id, page_guard)
                    .await
                {
                    MoveLinkForIndex::DuplicateKey => InsertIndex::DuplicateKey,
                    MoveLinkForIndex::WriteConflict => InsertIndex::WriteConflict,
                    MoveLinkForIndex::None => {
                        // move+insert does not find old row.
                        // so we can update index to point to self
                        if !index.compare_exchange(&key.vals, old_row_id, row_id) {
                            // there is another transaction update the unique index concurrently,
                            // we can directly fail.
                            return InsertIndex::WriteConflict;
                        }
                        let index_undo =
                            self.index_undo(row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
                        stmt.index_undo.push(index_undo);
                        InsertIndex::Ok
                    }
                    MoveLinkForIndex::Ok => {
                        // Once move+insert is done,
                        // we already locked both old and new row, and make undo chain linked.
                        // So any other transaction that want to modify the index with same key
                        // should fail because lock can not be acquired by them.
                        let res = index.compare_exchange(&key.vals, old_row_id, row_id);
                        assert!(res);
                        let index_undo =
                            self.index_undo(row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
                        stmt.index_undo.push(index_undo);
                        InsertIndex::Ok
                    }
                };
            }
        }
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        row_id: RowID,
        page_guard: &PageSharedGuard<'static, RowPage>,
        index_change_cols: &HashSet<usize>,
    ) -> UpdateIndex {
        let mut access = None;
        for (index, index_schema) in self.sec_idx.iter().zip(&self.schema.indexes) {
            debug_assert!(index.is_unique() == index_schema.unique);
            if index_schema.unique {
                if index_key_is_changed(index_schema, index_change_cols) {
                    let new_key = read_latest_index_key(
                        &self.schema,
                        index.index_no,
                        page_guard,
                        row_id,
                        &mut access,
                    );
                    match self
                        .update_unique_index_only_key_change(
                            buf_pool,
                            stmt,
                            index.unique().unwrap(),
                            new_key,
                            row_id,
                            page_guard,
                        )
                        .await
                    {
                        UpdateIndex::Ok => (),
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                    };
                } // otherwise, in-place update do not change row id, so we do nothing
            } else {
                todo!();
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    fn update_indexes_only_row_id_change<'a>(
        &self,
        stmt: &mut Statement,
        old_row_id: RowID,
        new_row_id: RowID,
        page_guard: &PageSharedGuard<'a, RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        let mut access = None;
        for (index, index_schema) in self.sec_idx.iter().zip(&self.schema.indexes) {
            debug_assert!(index.is_unique() == index_schema.unique);
            if index_schema.unique {
                let key = read_latest_index_key(
                    &self.schema,
                    index.index_no,
                    page_guard,
                    new_row_id,
                    &mut access,
                );
                match self.update_unique_index_only_row_id_change(
                    stmt,
                    index.unique().unwrap(),
                    key,
                    old_row_id,
                    new_row_id,
                ) {
                    UpdateIndex::Ok => (),
                    UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                    UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                }
            } else {
                todo!();
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        old_row_id: RowID,
        new_row_id: RowID,
        index_change_cols: &HashSet<usize>,
        page_guard: &PageSharedGuard<'static, RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        let mut access = None;
        for (index, index_schema) in self.sec_idx.iter().zip(&self.schema.indexes) {
            debug_assert!(index.is_unique() == index_schema.unique);
            if index_schema.unique {
                let key = read_latest_index_key(
                    &self.schema,
                    index.index_no,
                    &page_guard,
                    new_row_id,
                    &mut access,
                );
                if index_key_is_changed(index_schema, index_change_cols) {
                    // key change and row id change.
                    match self
                        .update_unique_index_key_and_row_id_change(
                            buf_pool,
                            stmt,
                            index.unique().unwrap(),
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
                    // only row id change.
                    match self.update_unique_index_only_row_id_change(
                        stmt,
                        index.unique().unwrap(),
                        key,
                        old_row_id,
                        new_row_id,
                    ) {
                        UpdateIndex::DuplicateKey => return UpdateIndex::DuplicateKey,
                        UpdateIndex::WriteConflict => return UpdateIndex::WriteConflict,
                        UpdateIndex::Ok => (),
                    }
                }
            } else {
                todo!();
            }
        }
        UpdateIndex::Ok
    }

    #[inline]
    fn defer_delete_indexes<'a>(
        &self,
        stmt: &mut Statement,
        row_id: RowID,
        page_guard: PageSharedGuard<'a, RowPage>,
    ) {
        let mut access = None;
        for (index, index_schema) in self.sec_idx.iter().zip(&self.schema.indexes) {
            debug_assert!(index.is_unique() == index_schema.unique);
            if index_schema.unique {
                let key = read_latest_index_key(
                    &self.schema,
                    index.index_no,
                    &page_guard,
                    row_id,
                    &mut access,
                );
                let index_undo = self.index_undo(row_id, IndexUndoKind::DeferDelete(key));
                stmt.index_undo.push(index_undo);
            } else {
                todo!();
            }
        }
    }

    async fn update_unique_index_key_and_row_id_change(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        index: &dyn UniqueIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<'static, RowPage>,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        match index.insert_if_not_exists(&key.vals, new_row_id) {
            None => {
                let index_undo = self.index_undo(new_row_id, IndexUndoKind::InsertUnique(key));
                stmt.index_undo.push(index_undo);
                UpdateIndex::Ok
            }
            Some(index_row_id) => {
                // new row id is the insert id so index value must not be the same.
                debug_assert!(index_row_id != new_row_id);
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
                    let res = index.compare_exchange(&key.vals, old_row_id, new_row_id);
                    assert!(res);
                    let index_undo =
                        self.index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
                    stmt.index_undo.push(index_undo);
                    return UpdateIndex::Ok;
                }
                return match self
                    .move_link_for_index(buf_pool, stmt, index_row_id, &key, new_row_id, new_guard)
                    .await
                {
                    MoveLinkForIndex::DuplicateKey => UpdateIndex::DuplicateKey,
                    MoveLinkForIndex::WriteConflict => UpdateIndex::WriteConflict,
                    MoveLinkForIndex::None => {
                        // move+insert does not find old row.
                        // so we can update index to point to self
                        if !index.compare_exchange(&key.vals, index_row_id, new_row_id) {
                            // there is another transaction update the unique index concurrently,
                            // we can directly fail.
                            return UpdateIndex::WriteConflict;
                        }
                        let index_undo = self
                            .index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                    MoveLinkForIndex::Ok => {
                        // Once move+insert is done,
                        // we already locked both old and new row, and make undo chain linked.
                        // So any other transaction that want to modify the index with same key
                        // should fail because lock can not be acquired by them.
                        let res = index.compare_exchange(&key.vals, index_row_id, new_row_id);
                        assert!(res);
                        let index_undo = self
                            .index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                };
            }
        }
    }

    #[inline]
    fn update_unique_index_only_row_id_change<'a>(
        &self,
        stmt: &mut Statement,
        index: &dyn UniqueIndex,
        key: SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> UpdateIndex {
        debug_assert!(old_row_id != new_row_id);
        let res = index.compare_exchange(&key.vals, old_row_id, new_row_id);
        assert!(res);
        let index_undo = self.index_undo(new_row_id, IndexUndoKind::UpdateUnique(key, old_row_id));
        stmt.index_undo.push(index_undo);
        UpdateIndex::Ok
    }

    /// Update unique index due to key change.
    /// In this scenario, we only need to insert pair of new key and row id
    /// into index. Keep old index entry as is.
    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        buf_pool: P,
        stmt: &mut Statement,
        index: &dyn UniqueIndex,
        new_key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<'static, RowPage>,
    ) -> UpdateIndex {
        match index.insert_if_not_exists(&new_key.vals, row_id) {
            None => {
                let index_undo = self.index_undo(row_id, IndexUndoKind::InsertUnique(new_key));
                stmt.index_undo.push(index_undo);
                UpdateIndex::Ok
            }
            Some(index_row_id) => {
                // There is already a row with same new key.
                // We have to check its status.
                if index_row_id == row_id {
                    // This is possible.
                    // For example, transaction update row(RowID=100) key=1 to key=2.
                    //
                    // Then index has following entries:
                    // key=1 -> RowID=100 (old version)
                    // key=2 -> RowID=100 (latest version)
                    //
                    // Then we update key=2 to key=1 again.
                    // Now we should have:
                    // key=1 -> RowID=100 (latest version)
                    // key=2 -> RowID=100 (old version)
                    //
                    // nothing to do in this case.
                    return UpdateIndex::Ok;
                }
                match self
                    .move_link_for_index(buf_pool, stmt, index_row_id, &new_key, row_id, page_guard)
                    .await
                {
                    MoveLinkForIndex::DuplicateKey => UpdateIndex::DuplicateKey,
                    MoveLinkForIndex::WriteConflict => UpdateIndex::WriteConflict,
                    MoveLinkForIndex::None => {
                        // no old row found.
                        if !index.compare_exchange(&new_key.vals, index_row_id, row_id) {
                            // there is another transaction update the unique index concurrently,
                            // we can directly fail.
                            return UpdateIndex::WriteConflict;
                        }
                        let index_undo = self
                            .index_undo(row_id, IndexUndoKind::UpdateUnique(new_key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                    MoveLinkForIndex::Ok => {
                        // Both old row(index points to) and new row are locked.
                        // we must succeed on updateing index.
                        let res = index.compare_exchange(&new_key.vals, index_row_id, row_id);
                        assert!(res);
                        let index_undo = self
                            .index_undo(row_id, IndexUndoKind::UpdateUnique(new_key, index_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                }
            }
        }
    }
}

#[inline]
fn validate_page_id(page_guard: &PageSharedGuard<'_, RowPage>, page_id: PageID) -> bool {
    if page_guard.page_id() != page_id {
        return false;
    }
    true
}

#[inline]
fn validate_page_row_range(
    page_guard: &PageSharedGuard<'_, RowPage>,
    page_id: PageID,
    row_id: RowID,
) -> bool {
    if page_guard.page_id() != page_id {
        return false;
    }
    page_guard.page().row_id_in_valid_range(row_id)
}

#[inline]
fn row_len(schema: &TableSchema, user_cols: &[Val]) -> usize {
    let var_len = schema
        .var_cols
        .iter()
        .map(|idx| {
            let val = &user_cols[*idx - 1];
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
    schema.fix_len + var_len
}

#[inline]
fn link_move_entry(new_entry: &mut OwnedRowUndo, move_entry: RowUndoRef, key: Option<SelectKey>) {
    // Here we setup reborn branch of a new inserted row to a deleted row,
    // since they share same key and we keep index single entry pointing to new row.
    new_entry.next.push(NextRowUndo {
        status: NextRowUndoStatus::SameAsPrev,
        entry: move_entry,
        branch: RowUndoBranch::from(key),
    });
}

enum LockRowForWrite<'a> {
    // lock success, returns optional last commit timestamp.
    Ok(Option<RowWriteAccess<'a>>, NextTrxCTS),
    // lock fail, there is another transaction modifying this row.
    WriteConflict,
    // row is invalid through index lookup.
    // this can happen when index entry is not garbage collected,
    // so some old key points to new version.
    InvalidIndex,
}

impl<'a> LockRowForWrite<'a> {
    #[inline]
    pub fn ok(self) -> Option<(Option<RowWriteAccess<'a>>, NextTrxCTS)> {
        match self {
            LockRowForWrite::Ok(access, next_cts) => Some((access, next_cts)),
            _ => None,
        }
    }
}

enum InsertRowIntoPage<'a> {
    Ok(RowID, PageSharedGuard<'a, RowPage>),
    NoSpaceOrRowID(
        Vec<Val>,
        RowUndoKind,
        Option<(RowID, PageSharedGuard<'a, RowPage>)>,
    ),
}

enum UpdateRowInplace {
    // We keep row page lock if there is any index change,
    // so we can read latest values from page.
    Ok(RowID, HashSet<usize>, PageSharedGuard<'static, RowPage>),
    RowNotFound,
    RowDeleted,
    WriteConflict,
    NoFreeSpace(
        RowID,
        Vec<(Val, Option<u16>)>,
        Vec<UpdateCol>,
        PageSharedGuard<'static, RowPage>,
    ),
}

enum DeleteInternal {
    Ok(PageSharedGuard<'static, RowPage>),
    NotFound,
    WriteConflict,
}

#[inline]
fn index_key_is_changed(index_schema: &IndexSchema, index_change_cols: &HashSet<usize>) -> bool {
    index_schema
        .keys
        .iter()
        .any(|key| index_change_cols.contains(&(key.user_col_idx as usize)))
}

#[inline]
fn read_latest_index_key<'a>(
    schema: &TableSchema,
    index_no: usize,
    page_guard: &'a PageSharedGuard<'a, RowPage>,
    row_id: RowID,
    access: &mut Option<RowReadAccess<'a>>,
) -> SelectKey {
    let index_schema = &schema.indexes[index_no];
    let mut new_key = SelectKey::null(index_no, index_schema.keys.len());
    for (pos, key) in index_schema.keys.iter().enumerate() {
        let row = access
            .get_or_insert_with(|| page_guard.read_row_by_id(row_id))
            .row();
        let val = row.clone_user_val(schema, key.user_col_idx as usize);
        new_key.vals[pos] = val;
    }
    new_key
}
