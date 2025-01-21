pub mod schema;
#[cfg(test)]
mod tests;

use crate::buffer::guard::PageSharedGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::index::{BlockIndex, RowLocation, SingleKeyIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{
    DeleteMvcc, InsertIndex, InsertMvcc, InsertRow, MoveInsert, ReadRow, SelectMvcc, UndoCol,
    UpdateCol, UpdateIndex, UpdateMvcc, UpdateRow,
};
use crate::row::{estimate_max_row_count, RowID, RowPage, RowRead};
use crate::stmt::Statement;
use crate::trx::redo::{RedoEntry, RedoKind};
use crate::trx::row::{RowLatestStatus, RowReadAccess, RowWriteAccess};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, NextRowUndo, NextRowUndoStatus, NextTrxCTS, OwnedRowUndo,
    RowUndoHead, RowUndoKind, RowUndoRef,
};
use crate::trx::{trx_is_committed, ActiveTrx};
use crate::value::{Val, PAGE_VAR_LEN_INLINE};
use std::mem;
use std::sync::Arc;

pub use schema::Schema;

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
pub struct Table<P> {
    pub table_id: TableID,
    pub schema: Arc<Schema>,
    pub blk_idx: Arc<BlockIndex<P>>,
    // todo: secondary indexes.
    pub sec_idx: Arc<dyn SingleKeyIndex>,
}

impl<P: BufferPool> Table<P> {
    /// Select row with MVCC.
    #[inline]
    pub async fn select_row(
        &self,
        buf_pool: &P,
        stmt: &Statement,
        key: Val,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        debug_assert!(self.schema.idx_type_match(&key));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        loop {
            match self.sec_idx.lookup(&key) {
                None => return SelectMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id) {
                    RowLocation::NotFound => return SelectMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page = buf_pool.get_page(page_id, LatchFallbackMode::Shared);
                        let page_guard = page.block_until_shared();
                        if !validate_page_id(&page_guard, page_id) {
                            continue;
                        }
                        return self
                            .select_row_in_page(stmt, page_guard, &key, row_id, user_read_set)
                            .await;
                    }
                },
            }
        }
    }

    #[inline]
    async fn select_row_in_page(
        &self,
        stmt: &Statement,
        page_guard: PageSharedGuard<'_, RowPage>,
        key: &Val,
        row_id: RowID,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return SelectMvcc::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        let access = self
            .lock_row_for_read(&stmt.trx, &page_guard, row_idx)
            .await;
        match access.read_row_mvcc(&stmt.trx, &self.schema, user_read_set, &key) {
            ReadRow::Ok(vals) => SelectMvcc::Ok(vals),
            ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
        }
    }

    /// Insert row with MVCC.
    /// This method will also take care of index update.
    #[inline]
    pub async fn insert_row(
        &self,
        buf_pool: &P,
        stmt: &mut Statement,
        cols: Vec<Val>,
    ) -> InsertMvcc {
        debug_assert!(cols.len() + 1 == self.schema.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.schema.user_col_type_match(idx, val))
        });
        let key = cols[self.schema.user_key_idx()].clone();
        // insert row into page with undo log linked.
        let (row_id, page_guard) =
            self.insert_row_internal(buf_pool, stmt, cols, RowUndoKind::Insert, None);
        // insert index
        match self
            .insert_index(buf_pool, stmt, key, row_id, page_guard)
            .await
        {
            InsertIndex::Ok => InsertMvcc::Ok(row_id),
            InsertIndex::DuplicateKey => InsertMvcc::DuplicateKey,
            InsertIndex::WriteConflict => InsertMvcc::WriteConflict,
        }
    }

    /// Update row with MVCC.
    /// This method is for update based on index lookup.
    /// It also takes care of index update.
    #[inline]
    pub async fn update_row(
        &self,
        buf_pool: &P,
        stmt: &mut Statement,
        key: Val,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        let key_change = self.key_change(&key, &update);
        loop {
            match self.sec_idx.lookup(&key) {
                None => return UpdateMvcc::NotFound,
                Some(row_id) => {
                    match self.blk_idx.find_row_id(buf_pool, row_id) {
                        RowLocation::NotFound => return UpdateMvcc::NotFound,
                        RowLocation::ColSegment(..) => todo!(),
                        RowLocation::RowPage(page_id) => {
                            let page = buf_pool.get_page(page_id, LatchFallbackMode::Shared);
                            let page_guard = page.block_until_shared();
                            if !validate_page_id(&page_guard, page_id) {
                                continue;
                            }
                            let res = self
                                .update_row_inplace(stmt, page_guard, &key, row_id, update)
                                .await;
                            match res {
                                UpdateRowInplace::Ok(new_row_id) => {
                                    debug_assert!(row_id == new_row_id);
                                    return match self.update_index(
                                        stmt, buf_pool, key, key_change, row_id, new_row_id,
                                    ) {
                                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                                    };
                                }
                                UpdateRowInplace::RowDeleted | UpdateRowInplace::RowNotFound => {
                                    return UpdateMvcc::NotFound
                                }
                                UpdateRowInplace::WriteConflict => {
                                    return UpdateMvcc::WriteConflict
                                }
                                UpdateRowInplace::NoFreeSpace(
                                    old_row_id,
                                    old_row,
                                    update,
                                    old_guard,
                                ) => {
                                    // in-place update failed, we transfer update into
                                    // move+update.
                                    let new_row_id = self.move_update(
                                        buf_pool, stmt, old_row, update, old_row_id, old_guard,
                                    );
                                    return match self.update_index(
                                        stmt, buf_pool, key, key_change, old_row_id, new_row_id,
                                    ) {
                                        UpdateIndex::Ok => UpdateMvcc::Ok(new_row_id),
                                        UpdateIndex::DuplicateKey => UpdateMvcc::DuplicateKey,
                                        UpdateIndex::WriteConflict => UpdateMvcc::WriteConflict,
                                    };
                                }
                            };
                        }
                    }
                }
            }
        }
    }

    /// Delete row with MVCC.
    /// This method is for delete based on index lookup.
    #[inline]
    pub async fn delete_row(&self, buf_pool: &P, stmt: &mut Statement, key: Val) -> DeleteMvcc {
        loop {
            match self.sec_idx.lookup(&key) {
                None => return DeleteMvcc::NotFound,
                Some(row_id) => match self.blk_idx.find_row_id(buf_pool, row_id) {
                    RowLocation::NotFound => return DeleteMvcc::NotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page = buf_pool.get_page(page_id, LatchFallbackMode::Shared);
                        let page_guard = page.block_until_shared();
                        if !validate_page_id(&page_guard, page_id) {
                            continue;
                        }
                        return self
                            .delete_row_internal(stmt, page_guard, row_id, &key)
                            .await;
                    }
                },
            }
        }
    }

    // Move update is similar to a delete+insert.
    #[inline]
    fn move_update<'a>(
        &self,
        buf_pool: &'a P,
        stmt: &mut Statement,
        old_row: Vec<(Val, Option<u16>)>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<'a, RowPage>,
    ) -> RowID {
        // calculate new row and undo entry.
        let (new_row, undo_kind) = {
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
                    // swap old value and new value, then put into undo columns
                    mem::swap(&mut uc.val, old_val);
                    undo_cols.push(UndoCol {
                        idx: uc.idx,
                        val: uc.val,
                        var_offset: var_offsets[uc.idx],
                    });
                }
            }
            (row, RowUndoKind::Update(undo_cols))
        };
        let (row_id, page_guard) = self.insert_row_internal(
            buf_pool,
            stmt,
            new_row,
            undo_kind,
            Some((old_id, old_guard)),
        );
        drop(page_guard); // unlock the page
        row_id
    }

    /// Move insert is similar to a delete+insert.
    /// But it triggered by duplicate key finding when updating index.
    /// The insert is already done and we additionally add a move entry to the
    /// already deleted version.
    #[inline]
    async fn move_insert(
        &self,
        buf_pool: &P,
        stmt: &mut Statement,
        row_id: RowID,
        key: &Val,
        new_id: RowID,
        new_guard: PageSharedGuard<'_, RowPage>,
    ) -> MoveInsert {
        loop {
            match self.blk_idx.find_row_id(buf_pool, row_id) {
                RowLocation::NotFound => return MoveInsert::None,
                RowLocation::ColSegment(..) => todo!(),
                RowLocation::RowPage(page_id) => {
                    let page_guard = buf_pool
                        .get_page(page_id, LatchFallbackMode::Shared)
                        .block_until_shared();
                    if !validate_page_row_range(&page_guard, page_id, row_id) {
                        continue;
                    }
                    let row_idx = page_guard.page().row_idx(row_id);
                    let mut lock_row = self
                        .lock_row_for_write(&stmt.trx, &page_guard, row_idx, key)
                        .await;
                    match &mut lock_row {
                        LockRowForWrite::InvalidIndex => return MoveInsert::None, // key changed so we are fine.
                        LockRowForWrite::WriteConflict => return MoveInsert::WriteConflict,
                        LockRowForWrite::Ok(access, old_cts) => {
                            let mut access = access.take().unwrap();
                            if !access.row().is_deleted() {
                                return MoveInsert::DuplicateKey;
                            }
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
                            let new_idx = new_guard.page().row_idx(new_id);
                            let lock_new = self
                                .lock_row_for_write(&stmt.trx, &new_guard, new_idx, key)
                                .await;
                            let (new_access, _) = lock_new.ok().expect("lock new row for insert");
                            debug_assert!(new_access.is_some());
                            let mut new_entry = stmt.row_undo.pop().expect("new entry for insert");
                            link_move_entry(&mut new_entry, move_entry.leak());

                            drop(new_access); // unlock new row
                            drop(new_guard); // unlock new page

                            stmt.row_undo.push(move_entry);
                            stmt.row_undo.push(new_entry);
                            // no redo required, because no change on row data.
                            return MoveInsert::Ok;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn insert_row_internal<'a>(
        &self,
        buf_pool: &'a P,
        stmt: &mut Statement,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut move_entry: Option<(RowID, PageSharedGuard<'a, RowPage>)>,
    ) -> (RowID, PageSharedGuard<'a, RowPage>) {
        let row_len = row_len(&self.schema, &insert);
        let row_count = estimate_max_row_count(row_len, self.schema.col_count());
        loop {
            let page_guard = self.get_insert_page(buf_pool, stmt, row_count);
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
    /// There might be move+update call this method, in such case, op_kind will be
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
                    link_move_entry(&mut new_entry, move_entry);
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
    async fn update_row_inplace<'a>(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<'a, RowPage>,
        key: &Val,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace<'a> {
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
            .lock_row_for_write(&stmt.trx, &page_guard, row_idx, key)
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
                        // perform in-place update.
                        let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                        for uc in &mut update {
                            if let Some((old, var_offset)) =
                                row.user_different(&self.schema, uc.idx, &uc.val)
                            {
                                undo_cols.push(UndoCol {
                                    idx: uc.idx,
                                    val: Val::from(old),
                                    var_offset,
                                });
                                redo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    // new value no longer needed, so safe to take it here.
                                    val: mem::take(&mut uc.val),
                                });
                                row.update_user_col(uc.idx, &uc.val);
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
                        drop(page_guard); // unlock the page, because we finish page update.
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
                        UpdateRowInplace::Ok(row_id)
                    }
                }
            }
        }
    }

    #[inline]
    async fn delete_row_internal(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<'_, RowPage>,
        row_id: RowID,
        key: &Val,
    ) -> DeleteMvcc {
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return DeleteMvcc::NotFound;
        }
        let row_idx = page.row_idx(row_id);
        let mut lock_row = self
            .lock_row_for_write(&stmt.trx, &page_guard, row_idx, key)
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => return DeleteMvcc::NotFound,
            LockRowForWrite::WriteConflict => return DeleteMvcc::WriteConflict,
            LockRowForWrite::Ok(access, old_cts) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return DeleteMvcc::NotFound;
                }
                access.delete_row();
                let mut new_entry =
                    OwnedRowUndo::new(self.table_id, page_id, row_id, RowUndoKind::Delete);
                access.build_undo_chain(&stmt.trx, &mut new_entry, mem::take(old_cts));
                drop(access); // unlock row
                drop(lock_row);
                drop(page_guard); // unlock page
                stmt.row_undo.push(new_entry);
                // create redo log
                let redo_entry = RedoEntry {
                    page_id,
                    row_id,
                    kind: RedoKind::Delete,
                };
                stmt.redo.push(redo_entry);
                DeleteMvcc::Ok
            }
        }
    }

    #[inline]
    fn get_insert_page<'a>(
        &self,
        buf_pool: &'a P,
        stmt: &mut Statement,
        row_count: usize,
    ) -> PageSharedGuard<'a, RowPage> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id) {
            let g = buf_pool.get_page(page_id, LatchFallbackMode::Shared);
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
    }

    // lock row will check write conflict on given row and lock it.
    #[inline]
    async fn lock_row_for_write<'a>(
        &self,
        trx: &ActiveTrx,
        page_guard: &'a PageSharedGuard<'a, RowPage>,
        row_idx: usize,
        key: &Val,
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
                        // For example, assume:
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
                        if row.is_key_different(&self.schema, key) {
                            return LockRowForWrite::InvalidIndex;
                        }
                        head.status = trx.status(); // lock the row.
                        return LockRowForWrite::Ok(Some(access), NextTrxCTS::Value(ts));
                    }
                    if !head.status.preparing() {
                        // uncommitted, write-write conflict.
                        return LockRowForWrite::WriteConflict;
                    }
                    if let Some(commit_notifier) = head.status.prepare_notifier() {
                        // unlock row(but logical row lock is still held)
                        drop(access);

                        // Here we do not unlock the page, because the preparation time of commit is supposed
                        // to be short.
                        // And as active transaction is using this page, we don't want page evictor swap it onto
                        // disk.
                        // Other transactions can still access this page and modify other rows.

                        let _ = commit_notifier.recv_async().await; // wait for that transaction to be committed.

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
                    if let Some(commit_notifier) = head.status.prepare_notifier() {
                        // unlock row
                        drop(access);
                        // Even if it's non-locking read, we still need to wait for the preparation to avoid partial read.
                        // For example:
                        // Suppose transaction T1 is committing with CTS 100,
                        // Transaction T2 starts with STS 101 and reads rows that are modified by T1.
                        // If we do not block on waiting for T1, we may read one row of old version, and another
                        // row with new version. This breaks ACID properties.

                        let _ = commit_notifier.recv_async().await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    } // there might be progress on preparation, so recheck.
                }
            }
        }
    }

    #[inline]
    fn index_undo(&self, kind: IndexUndoKind) -> IndexUndo {
        IndexUndo {
            table_id: self.table_id,
            kind,
        }
    }

    #[inline]
    fn key_change(&self, key: &Val, update: &[UpdateCol]) -> bool {
        let user_key_idx = self.schema.user_key_idx();
        if let Ok(pos) = update.binary_search_by_key(&user_key_idx, |uc| uc.idx) {
            let new_key = &update[pos];
            return key != &new_key.val;
        }
        false
    }

    #[inline]
    fn row_latest_status(&self, buf_pool: &P, row_id: RowID) -> RowLatestStatus {
        loop {
            match self.blk_idx.find_row_id(buf_pool, row_id) {
                RowLocation::NotFound => return RowLatestStatus::NotFound,
                RowLocation::ColSegment(..) => todo!(),
                RowLocation::RowPage(page_id) => {
                    let page_guard = buf_pool
                        .get_page(page_id, LatchFallbackMode::Shared)
                        .block_until_shared();
                    if !validate_page_row_range(&page_guard, page_id, row_id) {
                        continue;
                    }
                    let row_idx = page_guard.page().row_idx(row_id);
                    let access = page_guard.read_row(row_idx);
                    return access.latest_status();
                }
            }
        }
    }

    #[inline]
    async fn insert_index<'a>(
        &self,
        buf_pool: &'a P,
        stmt: &mut Statement,
        key: Val,
        row_id: RowID,
        page_guard: PageSharedGuard<'a, RowPage>,
    ) -> InsertIndex {
        match self.sec_idx.insert_if_not_exists(key.clone(), row_id) {
            None => {
                let index_undo = self.index_undo(IndexUndoKind::Insert(key, row_id));
                stmt.index_undo.push(index_undo);
                InsertIndex::Ok
            }
            Some(old_row_id) => {
                // we found there is already one existing row with same key.
                // so perform move+insert.
                return match self
                    .move_insert(buf_pool, stmt, old_row_id, &key, row_id, page_guard)
                    .await
                {
                    MoveInsert::DuplicateKey => InsertIndex::DuplicateKey,
                    MoveInsert::WriteConflict => InsertIndex::WriteConflict,
                    MoveInsert::None => {
                        // move+insert does not find old row. It's safe.
                        let index_undo = self.index_undo(IndexUndoKind::Insert(key, row_id));
                        stmt.index_undo.push(index_undo);
                        InsertIndex::Ok
                    }
                    MoveInsert::Ok => {
                        // Once move+insert is done,
                        // we already locked both old and new row, and make undo chain linked.
                        // So any other transaction that want to modify the index with same key
                        // should fail because lock can not be acquired by them.
                        let res = self.sec_idx.compare_exchange(&key, old_row_id, row_id);
                        assert!(res);
                        let index_undo =
                            self.index_undo(IndexUndoKind::Update(key, old_row_id, row_id));
                        stmt.index_undo.push(index_undo);
                        InsertIndex::Ok
                    }
                };
            }
        }
    }

    #[inline]
    fn update_index(
        &self,
        stmt: &mut Statement,
        buf_pool: &P,
        key: Val,
        key_change: bool,
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> UpdateIndex {
        let row_id_change = old_row_id != new_row_id;
        match (key_change, row_id_change) {
            (false, false) => UpdateIndex::Ok, // nothing changed
            (true, false) => {
                // Key changed, and row id not change.
                // Then, we try to insert new key with row id into index.
                match self.sec_idx.insert_if_not_exists(key.clone(), new_row_id) {
                    None => {
                        let index_undo = self.index_undo(IndexUndoKind::Insert(key, new_row_id));
                        stmt.index_undo.push(index_undo);
                        UpdateIndex::Ok
                    }
                    Some(index_row_id) => {
                        // There is already a row with same new key.
                        // We have to check its status.
                        if index_row_id == new_row_id {
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
                        let row_status = self.row_latest_status(buf_pool, index_row_id);
                        match row_status {
                            RowLatestStatus::Committed => UpdateIndex::DuplicateKey,
                            RowLatestStatus::Uncommitted => UpdateIndex::WriteConflict,
                            RowLatestStatus::Deleted => {
                                // todo: we need to link new row and old row,
                                // to make result of MVCC index lookup correct.
                                //
                                // Current design of undo chain does not support this scenario.
                                // We need to embed index information into undo chain to support
                                // one undo entry point to multiple old entries using
                                // different index key.
                                //
                                // there might also be optimization that we can identify
                                // the deleted row is globally visible, so we do not
                                // need to keep version chain.
                                todo!()
                            }
                            RowLatestStatus::NotFound => UpdateIndex::Ok,
                        }
                    }
                }
            }
            (false, true) => {
                // Key not changed, but row id changed.
                let res = self.sec_idx.compare_exchange(&key, old_row_id, new_row_id);
                assert!(res);
                let index_undo =
                    self.index_undo(IndexUndoKind::Update(key, old_row_id, new_row_id));
                stmt.index_undo.push(index_undo);
                UpdateIndex::Ok
            }
            (true, true) => {
                // Key changed and row id changed.
                match self.sec_idx.insert_if_not_exists(key.clone(), new_row_id) {
                    None => {
                        let index_undo = self.index_undo(IndexUndoKind::Insert(key, new_row_id));
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
                            let res = self.sec_idx.compare_exchange(&key, old_row_id, new_row_id);
                            assert!(res);
                            let index_undo =
                                self.index_undo(IndexUndoKind::Update(key, old_row_id, new_row_id));
                            stmt.index_undo.push(index_undo);
                            return UpdateIndex::Ok;
                        }
                        let row_status = self.row_latest_status(buf_pool, index_row_id);
                        match row_status {
                            RowLatestStatus::Committed => UpdateIndex::DuplicateKey,
                            RowLatestStatus::Uncommitted => UpdateIndex::WriteConflict,
                            RowLatestStatus::Deleted => {
                                todo!()
                            }
                            RowLatestStatus::NotFound => UpdateIndex::Ok,
                        }
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
fn row_len(schema: &Schema, user_cols: &[Val]) -> usize {
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
fn link_move_entry(new_entry: &mut OwnedRowUndo, move_entry: RowUndoRef) {
    // ref-count this pointer.
    debug_assert!(new_entry.next.is_none());
    new_entry.next = Some(NextRowUndo {
        status: NextRowUndoStatus::SameAsPrev,
        entry: move_entry,
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

enum UpdateRowInplace<'a> {
    Ok(RowID),
    RowNotFound,
    RowDeleted,
    WriteConflict,
    NoFreeSpace(
        RowID,
        Vec<(Val, Option<u16>)>,
        Vec<UpdateCol>,
        PageSharedGuard<'a, RowPage>,
    ),
}
