use crate::buffer::guard::PageSharedGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::index::RowLocation;
use crate::latch::LatchFallbackMode;
use crate::row::ops::{
    DeleteMvcc, InsertMvcc, InsertRow, MoveInsert, SelectMvcc, UpdateCol, UpdateMvcc, UpdateRow,
};
use crate::row::{estimate_max_row_count, RowID, RowPage, RowRead};
use crate::stmt::Statement;
use crate::table::{Schema, Table};
use crate::trx::redo::{RedoEntry, RedoKind};
use crate::trx::row::{RowReadAccess, RowWriteAccess};
use crate::trx::undo::{
    NextTrxCTS, NextUndoEntry, NextUndoStatus, OwnedUndoEntry, UndoEntryPtr, UndoHead, UndoKind,
};
use crate::trx::{trx_is_committed, ActiveTrx};
use crate::value::{Val, PAGE_VAR_LEN_INLINE};
use std::mem;
use std::ops::Deref;

/// MvccTable wraps a common table to provide MVCC functionalities.
///
/// The basic idea is to separate undo logs from data page.
/// So we have a separate undo array associated to each row in row page.
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
pub struct MvccTable<'a>(pub(super) &'a Table<'a>);

impl<'a> Deref for MvccTable<'a> {
    type Target = Table<'a>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> MvccTable<'a> {
    /// Select row with MVCC.
    #[inline]
    pub async fn select_row(
        &self,
        stmt: &mut Statement,
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
                None => return SelectMvcc::RowNotFound,
                Some(row_id) => match self.blk_idx.find_row_id(self.buf_pool, row_id) {
                    RowLocation::NotFound => return SelectMvcc::RowNotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page = self.buf_pool.get_page(page_id, LatchFallbackMode::Shared);
                        let page_guard = page.block_until_shared();
                        if !validate_page_id(&page_guard, page_id) {
                            continue;
                        }
                        let key = key.clone();
                        return self
                            .select_row_in_page(stmt, page_guard, key, row_id, user_read_set)
                            .await;
                    }
                },
            }
        }
    }

    #[inline]
    async fn select_row_in_page(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<'_, RowPage>,
        key: Val,
        row_id: RowID,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return SelectMvcc::RowNotFound;
        }
        let row_idx = page.row_idx(row_id);
        let access = self
            .lock_row_for_read(&stmt.trx, &page_guard, row_idx)
            .await;
        access.read_row_mvcc(&stmt.trx, &self.schema, user_read_set, &key)
    }

    /// Insert row with MVCC.
    #[inline]
    pub async fn insert_row(&self, stmt: &mut Statement, cols: Vec<Val>) -> InsertMvcc {
        debug_assert!(cols.len() + 1 == self.schema.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.schema.user_col_type_match(idx, val))
        });
        let key = cols[self.schema.user_key_idx()].clone();
        // insert row into page with undo log linked.
        let (row_id, page_guard) = self.insert_row_internal(stmt, cols, UndoKind::Insert, None);
        // update index
        if let Some(old_row_id) = self.sec_idx.insert_if_not_exists(key.clone(), row_id) {
            // we found there is already one existing row with same key.
            // so perform move+insert.
            match self
                .move_insert(stmt, old_row_id, key.clone(), row_id, page_guard)
                .await
            {
                MoveInsert::DuplicateKey => InsertMvcc::DuplicateKey,
                MoveInsert::WriteConflict => InsertMvcc::WriteConflict,
                MoveInsert::Ok | MoveInsert::None => InsertMvcc::Ok(row_id),
            }
        } else {
            InsertMvcc::Ok(row_id)
        }
    }

    /// Update row with MVCC.
    /// This method is for update based on index lookup.
    #[inline]
    pub async fn update_row(
        &self,
        stmt: &mut Statement,
        key: Val,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        loop {
            match self.sec_idx.lookup(&key) {
                None => return UpdateMvcc::RowNotFound,
                Some(row_id) => {
                    match self.blk_idx.find_row_id(self.buf_pool, row_id) {
                        RowLocation::NotFound => return UpdateMvcc::RowNotFound,
                        RowLocation::ColSegment(..) => todo!(),
                        RowLocation::RowPage(page_id) => {
                            let page = self.buf_pool.get_page(page_id, LatchFallbackMode::Shared);
                            let page_guard = page.block_until_shared();
                            if !validate_page_id(&page_guard, page_id) {
                                continue;
                            }
                            let key = key.clone();
                            let res = self
                                .update_row_inplace(stmt, page_guard, key, row_id, update)
                                .await;

                            return match res {
                                UpdateRowInplace::Ok(row_id) => UpdateMvcc::Ok(row_id),
                                UpdateRowInplace::RowDeleted => UpdateMvcc::RowDeleted,
                                UpdateRowInplace::RowNotFound => UpdateMvcc::RowNotFound,
                                UpdateRowInplace::WriteConflict => UpdateMvcc::WriteConflict,
                                UpdateRowInplace::NoFreeSpace(
                                    old_row_id,
                                    old_row,
                                    update,
                                    old_guard,
                                ) => {
                                    // in-place update failed, we transfer update into
                                    // move+update.
                                    self.move_update(stmt, old_row, update, old_row_id, old_guard)
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
    pub async fn delete_row(&self, stmt: &mut Statement, key: Val) -> DeleteMvcc {
        loop {
            match self.sec_idx.lookup(&key) {
                None => return DeleteMvcc::RowNotFound,
                Some(row_id) => match self.blk_idx.find_row_id(self.buf_pool, row_id) {
                    RowLocation::NotFound => return DeleteMvcc::RowNotFound,
                    RowLocation::ColSegment(..) => todo!(),
                    RowLocation::RowPage(page_id) => {
                        let page = self.buf_pool.get_page(page_id, LatchFallbackMode::Shared);
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
    fn move_update(
        &self,
        stmt: &mut Statement,
        mut old_row: Vec<Val>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<'a, RowPage>,
    ) -> UpdateMvcc {
        // calculate new row and undo entry.
        let (new_row, undo_kind) = {
            let mut undo_cols = vec![];
            for mut uc in update {
                let old_val = &mut old_row[uc.idx];
                if old_val != &uc.val {
                    // swap old value and new value, then put into undo columns
                    mem::swap(&mut uc.val, old_val);
                    undo_cols.push(uc);
                }
            }
            (old_row, UndoKind::Update(undo_cols))
        };
        let (row_id, page_guard) =
            self.insert_row_internal(stmt, new_row, undo_kind, Some((old_id, old_guard)));
        drop(page_guard); // unlock the page
        UpdateMvcc::Ok(row_id)
    }

    /// Move insert is similar to a delete+insert.
    /// But it triggered by duplicate key finding when updating index.
    /// The insert is already done and we additionally add a move entry to the
    /// already deleted version.
    #[inline]
    async fn move_insert(
        &self,
        stmt: &mut Statement,
        row_id: RowID,
        key: Val,
        new_id: RowID,
        new_guard: PageSharedGuard<'_, RowPage>,
    ) -> MoveInsert {
        loop {
            match self.blk_idx.find_row_id(self.buf_pool, row_id) {
                RowLocation::NotFound => return MoveInsert::None,
                RowLocation::ColSegment(..) => todo!(),
                RowLocation::RowPage(page_id) => {
                    let page_guard = self
                        .buf_pool
                        .get_page(page_id, LatchFallbackMode::Shared)
                        .block_until_shared();
                    if !validate_page_id(&page_guard, page_id) {
                        continue;
                    }
                    let page = page_guard.page();
                    if !page.row_id_in_valid_range(row_id) {
                        // no old row found
                        return MoveInsert::None;
                    }
                    let row_idx = page.row_idx(row_id);
                    let mut lock_row = self
                        .lock_row_for_write(&stmt.trx, &page_guard, row_idx, &key)
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
                            let mut move_entry = OwnedUndoEntry::new(
                                self.table_id,
                                page_id,
                                row_id,
                                UndoKind::Move(true),
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
                                .lock_row_for_write(&stmt.trx, &new_guard, new_idx, &key)
                                .await;
                            let (new_access, _) = lock_new.ok().expect("lock new row for insert");
                            debug_assert!(new_access.is_some());
                            let mut new_entry = stmt.trx.undo.pop().expect("new entry for insert");
                            link_move_entry(&mut new_entry, move_entry.leak());

                            drop(new_access); // unlock new row
                            drop(new_guard); // unlock new page

                            stmt.undo.push(move_entry);
                            stmt.undo.push(new_entry);
                            // no redo required, because no change on row data.
                            return MoveInsert::Ok;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn insert_row_internal(
        &self,
        stmt: &mut Statement,
        mut insert: Vec<Val>,
        mut undo_kind: UndoKind,
        mut move_entry: Option<(RowID, PageSharedGuard<'a, RowPage>)>,
    ) -> (RowID, PageSharedGuard<RowPage>) {
        let row_len = row_len(&self.schema, &insert);
        let row_count = estimate_max_row_count(row_len, self.schema.col_count());
        loop {
            let page_guard = self.get_insert_page(stmt, row_count);
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
    fn insert_row_to_page(
        &self,
        stmt: &mut Statement,
        page_guard: PageSharedGuard<'a, RowPage>,
        insert: Vec<Val>,
        undo_kind: UndoKind,
        move_entry: Option<(RowID, PageSharedGuard<'a, RowPage>)>,
    ) -> InsertRowIntoPage<'a> {
        debug_assert!({
            (matches!(undo_kind, UndoKind::Insert) && move_entry.is_none())
                || (matches!(undo_kind, UndoKind::Update(_)) && move_entry.is_some())
        });

        let page_id = page_guard.page_id();
        match page_guard.page().insert(&self.schema, &insert) {
            InsertRow::Ok(row_id) => {
                let row_idx = (row_id - page_guard.page().header.start_row_id) as usize;
                page_guard.page().row_idx(row_id);
                let mut access = page_guard.write_row(row_idx);
                // create undo log.
                let mut new_entry = OwnedUndoEntry::new(self.table_id, page_id, row_id, undo_kind);
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
                stmt.undo.push(new_entry);
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
        page_guard: PageSharedGuard<'a, RowPage>,
        key: Val,
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
            .lock_row_for_write(&stmt.trx, &page_guard, row_idx, &key)
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
                        let mut new_entry = OwnedUndoEntry::new(
                            self.table_id,
                            page_id,
                            row_id,
                            UndoKind::Move(false),
                        );
                        access.build_undo_chain(&stmt.trx, &mut new_entry, old_cts);
                        drop(access); // unlock row
                        drop(lock_row);
                        // Here we do not unlock page because we need to perform MOVE+UPDATE
                        // and link undo entries of two rows.
                        // The re-lock of current undo is required.
                        stmt.undo.push(new_entry);
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
                            if let Some(old) = row.user_different(&self.schema, uc.idx, &uc.val) {
                                undo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    val: Val::from(old),
                                });
                                redo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    // new value no longer needed, so safe to take it here.
                                    val: mem::take(&mut uc.val),
                                });
                                row.update_user_col(uc.idx, &uc.val);
                            }
                        }
                        let mut new_entry = OwnedUndoEntry::new(
                            self.table_id,
                            page_id,
                            row_id,
                            UndoKind::Update(undo_cols),
                        );
                        access.build_undo_chain(&stmt.trx, &mut new_entry, old_cts);
                        drop(access); // unlock the row.
                        drop(lock_row);
                        drop(page_guard); // unlock the page, because we finish page update.
                        stmt.undo.push(new_entry);
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
        if row_id < page.header.start_row_id
            || row_id >= page.header.start_row_id + page.header.max_row_count as u64
        {
            return DeleteMvcc::RowNotFound;
        }
        let row_idx = (row_id - page.header.start_row_id) as usize;
        let mut lock_row = self
            .lock_row_for_write(&stmt.trx, &page_guard, row_idx, key)
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => return DeleteMvcc::RowNotFound,
            LockRowForWrite::WriteConflict => return DeleteMvcc::WriteConflict,
            LockRowForWrite::Ok(access, old_cts) => {
                let mut access = access.take().unwrap();
                if access.row().is_deleted() {
                    return DeleteMvcc::RowAlreadyDeleted;
                }
                access.delete_row();
                let mut new_entry =
                    OwnedUndoEntry::new(self.table_id, page_id, row_id, UndoKind::Delete);
                access.build_undo_chain(&stmt.trx, &mut new_entry, mem::take(old_cts));
                drop(access); // unlock row
                drop(lock_row);
                drop(page_guard); // unlock page
                stmt.undo.push(new_entry);
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
    fn get_insert_page(
        &self,
        stmt: &mut Statement,
        row_count: usize,
    ) -> PageSharedGuard<'a, RowPage> {
        if let Some((page_id, row_id)) = stmt.load_active_insert_page(self.table_id) {
            let g = self.buf_pool.get_page(page_id, LatchFallbackMode::Shared);
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
            .get_insert_page(self.buf_pool, row_count, &self.schema)
    }

    // lock row will check write conflict on given row and lock it.
    #[inline]
    async fn lock_row_for_write(
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
                    let head = UndoHead {
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
    async fn lock_row_for_read(
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
    let header = &page_guard.page().header;
    row_id >= header.start_row_id && row_id < header.start_row_id + header.max_row_count as u64
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
fn link_move_entry(new_entry: &mut OwnedUndoEntry, move_entry: UndoEntryPtr) {
    // ref-count this pointer.
    debug_assert!(new_entry.next.is_none());
    new_entry.next = Some(NextUndoEntry {
        status: NextUndoStatus::SameAsPrev,
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
        UndoKind,
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
        Vec<Val>,
        Vec<UpdateCol>,
        PageSharedGuard<'a, RowPage>,
    ),
}

#[cfg(test)]
mod tests {
    use crate::buffer::FixedBufferPool;
    use crate::index::BlockIndex;
    use crate::index::PartitionIntIndex;
    use crate::row::ops::{SelectMvcc, UpdateCol};
    use crate::session::Session;
    use crate::table::{Schema, Table};
    use crate::trx::sys::{TransactionSystem, TrxSysConfig};
    use crate::value::Layout;
    use crate::value::Val;
    use std::sync::Arc;

    #[test]
    fn test_mvcc_insert_normal() {
        smol::block_on(async {
            const SIZE: i32 = 10000;

            let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
            let trx_sys = TrxSysConfig::default().build_static();

            let table = create_table(buf_pool);
            let table = table.mvcc();
            let mut session = Session::new();
            {
                let mut trx = session.begin_trx(trx_sys);
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let mut stmt = trx.start_stmt();
                    let res = table
                        .insert_row(&mut stmt, vec![Val::from(i), Val::from(&s[..])])
                        .await;
                    trx = stmt.commit();
                    assert!(res.is_ok());
                }
                session = trx_sys.commit(trx).await.unwrap();
            }
            {
                let mut trx = session.begin_trx(trx_sys);
                for i in 16..SIZE {
                    let mut stmt = trx.start_stmt();
                    let key = Val::from(i);
                    let res = table.select_row(&mut stmt, key, &[0, 1]).await;
                    match res {
                        SelectMvcc::Ok(vals) => {
                            assert!(vals.len() == 2);
                            assert!(&vals[0] == &Val::from(i));
                            let s = format!("{}", i);
                            assert!(&vals[1] == &Val::from(&s[..]));
                        }
                        _ => panic!("select fail"),
                    }
                    trx = stmt.commit();
                }
                let _ = trx_sys.commit(trx).await.unwrap();
            }

            unsafe {
                TransactionSystem::drop_static(trx_sys);
                FixedBufferPool::drop_static(buf_pool);
            }
        });
    }

    #[test]
    fn test_mvcc_update() {
        smol::block_on(async {
            const SIZE: i32 = 1000;

            let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
            let trx_sys = TrxSysConfig::default().build_static();
            {
                let table = create_table(buf_pool);
                let table = table.mvcc();

                let mut session = Session::new();
                // insert 1000 rows
                let mut trx = session.begin_trx(trx_sys);
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let mut stmt = trx.start_stmt();
                    let res = table
                        .insert_row(&mut stmt, vec![Val::from(i), Val::from(&s[..])])
                        .await;
                    trx = stmt.commit();
                    assert!(res.is_ok());
                }
                session = trx_sys.commit(trx).await.unwrap();

                // update 1 row with short value
                let mut trx = session.begin_trx(trx_sys);
                let k1 = Val::from(1i32);
                let s1 = "hello";
                let update1 = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(s1),
                }];
                let mut stmt = trx.start_stmt();
                let res = table.update_row(&mut stmt, k1, update1).await;
                assert!(res.is_ok());
                trx = stmt.commit();
                session = trx_sys.commit(trx).await.unwrap();

                // update 1 row with long value
                let mut trx = session.begin_trx(trx_sys);
                let k2 = Val::from(100i32);
                let s2: String = (0..50_000).map(|_| '1').collect();
                let update2 = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                }];
                let mut stmt = trx.start_stmt();
                let res = table.update_row(&mut stmt, k2, update2).await;
                assert!(res.is_ok());
                trx = stmt.commit();
                let _ = trx_sys.commit(trx).await.unwrap();
            }
            unsafe {
                TransactionSystem::drop_static(trx_sys);
                FixedBufferPool::drop_static(buf_pool);
            }
        });
    }

    #[test]
    fn test_mvcc_delete() {
        smol::block_on(async {
            const SIZE: i32 = 1000;

            let buf_pool = FixedBufferPool::with_capacity_static(1024 * 1024).unwrap();
            let trx_sys = TrxSysConfig::default().build_static();
            {
                let table = create_table(buf_pool);
                let table = table.mvcc();

                let mut session = Session::new();
                // insert 1000 rows
                let mut trx = session.begin_trx(trx_sys);
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let mut stmt = trx.start_stmt();
                    let res = table
                        .insert_row(&mut stmt, vec![Val::from(i), Val::from(&s[..])])
                        .await;
                    trx = stmt.commit();
                    assert!(res.is_ok());
                }
                session = trx_sys.commit(trx).await.unwrap();

                // delete 1 row
                let mut trx = session.begin_trx(trx_sys);
                let k1 = Val::from(1i32);
                let mut stmt = trx.start_stmt();
                let res = table.delete_row(&mut stmt, k1).await;
                assert!(res.is_ok());
                trx = stmt.commit();
                let _ = trx_sys.commit(trx).await.unwrap();
            }
            unsafe {
                TransactionSystem::drop_static(trx_sys);
                FixedBufferPool::drop_static(buf_pool);
            }
        });
    }

    fn create_table(buf_pool: &'static FixedBufferPool) -> Table<'static> {
        Table {
            table_id: 1,
            buf_pool,
            schema: Schema::new(vec![Layout::Byte4, Layout::VarByte], 0),
            blk_idx: BlockIndex::new(buf_pool).unwrap(),
            sec_idx: Arc::new(PartitionIntIndex::empty()),
        }
    }
}
