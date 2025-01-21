use crate::buffer::guard::PageSharedGuard;
use crate::row::ops::{ReadRow, UndoCol, UpdateCol, UpdateRow};
use crate::row::{Row, RowMut, RowPage, RowRead};
use crate::table::Schema;
use crate::trx::undo::{
    NextRowUndo, NextRowUndoStatus, NextTrxCTS, OwnedRowUndo, RowUndoHead, RowUndoKind, RowUndoRef,
};
use crate::trx::{trx_is_committed, ActiveTrx, SharedTrxStatus};
use crate::value::Val;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub struct RowReadAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    undo: RwLockReadGuard<'a, Option<RowUndoHead>>,
}

impl RowReadAccess<'_> {
    #[inline]
    pub fn row(&self) -> Row {
        self.page.row(self.row_idx)
    }

    #[inline]
    pub fn undo(&self) -> &Option<RowUndoHead> {
        &self.undo
    }

    #[inline]
    pub fn latest_status(&self) -> RowLatestStatus {
        if let Some(head) = &*self.undo {
            let ts = head.status.ts();
            if !trx_is_committed(ts) {
                return RowLatestStatus::Uncommitted;
            }
        }
        // the row is committed, check if it's deleted.
        if self.row().is_deleted() {
            return RowLatestStatus::Deleted;
        }
        RowLatestStatus::Committed
    }

    #[inline]
    pub fn read_row_mvcc(
        &self,
        trx: &ActiveTrx,
        schema: &Schema,
        user_read_set: &[usize],
        key: &Val,
    ) -> ReadRow {
        // let mut vals = BTreeMap::new();
        match &*self.undo {
            None => {
                let row = self.row();
                // latest version in row page.
                if row.is_deleted() {
                    return ReadRow::NotFound;
                }
                if row.is_key_different(schema, key) {
                    return ReadRow::InvalidIndex;
                }
                let vals = row.clone_vals_for_read_set(schema, user_read_set);
                ReadRow::Ok(vals)
            }
            Some(undo_head) => {
                // At this point, we already wait for preparation of commit is done.
                // So we only have two cases: uncommitted, and committed.
                let ts = undo_head.status.ts();
                if trx_is_committed(ts) {
                    if trx.sts > ts {
                        let row = self.row();
                        // we can see this version
                        if row.is_deleted() {
                            return ReadRow::NotFound;
                        }
                        if row.is_key_different(schema, key) {
                            return ReadRow::InvalidIndex;
                        }
                        let vals = row.clone_vals_for_read_set(schema, user_read_set);
                        return ReadRow::Ok(vals);
                    } // otherwise, go to next version
                } else {
                    let trx_id = trx.trx_id();
                    if trx_id == ts {
                        let row = self.row();
                        // self update, see the latest version
                        if row.is_deleted() {
                            return ReadRow::NotFound;
                        }
                        if row.is_key_different(schema, key) {
                            return ReadRow::InvalidIndex;
                        }
                        let vals = row.clone_vals_for_read_set(schema, user_read_set);
                        return ReadRow::Ok(vals);
                    } // otherwise, go to next version
                }
                // page data is invisible, we have to backtrace version chain
                match undo_head.entry.as_ref() {
                    None => {
                        // no next version, so nothing to be return.
                        return ReadRow::NotFound;
                    }
                    Some(entry) => {
                        let mut entry = entry.clone();
                        let read_set: BTreeSet<usize> = user_read_set.iter().cloned().collect();
                        let user_key_idx = schema.user_key_idx();
                        let read_set_contains_key = read_set.contains(&user_key_idx);
                        let mut ver = RowVersion {
                            deleted: self.row().is_deleted(),
                            read_set,
                            user_key_idx,
                            read_set_contains_key,
                            undo_key: key.clone(),
                            undo_vals: BTreeMap::new(),
                        };
                        loop {
                            match &entry.as_ref().kind {
                                RowUndoKind::Insert => {
                                    debug_assert!(!ver.deleted);
                                    ver.deleted = true; // insert is not seen, mark as deleted
                                }
                                RowUndoKind::Update(undo_cols) => {
                                    debug_assert!(!ver.deleted);
                                    ver.undo_update(undo_cols);
                                }
                                RowUndoKind::Delete => {
                                    debug_assert!(ver.deleted);
                                    ver.deleted = true; // delete is not seen, mark as not deleted.
                                }
                                RowUndoKind::Move(del) => {
                                    // we cannot determine the delete flag here,
                                    // because if move+insert, flag is true.
                                    // if move+update, flag is false.
                                    ver.deleted = *del; // recover moved status
                                }
                            }
                            match entry.as_ref().next.as_ref() {
                                None => {
                                    // No next version, we need to determine whether we should return row
                                    // by checking deleted flag.
                                    // For example:
                                    // If undo kind is DELETE, and next version does not exist.
                                    // That means we should should return the row before deletion.
                                    // If undo kind is INSERT, and next version does not exist.
                                    // That means we should return no row.
                                    if ver.deleted {
                                        return ReadRow::NotFound;
                                    }
                                    // check if key match
                                    return ver.get_visible_vals(schema, self.row());
                                }
                                Some(next) => {
                                    match next.status {
                                        NextRowUndoStatus::SameAsPrev => {
                                            let next_entry = next.entry.clone();
                                            entry = next_entry; // still invisible.
                                        }
                                        NextRowUndoStatus::CTS(cts) => {
                                            if trx.sts > cts {
                                                // current version is visible
                                                if ver.deleted {
                                                    return ReadRow::NotFound;
                                                }
                                                return ver.get_visible_vals(schema, self.row());
                                            }
                                            entry = next.entry.clone(); // still invisible
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// Version of current row.
struct RowVersion {
    deleted: bool,
    read_set: BTreeSet<usize>,
    user_key_idx: usize,
    read_set_contains_key: bool,
    undo_key: Val,
    undo_vals: BTreeMap<usize, Val>,
}

impl RowVersion {
    #[inline]
    fn undo_update(&mut self, upd_cols: &[UndoCol]) {
        // undo update
        for uc in upd_cols {
            if self.read_set.contains(&uc.idx) {
                self.undo_vals.insert(uc.idx, uc.val.clone());
            }
        }
        if !self.read_set_contains_key {
            // undo key
            if let Ok(key_pos) = upd_cols.binary_search_by_key(&self.user_key_idx, |uc| uc.idx) {
                self.undo_key = upd_cols[key_pos].val.clone();
            }
        }
    }

    #[inline]
    fn get_visible_vals(mut self, schema: &Schema, row: Row<'_>) -> ReadRow {
        if self.read_set_contains_key {
            let key_different = self
                .undo_vals
                .get(&self.user_key_idx)
                .map(|v| v == &self.undo_key)
                .unwrap_or_else(|| row.is_key_different(schema, &self.undo_key));
            if key_different {
                return ReadRow::InvalidIndex;
            }
        } else {
            if row.is_key_different(schema, &self.undo_key) {
                return ReadRow::InvalidIndex;
            }
        }
        let mut vals = Vec::with_capacity(self.read_set.len());
        for user_col_idx in &self.read_set {
            if let Some(v) = self.undo_vals.remove(user_col_idx) {
                vals.push(v);
            } else {
                vals.push(row.clone_user_val(schema, *user_col_idx))
            }
        }
        ReadRow::Ok(vals)
    }
}

pub struct RowWriteAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    undo: RwLockWriteGuard<'a, Option<RowUndoHead>>,
}

impl<'a> RowWriteAccess<'a> {
    #[inline]
    pub fn row(&self) -> Row<'a> {
        self.page.row(self.row_idx)
    }

    #[inline]
    pub fn row_mut(&self, var_offset: usize, var_end: usize) -> RowMut {
        self.page.row_mut(self.row_idx, var_offset, var_end)
    }

    #[inline]
    pub fn delete_row(&mut self) {
        self.page.set_deleted(self.row_idx, true);
    }

    #[inline]
    pub fn row_and_undo_mut(&mut self) -> (Row, &mut Option<RowUndoHead>) {
        let row = self.page.row(self.row_idx);
        (row, &mut *self.undo)
    }

    #[inline]
    pub fn update_row(&self, schema: &Schema, user_cols: &[UpdateCol]) -> UpdateRow {
        let var_len = self.row().var_len_for_update(user_cols);
        match self.page.request_free_space(var_len) {
            None => {
                let old_row = self.row().clone_vals_with_var_offsets(schema, false);
                UpdateRow::NoFreeSpace(old_row)
            }
            Some(offset) => {
                let row = self.row_mut(offset, offset + var_len);
                UpdateRow::Ok(row)
            }
        }
    }

    #[inline]
    pub fn undo_head(&self) -> &Option<RowUndoHead> {
        &*self.undo
    }

    #[inline]
    pub fn first_undo_entry(&self) -> Option<RowUndoRef> {
        self.undo.as_ref().and_then(|head| head.entry.clone())
    }

    /// Build undo chain.
    /// This method locks undo head and add new entry
    #[inline]
    pub fn build_undo_chain(
        &mut self,
        trx: &ActiveTrx,
        new_entry: &mut OwnedRowUndo,
        old_cts: NextTrxCTS,
    ) {
        let head = self.undo.get_or_insert_with(|| RowUndoHead {
            status: trx.status(),
            entry: None,
        });
        debug_assert!(head.status.ts() == trx.trx_id());
        // 1. Update head trx id.
        head.status = trx.status();
        // 2. Link new entry and head, or there might be case that
        //    new entry has non-null next pointer, and head must not have
        //    non-null next pointer.
        let old_entry = head.entry.replace(new_entry.leak());
        // 3. Link new and old.
        if let Some(entry) = old_entry {
            debug_assert!(new_entry.next.is_none());
            new_entry.next = Some(NextRowUndo {
                status: old_cts.undo_status(),
                entry,
            });
        }
    }

    #[inline]
    pub fn rollback_first_undo(&mut self, mut owned_entry: OwnedRowUndo) {
        let head = self.undo.as_mut().expect("undo head");
        match head.entry.take() {
            None => unreachable!(),
            Some(entry) => {
                debug_assert!(std::ptr::addr_eq(entry.as_ref(), &*owned_entry));
                // rollback row data
                match &owned_entry.kind {
                    RowUndoKind::Insert => {
                        self.page.set_deleted(self.row_idx, true);
                    }
                    RowUndoKind::Delete => {
                        self.page.set_deleted(self.row_idx, false);
                    }
                    RowUndoKind::Update(undo_cols) => {
                        for uc in undo_cols {
                            match &uc.val {
                                Val::Null => {
                                    self.page.set_null(self.row_idx, uc.idx, true);
                                }
                                Val::VarByte(var) => {
                                    let (pv, _) = self.page.add_var(
                                        var.as_bytes(),
                                        uc.var_offset.unwrap_or(0) as usize,
                                    );
                                    self.page.update_var(self.row_idx, uc.idx, pv);
                                }
                                Val::Byte1(v) => {
                                    self.page.update_val(self.row_idx, uc.idx, v);
                                }
                                Val::Byte2(v) => {
                                    self.page.update_val(self.row_idx, uc.idx, v);
                                }
                                Val::Byte4(v) => {
                                    self.page.update_val(self.row_idx, uc.idx, v);
                                }
                                Val::Byte8(v) => {
                                    self.page.update_val(self.row_idx, uc.idx, v);
                                }
                            }
                        }
                    }
                    RowUndoKind::Move(deleted) => {
                        self.page.set_deleted(self.row_idx, *deleted);
                    }
                }
                // rollback undo
                match owned_entry.next.take() {
                    None => {
                        // The entry to rollback is the only undo entry of this row.
                        // So data in row page is globally visible now, we can
                        // update undo status to CTS=GLOBAL_VISIBLE_CTS.
                        head.status = Arc::new(SharedTrxStatus::global_visible());
                    }
                    Some(next) => {
                        if let RowUndoKind::Move(_) = &next.entry.as_ref().kind {
                            // MOVE is undo entry of another row, so treat it as None.
                            head.status = Arc::new(SharedTrxStatus::global_visible());
                        } else {
                            match next.status {
                                NextRowUndoStatus::SameAsPrev => {
                                    // This entry belongs to same transaction,
                                    // So keep transaction status as it is.
                                }
                                NextRowUndoStatus::CTS(cts) => {
                                    head.status = Arc::new(SharedTrxStatus::new(cts));
                                }
                            }
                            head.entry = Some(next.entry);
                        }
                    }
                }
            }
        }
    }
}

impl<'a> PageSharedGuard<'a, RowPage> {
    #[inline]
    pub fn read_row(&self, row_idx: usize) -> RowReadAccess<'_> {
        let (fh, page) = self.header_and_page();
        let undo_map = fh.undo_map.as_ref().unwrap();
        let undo = undo_map.read(row_idx);
        RowReadAccess {
            page,
            row_idx,
            undo,
        }
    }

    #[inline]
    pub fn write_row(&self, row_idx: usize) -> RowWriteAccess<'_> {
        let (fh, page) = self.header_and_page();
        let undo_map = fh.undo_map.as_ref().unwrap();
        let undo = undo_map.write(row_idx);
        RowWriteAccess {
            page,
            row_idx,
            undo,
        }
    }
}

pub enum RowLatestStatus {
    NotFound,
    Deleted,
    Committed,
    Uncommitted,
}
