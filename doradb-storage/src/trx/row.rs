use crate::buffer::guard::PageSharedGuard;
use crate::row::ops::{SelectMvcc, UpdateCol, UpdateRow};
use crate::row::{Row, RowMut, RowPage, RowRead};
use crate::table::Schema;
use crate::trx::undo::UndoKind;
use crate::trx::undo::{
    NextTrxCTS, NextUndoEntry, NextUndoStatus, OwnedUndoEntry, UndoEntryPtr, UndoHead,
};
use crate::trx::{trx_is_committed, ActiveTrx};
use crate::value::Val;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::collections::{BTreeMap, BTreeSet};

pub struct RowReadAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    undo: RwLockReadGuard<'a, Option<UndoHead>>,
}

impl RowReadAccess<'_> {
    #[inline]
    pub fn row(&self) -> Row {
        self.page.row(self.row_idx)
    }

    #[inline]
    pub fn undo(&self) -> &Option<UndoHead> {
        &self.undo
    }

    #[inline]
    pub fn read_row_mvcc(
        &self,
        trx: &ActiveTrx,
        schema: &Schema,
        user_read_set: &[usize],
        key: &Val,
    ) -> SelectMvcc {
        // let mut vals = BTreeMap::new();
        match &*self.undo {
            None => {
                let row = self.row();
                // latest version in row page.
                if row.is_deleted() {
                    return SelectMvcc::RowNotFound;
                }
                if row.is_key_different(schema, key) {
                    return SelectMvcc::InvalidIndex;
                }
                let vals = row.clone_vals_for_read_set(schema, user_read_set);
                SelectMvcc::Ok(vals)
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
                            return SelectMvcc::RowNotFound;
                        }
                        if row.is_key_different(schema, key) {
                            return SelectMvcc::InvalidIndex;
                        }
                        let vals = row.clone_vals_for_read_set(schema, user_read_set);
                        return SelectMvcc::Ok(vals);
                    } // otherwise, go to next version
                } else {
                    let trx_id = trx.trx_id();
                    if trx_id == ts {
                        let row = self.row();
                        // self update, see the latest version
                        if row.is_deleted() {
                            return SelectMvcc::RowNotFound;
                        }
                        if row.is_key_different(schema, key) {
                            return SelectMvcc::InvalidIndex;
                        }
                        let vals = row.clone_vals_for_read_set(schema, user_read_set);
                        return SelectMvcc::Ok(vals);
                    } // otherwise, go to next version
                }
                // page data is invisible, we have to backtrace version chain
                match undo_head.entry.as_ref() {
                    None => {
                        // no next version, so nothing to be return.
                        return SelectMvcc::RowNotFound;
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
                                UndoKind::Insert => {
                                    debug_assert!(!ver.deleted);
                                    ver.deleted = true; // insert is not seen, mark as deleted
                                }
                                UndoKind::Update(upd_cols) => {
                                    debug_assert!(!ver.deleted);
                                    ver.undo_update(upd_cols);
                                }
                                UndoKind::Delete => {
                                    debug_assert!(ver.deleted);
                                    ver.deleted = true; // delete is not seen, mark as not deleted.
                                }
                                UndoKind::Move(del) => {
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
                                        return SelectMvcc::RowNotFound;
                                    }
                                    // check if key match
                                    return ver.get_visible_vals(schema, self.row());
                                }
                                Some(next) => {
                                    match next.status {
                                        NextUndoStatus::SameAsPrev => {
                                            let next_entry = next.entry.clone();
                                            entry = next_entry; // still invisible.
                                        }
                                        NextUndoStatus::CTS(cts) => {
                                            if trx.sts > cts {
                                                // current version is visible
                                                if ver.deleted {
                                                    return SelectMvcc::RowNotFound;
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
    fn undo_update(&mut self, upd_cols: &[UpdateCol]) {
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
    fn get_visible_vals(mut self, schema: &Schema, row: Row<'_>) -> SelectMvcc {
        if self.read_set_contains_key {
            let key_different = self
                .undo_vals
                .get(&self.user_key_idx)
                .map(|v| v == &self.undo_key)
                .unwrap_or_else(|| row.is_key_different(schema, &self.undo_key));
            if key_different {
                return SelectMvcc::InvalidIndex;
            }
        } else {
            if row.is_key_different(schema, &self.undo_key) {
                return SelectMvcc::InvalidIndex;
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
        SelectMvcc::Ok(vals)
    }
}

pub struct RowWriteAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    undo: RwLockWriteGuard<'a, Option<UndoHead>>,
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
    pub fn row_and_undo_mut(&mut self) -> (Row, &mut Option<UndoHead>) {
        let row = self.page.row(self.row_idx);
        (row, &mut *self.undo)
    }

    #[inline]
    pub fn update_row(&self, schema: &Schema, user_cols: &[UpdateCol]) -> UpdateRow {
        let var_len = self.row().var_len_for_update(user_cols);
        match self.page.request_free_space(var_len) {
            None => {
                let old_row = self.row().clone_vals(schema, false);
                UpdateRow::NoFreeSpace(old_row)
            }
            Some(offset) => {
                let row = self.row_mut(offset, offset + var_len);
                UpdateRow::Ok(row)
            }
        }
    }

    #[inline]
    pub fn undo_head(&self) -> &Option<UndoHead> {
        &*self.undo
    }

    #[inline]
    pub fn first_undo_entry(&self) -> Option<UndoEntryPtr> {
        self.undo.as_ref().and_then(|head| head.entry.clone())
    }

    /// Build undo chain.
    /// This method locks undo head and add new entry
    #[inline]
    pub fn build_undo_chain(
        &mut self,
        trx: &ActiveTrx,
        new_entry: &mut OwnedUndoEntry,
        old_cts: NextTrxCTS,
    ) {
        let head = self.undo.get_or_insert_with(|| UndoHead {
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
            new_entry.next = Some(NextUndoEntry {
                status: old_cts.undo_status(),
                entry,
            });
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
