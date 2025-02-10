use crate::buffer::guard::PageSharedGuard;
use crate::catalog::TableSchema;
use crate::row::ops::{ReadRow, SelectKey, UndoCol, UpdateCol, UpdateRow};
use crate::row::{Row, RowID, RowMut, RowPage, RowRead};
use crate::trx::undo::{
    NextRowUndo, NextRowUndoStatus, NextTrxCTS, OwnedRowUndo, RowUndoBranch, RowUndoHead,
    RowUndoKind, RowUndoRef,
};
use crate::trx::{trx_is_committed, ActiveTrx, SharedTrxStatus, TrxID, GLOBAL_VISIBLE_COMMIT_TS};
use crate::value::Val;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
        let cts = if let Some(head) = &*self.undo {
            let ts = head.status.ts();
            if !trx_is_committed(ts) {
                return RowLatestStatus::Uncommitted;
            }
            ts
        } else {
            GLOBAL_VISIBLE_COMMIT_TS
        };
        // the row is committed, check if it's deleted.
        RowLatestStatus::Committed(cts, self.row().is_deleted())
    }

    #[inline]
    pub fn read_row_latest(
        &self,
        schema: &TableSchema,
        user_read_set: &[usize],
        key: &SelectKey,
    ) -> ReadRow {
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

    #[inline]
    pub fn read_row_mvcc(
        &self,
        trx: &ActiveTrx,
        schema: &TableSchema,
        user_read_set: &[usize],
        key: &SelectKey,
    ) -> ReadRow {
        // let mut vals = BTreeMap::new();
        match &*self.undo {
            None => self.read_row_latest(schema, user_read_set, key),
            Some(undo_head) => {
                // At this point, we already wait for preparation of commit is done.
                // So we only have two cases: uncommitted, and committed.
                let ts = undo_head.status.ts();
                if trx_is_committed(ts) {
                    if trx.sts > ts {
                        // this version is visible
                        return self.read_row_latest(schema, user_read_set, key);
                    } // otherwise, go to next version
                } else {
                    let trx_id = trx.trx_id();
                    if trx_id == ts {
                        // self update, see the latest version
                        return self.read_row_latest(schema, user_read_set, key);
                    } // otherwise, go to next version
                }
                // page data is invisible, we have to backtrace version chain
                match undo_head.entry.as_ref() {
                    None => {
                        // Row page is not visible but no next version.
                        // MVCC guarantee that if current version is not visible,
                        // we can always see next version.
                        // Only if implementation is wrong, next version will be missing.
                        // So we throw.
                        panic!("next version is missing");
                    }
                    Some(entry) => {
                        // build row contrainer for version traversal.
                        let mut entry = entry.clone();
                        let read_set: BTreeSet<usize> = user_read_set.iter().cloned().collect();
                        let index_schema = &schema.indexes[key.index_no];
                        let user_key_idx_map: HashMap<usize, usize> = index_schema
                            .keys
                            .iter()
                            .enumerate()
                            .map(|(key_pos, key)| (key.user_col_idx as usize, key_pos))
                            .collect();
                        let read_set_contains_key = user_key_idx_map
                            .keys()
                            .all(|user_key_idx| read_set.contains(user_key_idx));
                        let undo_key = if read_set_contains_key {
                            None
                        } else {
                            let vals = self.row().clone_index_vals(schema, key.index_no);
                            Some(SelectKey {
                                index_no: key.index_no,
                                vals,
                            })
                        };
                        let mut ver = RowVersion {
                            deleted: self.row().is_deleted(),
                            read_set,
                            user_key_idx_map,
                            undo_key,
                            undo_vals: BTreeMap::new(),
                        };
                        // traverse version chain.
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
                            match entry.as_ref().find_next_version(key) {
                                None => {
                                    // No next version, we need to determine whether we should return row
                                    // by checking deleted flag.
                                    // For example:
                                    // If undo kind is DELETE, and next version does not exist.
                                    // That means we should return the row before deletion.
                                    // If undo kind is INSERT, and next version does not exist.
                                    // That means we should return no row.
                                    if ver.deleted {
                                        return ReadRow::NotFound;
                                    }
                                    // check if key match
                                    return ver.get_visible_vals(schema, self.row(), key);
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
                                                return ver.get_visible_vals(
                                                    schema,
                                                    self.row(),
                                                    key,
                                                );
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

    /// Check whether a key same as input can be found in version chain.
    /// This method is similar to read_row_mvcc() but only consider index key
    /// match logic.
    /// It is used by purge threads to correctly remove unused index entry.
    #[inline]
    pub fn is_any_version_matches_key(
        &self,
        schema: &TableSchema,
        key: &SelectKey,
        sts: TrxID,
    ) -> bool {
        match &*self.undo {
            None => {
                let row = self.row();
                return !row.is_deleted() && !row.is_key_different(schema, key);
            }
            Some(undo_head) => {
                let ts = undo_head.status.ts();
                if trx_is_committed(ts) {
                    if sts > ts {
                        let row = self.row();
                        return !row.is_deleted() && !row.is_key_different(schema, key);
                    }
                }
                // page data is invisible, we have to backtrace version chain.
                let row = self.row();
                let vals = row.clone_index_vals(schema, key.index_no);
                let mvcc_key = SelectKey::new(key.index_no, vals);
                let deleted = row.is_deleted();
                if !deleted && &mvcc_key == key {
                    return true;
                }
                match undo_head.entry.as_ref() {
                    None => unreachable!("next version is missing"),
                    Some(entry) => {
                        let mut entry = entry.clone();
                        let mapping: HashMap<usize, usize> = schema.indexes[key.index_no]
                            .keys
                            .iter()
                            .enumerate()
                            .map(|(key_no, key)| (key.user_col_idx as usize, key_no))
                            .collect();
                        let mut ver = KeyVersion {
                            deleted,
                            mvcc_key,
                            mapping,
                        };
                        // traverse version chain
                        loop {
                            match &entry.as_ref().kind {
                                RowUndoKind::Insert => {
                                    debug_assert!(!ver.deleted);
                                    ver.deleted = true;
                                }
                                RowUndoKind::Update(undo_cols) => {
                                    debug_assert!(!ver.deleted);
                                    ver.undo_update(undo_cols);
                                }
                                RowUndoKind::Delete => {
                                    debug_assert!(ver.deleted);
                                    ver.deleted = false;
                                }
                                RowUndoKind::Move(del) => {
                                    ver.deleted = *del;
                                }
                            }
                            // here we check if current version matches input key
                            if !ver.deleted && &ver.mvcc_key == key {
                                return true;
                            }
                            // check whether we should go to next version
                            match entry.as_ref().find_next_version(key) {
                                None => {
                                    return false;
                                }
                                Some(next) => {
                                    match next.status {
                                        NextRowUndoStatus::SameAsPrev => {
                                            let next_entry = next.entry.clone();
                                            entry = next_entry; // still invisible.
                                        }
                                        NextRowUndoStatus::CTS(cts) => {
                                            if sts > cts {
                                                // current version is visible
                                                return false;
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
    user_key_idx_map: HashMap<usize, usize>,
    // if undo key is none, key is stored in undo_vals.
    undo_key: Option<SelectKey>,
    undo_vals: BTreeMap<usize, Val>,
}

impl RowVersion {
    #[inline]
    fn undo_update(&mut self, undo: &[UndoCol]) {
        // undo update
        for u in undo {
            if self.read_set.contains(&u.idx) {
                self.undo_vals.insert(u.idx, u.val.clone());
            }
            if let Some(undo_key) = self.undo_key.as_mut() {
                if let Some(pos) = self.user_key_idx_map.get(&u.idx) {
                    undo_key.vals[*pos] = u.val.clone();
                }
            }
        }
    }

    #[inline]
    fn get_visible_vals(
        mut self,
        schema: &TableSchema,
        row: Row<'_>,
        search_key: &SelectKey,
    ) -> ReadRow {
        if let Some(undo_key) = self.undo_key {
            // compare key directly
            if search_key
                .vals
                .iter()
                .zip(&undo_key.vals)
                .any(|(v1, v2)| v1 != v2)
            {
                return ReadRow::InvalidIndex;
            }
        } else {
            // compare key using read set and latest row page
            let index_schema = &schema.indexes[search_key.index_no];
            let key_different = search_key.vals.iter().enumerate().any(|(pos, search_val)| {
                let user_col_idx = index_schema.keys[pos].user_col_idx as usize;
                if let Some(undo_val) = self.undo_vals.get(&user_col_idx) {
                    search_val != undo_val
                } else {
                    row.is_user_different(schema, user_col_idx, search_val)
                }
            });
            if key_different {
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

struct KeyVersion {
    deleted: bool,
    mvcc_key: SelectKey,
    // mapping of column number to key number
    mapping: HashMap<usize, usize>,
}

impl KeyVersion {
    #[inline]
    fn undo_update(&mut self, undo: &[UndoCol]) {
        for u in undo {
            if let Some(key_no) = self.mapping.get(&u.idx) {
                self.mvcc_key.vals[*key_no] = u.val.clone();
            }
        }
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
    pub fn update_row(&self, schema: &TableSchema, user_cols: &[UpdateCol]) -> UpdateRow {
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

    /// Build the main undo chain.
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
            debug_assert!(new_entry.next.is_empty());
            new_entry.next.push(NextRowUndo {
                status: old_cts.undo_status(),
                branch: RowUndoBranch::Main,
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
                match owned_entry.remove_next_main_version() {
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
        let (undo_map, page) = self.undo_map_and_page();
        let undo = undo_map.read(row_idx);
        RowReadAccess {
            page,
            row_idx,
            undo,
        }
    }

    #[inline]
    pub fn read_row_by_id(&self, row_id: RowID) -> RowReadAccess<'_> {
        let row_idx = self.page().row_idx(row_id);
        self.read_row(row_idx)
    }

    #[inline]
    pub fn write_row(&self, row_idx: usize) -> RowWriteAccess<'_> {
        let (undo_map, page) = self.undo_map_and_page();
        let undo = undo_map.write(row_idx);
        RowWriteAccess {
            page,
            row_idx,
            undo,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RowLatestStatus {
    NotFound,
    Committed(TrxID, bool),
    Uncommitted,
}
