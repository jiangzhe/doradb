use crate::buffer::frame::FrameContext;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::PageID;
use crate::catalog::TableMetadata;
use crate::row::ops::{ReadRow, SelectKey, UndoCol, UndoVal, UpdateCol, UpdateRow};
use crate::row::{Row, RowID, RowMut, RowPage, RowRead};
use crate::stmt::Statement;
use crate::table::TableID;
use crate::trx::recover::RecoverMap;
use crate::trx::undo::{
    IndexBranch, MainBranch, NextRowUndo, OwnedRowUndo, RowUndoHead, RowUndoKind, RowUndoRef,
    UndoMap, UndoStatus,
};
use crate::trx::{trx_is_committed, ActiveTrx, SharedTrxStatus, TrxID};
use crate::value::Val;
use event_listener::EventListener;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct RowReadAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    // undo: RwLockReadGuard<'a, Option<Box<RowUndoHead>>>,
    state: RowReadState<'a>,
}

impl<'a> RowReadAccess<'a> {
    #[inline]
    pub fn new(page: &'a RowPage, row_idx: usize, state: RowReadState<'a>) -> Self {
        RowReadAccess {
            page,
            row_idx,
            state,
        }
    }

    #[inline]
    pub fn row(&self) -> Row {
        self.page.row(self.row_idx)
    }

    #[inline]
    pub fn any_old_version_exists(&self) -> bool {
        match &self.state {
            RowReadState::Undo(g) => g.is_some(),
            RowReadState::Recover(_) => false,
        }
    }

    #[inline]
    pub fn ts(&self) -> Option<TrxID> {
        match &self.state {
            RowReadState::Undo(head) => head.as_ref().map(|h| h.ts()),
            RowReadState::Recover(rec) => rec.at(self.row_idx),
        }
    }

    #[inline]
    pub fn read_row_latest(
        &self,
        metadata: &TableMetadata,
        user_read_set: &[usize],
        key: Option<&SelectKey>,
    ) -> ReadRow {
        let row = self.row();
        // latest version in row page.
        if row.is_deleted() {
            return ReadRow::NotFound;
        }
        if let Some(key) = key {
            if row.is_key_different(metadata, key) {
                return ReadRow::InvalidIndex;
            }
        }
        let vals = row.clone_vals_for_read_set(metadata, user_read_set);
        ReadRow::Ok(vals)
    }

    #[inline]
    pub fn read_row_mvcc(
        &self,
        trx: &ActiveTrx,
        metadata: &TableMetadata,
        user_read_set: &[usize],
        key: &SelectKey,
    ) -> ReadRow {
        match &self.state {
            RowReadState::Undo(undo) => match &**undo {
                None => self.read_row_latest(metadata, user_read_set, Some(key)),
                Some(undo_head) => {
                    // At this point, we already wait for preparation of commit is done.
                    // So we only have two cases: uncommitted, and committed.
                    let ts = undo_head.ts();
                    if trx_is_committed(ts) {
                        if trx.sts > ts {
                            // This version is visible
                            return self.read_row_latest(metadata, user_read_set, Some(key));
                        } // Otherwise, go to next version
                    } else {
                        let trx_id = trx.trx_id();
                        if trx_id == ts {
                            // Self update, see the latest version
                            return self.read_row_latest(metadata, user_read_set, Some(key));
                        } // Otherwise, go to next version
                    }
                    // Page data is invisible, we have to backtrace version chain
                    // Prepare visitor of version chain.
                    let mut next = &undo_head.next;
                    let read_set: BTreeSet<usize> = user_read_set.iter().cloned().collect();
                    let index_spec = &metadata.index_specs[key.index_no];
                    let user_key_idx_map: HashMap<usize, usize> = index_spec
                        .index_cols
                        .iter()
                        .enumerate()
                        .map(|(key_pos, key)| (key.col_no as usize, key_pos))
                        .collect();
                    let read_set_contains_key = user_key_idx_map
                        .keys()
                        .all(|user_key_idx| read_set.contains(user_key_idx));
                    let undo_key = if read_set_contains_key {
                        None
                    } else {
                        let vals = self.row().clone_index_vals(metadata, key.index_no);
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
                    loop {
                        let entry;
                        // First we check index branch with matched key.
                        if let Some(ib) = next.index_branch(key) {
                            // Because index branch jump to version of another row,
                            // we should first apply the differences between current
                            // row and that row.
                            ver.undo_update(&ib.undo_vals);
                            // Index branch only contains non-deleted version.
                            // So delete flag is not used.
                            debug_assert!(!ver.deleted);
                            if trx.sts > ib.cts {
                                // current version is visible
                                return ver.get_visible_vals(metadata, self.row(), key);
                            }
                            entry = ib.entry.as_ref();
                        } else {
                            // Key not match, go to main branch
                            entry = next.main.entry.as_ref();
                        }
                        // visit undo log
                        match &entry.kind {
                            RowUndoKind::Lock => (), // do nothing.
                            RowUndoKind::Insert => {
                                debug_assert!(!ver.deleted);
                                ver.deleted = true; // insert is not seen, mark as deleted
                            }
                            RowUndoKind::Update(undo_vals) => {
                                debug_assert!(!ver.deleted);
                                ver.undo_update(undo_vals);
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
                        match entry.next.as_ref() {
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
                                return ver.get_visible_vals(metadata, self.row(), key);
                            }
                            Some(nx) => {
                                let ts = nx.main.status.ts();
                                if trx.sts > ts {
                                    // current version is visible
                                    if ver.deleted {
                                        return ReadRow::NotFound;
                                    }
                                    return ver.get_visible_vals(metadata, self.row(), key);
                                }
                                next = nx; // still invisible
                            }
                        }
                    }
                }
            },
            RowReadState::Recover(_) => {
                // no mvcc support for recovery mode.
                unreachable!("no mvcc support for recovery mode")
            }
        }
    }

    /// Check whether a same key can be found in version chain.
    /// This method is similar to read_row_mvcc() but some differences:
    /// 1. Uncommitted versions are also need to be considered.
    /// 2. Only need to find the first version which has same key.
    /// 3. There is no STS restriction.
    ///    So we may need to go to the oldest version.
    /// 4. Only consider main branch.
    ///
    /// This method is used by purge threads to correctly remove unnecessary index entry.
    #[inline]
    pub fn any_version_matches_key(&self, metadata: &TableMetadata, key: &SelectKey) -> bool {
        // Check page data first.
        let row = self.row();
        let deleted = row.is_deleted();
        if !row.is_key_different(&metadata, key) && !deleted {
            return false; // matched key found in page.
        }
        // Page data does not match, check version chain.
        match &self.state {
            RowReadState::Recover(_) => false,
            RowReadState::Undo(undo) => match &**undo {
                None => false,
                Some(undo_head) => {
                    // Page data is already checked, we can traverse version
                    // chain now.
                    let mut entry = undo_head.next.main.entry.as_ref();
                    let vals = row.clone_index_vals(metadata, key.index_no);
                    let mvcc_key = SelectKey::new(key.index_no, vals);
                    let mapping: HashMap<usize, usize> = metadata.index_specs[key.index_no]
                        .index_cols
                        .iter()
                        .enumerate()
                        .map(|(key_no, key)| (key.col_no as usize, key_no))
                        .collect();
                    let mut ver = KeyVersion {
                        deleted,
                        mvcc_key,
                        mapping,
                    };
                    // Traverse version chain until oldest version.
                    loop {
                        match &entry.kind {
                            RowUndoKind::Lock => (), // do nothing.
                            RowUndoKind::Insert => {
                                debug_assert!(!ver.deleted);
                                ver.deleted = true;
                            }
                            RowUndoKind::Update(undo_vals) => {
                                debug_assert!(!ver.deleted);
                                ver.undo_update(undo_vals);
                            }
                            RowUndoKind::Delete => {
                                debug_assert!(ver.deleted);
                                ver.deleted = false;
                            }
                            RowUndoKind::Move(del) => {
                                ver.deleted = *del;
                            }
                        }
                        // Here we check if current version matches input key
                        if !ver.deleted && &ver.mvcc_key == key {
                            return true;
                        }
                        // We only need to go through main branch, because Index
                        // branch won't have different key than those in main
                        // branch.
                        match entry.next.as_ref() {
                            None => {
                                return false;
                            }
                            Some(next) => {
                                entry = next.main.entry.as_ref();
                            }
                        }
                    }
                }
            },
        }
    }
}

pub enum RowReadState<'a> {
    Undo(RwLockReadGuard<'a, Option<Box<RowUndoHead>>>),
    Recover(&'a RecoverMap),
}

impl<'a> RowReadState<'a> {
    #[inline]
    fn from_ctx(ctx: &'a FrameContext, row_idx: usize) -> Self {
        match ctx {
            FrameContext::UndoMap(undo) => RowReadState::Undo(undo.read(row_idx)),
            FrameContext::RecoverMap(rec) => RowReadState::Recover(rec),
        }
    }
}

pub struct ReadAllRows<'a> {
    ctx: &'a FrameContext,
    page: &'a RowPage,
    start_idx: usize,
    end_idx: usize,
}

impl<'a> Iterator for ReadAllRows<'a> {
    type Item = RowReadAccess<'a>;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let row_idx = self.start_idx;
        if row_idx >= self.end_idx {
            return None;
        }
        self.start_idx += 1;
        Some(RowReadAccess::new(
            self.page,
            row_idx,
            RowReadState::from_ctx(self.ctx, row_idx),
        ))
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
    fn undo_update<T: UndoVal>(&mut self, undo_vals: &[T]) {
        // undo update
        for u in undo_vals {
            if self.read_set.contains(&u.idx()) {
                self.undo_vals.insert(u.idx(), u.val().clone());
            }
            if let Some(undo_key) = self.undo_key.as_mut() {
                if let Some(pos) = self.user_key_idx_map.get(&u.idx()) {
                    undo_key.vals[*pos] = u.val().clone();
                }
            }
        }
    }

    #[inline]
    fn get_visible_vals(
        mut self,
        metadata: &TableMetadata,
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
            let index_spec = &metadata.index_specs[search_key.index_no];
            let key_different = search_key.vals.iter().enumerate().any(|(pos, search_val)| {
                let user_col_idx = index_spec.index_cols[pos].col_no as usize;
                if let Some(undo_val) = self.undo_vals.get(&user_col_idx) {
                    search_val != undo_val
                } else {
                    row.is_different(metadata, user_col_idx, search_val)
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
                vals.push(row.clone_val(metadata, *user_col_idx))
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
    undo_map: &'a UndoMap,
    row_idx: usize,
    undo: RwLockWriteGuard<'a, Option<Box<RowUndoHead>>>,
}

impl<'a> RowWriteAccess<'a> {
    #[inline]
    pub fn new(
        page: &'a RowPage,
        undo_map: &'a UndoMap,
        row_idx: usize,
        undo: RwLockWriteGuard<'a, Option<Box<RowUndoHead>>>,
    ) -> Self {
        undo_map.version.fetch_add(1, Ordering::Release);
        RowWriteAccess {
            page,
            undo_map,
            row_idx,
            undo,
        }
    }

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
    pub fn row_and_undo_mut(&mut self) -> (Row, &mut Option<Box<RowUndoHead>>) {
        let row = self.page.row(self.row_idx);
        (row, &mut *self.undo)
    }

    #[inline]
    pub fn update_row(&self, metadata: &TableMetadata, cols: &[UpdateCol]) -> UpdateRow {
        let var_len = self.row().var_len_for_update(cols);
        match self.page.request_free_space(var_len) {
            None => {
                let old_row = self.row().clone_vals_with_var_offsets(metadata);
                UpdateRow::NoFreeSpace(old_row)
            }
            Some(offset) => {
                let row = self.row_mut(offset, offset + var_len);
                UpdateRow::Ok(row)
            }
        }
    }

    #[inline]
    pub fn undo_head(&self) -> &Option<Box<RowUndoHead>> {
        &*self.undo
    }

    /// Returns first undo entry on main branch of the chain.
    #[inline]
    pub fn first_undo_entry(&self) -> Option<RowUndoRef> {
        self.undo.as_ref().map(|head| head.next.main.entry.clone())
    }

    #[inline]
    fn add_undo_head(&mut self, status: Arc<SharedTrxStatus>, entry: RowUndoRef) {
        debug_assert!(self.undo.is_none());
        self.undo_map
            .maybe_invisible
            .fetch_add(1, Ordering::Release);
        let head = RowUndoHead::new(status, entry);
        self.undo.replace(Box::new(head));
    }

    #[inline]
    fn remove_undo_head(&mut self) {
        self.undo_map
            .maybe_invisible
            .fetch_sub(1, Ordering::Release);
        self.undo.take();
    }

    /// Add a Lock undo entry as a transaction-level logical row lock.
    #[inline]
    pub fn lock_undo(
        &mut self,
        stmt: &mut Statement,
        metadata: &TableMetadata,
        table_id: TableID,
        page_id: PageID,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockUndo {
        let row = self.page.row(self.row_idx);
        match &mut *self.undo {
            None => {
                let entry = OwnedRowUndo::new(table_id, page_id, row_id, RowUndoKind::Lock);
                self.add_undo_head(stmt.trx.status(), entry.leak());
                stmt.row_undo.push(entry);
                LockUndo::Ok
            }
            Some(undo_head) => {
                if stmt.trx.is_same_trx(undo_head) {
                    let mut entry = OwnedRowUndo::new(table_id, page_id, row_id, RowUndoKind::Lock);
                    let new_next = NextRowUndo::new(MainBranch {
                        entry: entry.leak(),
                        status: UndoStatus::Ref(stmt.trx.status()),
                    });
                    let old_next = mem::replace(&mut undo_head.next, new_next);
                    entry.next = Some(old_next);
                    stmt.row_undo.push(entry);
                    return LockUndo::Ok;
                }
                let old_cts = undo_head.ts();
                if trx_is_committed(old_cts) {
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
                    if let Some(key) = key {
                        if row.is_key_different(metadata, key) {
                            return LockUndo::InvalidIndex;
                        }
                    }
                    let mut entry = OwnedRowUndo::new(table_id, page_id, row_id, RowUndoKind::Lock);
                    let new_next = NextRowUndo::new(MainBranch {
                        entry: entry.leak(),
                        status: UndoStatus::Ref(stmt.trx.status()),
                    });
                    let old_next = mem::replace(&mut undo_head.next, new_next);
                    entry.next = Some(old_next);
                    stmt.row_undo.push(entry);
                    return LockUndo::Ok;
                }
                // Transaction is not committed, we need to check if it's prepared for commit.
                // If yes, wait for it to finish commit.
                // Otherwise, throw error.
                if !undo_head.preparing() {
                    return LockUndo::WriteConflict;
                }
                LockUndo::Preparing(undo_head.prepare_listener())
            }
        }
    }

    /// find one version of the old row that matches given key.
    /// There are several scenarios:
    /// 1. The old row is being modified.
    ///    a) Modifier is another transaction, throws conflict error.
    ///    b) Modifier is self, continue.
    /// 2. The old row matches key and is not deleted. Just throw dup-key error.
    /// 3. The old row matches key and is deleted.
    ///    a) DELETE undo entry does not exists, mean no transaction can see the
    ///       non-deleted version, so we just return none.
    ///    b) DELELTE undo entry still exists, means some transaction still has
    ///       access to non-deleted version, so we record new-to-old modifications
    ///       and link new row to DELETE entry.
    /// 4. The old row does not match key and no old version with same key found.
    ///    Return none.
    /// 5. The old row does not match key but one old version with same key found.
    ///    Add record modifications and then link new row to that specific entry.
    #[inline]
    pub fn find_old_version_for_unique_key(
        &self,
        metadata: &TableMetadata,
        key: &SelectKey,
        trx: &ActiveTrx,
    ) -> FindOldVersion {
        match &*self.undo {
            None => {
                let row = self.row();
                if !row.is_key_different(metadata, key) {
                    if !row.is_deleted() {
                        // Scenario #2
                        return FindOldVersion::DuplicateKey;
                    }
                    // Scenario #3.a
                    return FindOldVersion::None;
                }
                // Scenario #4
                FindOldVersion::None
            }
            Some(undo_head) => {
                let ts = undo_head.ts();
                if !trx_is_committed(ts) && !trx.is_same_trx(undo_head) {
                    // Scenario #1.a
                    return FindOldVersion::WriteConflict;
                }
                let row = self.row();
                // Old row matches key.
                if !row.is_key_different(metadata, key) {
                    if !row.is_deleted() {
                        // Scenario #2
                        return FindOldVersion::DuplicateKey;
                    }
                    // Scenario #3.b
                    // The first undo entry must be DELETE as page data is deleted.
                    // No chance to be MOVE because, if index points to a row which
                    // is moved, this row must be in an ongoing transaction, not
                    // a committed transaction(if committed, index should point to
                    // the newest version).
                    debug_assert!(matches!(
                        undo_head.next.main.entry.as_ref().kind,
                        RowUndoKind::Delete
                    ));
                    // Collect old row to calculate delta for link.
                    let old_row = row.clone_vals(metadata);
                    return FindOldVersion::Ok(old_row, ts, undo_head.next.main.entry.clone());
                }
                // Old row does not match key.
                // Traverse version chain to find matched version.
                let mut entry = undo_head.next.main.entry.clone();
                let mut cts = ts;
                let mut deleted = row.is_deleted();
                let mut vals = row.clone_vals(metadata);
                // Traverse version chain until oldest version.
                loop {
                    match &entry.as_ref().kind {
                        RowUndoKind::Lock => (), // do nothing.
                        RowUndoKind::Insert => {
                            debug_assert!(!deleted);
                            deleted = true;
                        }
                        RowUndoKind::Update(undo_vals) => {
                            debug_assert!(!deleted);
                            for uc in undo_vals {
                                vals[uc.idx] = uc.val.clone();
                            }
                        }
                        RowUndoKind::Delete => {
                            debug_assert!(deleted);
                            deleted = false;
                        }
                        RowUndoKind::Move(del) => {
                            deleted = *del;
                        }
                    }
                    // Here we check if current version matches input key
                    if !deleted && metadata.match_key(key, &vals) {
                        return FindOldVersion::Ok(vals, cts, entry);
                    }
                    // We only need to go through main branch, because Index
                    // branch won't have different key than those in main
                    // branch.
                    match entry.as_ref().next.as_ref() {
                        None => {
                            return FindOldVersion::None;
                        }
                        Some(next) => {
                            cts = next.main.status.ts();
                            entry = next.main.entry.clone();
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn link_for_unique_index(
        &mut self,
        key: SelectKey,
        cts: TrxID,
        entry: RowUndoRef,
        undo_vals: Vec<UpdateCol>,
    ) {
        let undo_head = self.undo.as_mut().expect("undo head");
        undo_head.next.indexes.push(IndexBranch {
            key,
            cts,
            entry,
            undo_vals,
        })
    }

    /// Purge undo chain according to minimum active STS.
    /// This method only remove out-of-date versions from next list.
    /// The real deletion of undo log is performed later.
    #[inline]
    pub fn purge_undo_chain(&mut self, min_active_sts: TrxID) {
        match &mut *self.undo {
            None => return,
            Some(undo_head) => {
                if undo_head.purge_ts >= min_active_sts {
                    // Another thread already prune this version chain.
                    return;
                }
                undo_head.purge_ts = min_active_sts;

                // Check whether the head can be purged.
                let ts = undo_head.ts();
                if trx_is_committed(ts) {
                    if ts < min_active_sts {
                        // First entry is too old. That means page data is globally visible.
                        // So we can remove the entire undo head.
                        self.undo.take();
                        // Update maybe_invisible because we remove the entire version chain.
                        self.undo_map
                            .maybe_invisible
                            .fetch_sub(1, Ordering::Relaxed);
                        return;
                    } // first entry can not be purged.
                } // uncommitted entry cannot be purged.
                let mut entry = undo_head.next.main.entry.as_mut();
                loop {
                    let mut entry_next = mem::take(&mut entry.next);
                    if entry_next.is_none() {
                        return;
                    }
                    let next = entry_next.as_mut().unwrap();
                    // pruge main branch
                    if next.main.status.can_purge(min_active_sts) {
                        // main branch can be purged means index branches can also
                        // be purged, because index branches have smaller timestamps.
                        entry.next.take();
                        return;
                    }
                    // purge index branches
                    let mut idx = next.indexes.len();
                    // remove old links.
                    while idx > 0 {
                        idx -= 1;
                        if next.indexes[idx].cts < min_active_sts {
                            next.indexes.swap_remove(idx);
                        }
                    }
                    // update back
                    entry.next = entry_next;
                    // go to next version, which should be main branch.
                    entry = entry.next.as_mut().unwrap().main.entry.as_mut();
                }
            }
        }
    }

    /// Rollback first undo log in the chain.
    #[inline]
    pub fn rollback_first_undo(&mut self, mut owned_entry: OwnedRowUndo) {
        let head = self.undo.as_mut().expect("undo head");
        let entry = &mut head.next.main.entry;
        debug_assert!({
            let input_ref = &*owned_entry;
            std::ptr::addr_eq(entry.as_ref(), input_ref)
        });
        // rollback row data
        match &owned_entry.kind {
            RowUndoKind::Lock => (), // do nothing.
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
                            let (pv, _) = self
                                .page
                                .add_var(var.as_bytes(), uc.var_offset.unwrap_or(0) as usize);
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
                self.remove_undo_head();
            }
            Some(next) => {
                if let RowUndoKind::Move(_) = &next.main.entry.as_ref().kind {
                    // MOVE is undo entry of another row, can only follow UPDATE.
                    debug_assert!(matches!(owned_entry.kind, RowUndoKind::Update(_)));
                    // Out-of-place update inserts new row just like a insert operation
                    // so we should mark it as deleted.
                    self.page.set_deleted(self.row_idx, true);
                    self.remove_undo_head();
                    return;
                }
                head.next = next;
            }
        }
    }
}

impl<'a> Drop for RowWriteAccess<'a> {
    #[inline]
    fn drop(&mut self) {
        self.undo_map.version.fetch_add(1, Ordering::Release);
    }
}

impl PageSharedGuard<RowPage> {
    /// Acquire read latch for single row with offset.
    #[inline]
    pub fn read_row(&self, row_idx: usize) -> RowReadAccess<'_> {
        let (ctx, page) = self.ctx_and_page();
        RowReadAccess::new(page, row_idx, RowReadState::from_ctx(ctx, row_idx))
    }

    #[inline]
    pub fn read_all_rows(&self) -> ReadAllRows<'_> {
        let (ctx, page) = self.ctx_and_page();
        let end_idx = page.header.row_count();
        ReadAllRows {
            ctx,
            page,
            start_idx: 0,
            end_idx,
        }
    }

    /// Acquire read latch for single row with row id.
    #[inline]
    pub fn read_row_by_id(&self, row_id: RowID) -> RowReadAccess<'_> {
        let row_idx = self.page().row_idx(row_id);
        self.read_row(row_idx)
    }

    /// Acquire write latch for single row with offset.
    /// In recovery mode, this method is not invoked, because we
    /// hold exclusive page guard and directly change values on each row.
    #[inline]
    pub fn write_row(&self, row_idx: usize) -> RowWriteAccess<'_> {
        let (ctx, page) = self.ctx_and_page();
        let undo_map = ctx
            .undo()
            .expect("write_row not supported without undo map");
        let undo = undo_map.write(row_idx);
        RowWriteAccess::new(page, undo_map, row_idx, undo)
    }

    /// Acquire write latch for single row with row id.
    #[inline]
    pub fn write_row_by_id(&self, row_id: RowID) -> RowWriteAccess<'_> {
        let row_idx = self.page().row_idx(row_id);
        self.write_row(row_idx)
    }
}

#[derive(Debug, Clone)]
pub enum RowLatestStatus {
    NotFound,
    Committed(TrxID, bool),
    Uncommitted,
}

pub enum LockUndo {
    Ok,
    WriteConflict,
    InvalidIndex,
    // row is locked by a preparing transaction.
    Preparing(Option<EventListener>),
}

pub enum LockRowForWrite<'a> {
    // lock success, returns optional last commit timestamp.
    Ok(Option<RowWriteAccess<'a>>),
    // lock fail, there is another transaction modifying this row.
    WriteConflict,
    // row is invalid through index lookup.
    // this can happen when index entry is not garbage collected,
    // so some old key points to new version.
    InvalidIndex,
}

impl<'a> LockRowForWrite<'a> {
    #[inline]
    pub fn ok(self) -> Option<RowWriteAccess<'a>> {
        match self {
            LockRowForWrite::Ok(access) => access,
            _ => None,
        }
    }
}

pub enum FindOldVersion {
    Ok(Vec<Val>, TrxID, RowUndoRef),
    WriteConflict,
    DuplicateKey,
    None,
}
