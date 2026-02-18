use crate::buffer::frame::FrameContext;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{TableID, TableMetadata};
use crate::row::ops::{ReadRow, SelectKey, UndoCol, UndoVal, UpdateCol, UpdateRow};
use crate::row::{Row, RowID, RowMut, RowPage, RowRead};
use crate::stmt::Statement;
use crate::trx::recover::RecoverMap;
use crate::trx::undo::{
    IndexBranch, MainBranch, NextRowUndo, OwnedRowUndo, RowUndoHead, RowUndoKind, RowUndoRef,
    UndoStatus,
};
use crate::trx::ver_map::{RowPageState, RowVersionReadGuard, RowVersionWriteGuard};
use crate::trx::{ActiveTrx, SharedTrxStatus, TrxID, trx_is_committed};
use crate::value::Val;
use event_listener::EventListener;
use parking_lot::RwLockReadGuard;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;
use std::sync::Arc;

/// Read row with latest or visible version.
pub struct RowReadAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    state: RowReadState<'a>,
}

impl<'a> RowReadAccess<'a> {
    /// Acquire read latch for single row with offset.
    #[inline]
    pub fn new(page: &'a RowPage, ctx: &'a FrameContext, row_idx: usize) -> Self {
        let state = RowReadState::from_ctx(ctx, row_idx);
        RowReadAccess {
            page,
            row_idx,
            state,
        }
    }

    #[inline]
    pub fn row(&self) -> Row<'_> {
        self.page.row(self.row_idx)
    }

    #[inline]
    pub fn any_old_version_exists(&self) -> bool {
        match &self.state {
            RowReadState::RowVer(g) => g.is_some(),
            RowReadState::Recover(_) => false,
        }
    }

    #[inline]
    pub fn ts(&self) -> Option<TrxID> {
        match &self.state {
            RowReadState::RowVer(head) => head.as_ref().map(|h| h.ts()),
            RowReadState::Recover(rec) => rec.at(self.row_idx),
        }
    }

    #[allow(clippy::borrowed_box)]
    #[inline]
    pub fn undo_head(&self) -> Option<&Box<RowUndoHead>> {
        match &self.state {
            RowReadState::RowVer(guard) => guard.as_ref(),
            RowReadState::Recover(_) => None,
        }
    }

    /// Returns first undo entry on main branch of the chain.
    #[inline]
    pub fn first_undo_entry(&self) -> Option<RowUndoRef> {
        match &self.state {
            RowReadState::RowVer(guard) => guard.as_ref().map(|head| head.next.main.entry.clone()),
            RowReadState::Recover(_) => None,
        }
    }

    #[inline]
    pub fn read_row_latest(
        &self,
        metadata: &TableMetadata,
        read_set: &[usize],
        key: Option<&SelectKey>,
    ) -> ReadRow {
        let row = self.row();
        // latest version in row page.
        if row.is_deleted() {
            return ReadRow::NotFound;
        }
        if let Some(key) = key
            && row.is_key_different(metadata, key)
        {
            return ReadRow::InvalidIndex;
        }
        let vals = row.vals_for_read_set(metadata, read_set);
        ReadRow::Ok(vals)
    }

    #[inline]
    pub fn read_row_mvcc(
        &self,
        trx: &ActiveTrx,
        metadata: &TableMetadata,
        read_set: &[usize],
        key: Option<&SelectKey>,
    ) -> ReadRow {
        match &self.state {
            RowReadState::RowVer(undo) => match &**undo {
                None => self.read_row_latest(metadata, read_set, key),
                Some(undo_head) => {
                    // At this point, we already wait for preparation of commit is done.
                    // So we only have two cases: uncommitted, and committed.
                    let ts = undo_head.ts();
                    if trx_is_committed(ts) {
                        if trx.sts > ts {
                            // This version is visible
                            return self.read_row_latest(metadata, read_set, key);
                        } // Otherwise, go to next version
                    } else {
                        let trx_id = trx.trx_id();
                        if trx_id == ts {
                            // Self update, see the latest version
                            return self.read_row_latest(metadata, read_set, key);
                        } // Otherwise, go to next version
                    }
                    // Page data is invisible, we have to backtrace version chain
                    // Prepare visitor of version chain.
                    let mut next = &undo_head.next;
                    let read_set: BTreeSet<usize> = read_set.iter().cloned().collect();
                    let key_tracker = if let Some(key) = key {
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
                        Some(IndexKeyTracker {
                            user_key_idx_map,
                            undo_key,
                        })
                    } else {
                        None
                    };
                    let mut ver = RowVersion {
                        deleted: self.row().is_deleted(),
                        read_set,
                        key_tracker,
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
                                ver.deleted = false; // delete is not seen, mark as not deleted.
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

    /// find one version of the old row that matches given key.
    /// There are several scenarios:
    /// 1. The old row is being modified.
    ///    a) Modifier is another transaction, throws conflict error.
    ///    b) Modifier is self, continue.
    /// 2. The old row matches key and is not deleted. Just throw dup-key error.
    /// 3. The old row matches key and is deleted.
    ///    a) DELETE undo entry does not exists, mean no transaction can see the
    ///    non-deleted version, so we just return none.
    ///    b) DELELTE undo entry still exists, means some transaction still has
    ///    access to non-deleted version, so we record new-to-old modifications
    ///    and link new row to DELETE entry.
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
        let undo = match &self.state {
            RowReadState::Recover(_) => unreachable!(),
            RowReadState::RowVer(undo) => undo,
        };

        match &**undo {
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
                // This is safe because we already lock the row and
                // prevent GC thread from pruging old versions.
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
        if !row.is_key_different(metadata, key) && !deleted {
            return false; // matched key found in page.
        }
        // Page data does not match, check version chain.
        match &self.state {
            RowReadState::Recover(_) => false,
            RowReadState::RowVer(undo) => match &**undo {
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
    RowVer(RowVersionReadGuard<'a>),
    Recover(&'a RecoverMap),
}

impl<'a> RowReadState<'a> {
    #[inline]
    fn from_ctx(ctx: &'a FrameContext, row_idx: usize) -> Self {
        match ctx {
            FrameContext::RowVerMap(ver) => RowReadState::RowVer(ver.read_latch(row_idx)),
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

impl<'a> ReadAllRows<'a> {
    #[inline]
    pub fn new(page: &'a RowPage, ctx: &'a FrameContext) -> Self {
        let end_idx = page.header.row_count();
        ReadAllRows {
            ctx,
            page,
            start_idx: 0,
            end_idx,
        }
    }

    #[inline]
    pub fn metadata(&self) -> Option<&TableMetadata> {
        self.ctx.row_ver().map(|m| &*m.metadata)
    }
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
        Some(RowReadAccess::new(self.page, self.ctx, row_idx))
    }
}

// Version of current row.
struct RowVersion {
    deleted: bool,
    read_set: BTreeSet<usize>,
    key_tracker: Option<IndexKeyTracker>,
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
            if let Some(tracker) = self.key_tracker.as_mut()
                && let Some(undo_key) = tracker.undo_key.as_mut()
                && let Some(pos) = tracker.user_key_idx_map.get(&u.idx())
            {
                undo_key.vals[*pos] = u.val().clone();
            }
        }
    }

    #[inline]
    fn get_visible_vals(
        mut self,
        metadata: &TableMetadata,
        row: Row<'_>,
        search_key: Option<&SelectKey>,
    ) -> ReadRow {
        if let Some(search_key) = search_key {
            // If search key is provided, we need to validate key before
            // returning visible values.
            if let Some(tracker) = self.key_tracker.as_ref()
                && let Some(undo_key) = tracker.undo_key.as_ref()
            {
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
        }
        let mut vals = Vec::with_capacity(self.read_set.len());
        for user_col_idx in &self.read_set {
            if let Some(v) = self.undo_vals.remove(user_col_idx) {
                vals.push(v);
            } else {
                vals.push(row.val(metadata, *user_col_idx))
            }
        }
        ReadRow::Ok(vals)
    }
}

struct IndexKeyTracker {
    user_key_idx_map: HashMap<usize, usize>,
    undo_key: Option<SelectKey>,
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
    guard: RowVersionWriteGuard<'a>,
    _state_guard: RwLockReadGuard<'a, RowPageState>,
}

impl<'a> RowWriteAccess<'a> {
    #[inline]
    pub fn new(
        page: &'a RowPage,
        ctx: &'a FrameContext,
        row_idx: usize,
        sts: Option<TrxID>,
        ins_or_update: bool,
    ) -> Self {
        let ver_map = ctx
            .row_ver()
            .expect("write_row not supported without undo map");
        let state_guard = ver_map.read_state();
        Self::new_with_state_guard(page, ctx, row_idx, sts, ins_or_update, state_guard)
    }

    #[inline]
    pub fn new_with_state_guard(
        page: &'a RowPage,
        ctx: &'a FrameContext,
        row_idx: usize,
        sts: Option<TrxID>,
        ins_or_update: bool,
        state_guard: RwLockReadGuard<'a, RowPageState>,
    ) -> Self {
        let ver_map = ctx
            .row_ver()
            .expect("write_row not supported without undo map");
        let guard = ver_map.write_latch(row_idx, sts, ins_or_update);
        RowWriteAccess {
            page,
            row_idx,
            guard,
            _state_guard: state_guard,
        }
    }

    #[inline]
    pub fn row(&self) -> Row<'a> {
        self.page.row(self.row_idx)
    }

    #[inline]
    pub fn row_mut(&self, var_offset: usize, var_end: usize) -> RowMut<'_> {
        self.page.row_mut(self.row_idx, var_offset, var_end)
    }

    #[inline]
    pub fn page_state(&self) -> RowPageState {
        *self._state_guard
    }

    #[inline]
    pub fn delete_row(&mut self) {
        let res = self.page.set_deleted(self.row_idx, true);
        debug_assert!(res);
        self.page.inc_approx_deleted();
    }

    #[inline]
    pub fn row_and_undo_mut(&mut self) -> (Row<'_>, &mut Option<Box<RowUndoHead>>) {
        let row = self.page.row(self.row_idx);
        (row, &mut *self.guard)
    }

    #[inline]
    pub fn update_row(
        &self,
        metadata: &TableMetadata,
        cols: &[UpdateCol],
        frozen: bool,
    ) -> UpdateRow<'_> {
        if frozen {
            let old_row = self.row().vals_with_var_offsets(metadata);
            return UpdateRow::NoFreeSpaceOrFrozen(old_row);
        }
        let var_len = self.row().var_len_for_update(cols);
        if var_len == 0 {
            // fast path, no change on var-length column.
            let offset = self.page.header.var_field_offset();
            let row = self.row_mut(offset, offset);
            return UpdateRow::Ok(row);
        }
        match self.page.request_free_space(var_len) {
            None => {
                let old_row = self.row().vals_with_var_offsets(metadata);
                UpdateRow::NoFreeSpaceOrFrozen(old_row)
            }
            Some(offset) => {
                let row = self.row_mut(offset, offset + var_len);
                UpdateRow::Ok(row)
            }
        }
    }

    #[inline]
    fn add_undo_head(&mut self, status: Arc<SharedTrxStatus>, entry: RowUndoRef) {
        debug_assert!(self.guard.is_none());
        let head = RowUndoHead::new(status, entry);
        self.guard.replace(Box::new(head));
    }

    #[inline]
    fn remove_undo_head(&mut self) {
        self.guard.take();
    }

    /// Add a Lock undo entry as a transaction-level logical row lock.
    #[inline]
    pub fn lock_undo(
        &mut self,
        stmt: &mut Statement,
        metadata: &TableMetadata,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
        key: Option<&SelectKey>,
    ) -> LockUndo {
        let row = self.page.row(self.row_idx);
        match &mut *self.guard {
            None => {
                let entry = OwnedRowUndo::new(table_id, Some(page_id), row_id, RowUndoKind::Lock);
                self.add_undo_head(stmt.trx.status(), entry.leak());
                stmt.row_undo.push(entry);
                LockUndo::Ok
            }
            Some(undo_head) => {
                if stmt.trx.is_same_trx(undo_head) {
                    let mut entry =
                        OwnedRowUndo::new(table_id, Some(page_id), row_id, RowUndoKind::Lock);
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
                    if let Some(key) = key
                        && row.is_key_different(metadata, key)
                    {
                        return LockUndo::InvalidIndex;
                    }
                    let mut entry =
                        OwnedRowUndo::new(table_id, Some(page_id), row_id, RowUndoKind::Lock);
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

    #[inline]
    pub fn link_for_unique_index(
        &mut self,
        key: SelectKey,
        cts: TrxID,
        entry: RowUndoRef,
        undo_vals: Vec<UpdateCol>,
    ) {
        let undo_head = self.guard.as_mut().expect("undo head");
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
        match &mut *self.guard {
            None => (),
            Some(undo_head) => {
                if undo_head.purge_ts >= min_active_sts {
                    // Another thread already prune this version chain.
                    return;
                }
                undo_head.purge_ts = min_active_sts;

                // Check whether the head can be purged.
                let ts = undo_head.ts();
                if trx_is_committed(ts) && ts < min_active_sts {
                    // First entry is too old. That means page data is globally visible.
                    // So we can remove the entire undo head.
                    self.guard.take();
                    return;
                }
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
    pub fn rollback_first_undo(&mut self, metadata: &TableMetadata, mut owned_entry: OwnedRowUndo) {
        let head = self.guard.as_mut().expect("undo head");
        let entry = &mut head.next.main.entry;
        debug_assert!({
            let input_ref = &*owned_entry;
            std::ptr::addr_eq(entry.as_ref(), input_ref)
        });
        // rollback row data
        match &owned_entry.kind {
            RowUndoKind::Lock => (), // do nothing.
            RowUndoKind::Insert => {
                let res = self.page.set_deleted(self.row_idx, true);
                debug_assert!(res);
                self.page.inc_approx_deleted();
            }
            RowUndoKind::Delete => {
                let res = self.page.set_deleted(self.row_idx, false);
                debug_assert!(res);
                self.page.dec_approx_deleted();
            }
            RowUndoKind::Update(undo_cols) => {
                // Here we try to rollback changes on this page
                // and prefer to reuse the space occupied by old value.
                for uc in undo_cols {
                    self.page.update_col(
                        metadata,
                        self.row_idx,
                        uc.idx,
                        &uc.val,
                        uc.var_offset.unwrap_or(0) as usize,
                        true,
                    );
                }
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
                head.next = next;
            }
        }
    }

    #[inline]
    pub fn enable_ins_or_update(&mut self) {
        self.guard.enable_ins_or_update();
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

impl LockUndo {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, LockUndo::Ok)
    }
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
    // row page is transitioning, caller should retry.
    RetryInTransition,
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
