use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::page::VersionedPageID;
use crate::catalog::{TableColumnLayout, TableMetadata};
use crate::id::{RowID, TableID, TrxID};
use crate::index::{BTreeKeyEncoder, IndexLookupCandidate};
use crate::map::FastHashMap;
use crate::recovery::RowRecoveryMap;
use crate::row::ops::{ReadRow, RowUpdateView, SelectKey, UndoCol, UndoVal, UpdateCol, UpdateRow};
use crate::row::{Row, RowMut, RowPage, RowRead};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{
    IndexBranch, IndexBranchTarget, MainBranch, NextRowUndo, OwnedRowUndo, RowUndoHead,
    RowUndoKind, RowUndoRef, UndoStatus,
};
use crate::trx::ver_map::{RowPageState, RowVersionMap, RowVersionReadGuard, RowVersionWriteGuard};
use crate::trx::{SharedTrxStatus, TrxContext, TrxRuntime, trx_is_committed};
use crate::value::Val;
use event_listener::EventListener;
use parking_lot::RwLockReadGuard;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Result of checking whether the latest physical row image can be modified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadLatestRow {
    /// The latest page image is readable by this transaction.
    Readable,
    /// The latest page image is deleted.
    NotFound,
    /// Another active transaction owns the latest page image.
    WriteConflict,
}

/// Read row with latest or visible version.
pub(crate) struct RowReadAccess<'a> {
    page: &'a RowPage,
    row_idx: usize,
    state: RowReadState<'a>,
}

impl<'a> RowReadAccess<'a> {
    /// Acquire read latch for single row with offset.
    #[inline]
    fn new(page_guard: &'a PageSharedGuard<RowPage>, row_idx: usize) -> Self {
        let state = RowReadState::from_guard(page_guard, row_idx);
        Self::from_state(page_guard.page(), row_idx, state)
    }

    #[inline]
    fn from_state(page: &'a RowPage, row_idx: usize, state: RowReadState<'a>) -> Self {
        RowReadAccess {
            page,
            row_idx,
            state,
        }
    }

    /// Returns the latest row image from the page.
    #[inline]
    pub(crate) fn row(&self) -> Row<'_> {
        self.page.row(self.row_idx)
    }

    /// Checks whether this transaction may modify the latest physical row image.
    ///
    /// Row modification is a current read: committed heads are read directly
    /// regardless of the transaction snapshot, and an active head owned by
    /// this transaction is allowed for read-your-own-write. Another active
    /// owner is a write conflict; this path never reconstructs an older image.
    #[inline]
    pub(crate) fn read_latest(&self, ctx: &TrxContext) -> ReadLatestRow {
        if let RowReadState::RowVer(undo) = &self.state
            && let Some(undo_head) = &**undo
            && !trx_is_committed(undo_head.ts())
            && !ctx.is_same_trx(undo_head)
        {
            return ReadLatestRow::WriteConflict;
        }
        if self.row().is_deleted() {
            ReadLatestRow::NotFound
        } else {
            ReadLatestRow::Readable
        }
    }

    /// Clones one column from the latest physical row image without a value vector.
    #[inline]
    pub(crate) fn read_latest_value(
        &self,
        column_layout: &TableColumnLayout,
        column_no: usize,
    ) -> Val {
        self.row().val(column_layout, column_no)
    }

    /// Returns the timestamp associated with this row read state, if present.
    #[inline]
    #[expect(dead_code, reason = "reserved row-state timestamp diagnostic")]
    pub(crate) fn ts(&self) -> Option<TrxID> {
        match &self.state {
            RowReadState::RowVer(head) => head.as_ref().map(|h| h.ts()),
            RowReadState::Recover(rec) => rec.at(self.row_idx),
        }
    }

    /// Returns the current undo head for hot-row MVCC reads.
    #[inline]
    pub(crate) fn undo_head(&self) -> Option<&RowUndoHead> {
        match &self.state {
            RowReadState::RowVer(guard) => guard.as_ref().map(|h| h.as_ref()),
            RowReadState::Recover(_) => None,
        }
    }

    /// Returns first undo entry on main branch of the chain.
    #[inline]
    pub(crate) fn first_undo_entry(&self) -> Option<RowUndoRef> {
        match &self.state {
            RowReadState::RowVer(guard) => guard.as_ref().map(|head| head.next.main.entry.clone()),
            RowReadState::Recover(_) => None,
        }
    }

    /// Reads the latest physical row image without walking MVCC undo.
    #[inline]
    pub(crate) fn read_row_latest(
        &self,
        metadata: &TableMetadata,
        read_set: &[usize],
        key: Option<(usize, &[Val])>,
    ) -> ReadRow {
        let row = self.row();
        // latest version in row page.
        if row.is_deleted() {
            return ReadRow::NotFound;
        }
        if let Some((index_no, key_vals)) = key {
            let Some(index_spec) = metadata.idx.index_spec(index_no) else {
                return ReadRow::InvalidIndex;
            };
            if row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
                return ReadRow::InvalidIndex;
            }
        }
        let vals = row.vals_for_read_set(metadata.col.as_ref(), read_set);
        ReadRow::Ok(vals)
    }

    /// Reads the latest row image and validates exact index candidate identity.
    #[inline]
    pub(crate) fn read_row_latest_index_candidate(
        &self,
        metadata: &TableMetadata,
        read_set: &[usize],
        recheck: &IndexCandidateRecheck<'_>,
    ) -> ReadRow {
        let row = self.row();
        if row.is_deleted() {
            return ReadRow::NotFound;
        }
        let Some(index_spec) = metadata.idx.index_spec(recheck.index_no) else {
            return ReadRow::InvalidIndex;
        };
        let key_vals = index_spec
            .cols
            .iter()
            .map(|key| row.val(metadata.col.as_ref(), key.col_no as usize))
            .collect::<Vec<_>>();
        if !recheck.matches_key(&key_vals) {
            return ReadRow::InvalidIndex;
        }
        let vals = row.vals_for_read_set(metadata.col.as_ref(), read_set);
        ReadRow::Ok(vals)
    }

    /// Reads the row version visible to the transaction context.
    #[inline]
    pub(crate) fn read_row_mvcc(
        &self,
        ctx: &TrxContext,
        metadata: &TableMetadata,
        read_set: &[usize],
        key: Option<(usize, &[Val])>,
    ) -> ReadRow {
        match &self.state {
            RowReadState::RowVer(undo) => match &**undo {
                None => self.read_row_latest(metadata, read_set, key),
                Some(undo_head) => {
                    // A hot row page stores the newest physical image, while
                    // the undo head stores the transaction status that decides
                    // whether this reader can use that image directly. Commit
                    // preparation has already been waited out before this
                    // read, so the head is either committed or still owned by
                    // an active transaction.
                    let ts = undo_head.ts();
                    if trx_is_committed(ts) {
                        if ctx.sts() > ts {
                            // The latest row-page image committed before this
                            // snapshot, so no undo traversal is needed.
                            return self.read_row_latest(metadata, read_set, key);
                        } // Otherwise, go to next version
                    } else {
                        let trx_id = ctx.trx_id();
                        if trx_id == ts {
                            // Read-your-own-write: the uncommitted page image
                            // belongs to this transaction.
                            return self.read_row_latest(metadata, read_set, key);
                        } // Otherwise, go to next version
                    }
                    // The page image is too new for this snapshot. Walk the
                    // hot undo chain and apply inverse operations until the
                    // visible version is reconstructed.
                    let mut next = &undo_head.next;
                    let read_set: BTreeSet<usize> = read_set.iter().copied().collect();
                    let key_tracker = if let Some((index_no, key_vals)) = key {
                        // Index lookups may route through a latest row whose
                        // current key no longer matches the lookup key. Track
                        // enough key columns to validate older reconstructed
                        // versions, even when the user read set omitted them.
                        let Some(index_spec) = metadata.idx.index_spec(index_no) else {
                            return ReadRow::InvalidIndex;
                        };
                        if index_spec.cols.len() != key_vals.len() {
                            return ReadRow::InvalidIndex;
                        }
                        let user_key_idx_map: FastHashMap<usize, usize> = index_spec
                            .cols
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
                            let vals = index_spec
                                .cols
                                .iter()
                                .map(|key| {
                                    self.row().val(metadata.col.as_ref(), key.col_no as usize)
                                })
                                .collect();
                            Some(SelectKey { index_no, vals })
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
                            // A unique-index branch jumps from the latest key
                            // owner to an older owner that may have a different
                            // RowID. Apply the delta from the current row image
                            // to that older owner before checking its target.
                            ver.undo_update(&ib.undo_vals);
                            // Index branch only contains non-deleted version.
                            // So delete flag is not used.
                            debug_assert!(!ver.deleted);
                            match &ib.target {
                                IndexBranchTarget::Hot {
                                    cts,
                                    entry: hot_entry,
                                } => {
                                    if ctx.sts() > *cts {
                                        // The old same-key hot owner stopped
                                        // being visible before this snapshot,
                                        // so the current reconstructed owner is
                                        // the visible one.
                                        return ver.get_visible_vals(metadata, self.row(), key);
                                    }
                                    // Older snapshots continue from the old
                                    // hot owner's undo chain.
                                    entry = hot_entry.as_ref();
                                }
                                IndexBranchTarget::ColdTerminal { delete_cts } => {
                                    // A cold terminal branch has no older undo
                                    // chain. `ver` already holds the cold image
                                    // reconstructed from undo_vals. It remains
                                    // visible only to snapshots at or before
                                    // its committed CDB delete timestamp; a
                                    // missing timestamp means the current
                                    // uncommitted transaction owns the cold
                                    // delete marker.
                                    if let Some(delete_cts) = delete_cts
                                        && ctx.sts() > *delete_cts
                                    {
                                        return ReadRow::NotFound;
                                    }
                                    return ver.get_visible_vals(metadata, self.row(), key);
                                }
                            }
                        } else {
                            // Key not match, go to main branch
                            entry = next.main.entry.as_ref();
                        }
                        // Apply the inverse of the foreground operation that
                        // produced this undo entry.
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
                                if ctx.sts() > ts {
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

    /// Reads the row version visible to the transaction and validates exact
    /// secondary-index candidate identity.
    #[inline]
    pub(crate) fn read_row_mvcc_index_candidate(
        &self,
        ctx: &TrxContext,
        metadata: &TableMetadata,
        read_set: &[usize],
        recheck: &IndexCandidateRecheck<'_>,
    ) -> ReadRow {
        match &self.state {
            RowReadState::RowVer(undo) => match &**undo {
                None => self.read_row_latest_index_candidate(metadata, read_set, recheck),
                Some(undo_head) => {
                    let ts = undo_head.ts();
                    if trx_is_committed(ts) {
                        if ctx.sts() > ts {
                            return self
                                .read_row_latest_index_candidate(metadata, read_set, recheck);
                        }
                    } else if ctx.trx_id() == ts {
                        return self.read_row_latest_index_candidate(metadata, read_set, recheck);
                    }

                    let Some(index_spec) = metadata.idx.index_spec(recheck.index_no) else {
                        return ReadRow::InvalidIndex;
                    };
                    let mut next = &undo_head.next;
                    let read_set: BTreeSet<usize> = read_set.iter().copied().collect();
                    let user_key_idx_map: FastHashMap<usize, usize> = index_spec
                        .cols
                        .iter()
                        .enumerate()
                        .map(|(key_pos, key)| (key.col_no as usize, key_pos))
                        .collect();
                    let undo_key = SelectKey {
                        index_no: recheck.index_no,
                        vals: index_spec
                            .cols
                            .iter()
                            .map(|key| self.row().val(metadata.col.as_ref(), key.col_no as usize))
                            .collect(),
                    };
                    let mut ver = RowVersion {
                        deleted: self.row().is_deleted(),
                        read_set,
                        key_tracker: Some(IndexKeyTracker {
                            user_key_idx_map,
                            undo_key: Some(undo_key),
                        }),
                        undo_vals: BTreeMap::new(),
                    };
                    loop {
                        let entry;
                        if let Some(ib) = next.indexes.iter().find(|ib| recheck.matches_branch(ib))
                        {
                            ver.undo_update(&ib.undo_vals);
                            debug_assert!(!ver.deleted);
                            match &ib.target {
                                IndexBranchTarget::Hot {
                                    cts,
                                    entry: hot_entry,
                                } => {
                                    if ctx.sts() > *cts {
                                        return ver.get_visible_vals_for_index_candidate(
                                            metadata,
                                            self.row(),
                                            recheck,
                                        );
                                    }
                                    entry = hot_entry.as_ref();
                                }
                                IndexBranchTarget::ColdTerminal { delete_cts } => {
                                    if let Some(delete_cts) = delete_cts
                                        && ctx.sts() > *delete_cts
                                    {
                                        return ReadRow::NotFound;
                                    }
                                    return ver.get_visible_vals_for_index_candidate(
                                        metadata,
                                        self.row(),
                                        recheck,
                                    );
                                }
                            }
                        } else {
                            entry = next.main.entry.as_ref();
                        }
                        match &entry.kind {
                            RowUndoKind::Lock => (),
                            RowUndoKind::Insert => {
                                debug_assert!(!ver.deleted);
                                ver.deleted = true;
                            }
                            RowUndoKind::Update(undo_vals) => {
                                debug_assert!(!ver.deleted);
                                ver.undo_update(undo_vals);
                            }
                            RowUndoKind::Delete => {
                                ver.deleted = false;
                            }
                        }
                        match entry.next.as_ref() {
                            None => {
                                if ver.deleted {
                                    return ReadRow::NotFound;
                                }
                                return ver.get_visible_vals_for_index_candidate(
                                    metadata,
                                    self.row(),
                                    recheck,
                                );
                            }
                            Some(nx) => {
                                let ts = nx.main.status.ts();
                                if ctx.sts() > ts {
                                    if ver.deleted {
                                        return ReadRow::NotFound;
                                    }
                                    return ver.get_visible_vals_for_index_candidate(
                                        metadata,
                                        self.row(),
                                        recheck,
                                    );
                                }
                                next = nx;
                            }
                        }
                    }
                }
            },
            RowReadState::Recover(_) => {
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
    pub(crate) fn find_old_version_for_unique_key(
        &self,
        metadata: &TableMetadata,
        index_no: usize,
        key_vals: &[Val],
        ctx: &TrxContext,
    ) -> FindOldVersion {
        let undo = match &self.state {
            RowReadState::Recover(_) => unreachable!(),
            RowReadState::RowVer(undo) => undo,
        };
        let Some(index_spec) = metadata.idx.index_spec(index_no) else {
            return FindOldVersion::None;
        };

        match &**undo {
            None => {
                let row = self.row();
                if !row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
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
                if !trx_is_committed(ts) && !ctx.is_same_trx(undo_head) {
                    // Scenario #1.a
                    return FindOldVersion::WriteConflict;
                }
                let row = self.row();
                // Old row matches key.
                if !row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
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
                    let old_row = row.clone_vals(metadata.col.as_ref());
                    return FindOldVersion::Ok(old_row, ts, undo_head.next.main.entry.clone());
                }
                // Old row does not match key.
                // Traverse version chain to find matched version.
                let mut entry = undo_head.next.main.entry.clone();
                let mut cts = ts;
                let mut deleted = row.is_deleted();
                let mut vals = row.clone_vals(metadata.col.as_ref());
                // Traverse version chain until oldest version.
                // This is safe because we already lock the row and prevent
                // purge coordination from removing old versions.
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
                    if !deleted && metadata.idx.match_key(index_no, key_vals, &vals) {
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
    pub(crate) fn any_version_matches_key(
        &self,
        metadata: &TableMetadata,
        index_no: usize,
        key_vals: &[Val],
    ) -> bool {
        let Some(index_spec) = metadata.idx.index_spec(index_no) else {
            return false;
        };
        // Check page data first.
        let row = self.row();
        let deleted = row.is_deleted();
        if !row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) && !deleted {
            return true; // matched key found in page.
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
                    let vals = index_spec
                        .cols
                        .iter()
                        .map(|key| row.val(metadata.col.as_ref(), key.col_no as usize))
                        .collect();
                    let mvcc_key = SelectKey::new(index_no, vals);
                    let mapping: FastHashMap<usize, usize> = index_spec
                        .cols
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
                        if !ver.deleted
                            && ver.mvcc_key.index_no == index_no
                            && ver.mvcc_key.vals.as_slice() == key_vals
                        {
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

/// Row-read storage state selected from the frame context.
pub(crate) enum RowReadState<'a> {
    /// Hot row page with a version map guard.
    RowVer(RowVersionReadGuard<'a>),
    /// Recovery-time row page with a recovery timestamp map.
    Recover(&'a RowRecoveryMap),
}

impl<'a> RowReadState<'a> {
    #[inline]
    fn from_guard(page_guard: &'a PageSharedGuard<RowPage>, row_idx: usize) -> Self {
        if let Some(rec) = page_guard.try_rmap() {
            RowReadState::Recover(rec)
        } else {
            RowReadState::RowVer(page_guard.unwrap_vmap().read_latch(row_idx))
        }
    }
}

/// Exact secondary-index candidate identity used during hot-row MVCC recheck.
pub(crate) struct IndexCandidateRecheck<'a> {
    /// Target secondary-index slot.
    pub(crate) index_no: usize,
    /// Whether the target index is unique.
    pub(crate) unique: bool,
    /// Candidate emitted by the secondary-index stream.
    pub(crate) candidate: &'a IndexLookupCandidate,
    /// Encoder for the target index identity.
    pub(crate) encoder: &'a BTreeKeyEncoder,
}

impl IndexCandidateRecheck<'_> {
    #[inline]
    fn matches_key(&self, key_vals: &[Val]) -> bool {
        let encoded = if self.unique {
            self.encoder.encode(key_vals)
        } else {
            self.encoder
                .encode_pair(key_vals, Val::from(self.candidate.row_id))
        };
        encoded.as_bytes() == self.candidate.encoded_key.as_bytes()
    }

    #[inline]
    fn matches_branch(&self, branch: &IndexBranch) -> bool {
        self.unique && branch.key.index_no == self.index_no && self.matches_key(&branch.key.vals)
    }
}

/// Iterator over all rows protected by one shared row-page guard.
pub(crate) struct ReadAllRows<'a> {
    page_guard: &'a PageSharedGuard<RowPage>,
    start_idx: usize,
    end_idx: usize,
}

impl<'a> ReadAllRows<'a> {
    /// Create an iterator over all row indexes in the page.
    #[inline]
    fn new(page_guard: &'a PageSharedGuard<RowPage>) -> Self {
        let end_idx = page_guard.page().header.row_count();
        ReadAllRows {
            page_guard,
            start_idx: 0,
            end_idx,
        }
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
        Some(self.page_guard.read_row(row_idx))
    }
}

impl PageSharedGuard<RowPage> {
    /// Acquires read access to one row slot.
    #[inline]
    pub(crate) fn read_row(&self, row_idx: usize) -> RowReadAccess<'_> {
        RowReadAccess::new(self, row_idx)
    }

    /// Acquires read access to one row identified by its row ID.
    #[inline]
    pub(crate) fn read_row_by_id(&self, row_id: RowID) -> RowReadAccess<'_> {
        self.read_row(self.page().row_idx(row_id))
    }

    /// Iterates over all populated row slots with row-aware read access.
    #[inline]
    pub(crate) fn read_all_rows(&self) -> ReadAllRows<'_> {
        ReadAllRows::new(self)
    }

    /// Acquires write access to one row slot and read-locks page state.
    #[inline]
    pub(crate) fn write_row(&self, row_idx: usize) -> RowWriteAccess<'_> {
        RowWriteAccess::new(self, row_idx)
    }

    /// Acquires write access to one row identified by its row ID.
    #[inline]
    pub(crate) fn write_row_by_id(&self, row_id: RowID) -> RowWriteAccess<'_> {
        self.write_row(self.page().row_idx(row_id))
    }

    /// Acquires write access while retaining an existing page-state guard.
    #[inline]
    pub(crate) fn write_row_with_state_guard<'a>(
        &'a self,
        row_idx: usize,
        state_guard: RwLockReadGuard<'a, RowPageState>,
    ) -> RowWriteAccess<'a> {
        RowWriteAccess::new_with_state_guard(self, row_idx, state_guard)
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
        search_key: Option<(usize, &[Val])>,
    ) -> ReadRow {
        if let Some((index_no, key_vals)) = search_key {
            let Some(index_spec) = metadata.idx.index_spec(index_no) else {
                return ReadRow::InvalidIndex;
            };
            if index_spec.cols.len() != key_vals.len() {
                return ReadRow::InvalidIndex;
            }
            // If search key is provided, we need to validate key before
            // returning visible values.
            if let Some(tracker) = self.key_tracker.as_ref()
                && let Some(undo_key) = tracker.undo_key.as_ref()
            {
                // compare key directly
                if key_vals.iter().zip(&undo_key.vals).any(|(v1, v2)| v1 != v2) {
                    return ReadRow::InvalidIndex;
                }
            } else {
                // compare key using read set and latest row page
                let key_different = key_vals.iter().enumerate().any(|(pos, search_val)| {
                    let col_idx = index_spec.cols[pos].col_no as usize;
                    if let Some(undo_val) = self.undo_vals.get(&col_idx) {
                        search_val != undo_val
                    } else {
                        row.is_different(metadata.col.as_ref(), col_idx, search_val)
                    }
                });
                if key_different {
                    return ReadRow::InvalidIndex;
                }
            }
        }
        let mut vals = Vec::with_capacity(self.read_set.len());
        for col_idx in &self.read_set {
            if let Some(v) = self.undo_vals.remove(col_idx) {
                vals.push(v);
            } else {
                vals.push(row.val(metadata.col.as_ref(), *col_idx))
            }
        }
        ReadRow::Ok(vals)
    }

    #[inline]
    fn get_visible_vals_for_index_candidate(
        mut self,
        metadata: &TableMetadata,
        row: Row<'_>,
        recheck: &IndexCandidateRecheck<'_>,
    ) -> ReadRow {
        let Some(index_spec) = metadata.idx.index_spec(recheck.index_no) else {
            return ReadRow::InvalidIndex;
        };
        let matches_candidate = if let Some(undo_key) = self
            .key_tracker
            .as_ref()
            .and_then(|tracker| tracker.undo_key.as_ref())
        {
            recheck.matches_key(&undo_key.vals)
        } else {
            let key_vals = index_spec
                .cols
                .iter()
                .map(|key| {
                    let col_no = key.col_no as usize;
                    self.undo_vals
                        .get(&col_no)
                        .cloned()
                        .unwrap_or_else(|| row.val(metadata.col.as_ref(), col_no))
                })
                .collect::<Vec<_>>();
            recheck.matches_key(&key_vals)
        };
        if !matches_candidate {
            return ReadRow::InvalidIndex;
        }
        let mut vals = Vec::with_capacity(self.read_set.len());
        for col_idx in &self.read_set {
            if let Some(v) = self.undo_vals.remove(col_idx) {
                vals.push(v);
            } else {
                vals.push(row.val(metadata.col.as_ref(), *col_idx));
            }
        }
        ReadRow::Ok(vals)
    }
}

struct IndexKeyTracker {
    user_key_idx_map: FastHashMap<usize, usize>,
    undo_key: Option<SelectKey>,
}

struct KeyVersion {
    deleted: bool,
    mvcc_key: SelectKey,
    // mapping of column number to key number
    mapping: FastHashMap<usize, usize>,
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

/// Exclusive row write access with the row-version and page-state guards held.
pub(crate) struct RowWriteAccess<'a> {
    page: &'a RowPage,
    dirty: &'a AtomicBool,
    row_idx: usize,
    guard: RowVersionWriteGuard<'a>,
    _state_guard: RwLockReadGuard<'a, RowPageState>,
    frozen_version_map: Option<&'a RowVersionMap>,
}

impl<'a> RowWriteAccess<'a> {
    /// Acquire write access to one row and read-lock the page state.
    #[inline]
    fn new(page_guard: &'a PageSharedGuard<RowPage>, row_idx: usize) -> Self {
        let ver_map = page_guard.unwrap_vmap();
        let state_guard = ver_map.read_state();
        Self::new_with_state_guard(page_guard, row_idx, state_guard)
    }

    /// Acquire write access using an existing page-state guard.
    #[inline]
    fn new_with_state_guard(
        page_guard: &'a PageSharedGuard<RowPage>,
        row_idx: usize,
        state_guard: RwLockReadGuard<'a, RowPageState>,
    ) -> Self {
        let ver_map = page_guard.unwrap_vmap();
        Self::from_parts(
            page_guard.page(),
            ver_map,
            page_guard.bf().dirty_flag(),
            row_idx,
            state_guard,
        )
    }

    #[inline]
    fn from_parts(
        page: &'a RowPage,
        ver_map: &'a RowVersionMap,
        dirty: &'a AtomicBool,
        row_idx: usize,
        state_guard: RwLockReadGuard<'a, RowPageState>,
    ) -> Self {
        let guard = ver_map.write_latch(row_idx);
        let frozen_version_map = if *state_guard == RowPageState::Frozen {
            // The row latch and page-state guard are already held, so a final
            // page-state writer cannot pass this modifier before its paired
            // closing bump is published from `Drop`.
            ver_map.begin_frozen_mutation();
            Some(ver_map)
        } else {
            None
        };
        RowWriteAccess {
            page,
            dirty,
            row_idx,
            guard,
            _state_guard: state_guard,
            frozen_version_map,
        }
    }

    /// Mark the underlying page as dirty.
    #[inline]
    pub(crate) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Returns the latest immutable row image.
    #[inline]
    pub(crate) fn row(&self) -> Row<'a> {
        self.page.row(self.row_idx)
    }

    /// Returns a mutable row view for an update with the supplied variable range.
    #[inline]
    pub(crate) fn row_mut(&self, var_offset: usize, var_end: usize) -> RowMut<'_> {
        self.mark_dirty();
        self.page.row_mut(self.row_idx, var_offset, var_end)
    }

    /// Returns the row page state observed by this write access.
    #[inline]
    pub(crate) fn page_state(&self) -> RowPageState {
        *self._state_guard
    }

    /// Marks the row as deleted in the page image.
    #[inline]
    pub(crate) fn delete_row(&mut self) {
        self.mark_dirty();
        let res = self.page.set_deleted(self.row_idx, true);
        debug_assert!(res);
        self.page.inc_approx_deleted();
    }

    /// Prepare a row update or return the old image when space/state prevents it.
    #[inline]
    pub(crate) fn update_row(
        &self,
        col_layout: &TableColumnLayout,
        update: RowUpdateView<'_>,
        frozen: bool,
    ) -> UpdateRow<'_> {
        if frozen {
            let old_row = self.row().clone_vals(col_layout);
            return UpdateRow::NoFreeSpaceOrFrozen(old_row);
        }
        let var_len = self.row().var_len_for_update(update);
        if var_len == 0 {
            // fast path, no change on var-length column.
            let offset = self.page.header.var_field_offset();
            let row = self.row_mut(offset, offset);
            return UpdateRow::Ok(row);
        }
        match self.page.request_free_space(var_len) {
            None => {
                let old_row = self.row().clone_vals(col_layout);
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

    /// Acquires modification ownership for the latest physical row image.
    ///
    /// Row writes never target an older image reconstructed from MVCC undo.
    /// The current transaction may extend its own active chain; another active
    /// owner conflicts or, while preparing, is awaited by the caller.
    #[expect(clippy::too_many_arguments, reason = "code style")]
    #[inline]
    pub(crate) fn lock_undo(
        &mut self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        metadata: &TableMetadata,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
        key: Option<(usize, &[Val])>,
    ) -> LockUndo {
        rt.debug_assert_table_write_lock_held(table_id);
        let ctx = rt.ctx();
        let row = self.page.row(self.row_idx);
        match &mut *self.guard {
            None => {
                // No undo head exists yet, so this transaction becomes the
                // first writer for the hot row. The `Lock` entry is pushed to
                // statement-owned row undo before the actual row-page change is
                // made.
                let entry = OwnedRowUndo::new(table_id, Some(page_id), row_id, RowUndoKind::Lock);
                self.add_undo_head(ctx.status(), entry.leak());
                effects.push_row_undo(entry);
                LockUndo::Ok
            }
            Some(undo_head) => {
                if ctx.is_same_trx(undo_head) {
                    // Re-entrant write by the same transaction. Chain another
                    // provisional lock entry so each statement-level operation
                    // can roll back independently in reverse order.
                    let mut entry =
                        OwnedRowUndo::new(table_id, Some(page_id), row_id, RowUndoKind::Lock);
                    let new_next = NextRowUndo::new(MainBranch {
                        entry: entry.leak(),
                        status: UndoStatus::Ref(ctx.status()),
                    });
                    let old_next = mem::replace(&mut undo_head.next, new_next);
                    entry.next = Some(old_next);
                    effects.push_row_undo(entry);
                    return LockUndo::Ok;
                }
                let old_cts = undo_head.ts();
                if trx_is_committed(old_cts) {
                    // The previous writer has committed, so this transaction
                    // can own a new undo head. Key validation filters stale
                    // index candidates before the row is locked.
                    //
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
                    if let Some((index_no, key_vals)) = key {
                        let Some(index_spec) = metadata.idx.index_spec(index_no) else {
                            return LockUndo::InvalidIndex;
                        };
                        if row.is_key_different(metadata.col.as_ref(), index_spec, key_vals) {
                            return LockUndo::InvalidIndex;
                        }
                    }
                    let mut entry =
                        OwnedRowUndo::new(table_id, Some(page_id), row_id, RowUndoKind::Lock);
                    let new_next = NextRowUndo::new(MainBranch {
                        entry: entry.leak(),
                        status: UndoStatus::Ref(ctx.status()),
                    });
                    let old_next = mem::replace(&mut undo_head.next, new_next);
                    entry.next = Some(old_next);
                    effects.push_row_undo(entry);
                    return LockUndo::Ok;
                }
                // Another active transaction owns the hot-row lock. If it is
                // preparing commit, wait and retry because the status will
                // shortly become committed; otherwise report a write conflict.
                if !undo_head.preparing() {
                    return LockUndo::WriteConflict;
                }
                LockUndo::Preparing(undo_head.prepare_listener())
            }
        }
    }

    /// Link this row's unique-key branch to an older hot owner.
    #[inline]
    pub(crate) fn link_for_unique_index(
        &mut self,
        key: SelectKey,
        cts: TrxID,
        entry: RowUndoRef,
        undo_vals: Vec<UpdateCol>,
    ) {
        // Runtime unique-key link to an older hot owner. The branch is stored
        // on the new owner's undo head so readers following the latest unique
        // mapping can still reach snapshots where `entry` was the visible
        // same-key owner.
        let undo_head = self.guard.as_mut().expect("undo head");
        undo_head.next.indexes.push(IndexBranch {
            key,
            target: IndexBranchTarget::Hot { cts, entry },
            undo_vals,
        })
    }

    /// Link this row's unique-key branch to an older cold terminal image.
    #[inline]
    pub(crate) fn link_for_unique_index_cold_terminal(
        &mut self,
        key: SelectKey,
        delete_cts: Option<TrxID>,
        undo_vals: Vec<UpdateCol>,
    ) {
        // The old owner is a persisted LWC row, so the branch records only the
        // reconstructed image and optional committed delete timestamp. There is
        // no RowUndoRef continuation because cold rows do not have row-page
        // undo chains after checkpoint.
        let undo_head = self.guard.as_mut().expect("undo head");
        undo_head.next.indexes.push(IndexBranch {
            key,
            target: IndexBranchTarget::ColdTerminal { delete_cts },
            undo_vals,
        })
    }

    /// Purge undo chain according to minimum active STS.
    /// This method removes out-of-date versions from the next list.
    /// The real deletion of undo logs is performed later.
    #[inline]
    pub(crate) fn purge_undo_chain(&mut self, min_active_sts: TrxID) {
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
                    // The newest hot row-page image is older than every active
                    // snapshot. No reader can need older main or unique-index
                    // branches, so the whole undo head can be detached.
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
                    // purge main branch
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
                        if next.indexes[idx]
                            .target
                            .purge_cts()
                            .is_some_and(|cts| cts < min_active_sts)
                        {
                            // This runtime unique branch only preserves an
                            // older owner for snapshots at or before its CTS.
                            // Once that CTS is below the oldest active
                            // snapshot, the latest mapping alone is enough.
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
    pub(crate) fn rollback_first_undo(
        &mut self,
        metadata: &TableMetadata,
        mut owned_entry: OwnedRowUndo,
    ) {
        let dirty = self.dirty;
        let head = self.guard.as_mut().expect("undo head");
        let entry = &mut head.next.main.entry;
        debug_assert!({
            let input_ref = &*owned_entry;
            std::ptr::addr_eq(entry.as_ref(), input_ref)
        });
        // Roll back the hot row page by applying the inverse of the first undo
        // entry. Index undo is handled separately by transaction rollback.
        match &owned_entry.kind {
            RowUndoKind::Lock => (), // do nothing.
            RowUndoKind::Insert => {
                dirty.store(true, Ordering::Release);
                // The inserted image never existed before this transaction.
                let res = self.page.set_deleted(self.row_idx, true);
                debug_assert!(res);
                self.page.inc_approx_deleted();
            }
            RowUndoKind::Delete => {
                dirty.store(true, Ordering::Release);
                // The row-page image still carries the deleted row values.
                // Clearing the bit restores the pre-delete latest image.
                let res = self.page.set_deleted(self.row_idx, false);
                debug_assert!(res);
                self.page.dec_approx_deleted();
            }
            RowUndoKind::Update(undo_cols) => {
                if !undo_cols.is_empty() {
                    dirty.store(true, Ordering::Release);
                }
                // Restore changed columns from before-images and prefer to
                // reuse the variable-length space captured by the undo entry.
                for uc in undo_cols {
                    self.page.update_col(
                        metadata.col.as_ref(),
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
                // The restored row-page image no longer needs an undo status.
                self.remove_undo_head();
            }
            Some(next) => {
                head.next = next;
            }
        }
    }
}

impl Drop for RowWriteAccess<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(ver_map) = self.frozen_version_map {
            // This runs before Rust releases either retained lock field.
            ver_map.finish_frozen_mutation();
        }
    }
}

/// Result of trying to install or reuse a row undo lock.
pub(crate) enum LockUndo {
    /// The row undo lock is available or installed.
    Ok,
    /// Another active transaction owns the row.
    WriteConflict,
    /// The index entry does not identify this row version.
    InvalidIndex,
    /// The row is locked by a preparing transaction.
    Preparing(Option<EventListener>),
}

impl LockUndo {
    /// Returns whether the lock attempt succeeded.
    #[inline]
    pub(crate) fn is_ok(&self) -> bool {
        matches!(self, LockUndo::Ok)
    }
}

/// Result of locking one row for foreground write.
pub(crate) enum LockRowForWrite<'a> {
    /// Lock succeeded with optional write access.
    Ok(Option<RowWriteAccess<'a>>),
    /// Another transaction is modifying this row.
    WriteConflict,
    /// Row is invalid through index lookup because an old key points to a new version.
    InvalidIndex,
    /// Row page is transitioning and the caller should retry.
    RetryInTransition,
}

/// Result of searching a row's old versions for a unique-key owner.
pub(crate) enum FindOldVersion {
    /// Matching old version found with values, timestamp, and undo entry.
    Ok(Vec<Val>, TrxID, RowUndoRef),
    /// Another active transaction owns a conflicting old version.
    WriteConflict,
    /// A visible old version already owns the unique key.
    DuplicateKey,
    /// No matching old version exists.
    None,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::catalog::{
        ActiveIndexSpec, ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
    };
    use crate::trx::undo::RowUndoHead;
    use crate::trx::{MIN_ACTIVE_TRX_ID, ver_map::RowVersionMap};
    use crate::value::ValKind;
    use std::collections::BTreeMap;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    fn sparse_metadata() -> TableMetadata {
        TableMetadata::try_new_with_next_index_no(
            vec![
                ColumnSpec::new("c0", ValKind::I32, ColumnAttributes::empty()),
                ColumnSpec::new("c1", ValKind::I32, ColumnAttributes::empty()),
            ],
            vec![ActiveIndexSpec::new(
                0,
                IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::PK),
            )],
            2,
        )
        .unwrap()
    }

    fn row_page(metadata: &TableMetadata) -> RowPage {
        let mut page = RowPage::new_test_page();
        page.init(RowID::new(100), 4, metadata.col.as_ref());
        assert!(
            page.insert(metadata.col.as_ref(), &[Val::from(10i32), Val::from(20i32)])
                .is_ok()
        );
        page
    }

    fn row_page_with_two_rows(metadata: &TableMetadata) -> RowPage {
        let page = row_page(metadata);
        assert!(
            page.insert(metadata.col.as_ref(), &[Val::from(30i32), Val::from(40i32)])
                .is_ok()
        );
        page
    }

    fn test_trx_context(sts: TrxID) -> TrxContext {
        TrxContext {
            status: Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + sts.as_u64())),
            sts,
            gc_no: 0,
        }
    }

    fn install_test_undo_head(row_ver: &RowVersionMap, status: Arc<SharedTrxStatus>) {
        let undo = OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), RowUndoKind::Lock);
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(status, undo.leak())));
    }

    fn test_row_read_access<'a>(
        page: &'a RowPage,
        row_ver: &'a RowVersionMap,
        row_idx: usize,
    ) -> RowReadAccess<'a> {
        RowReadAccess::from_state(
            page,
            row_idx,
            RowReadState::RowVer(row_ver.read_latch(row_idx)),
        )
    }

    fn test_recovery_row_read_access<'a>(
        page: &'a RowPage,
        rec_map: &'a RowRecoveryMap,
        row_idx: usize,
    ) -> RowReadAccess<'a> {
        RowReadAccess::from_state(page, row_idx, RowReadState::Recover(rec_map))
    }

    pub(crate) fn test_row_write_access<'a>(
        page: &'a RowPage,
        row_ver: &'a RowVersionMap,
        dirty: &'a AtomicBool,
        row_idx: usize,
    ) -> RowWriteAccess<'a> {
        let state_guard = row_ver.read_state();
        RowWriteAccess::from_parts(page, row_ver, dirty, row_idx, state_guard)
    }

    #[test]
    fn test_read_latest_uses_committed_page_image_newer_than_snapshot() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 99));
        status.commit_for_test(TrxID::new(10));
        install_test_undo_head(&row_ver, status);
        let access = test_row_read_access(&page, &row_ver, 0);
        let trx_ctx = test_trx_context(TrxID::new(1));

        assert_eq!(access.read_latest(&trx_ctx), ReadLatestRow::Readable);
        assert_eq!(
            access.read_latest_value(metadata.col.as_ref(), 1),
            Val::from(20i32)
        );
    }

    #[test]
    fn test_read_latest_allows_own_head_and_rejects_foreign_active_head() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let trx_ctx = test_trx_context(TrxID::new(1));
        install_test_undo_head(&row_ver, trx_ctx.status());
        {
            let access = test_row_read_access(&page, &row_ver, 0);
            assert_eq!(access.read_latest(&trx_ctx), ReadLatestRow::Readable);
        }

        install_test_undo_head(
            &row_ver,
            Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 100)),
        );
        let access = test_row_read_access(&page, &row_ver, 0);
        assert_eq!(access.read_latest(&trx_ctx), ReadLatestRow::WriteConflict);
    }

    #[test]
    fn test_read_latest_checks_foreign_owner_before_deleted_image() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        assert!(page.set_deleted(0, true));
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 99));
        install_test_undo_head(&row_ver, Arc::clone(&status));
        let access = test_row_read_access(&page, &row_ver, 0);
        let trx_ctx = test_trx_context(TrxID::new(1));

        assert_eq!(access.read_latest(&trx_ctx), ReadLatestRow::WriteConflict);
        status.commit_for_test(TrxID::new(10));
        assert_eq!(access.read_latest(&trx_ctx), ReadLatestRow::NotFound);
    }

    #[test]
    fn test_row_write_access_pairs_frozen_mutation_version() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        *row_ver.write_state() = RowPageState::Frozen;
        let dirty = AtomicBool::new(false);

        assert_eq!(row_ver.frozen_mutation_version(), 0);
        {
            let _access = test_row_write_access(&page, &row_ver, &dirty, 0);
            assert_eq!(row_ver.frozen_mutation_version(), 1);
        }
        assert_eq!(row_ver.frozen_mutation_version(), 2);

        *row_ver.write_state() = RowPageState::Active;
        drop(test_row_write_access(&page, &row_ver, &dirty, 0));
        assert_eq!(row_ver.frozen_mutation_version(), 2);
    }

    #[test]
    fn test_row_write_access_marks_dirty_for_page_image_mutations() {
        let metadata = sparse_metadata();
        let page = row_page_with_two_rows(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let dirty = AtomicBool::new(false);

        drop(test_row_write_access(&page, &row_ver, &dirty, 0));
        assert!(!dirty.load(Ordering::Acquire));

        let state_guard = row_ver.read_state();
        let access = RowWriteAccess::from_parts(&page, &row_ver, &dirty, 0, state_guard);
        access.mark_dirty();
        drop(access);
        assert!(dirty.load(Ordering::Acquire));

        dirty.store(false, Ordering::Release);
        {
            let access = test_row_write_access(&page, &row_ver, &dirty, 0);
            let offset = page.header.var_field_offset();
            let _row = access.row_mut(offset, offset);
        }
        assert!(dirty.load(Ordering::Acquire));

        dirty.store(false, Ordering::Release);
        {
            let mut access = test_row_write_access(&page, &row_ver, &dirty, 1);
            access.delete_row();
        }
        assert!(dirty.load(Ordering::Acquire));
    }

    #[test]
    fn test_row_write_access_closes_frozen_version_during_unwind() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        *row_ver.write_state() = RowPageState::Frozen;
        let dirty = AtomicBool::new(false);

        let result = catch_unwind(AssertUnwindSafe(|| {
            let _access = test_row_write_access(&page, &row_ver, &dirty, 0);
            panic!("exercise unwind cleanup");
        }));

        assert!(result.is_err());
        assert_eq!(row_ver.frozen_mutation_version(), 2);
    }

    #[test]
    fn test_overlapping_frozen_writers_do_not_use_version_parity_as_quiescence() {
        let metadata = sparse_metadata();
        let page = row_page_with_two_rows(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        *row_ver.write_state() = RowPageState::Frozen;
        let dirty = AtomicBool::new(false);

        let first = test_row_write_access(&page, &row_ver, &dirty, 0);
        assert_eq!(row_ver.frozen_mutation_version(), 1);
        let second = test_row_write_access(&page, &row_ver, &dirty, 1);
        assert_eq!(row_ver.frozen_mutation_version(), 2);

        drop(first);
        assert_eq!(row_ver.frozen_mutation_version(), 3);
        drop(second);
        assert_eq!(row_ver.frozen_mutation_version(), 4);
        assert_ne!(row_ver.frozen_mutation_version(), 0);
    }

    #[test]
    fn test_active_page_mutations_do_not_change_frozen_version() {
        let metadata = sparse_metadata();
        let page = row_page_with_two_rows(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let dirty = AtomicBool::new(false);
        assert_eq!(row_ver.inspect_state(), RowPageState::Active);

        assert!(
            page.insert(metadata.col.as_ref(), &[Val::from(50i32), Val::from(60i32)])
                .is_ok()
        );
        {
            let _access = test_row_write_access(&page, &row_ver, &dirty, 0);
            page.update_col(metadata.col.as_ref(), 0, 0, &Val::from(11i32), 0, true);
        }
        {
            let mut access = test_row_write_access(&page, &row_ver, &dirty, 1);
            access.delete_row();
        }

        let rollback_status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 10));
        let rollback_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), RowUndoKind::Lock);
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(
            rollback_status,
            rollback_undo.leak(),
        )));
        // Statement rollback and full transaction rollback share this row
        // restoration boundary, so exercise two independent rollback passes.
        test_row_write_access(&page, &row_ver, &dirty, 0)
            .rollback_first_undo(&metadata, rollback_undo);
        let trx_rollback_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), RowUndoKind::Lock);
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(
            Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 11)),
            trx_rollback_undo.leak(),
        )));
        test_row_write_access(&page, &row_ver, &dirty, 0)
            .rollback_first_undo(&metadata, trx_rollback_undo);

        let purge_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(101), RowUndoKind::Lock);
        *row_ver.write_latch(1) = Some(Box::new(RowUndoHead::new(
            Arc::new(SharedTrxStatus::new(TrxID::new(20))),
            purge_undo.leak(),
        )));
        test_row_write_access(&page, &row_ver, &dirty, 1).purge_undo_chain(TrxID::new(21));

        assert_eq!(row_ver.frozen_mutation_version(), 0);
    }

    #[test]
    fn test_frozen_delete_rollbacks_and_purge_publish_paired_versions() {
        let metadata = sparse_metadata();
        let page = row_page_with_two_rows(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        *row_ver.write_state() = RowPageState::Frozen;
        let dirty = AtomicBool::new(false);
        let repeated_status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 9));
        let repeated_first_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), RowUndoKind::Lock);
        let repeated_second_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(101), RowUndoKind::Lock);
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(
            Arc::clone(&repeated_status),
            repeated_first_undo.leak(),
        )));
        *row_ver.write_latch(1) = Some(Box::new(RowUndoHead::new(
            repeated_status,
            repeated_second_undo.leak(),
        )));

        {
            let mut access = test_row_write_access(&page, &row_ver, &dirty, 0);
            assert_eq!(row_ver.frozen_mutation_version(), 1);
            access.delete_row();
        }
        assert_eq!(row_ver.frozen_mutation_version(), 2);
        {
            let mut access = test_row_write_access(&page, &row_ver, &dirty, 1);
            assert_eq!(row_ver.frozen_mutation_version(), 3);
            access.delete_row();
        }
        assert_eq!(row_ver.frozen_mutation_version(), 4);

        let rollback_status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 10));
        let rollback_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), RowUndoKind::Lock);
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(
            rollback_status,
            rollback_undo.leak(),
        )));
        let prepared_version = row_ver.frozen_mutation_version();
        // First model statement rollback of the current undo entry.
        {
            let mut access = test_row_write_access(&page, &row_ver, &dirty, 0);
            assert_eq!(row_ver.frozen_mutation_version(), prepared_version + 1);
            access.rollback_first_undo(&metadata, rollback_undo);
        }
        assert_eq!(row_ver.frozen_mutation_version(), prepared_version + 2);

        let trx_rollback_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(100), RowUndoKind::Lock);
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(
            Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 11)),
            trx_rollback_undo.leak(),
        )));
        let prepared_version = row_ver.frozen_mutation_version();
        // Then model the identical boundary reached by transaction rollback.
        test_row_write_access(&page, &row_ver, &dirty, 0)
            .rollback_first_undo(&metadata, trx_rollback_undo);
        assert_eq!(row_ver.frozen_mutation_version(), prepared_version + 2);

        let purge_undo =
            OwnedRowUndo::new(TableID::new(1), None, RowID::new(101), RowUndoKind::Lock);
        *row_ver.write_latch(1) = Some(Box::new(RowUndoHead::new(
            Arc::new(SharedTrxStatus::new(TrxID::new(20))),
            purge_undo.leak(),
        )));
        let prepared_version = row_ver.frozen_mutation_version();
        {
            let mut access = test_row_write_access(&page, &row_ver, &dirty, 1);
            assert_eq!(row_ver.frozen_mutation_version(), prepared_version + 1);
            access.purge_undo_chain(TrxID::new(21));
        }
        assert_eq!(row_ver.frozen_mutation_version(), prepared_version + 2);
    }

    #[test]
    fn test_read_row_latest_inactive_index_returns_invalid_index() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let rec_map = RowRecoveryMap::new(TrxID::new(0));
        let access = test_recovery_row_read_access(&page, &rec_map, 0);
        let key = SelectKey::new(1, vec![Val::from(10i32)]);

        let res = access.read_row_latest(&metadata, &[0], Some((key.index_no, &key.vals)));

        assert!(matches!(res, ReadRow::InvalidIndex));
    }

    #[test]
    fn test_read_row_mvcc_inactive_index_returns_invalid_index() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let undo = OwnedRowUndo::new(
            TableID::new(1),
            None,
            RowID::new(100),
            RowUndoKind::Update(vec![UndoCol {
                idx: 0,
                val: Val::from(9i32),
                var_offset: None,
            }]),
        );
        *row_ver.write_latch(0) = Some(Box::new(RowUndoHead::new(
            Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 99)),
            undo.leak(),
        )));
        let access = test_row_read_access(&page, &row_ver, 0);
        let trx_ctx = test_trx_context(TrxID::new(1));
        let key = SelectKey::new(1, vec![Val::from(10i32)]);

        let res = access.read_row_mvcc(&trx_ctx, &metadata, &[0], Some((key.index_no, &key.vals)));

        assert!(matches!(res, ReadRow::InvalidIndex));
    }

    #[test]
    fn test_any_version_matches_key_inactive_index_returns_false() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let rec_map = RowRecoveryMap::new(TrxID::new(0));
        let access = test_recovery_row_read_access(&page, &rec_map, 0);
        let key = SelectKey::new(1, vec![Val::from(10i32)]);

        assert!(!access.any_version_matches_key(&metadata, key.index_no, &key.vals));
    }

    #[test]
    fn test_any_version_matches_key_latest_page_row_returns_true() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let rec_map = RowRecoveryMap::new(TrxID::new(0));
        let access = test_recovery_row_read_access(&page, &rec_map, 0);
        let key = SelectKey::new(0, vec![Val::from(10i32)]);

        assert!(access.any_version_matches_key(&metadata, key.index_no, &key.vals));
    }

    #[test]
    fn test_row_version_visible_vals_inactive_index_returns_invalid_index() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let ver = RowVersion {
            deleted: false,
            read_set: [0usize].into_iter().collect(),
            key_tracker: None,
            undo_vals: BTreeMap::new(),
        };
        let key = SelectKey::new(1, vec![Val::from(10i32)]);

        let res = ver.get_visible_vals(&metadata, page.row(0), Some((key.index_no, &key.vals)));

        assert!(matches!(res, ReadRow::InvalidIndex));
    }

    #[test]
    fn test_find_old_version_for_unique_key_inactive_index_returns_none() {
        let metadata = sparse_metadata();
        let page = row_page(&metadata);
        let row_ver = RowVersionMap::new(Arc::clone(&metadata.col), 4);
        let access = test_row_read_access(&page, &row_ver, 0);
        let key = SelectKey::new(1, vec![Val::from(10i32)]);
        let trx_ctx = test_trx_context(TrxID::new(1));

        let res =
            access.find_old_version_for_unique_key(&metadata, key.index_no, &key.vals, &trx_ctx);

        assert!(matches!(res, FindOldVersion::None));
    }
}
