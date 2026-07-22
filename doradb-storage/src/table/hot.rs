use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::catalog::TableMetadata;
use crate::error::{OperationError, OperationResult};
use crate::id::{RowID, TableID};
use crate::log::redo::{RowRedo, RowRedoKind};
use crate::map::FastHashMap;
use crate::row::ops::{
    ReadRow, RowUpdateInput, SelectKey, SelectMvcc, UndoCol, UpdateCol, UpdateRow,
};
use crate::row::{RowPage, RowRead, var_len_for_insert};
use crate::trx::TrxRuntime;
use crate::trx::row::{LockRowForWrite, LockUndo};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, IndexBranchTarget, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::value::Val;
use error_stack::Report;
use std::mem::replace;

/// Result of inserting a row into one hot row page.
pub(super) enum InsertRowIntoPage {
    Ok(RowID, PageSharedGuard<RowPage>),
    NoSpaceOrFrozen(Vec<Val>, RowUndoKind, Vec<IndexBranch>),
}

/// Result of updating one hot row in place.
pub(super) enum UpdateRowInplace {
    // The caller keeps the row page lock if there is any index change,
    // so it can read latest values from page.
    // The hash map stores the changed column number and its old value.
    // for other columns in the changed index, we can read value(old and new are same)
    // from current page.
    Ok(RowID, FastHashMap<usize, Val>),
    RowNotFound(RowUpdateInput),
    RowDeleted(RowUpdateInput),
    RetryInTransition(RowUpdateInput),
    NoFreeSpaceOrFrozen(RowID, Vec<Val>, RowUpdateInput),
}

/// Result of deleting one hot row.
pub(super) enum DeleteInternal {
    Ok,
    NotFound,
    RetryInTransition,
}

/// Hot row-page insert context shared by catalog and user-table accessors.
pub(super) struct RowInserter<'m, 'r> {
    metadata: &'m TableMetadata,
    table_id: TableID,
    rt: TrxRuntime<'r>,
}

impl<'m, 'r> RowInserter<'m, 'r> {
    /// Create a hot-row inserter for one metadata snapshot and transaction.
    #[inline]
    pub(super) fn new(table_id: TableID, metadata: &'m TableMetadata, rt: TrxRuntime<'r>) -> Self {
        Self {
            metadata,
            table_id,
            rt,
        }
    }

    /// Insert one row into an active hot row page.
    #[inline]
    pub(super) fn insert_to_page(
        &self,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        cols: Vec<Val>,
        undo_kind: RowUndoKind,
        index_branches: Vec<IndexBranch>,
    ) -> InsertRowIntoPage {
        debug_assert!(matches!(undo_kind, RowUndoKind::Insert));
        let page_id = page_guard.page_id();
        let versioned_page_id = page_guard.versioned_page_id();
        let page = page_guard.page();
        let ver_map = page_guard.unwrap_vmap();
        let state_guard = ver_map.read_state();
        if *state_guard != RowPageState::Active {
            return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
        }
        debug_assert!(self.metadata.col.col_count() == page.header.col_count as usize);
        debug_assert!(cols.len() == page.header.col_count as usize);

        let var_len = var_len_for_insert(self.metadata.col.as_ref(), &cols);
        let (row_idx, var_offset) =
            if let Some((row_idx, var_offset)) = page.request_row_idx_and_free_space(var_len) {
                (row_idx, var_offset)
            } else {
                return InsertRowIntoPage::NoSpaceOrFrozen(cols, undo_kind, index_branches);
            };
        // Before real insert, we need to lock the row.
        let row_id = page.header.start_row_id + row_idx as u64;
        let mut access = page_guard.write_row_with_state_guard(row_idx, state_guard);
        // Row slot reservation above has already modified the page.
        access.mark_dirty();
        let res = access.lock_undo(
            self.rt,
            effects,
            self.metadata,
            self.table_id,
            versioned_page_id,
            row_id,
            None,
        );
        debug_assert!(res.is_ok());
        // Apply insert
        let mut new_row = page.new_row(row_idx, var_offset);
        for v in &cols {
            new_row.add_col(self.metadata.col.as_ref(), v);
        }
        let new_row_id = new_row.finish();
        debug_assert!(new_row_id == row_id);
        effects.update_last_row_undo(undo_kind);
        for branch in index_branches {
            match branch.target {
                IndexBranchTarget::Hot { cts, entry } => {
                    access.link_for_unique_index(branch.key, cts, entry, branch.undo_vals);
                }
                IndexBranchTarget::ColdTerminal { delete_cts } => {
                    access.link_for_unique_index_cold_terminal(
                        branch.key,
                        delete_cts,
                        branch.undo_vals,
                    );
                }
            }
        }
        drop(access);

        // Here we do not unlock the page because we need to verify validity of unique index update
        // according to this insert.
        // There might be scenario that a deleted row or old version of updated row shares the same
        // key with this insert.
        // Then we have to link insert's undo head to that version via *INDEX* branch.
        // Hold the page guard in order to re-lock the undo head fast.
        //
        // create redo log.
        let redo_entry = RowRedo {
            page_id,
            row_id,
            kind: RowRedoKind::Insert(cols),
        };
        // Store redo in the statement buffer until the statement succeeds.
        effects.insert_row_redo(self.table_id, redo_entry);
        InsertRowIntoPage::Ok(row_id, page_guard)
    }
}

/// Prepared replacement state for a hot-row move update.
pub(super) struct PreparedHotMoveUpdate {
    /// Replacement row values to insert as the new hot row.
    pub(super) row: Vec<Val>,
    /// Indexed columns changed by the update, keyed by column number.
    pub(super) index_change_cols: FastHashMap<usize, Val>,
    /// Runtime unique-index branches linking the replacement to the old row.
    pub(super) index_branches: Vec<IndexBranch>,
}

/// Hot row-page mutation context shared by catalog and user-table accessors.
pub(super) struct HotRowMutator<'m, 'r, 'g> {
    metadata: &'m TableMetadata,
    table_id: TableID,
    rt: TrxRuntime<'r>,
    page_guard: &'g PageSharedGuard<RowPage>,
    row_id: RowID,
}

impl<'m, 'r, 'g> HotRowMutator<'m, 'r, 'g> {
    /// Create a hot-row mutator for one page row, metadata snapshot, and transaction.
    #[inline]
    pub(super) fn new(
        table_id: TableID,
        metadata: &'m TableMetadata,
        rt: TrxRuntime<'r>,
        page_guard: &'g PageSharedGuard<RowPage>,
        row_id: RowID,
    ) -> Self {
        Self {
            metadata,
            table_id,
            rt,
            page_guard,
            row_id,
        }
    }

    /// Lock a hot row by installing a provisional row undo entry.
    #[expect(clippy::await_holding_lock, reason = "clippy false positive")]
    #[inline]
    pub(super) async fn lock_for_write(
        &self,
        effects: &mut StmtEffects,
        key: Option<(usize, &[Val])>,
    ) -> LockRowForWrite<'g> {
        let page_guard = self.page_guard;
        let row_id = self.row_id;
        let page = page_guard.page();
        let ver_map = page_guard.unwrap_vmap();
        loop {
            #[cfg(test)]
            {
                use super::test_hooks::run_test_hot_row_write_before_state_lock_hook;
                run_test_hot_row_write_before_state_lock_hook();
            }
            let state_guard = ver_map.read_state();
            if *state_guard == RowPageState::Transition {
                return LockRowForWrite::RetryInTransition;
            }
            let mut access =
                page_guard.write_row_with_state_guard(page.row_idx(row_id), state_guard);
            let lock_undo = access.lock_undo(
                self.rt,
                effects,
                self.metadata,
                self.table_id,
                page_guard.versioned_page_id(),
                row_id,
                key,
            );
            match lock_undo {
                LockUndo::Ok => return LockRowForWrite::Ok(Some(access)),
                LockUndo::InvalidIndex => return LockRowForWrite::InvalidIndex,
                LockUndo::WriteConflict => return LockRowForWrite::WriteConflict,
                LockUndo::Preparing(listener) => {
                    if let Some(listener) = listener {
                        drop(access);

                        // Here we do not unlock the page, because the preparation time of commit is supposed
                        // to be short.
                        // And as active transaction is using this page, we don't want page evictor swap it onto
                        // disk.
                        // Other transactions can still access this page and modify other rows.

                        listener.await; // wait for that transaction to be committed.

                        // now we get back on current page.
                        // maybe another thread modify our row before the lock acquisition,
                        // so we need to recheck.
                    }
                }
            }
        }
    }

    /// Delete the hot row after validating that it belongs to the page.
    #[inline]
    pub(super) async fn delete(
        &self,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> OperationResult<DeleteInternal> {
        let redo_kind = if log_by_key {
            RowRedoKind::DeleteByPrimaryKey(SelectKey::new(index_no, key_vals.to_vec()))
        } else {
            RowRedoKind::Delete
        };
        self.delete_inner(effects, Some((index_no, key_vals)), redo_kind)
            .await
    }

    /// Delete one known page/row without revalidating an index lookup key.
    #[inline]
    pub(super) async fn delete_known_row(
        &self,
        effects: &mut StmtEffects,
    ) -> OperationResult<DeleteInternal> {
        self.delete_inner(effects, None, RowRedoKind::Delete).await
    }

    #[inline]
    async fn delete_inner(
        &self,
        effects: &mut StmtEffects,
        lookup_key: Option<(usize, &[Val])>,
        redo_kind: RowRedoKind,
    ) -> OperationResult<DeleteInternal> {
        let page_guard = self.page_guard;
        let row_id = self.row_id;
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        if !page.row_id_in_valid_range(row_id) {
            return Ok(DeleteInternal::NotFound);
        }
        // The undo-head lock is the conflict check for hot delete. Once the
        // lock is owned, the row-page delete bit becomes the latest image and
        // the same undo entry is rewritten to `Delete`.
        let mut lock_row = self.lock_for_write(effects, lookup_key).await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => Ok(DeleteInternal::NotFound),
            LockRowForWrite::WriteConflict => Err(Report::new(OperationError::WriteConflict)
                .attach("delete MVCC row-page write lock")),
            LockRowForWrite::RetryInTransition => Ok(DeleteInternal::RetryInTransition),
            LockRowForWrite::Ok(access) => {
                let mut access = access
                    .take()
                    .expect("successful hot-row write lock must provide row write access");
                if access.row().is_deleted() {
                    return Ok(DeleteInternal::NotFound);
                }
                access.delete_row();
                // update LOCK entry to DELETE entry.
                effects.update_last_row_undo(RowUndoKind::Delete);
                drop(access); // unlock row.
                drop(lock_row);
                // A keyed caller retains the page guard to derive current index
                // keys. A known-row caller may release it when keys were captured
                // before mutation.
                // create redo log.
                let redo_entry = RowRedo {
                    page_id,
                    row_id,
                    kind: redo_kind,
                };
                effects.insert_row_redo(self.table_id, redo_entry);
                Ok(DeleteInternal::Ok)
            }
        }
    }

    /// Update a locked hot row in place, or report move-update state.
    #[inline]
    pub(super) async fn update_inplace(
        &self,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        update: RowUpdateInput,
        log_by_key: bool,
    ) -> OperationResult<UpdateRowInplace> {
        let redo_key = log_by_key.then(|| SelectKey::new(index_no, key_vals.to_vec()));
        self.update_known_row_inner(effects, update, Some((index_no, key_vals)), redo_key)
            .await
    }

    /// Update one known page/row without revalidating an index lookup key.
    #[inline]
    pub(super) async fn update_known_row(
        &self,
        effects: &mut StmtEffects,
        update: RowUpdateInput,
    ) -> OperationResult<UpdateRowInplace> {
        self.update_known_row_inner(effects, update, None, None)
            .await
    }

    #[inline]
    async fn update_known_row_inner(
        &self,
        effects: &mut StmtEffects,
        update: RowUpdateInput,
        lookup_key: Option<(usize, &[Val])>,
        redo_key: Option<SelectKey>,
    ) -> OperationResult<UpdateRowInplace> {
        let page_guard = self.page_guard;
        let row_id = self.row_id;
        let page_id = page_guard.page_id();
        let page = page_guard.page();
        debug_assert!(
            update.as_view().is_valid_for(self.metadata.col.as_ref()),
            "row update values must be ordered, in range, and type-compatible"
        );
        if row_id < page.header.start_row_id
            || row_id >= page.header.start_row_id + page.header.max_row_count as u64
        {
            return Ok(UpdateRowInplace::RowNotFound(update));
        }
        // Modification is a current read: update the latest physical page
        // image, never an older version reconstructed from MVCC undo. The image
        // must remain stable until the undo-head lock is installed. The lock
        // path also rejects stale index candidates whose latest key differs.
        let mut lock_row = self.lock_for_write(effects, lookup_key).await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => Ok(UpdateRowInplace::RowNotFound(update)),
            LockRowForWrite::WriteConflict => Err(Report::new(OperationError::WriteConflict)
                .attach("update MVCC row-page write lock")),
            LockRowForWrite::RetryInTransition => Ok(UpdateRowInplace::RetryInTransition(update)),
            LockRowForWrite::Ok(access) => {
                let mut access = access
                    .take()
                    .expect("successful hot-row write lock must provide row write access");
                let frozen = access.page_state() == RowPageState::Frozen;
                if access.row().is_deleted() {
                    drop(access);
                    drop(lock_row);
                    return Ok(UpdateRowInplace::RowDeleted(update));
                }
                Ok(
                    match access.update_row(self.metadata.col.as_ref(), update.as_view(), frozen) {
                        UpdateRow::NoFreeSpaceOrFrozen(old_row) => {
                            // The hot row cannot be updated in place because the
                            // page has no reusable space or has been frozen for
                            // tuple movement. Convert this statement into a move
                            // update: delete the old RowID with undo, insert the
                            // replacement as a new hot RowID, and connect unique
                            // owners with runtime branches when older snapshots
                            // may still need the old version.
                            //
                            // Mark page data as deleted.
                            access.delete_row();
                            // Update LOCK entry to DELETE entry.
                            effects.update_last_row_undo(RowUndoKind::Delete);
                            drop(access); // unlock row
                            drop(lock_row);
                            // Here we do not unlock page because we need to perform out-of-place
                            // update and link undo entries of two rows via index branches.
                            // The re-lock of current undo is required.
                            let redo_entry = RowRedo {
                                page_id,
                                row_id,
                                // use DELETE for redo is ok, no version chain should be maintained if recovering from redo.
                                kind: redo_key
                                    .clone()
                                    .map(RowRedoKind::DeleteByPrimaryKey)
                                    .unwrap_or(RowRedoKind::Delete),
                            };
                            effects.insert_row_redo(self.table_id, redo_entry);
                            UpdateRowInplace::NoFreeSpaceOrFrozen(row_id, old_row, update)
                        }
                        UpdateRow::Ok(mut row) => {
                            // In-place update keeps the RowID stable. Changed new
                            // values are moved into redo on the successful path.
                            let mut index_change_cols = FastHashMap::default();
                            // perform in-place update.
                            let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                            for UpdateCol { idx, val } in update {
                                if let Some((old_val, var_offset)) =
                                    row.different(self.metadata.col.as_ref(), idx, &val)
                                {
                                    // we also check whether the value change is related to any index,
                                    // so we can update index later.
                                    if self.metadata.idx.index_columns().contains(&idx) {
                                        index_change_cols.insert(idx, old_val.clone());
                                    }
                                    // actual update
                                    row.update_col(self.metadata.col.as_ref(), idx, &val);
                                    // record undo and redo
                                    undo_cols.push(UndoCol {
                                        idx,
                                        val: old_val,
                                        var_offset,
                                    });
                                    redo_cols.push(UpdateCol { idx, val });
                                }
                            }
                            // The provisional row lock now becomes the operation
                            // kind that MVCC reads and rollback will interpret.
                            effects.update_last_row_undo(RowUndoKind::Update(undo_cols));
                            drop(access); // unlock the row.
                            drop(lock_row);
                            // we may still need this page if we'd like to update index.
                            if !redo_cols.is_empty() {
                                // A no-op update still used a row lock, but only a
                                // real value change needs redo.
                                let redo_entry = RowRedo {
                                    page_id,
                                    row_id,
                                    kind: redo_key
                                        .map(|key| {
                                            RowRedoKind::UpdateByPrimaryKey(key, redo_cols.clone())
                                        })
                                        .unwrap_or(RowRedoKind::Update(redo_cols)),
                                };
                                effects.insert_row_redo(self.table_id, redo_entry);
                            }
                            UpdateRowInplace::Ok(row_id, index_change_cols)
                        }
                    },
                )
            }
        }
    }

    /// Prepare a replacement row and runtime branches for a hot-row move update.
    #[inline]
    pub(super) fn prepare_move_update(
        &self,
        old_row: Vec<Val>,
        update: RowUpdateInput,
    ) -> PreparedHotMoveUpdate {
        let old_guard = self.page_guard;
        let old_id = self.row_id;
        // Build the replacement hot row and remember indexed old values. The
        // caller already turned the old hot row's first undo entry into
        // `Delete`, so this path is logically delete-old plus insert-new.
        let (new_row, undo_vals, index_change_cols) = {
            let mut index_change_cols = FastHashMap::default();
            match update {
                RowUpdateInput::Sparse(update) => {
                    let mut undo_vals = Vec::with_capacity(update.len());
                    let mut row = old_row;
                    for UpdateCol { idx, val } in update {
                        if row[idx] != val {
                            let old_val = replace(&mut row[idx], val);
                            if self.metadata.idx.index_columns().contains(&idx) {
                                index_change_cols.insert(idx, old_val.clone());
                            }
                            undo_vals.push(UpdateCol { idx, val: old_val });
                        }
                    }
                    (row, undo_vals, index_change_cols)
                }
                RowUpdateInput::FullRow(row) => {
                    let mut undo_vals = Vec::with_capacity(row.len());
                    debug_assert!(row.len() == old_row.len());
                    for (idx, old_val) in old_row.into_iter().enumerate() {
                        if old_val != row[idx] {
                            if self.metadata.idx.index_columns().contains(&idx) {
                                index_change_cols.insert(idx, old_val.clone());
                            }
                            undo_vals.push(UpdateCol { idx, val: old_val });
                        }
                    }
                    (row, undo_vals, index_change_cols)
                }
            }
        };
        let index_branches = {
            // Unique indexes keep only the latest owner in MemIndex. When a
            // move update changes RowID, the new hot row's insert undo carries
            // runtime branches back to the deleted old hot row so older
            // snapshots can still resolve the previous unique-key owner.
            let old_access = old_guard.read_row_by_id(old_id);
            let undo_head = old_access.undo_head().expect("undo head");
            debug_assert!(self.rt.is_same_trx(undo_head));
            let old_entry = old_access.first_undo_entry().expect("old undo entry");
            debug_assert!(matches!(old_entry.as_ref().kind, RowUndoKind::Delete));
            self.metadata
                .idx
                .active_indexes()
                .filter(|(_, index)| index.unique())
                .map(|(index_no, index)| {
                    let vals = index
                        .cols
                        .iter()
                        .map(|key| new_row[key.col_no as usize].clone())
                        .collect();
                    IndexBranch {
                        key: SelectKey::new(index_no, vals),
                        target: IndexBranchTarget::Hot {
                            cts: undo_head.ts(),
                            entry: old_entry.clone(),
                        },
                        // This deep-clones the same changed-column delta once per
                        // unique index. Multiple unique indexes are rare, and this
                        // moved-row path is cold, so keep the simpler Vec ownership
                        // until profiling justifies sharing it with Arc<[UpdateCol]>.
                        undo_vals: undo_vals.clone(),
                    }
                })
                .collect::<Vec<_>>()
        };
        PreparedHotMoveUpdate {
            row: new_row,
            index_change_cols,
            index_branches,
        }
    }
}

/// Read a validated hot row page through MVCC.
#[inline]
pub(super) fn read_hot_row_mvcc(
    rt: TrxRuntime<'_>,
    metadata: &TableMetadata,
    page_guard: &PageSharedGuard<RowPage>,
    row_id: RowID,
    key: Option<(usize, &[Val])>,
    read_set: &[usize],
) -> SelectMvcc {
    let access = page_guard.read_row_by_id(row_id);
    match access.read_row_mvcc(rt.ctx(), metadata, read_set, key) {
        ReadRow::Ok(vals) => SelectMvcc::Found(vals),
        ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
    }
}
