use crate::buffer::guard::PageSharedGuard;
use crate::catalog::TableMetadata;
use crate::id::{RowID, TableID};
use crate::log::redo::{RowRedo, RowRedoKind};
use crate::map::FastHashMap;
use crate::row::ops::{
    ReadRow, RowUpdateInput, SelectKey, SelectMvcc, UndoCol, UpdateCol, UpdateRow,
};
use crate::row::{RowPage, RowRead, var_len_for_insert};
use crate::table::{DeleteInternal, InsertRowIntoPage, UpdateRowInplace};
use crate::trx::TrxRuntime;
use crate::trx::row::{LockRowForWrite, LockUndo, RowReadAccess, RowWriteAccess};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, IndexBranchTarget, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::value::Val;
use std::mem::replace;

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
        let (page_ctx, page) = page_guard.ctx_and_page();
        let ver_map = page_ctx
            .row_ver()
            .expect("hot-row inserts require row-version metadata on page context");
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
        let mut access = RowWriteAccess::new_with_state_guard(page, page_ctx, row_idx, state_guard);
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

/// Hot row-page delete context shared by catalog and user-table accessors.
pub(super) struct HotRowDeleter<'m, 'r> {
    metadata: &'m TableMetadata,
    table_id: TableID,
    rt: TrxRuntime<'r>,
}

impl<'m, 'r> HotRowDeleter<'m, 'r> {
    /// Create a hot-row deleter for one metadata snapshot and transaction.
    #[inline]
    pub(super) fn new(table_id: TableID, metadata: &'m TableMetadata, rt: TrxRuntime<'r>) -> Self {
        Self {
            metadata,
            table_id,
            rt,
        }
    }

    /// Delete one hot row after validating that the row belongs to the page.
    #[inline]
    pub(super) async fn delete(
        &self,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        index_no: usize,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> DeleteInternal {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
        if !page.row_id_in_valid_range(row_id) {
            return DeleteInternal::NotFound;
        }
        // The undo-head lock is the conflict check for hot delete. Once the
        // lock is owned, the row-page delete bit becomes the latest image and
        // the same undo entry is rewritten to `Delete`.
        let mut lock_row = HotRowUpdater::new(self.table_id, self.metadata, self.rt)
            .lock_for_write(effects, &page_guard, row_id, Some((index_no, key_vals)))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => DeleteInternal::NotFound,
            LockRowForWrite::WriteConflict => DeleteInternal::WriteConflict,
            LockRowForWrite::RetryInTransition => DeleteInternal::RetryInTransition,
            LockRowForWrite::Ok(access) => {
                let mut access = access
                    .take()
                    .expect("successful hot-row write lock must provide row write access");
                if access.row().is_deleted() {
                    return DeleteInternal::NotFound;
                }
                access.delete_row();
                // update LOCK entry to DELETE entry.
                effects.update_last_row_undo(RowUndoKind::Delete);
                drop(access); // unlock row.
                drop(lock_row);
                // hold page lock in order to update index later.
                // create redo log.
                let redo_entry = RowRedo {
                    page_id,
                    row_id,
                    kind: if log_by_key {
                        RowRedoKind::DeleteByPrimaryKey(SelectKey::new(index_no, key_vals.to_vec()))
                    } else {
                        RowRedoKind::Delete
                    },
                };
                effects.insert_row_redo(self.table_id, redo_entry);
                DeleteInternal::Ok(page_guard)
            }
        }
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

/// Hot row-page update context shared by catalog and user-table accessors.
pub(super) struct HotRowUpdater<'m, 'r> {
    metadata: &'m TableMetadata,
    table_id: TableID,
    rt: TrxRuntime<'r>,
}

impl<'m, 'r> HotRowUpdater<'m, 'r> {
    /// Create a hot-row updater for one metadata snapshot and transaction.
    #[inline]
    pub(super) fn new(table_id: TableID, metadata: &'m TableMetadata, rt: TrxRuntime<'r>) -> Self {
        Self {
            metadata,
            table_id,
            rt,
        }
    }

    /// Lock a hot row by installing a provisional row undo entry.
    #[expect(clippy::await_holding_lock, reason = "clippy false positive")]
    #[inline]
    pub(super) async fn lock_for_write<'b>(
        &self,
        effects: &mut StmtEffects,
        page_guard: &'b PageSharedGuard<RowPage>,
        row_id: RowID,
        key: Option<(usize, &[Val])>,
    ) -> LockRowForWrite<'b> {
        let (page_ctx, page) = page_guard.ctx_and_page();
        let ver_map = page_ctx
            .row_ver()
            .expect("hot-row writes require row-version metadata on page context");
        loop {
            let state_guard = ver_map.read_state();
            if *state_guard == RowPageState::Transition {
                return LockRowForWrite::RetryInTransition;
            }
            let mut access = RowWriteAccess::new_with_state_guard(
                page,
                page_ctx,
                page.row_idx(row_id),
                state_guard,
            );
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

    /// Update a locked hot row in place, or report move-update state.
    #[expect(
        clippy::too_many_arguments,
        reason = "hot row updates require row, key, update, and logging context"
    )]
    #[inline]
    pub(super) async fn update_inplace(
        &self,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        update: RowUpdateInput,
        log_by_key: bool,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
        debug_assert!(
            update.as_view().is_valid_for(self.metadata.col.as_ref()),
            "row update values must be ordered, in range, and type-compatible"
        );
        if row_id < page.header.start_row_id
            || row_id >= page.header.start_row_id + page.header.max_row_count as u64
        {
            return UpdateRowInplace::RowNotFound(update);
        }
        // The row-page image must not change until the undo-head lock is
        // installed. The lock path also rejects stale index candidates whose
        // latest hot-row key no longer matches the lookup key.
        let mut lock_row = self
            .lock_for_write(effects, &page_guard, row_id, Some((index_no, key_vals)))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => UpdateRowInplace::RowNotFound(update),
            LockRowForWrite::WriteConflict => UpdateRowInplace::WriteConflict,
            LockRowForWrite::RetryInTransition => UpdateRowInplace::RetryInTransition(update),
            LockRowForWrite::Ok(access) => {
                let mut access = access
                    .take()
                    .expect("successful hot-row write lock must provide row write access");
                let frozen = access.page_state() == RowPageState::Frozen;
                if access.row().is_deleted() {
                    drop(access);
                    drop(lock_row);
                    return UpdateRowInplace::RowDeleted(update);
                }
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
                            kind: if log_by_key {
                                RowRedoKind::DeleteByPrimaryKey(SelectKey::new(
                                    index_no,
                                    key_vals.to_vec(),
                                ))
                            } else {
                                RowRedoKind::Delete
                            },
                        };
                        effects.insert_row_redo(self.table_id, redo_entry);
                        UpdateRowInplace::NoFreeSpace(row_id, old_row, update, page_guard)
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
                                kind: if log_by_key {
                                    RowRedoKind::UpdateByPrimaryKey(
                                        SelectKey::new(index_no, key_vals.to_vec()),
                                        redo_cols,
                                    )
                                } else {
                                    RowRedoKind::Update(redo_cols)
                                },
                            };
                            effects.insert_row_redo(self.table_id, redo_entry);
                        }
                        UpdateRowInplace::Ok(row_id, index_change_cols, page_guard)
                    }
                }
            }
        }
    }

    /// Prepare a replacement row and runtime branches for a hot-row move update.
    #[inline]
    pub(super) fn prepare_move_update(
        &self,
        old_row: Vec<Val>,
        update: RowUpdateInput,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> PreparedHotMoveUpdate {
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
            let (page_ctx, page) = old_guard.ctx_and_page();
            let old_access = RowReadAccess::new(page, page_ctx, page.row_idx(old_id));
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
        old_guard.set_dirty(); // mark as dirty page.
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
    let (page_ctx, page) = page_guard.ctx_and_page();
    let access = RowReadAccess::new(page, page_ctx, page.row_idx(row_id));
    match access.read_row_mvcc(rt.ctx(), metadata, read_set, key) {
        ReadRow::Ok(vals) => SelectMvcc::Found(vals),
        ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
    }
}
