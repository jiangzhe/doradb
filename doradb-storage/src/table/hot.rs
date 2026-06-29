use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::catalog::TableMetadata;
use crate::id::{RowID, TableID};
use crate::log::redo::{RowRedo, RowRedoKind};
use crate::map::FastHashMap;
use crate::row::ops::{SelectKey, UndoCol, UpdateCol, UpdateRow};
use crate::row::{RowPage, RowRead};
use crate::table::UpdateRowInplace;
use crate::trx::TrxRuntime;
use crate::trx::row::{LockRowForWrite, LockUndo, RowReadAccess, RowWriteAccess};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, IndexBranchTarget, RowUndoKind};
use crate::trx::ver_map::RowPageState;
use crate::value::Val;
use std::mem;

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
        key: Option<&SelectKey>,
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
                Some(self.rt.sts()),
                false,
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
    #[inline]
    pub(super) async fn update_inplace(
        &self,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        key: &SelectKey,
        row_id: RowID,
        mut update: Vec<UpdateCol>,
    ) -> UpdateRowInplace {
        let page_id = page_guard.page_id();
        let (_, page) = page_guard.ctx_and_page();
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
        // The row-page image must not change until the undo-head lock is
        // installed. The lock path also rejects stale index candidates whose
        // latest hot-row key no longer matches the lookup key.
        let mut lock_row = self
            .lock_for_write(effects, &page_guard, row_id, Some(key))
            .await;
        match &mut lock_row {
            LockRowForWrite::InvalidIndex => UpdateRowInplace::RowNotFound,
            LockRowForWrite::WriteConflict => UpdateRowInplace::WriteConflict,
            LockRowForWrite::RetryInTransition => UpdateRowInplace::RetryInTransition,
            LockRowForWrite::Ok(access) => {
                let mut access = access
                    .take()
                    .expect("successful hot-row write lock must provide row write access");
                let frozen = access.page_state() == RowPageState::Frozen;
                if access.row().is_deleted() {
                    return UpdateRowInplace::RowDeleted;
                }
                match access.update_row(self.metadata.col.as_ref(), &update, frozen) {
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
                            kind: RowRedoKind::Delete,
                        };
                        effects.insert_row_redo(self.table_id, redo_entry);
                        UpdateRowInplace::NoFreeSpace(row_id, old_row, update, page_guard)
                    }
                    UpdateRow::Ok(mut row) => {
                        // In-place update keeps the RowID stable. Only changed
                        // columns are copied into undo/redo; indexed old values
                        // are retained so MemIndex can shadow or remap keys
                        // after the row latch is released.
                        let mut index_change_cols = FastHashMap::default();
                        // perform in-place update.
                        let (mut undo_cols, mut redo_cols) = (vec![], vec![]);
                        for uc in &mut update {
                            if let Some((old_val, var_offset)) =
                                row.different(self.metadata.col.as_ref(), uc.idx, &uc.val)
                            {
                                let new_val = mem::take(&mut uc.val);
                                // we also check whether the value change is related to any index,
                                // so we can update index later.
                                if self.metadata.idx.index_columns().contains(&uc.idx) {
                                    index_change_cols.insert(uc.idx, old_val.clone());
                                }
                                // actual update
                                row.update_col(self.metadata.col.as_ref(), uc.idx, &new_val);
                                // record undo and redo
                                undo_cols.push(UndoCol {
                                    idx: uc.idx,
                                    val: old_val,
                                    var_offset,
                                });
                                redo_cols.push(UpdateCol {
                                    idx: uc.idx,
                                    // new value no longer needed, so safe to take it here.
                                    val: new_val,
                                });
                            }
                        }
                        // The provisional row lock now becomes the operation
                        // kind that MVCC reads and rollback will interpret.
                        effects.update_last_row_undo(RowUndoKind::Update(undo_cols));
                        // Mark this access as update, so page-level max_ins_sts will be updated.
                        access.enable_ins_or_update();
                        drop(access); // unlock the row.
                        drop(lock_row);
                        // we may still need this page if we'd like to update index.
                        if !redo_cols.is_empty() {
                            // A no-op update still used a row lock, but only a
                            // real value change needs redo.
                            let redo_entry = RowRedo {
                                page_id,
                                row_id,
                                kind: RowRedoKind::Update(redo_cols),
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
        old_row: Vec<(Val, Option<u16>)>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> PreparedHotMoveUpdate {
        // Build the replacement hot row and remember indexed old values. The
        // caller already turned the old hot row's first undo entry into
        // `Delete`, so this path is logically delete-old plus insert-new.
        let (new_row, old_vals, index_change_cols) = {
            let mut index_change_cols = FastHashMap::default();
            let mut row = Vec::with_capacity(old_row.len());
            let mut old_vals = Vec::with_capacity(old_row.len());
            for (v, _) in old_row {
                old_vals.push(v.clone());
                row.push(v);
            }
            for mut uc in update {
                let old_val = &mut row[uc.idx];
                if old_val != &uc.val {
                    if self.metadata.idx.index_columns().contains(&uc.idx) {
                        index_change_cols.insert(uc.idx, old_val.clone());
                    }
                    // swap old value and new value
                    mem::swap(&mut uc.val, old_val);
                }
            }
            (row, old_vals, index_change_cols)
        };
        let undo_vals: Vec<UpdateCol> = new_row
            .iter()
            .enumerate()
            .filter_map(|(idx, val)| {
                if val != &old_vals[idx] {
                    Some(UpdateCol {
                        idx,
                        val: old_vals[idx].clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
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

/// Prepared replacement state for a hot-row move update.
pub(super) struct PreparedHotMoveUpdate {
    /// Replacement row values to insert as the new hot row.
    pub(super) row: Vec<Val>,
    /// Indexed columns changed by the update, keyed by column number.
    pub(super) index_change_cols: FastHashMap<usize, Val>,
    /// Runtime unique-index branches linking the replacement to the old row.
    pub(super) index_branches: Vec<IndexBranch>,
}
