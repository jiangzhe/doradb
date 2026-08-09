//! Index-driven MVCC mutation orchestration.
//!
//! The mutation stream owns candidate traversal, while [`IndexMutator`]
//! retains the statement-scoped table, transaction, root, validation, and
//! effect context used to resolve and mutate each candidate sequentially.

use super::{
    ColdLatestRow, LazyRow, LazyRowSource, RowIdMove, UserTableAccessor, WriteIndexKeySet,
    read_latest_cold_row,
};
use crate::buffer::guard::PageSharedGuard;
use crate::error::{
    DataIntegrityError, DiscloseError, DiscloseResultExt, MultiDomainResultExt, OperationError,
    Result,
};
use crate::id::{PageID, RowID};
use crate::index::{LwcRowLocation, RowLocation};
use crate::row::RowPage;
use crate::row::ops::{RowMutation, RowUpdateInput, TableMutationOutcome, UpdateCol};
use crate::table::dml_validator::DmlValidator;
use crate::table::hot::{DeleteInternal, HotRowMutator, UpdateRowInplace};
use crate::table::{DeletionClaim, DeletionError, TableRootSnapshot};
use crate::trx::TrxRuntime;
use crate::trx::row::{BoundIndexCandidate, LockRowForWrite, RowWriteAccess};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::mem;
use std::sync::Arc;

/// Whether candidate processing finished or must restart row-location resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateProgress {
    Done,
    RetryLocation,
}

/// Statement-scoped context for index-driven row mutation.
pub(super) struct IndexMutator<'a, 'op, 'r, 'ctx> {
    accessor: &'a UserTableAccessor<'op>,
    rt: TrxRuntime<'r>,
    effects: &'a mut StmtEffects,
    root_snapshot: &'a TableRootSnapshot<'ctx>,
    validator: Option<DmlValidator<'a>>,
}

impl<'a, 'op, 'r, 'ctx> IndexMutator<'a, 'op, 'r, 'ctx> {
    /// Creates one mutator reused for every candidate emitted by the operation.
    #[inline]
    pub(super) fn new(
        accessor: &'a UserTableAccessor<'op>,
        rt: TrxRuntime<'r>,
        effects: &'a mut StmtEffects,
        root_snapshot: &'a TableRootSnapshot<'ctx>,
        validate_updates: bool,
    ) -> Self {
        Self {
            accessor,
            rt,
            effects,
            root_snapshot,
            validator: validate_updates.then(|| DmlValidator::new(accessor.metadata())),
        }
    }

    /// Resolves and mutates one candidate, restarting its physical route after waits.
    pub(super) async fn mutate_index_candidate<F>(
        &mut self,
        candidate: BoundIndexCandidate<'_>,
        value_buffer: &mut Vec<Val>,
        outcome: &mut TableMutationOutcome,
        mutate_row: &mut F,
    ) -> Result<()>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        loop {
            let location = self
                .accessor
                .find_row_location(self.rt.pool_guards(), candidate.row_id)
                .await
                .disclose()?;
            let progress = match location {
                RowLocation::NotFound => CandidateProgress::Done,
                RowLocation::RowPage(page_id) => {
                    self.mutate_hot_index_candidate(
                        &candidate,
                        page_id,
                        value_buffer,
                        outcome,
                        mutate_row,
                    )
                    .await?
                }
                RowLocation::LwcBlock(location) => {
                    self.mutate_cold_index_candidate(
                        &candidate,
                        location,
                        value_buffer,
                        outcome,
                        mutate_row,
                    )
                    .await?
                }
            };
            match progress {
                CandidateProgress::Done => return Ok(()),
                CandidateProgress::RetryLocation => (),
            }
        }
    }

    /// Acquires one hot candidate and applies its callback while ownership is held.
    async fn mutate_hot_index_candidate<F>(
        &mut self,
        candidate: &BoundIndexCandidate<'_>,
        page_id: PageID,
        value_buffer: &mut Vec<Val>,
        outcome: &mut TableMutationOutcome,
        mutate_row: &mut F,
    ) -> Result<CandidateProgress>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        let accessor = self.accessor;
        let Some(page_guard) = accessor
            .mem()
            .try_get_validated_row_page_shared_result(
                self.rt.pool_guards(),
                page_id,
                candidate.row_id,
            )
            .await
            .disclose()?
        else {
            return Ok(CandidateProgress::RetryLocation);
        };
        let mut locked = HotRowMutator::new(
            accessor.table_id(),
            accessor.metadata(),
            self.rt,
            &page_guard,
            candidate.row_id,
        )
        .lock_index_candidate_for_write(self.effects, candidate)
        .await
        .disclose()?;
        let access = match &mut locked {
            LockRowForWrite::InvalidIndex => return Ok(CandidateProgress::Done),
            LockRowForWrite::WriteConflict => {
                return Err(Report::new(OperationError::WriteConflict)
                    .attach(format!(
                        "index-driven mutation hot-row ownership: row_id={}",
                        candidate.row_id
                    ))
                    .disclose());
            }
            LockRowForWrite::RetryInTransition => {
                drop(locked);
                drop(page_guard);
                accessor
                    .wait_transition_route_or_poison(self.rt, candidate.row_id)
                    .await
                    .disclose()?;
                return Ok(CandidateProgress::RetryLocation);
            }
            LockRowForWrite::Ok(access) => access
                .take()
                .expect("candidate ownership must retain hot-row write access"),
        };
        drop(locked);
        self.mutate_owned_hot_index_candidate(
            candidate,
            &page_guard,
            access,
            value_buffer,
            outcome,
            mutate_row,
        )
        .await?;
        Ok(CandidateProgress::Done)
    }

    /// Validates, claims, and mutates one persisted candidate.
    async fn mutate_cold_index_candidate<F>(
        &mut self,
        candidate: &BoundIndexCandidate<'_>,
        location: LwcRowLocation,
        value_buffer: &mut Vec<Val>,
        outcome: &mut TableMutationOutcome,
        mutate_row: &mut F,
    ) -> Result<CandidateProgress>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        let accessor = self.accessor;
        let LwcRowLocation {
            block_id,
            row_idx,
            row_shape_fingerprint,
        } = location;
        let storage = accessor.column_storage();
        let file_kind = storage.file().file_kind();
        let persisted = storage
            .load_lwc_block(self.rt.pool_guards().disk_guard(), block_id)
            .await
            .disclose()?;
        let block = persisted.block();
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, row shape fingerprint mismatch"
                ))
                .disclose());
        }
        let index_spec = accessor
            .metadata()
            .idx
            .require_index_spec(candidate.index_no)
            .expect("IndexWrite admission must retain an active index spec");
        let key_vals = block
            .decode_index_key_values(accessor.metadata().col.as_ref(), index_spec, row_idx)
            .attach_with(|| format!("file={file_kind}, block=lwc_block, block_id={block_id}"))
            .disclose()?;
        if !candidate.matches_key(&key_vals) {
            return Ok(CandidateProgress::Done);
        }
        match read_latest_cold_row(
            accessor.lwc_deletion_buffer(),
            self.rt.status().as_ref(),
            candidate.row_id,
            false,
        ) {
            ColdLatestRow::Readable => (),
            ColdLatestRow::NotFound => return Ok(CandidateProgress::Done),
            ColdLatestRow::WriteConflict => {
                return Err(Report::new(OperationError::WriteConflict)
                    .attach(format!(
                        "index-driven mutation cold-row ownership: row_id={}",
                        candidate.row_id
                    ))
                    .disclose());
            }
            ColdLatestRow::Preparing(listener) => {
                drop(persisted);
                self.rt.wait_prepare_or_poison(listener).await.disclose()?;
                return Ok(CandidateProgress::RetryLocation);
            }
        }
        accessor.debug_assert_table_write_lock_held(self.rt);
        match accessor.lwc_deletion_buffer().claim_ref(
            candidate.row_id,
            Arc::clone(self.rt.status()),
            self.rt.sts(),
        ) {
            Ok(DeletionClaim::Acquired) => (),
            Ok(DeletionClaim::Preparing(listener)) => {
                drop(persisted);
                self.rt.wait_prepare_or_poison(listener).await.disclose()?;
                return Ok(CandidateProgress::RetryLocation);
            }
            Err(DeletionError::AlreadyDeleted) => {
                return Ok(CandidateProgress::Done);
            }
            Err(DeletionError::WriteConflict) => {
                return Err(Report::new(OperationError::WriteConflict)
                    .attach(format!(
                        "index-driven mutation cold-row ownership: row_id={}",
                        candidate.row_id
                    ))
                    .disclose());
            }
        }
        self.effects.push_row_undo(OwnedRowUndo::new(
            self.effects.stmt_no(),
            accessor.table_id(),
            None,
            candidate.row_id,
            RowUndoKind::Lock,
        ));
        let source = LazyRowSource::Cold {
            block,
            column_layout: accessor.metadata().col.as_ref(),
            row_idx,
            file_kind,
            block_id,
        };
        let mut lazy_row = LazyRow::new(
            source,
            mem::take(value_buffer),
            accessor.metadata().col.col_count(),
        );
        match mutate_row(&mut lazy_row)? {
            RowMutation::Skip => {
                *value_buffer = lazy_row.into_reusable_buffer();
                self.cancel_owned_cold_row(candidate.row_id);
                drop(persisted);
            }
            RowMutation::Delete => {
                outcome.delete_count += 1;
                let (index_keys, reusable) = lazy_row.into_index_keys(accessor).disclose()?;
                *value_buffer = reusable;
                drop(persisted);
                accessor
                    .finish_owned_cold_delete_effects(
                        self.rt,
                        self.effects,
                        candidate.row_id,
                        index_keys,
                        self.root_snapshot,
                    )
                    .await
                    .disclose()?;
            }
            RowMutation::Update(update) => {
                outcome.update_count += 1;
                accessor
                    .validate_table_mutation_update(self.validator.as_ref(), &update)
                    .disclose()?;
                if candidate.unique {
                    self.validate_unique_driver_update(&mut lazy_row, candidate, &update)?;
                }
                if update.is_empty() {
                    *value_buffer = lazy_row.into_reusable_buffer();
                    self.cancel_owned_cold_row(candidate.row_id);
                    drop(persisted);
                } else {
                    let (old_row, reusable) = lazy_row.into_full_row().disclose()?;
                    *value_buffer = reusable;
                    drop(persisted);
                    accessor
                        .update_owned_cold_row(
                            self.rt,
                            self.effects,
                            candidate.row_id,
                            old_row,
                            update,
                            self.root_snapshot,
                        )
                        .await
                        .disclose()?;
                }
            }
        }
        Ok(CandidateProgress::Done)
    }

    /// Applies the callback and selected action to an already-owned hot row.
    async fn mutate_owned_hot_index_candidate<F>(
        &mut self,
        candidate: &BoundIndexCandidate<'_>,
        page_guard: &PageSharedGuard<RowPage>,
        access: RowWriteAccess<'_>,
        value_buffer: &mut Vec<Val>,
        outcome: &mut TableMutationOutcome,
        mutate_row: &mut F,
    ) -> Result<()>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        let accessor = self.accessor;
        let source = LazyRowSource::HotWrite {
            access,
            column_layout: accessor.metadata().col.as_ref(),
        };
        let mut lazy_row = LazyRow::new(
            source,
            mem::take(value_buffer),
            accessor.metadata().col.col_count(),
        );
        match mutate_row(&mut lazy_row)? {
            RowMutation::Skip => {
                let (access, reusable) = lazy_row.into_hot_write_reusable_buffer();
                *value_buffer = reusable;
                self.cancel_owned_hot_row(access);
                Ok(())
            }
            RowMutation::Delete => {
                outcome.delete_count += 1;
                let (access, index_keys, reusable) =
                    lazy_row.into_hot_write_index_keys(accessor).disclose()?;
                *value_buffer = reusable;
                let result = HotRowMutator::new(
                    accessor.table_id(),
                    accessor.metadata(),
                    self.rt,
                    page_guard,
                    candidate.row_id,
                )
                .delete_owned_row(self.effects, access);
                assert!(
                    matches!(result, DeleteInternal::Ok),
                    "owned hot candidate changed while its write latch was retained"
                );
                let proof = accessor.owned_row_page_index_set_proof(
                    candidate.row_id,
                    index_keys,
                    self.root_snapshot,
                );
                accessor
                    .defer_delete_owned_row_index_set(self.rt, self.effects, proof)
                    .await
                    .attach("index-driven mutation hot delete index masking")
                    .disclose()
            }
            RowMutation::Update(update) => {
                outcome.update_count += 1;
                accessor
                    .validate_table_mutation_update(self.validator.as_ref(), &update)
                    .disclose()?;
                if candidate.unique {
                    self.validate_unique_driver_update(&mut lazy_row, candidate, &update)?;
                }
                let (access, reusable) = lazy_row.into_hot_write_reusable_buffer();
                *value_buffer = reusable;
                if update.is_empty() {
                    self.cancel_owned_hot_row(access);
                    return Ok(());
                }
                let result = HotRowMutator::new(
                    accessor.table_id(),
                    accessor.metadata(),
                    self.rt,
                    page_guard,
                    candidate.row_id,
                )
                .update_owned_row(
                    self.effects,
                    RowUpdateInput::Sparse(update),
                    access,
                );
                match result {
                    UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                        debug_assert_eq!(candidate.row_id, new_row_id);
                        if !index_change_cols.is_empty() {
                            accessor
                                .update_indexes_only_key_change(
                                    self.rt,
                                    self.effects,
                                    candidate.row_id,
                                    page_guard,
                                    &index_change_cols,
                                    self.root_snapshot,
                                )
                                .await
                                .attach("index-driven mutation hot key change")
                                .disclose()?;
                        }
                        Ok(())
                    }
                    UpdateRowInplace::NoFreeSpaceOrFrozen(old_row_id, old_row, update) => {
                        let old_index_keys = WriteIndexKeySet::from_full_row(accessor, &old_row);
                        let move_guard = accessor
                            .mem()
                            .must_get_row_page_shared(self.rt.pool_guards(), page_guard.page_id())
                            .await
                            .disclose()?;
                        let (new_row_id, index_change_cols, new_guard) = accessor
                            .move_update_for_space(
                                self.rt,
                                self.effects,
                                old_row,
                                update,
                                old_row_id,
                                move_guard,
                            )
                            .await
                            .disclose()?;
                        let proof = accessor.owned_row_page_index_set_proof(
                            old_row_id,
                            old_index_keys,
                            self.root_snapshot,
                        );
                        if index_change_cols.is_empty() {
                            accessor
                                .update_indexes_only_row_id_change(
                                    self.rt,
                                    self.effects,
                                    old_row_id,
                                    new_row_id,
                                    proof,
                                )
                                .await
                                .attach("index-driven mutation hot move index update")
                                .disclose()?;
                        } else {
                            accessor
                                .update_indexes_may_both_change(
                                    self.rt,
                                    self.effects,
                                    RowIdMove::new(old_row_id, new_row_id),
                                    &index_change_cols,
                                    &new_guard,
                                    proof,
                                )
                                .await
                                .attach("index-driven mutation hot move index update")
                                .disclose()?;
                        }
                        Ok(())
                    }
                    UpdateRowInplace::RowDeleted(_)
                    | UpdateRowInplace::RowNotFound(_)
                    | UpdateRowInplace::RetryInTransition(_) => {
                        unreachable!(
                            "owned hot candidate changed while its write latch was retained"
                        )
                    }
                }
            }
        }
    }

    #[inline]
    fn cancel_owned_hot_row(&mut self, mut access: RowWriteAccess<'_>) {
        let metadata = self.accessor.metadata();
        self.effects.cancel_last_row_undo_lock(|undo| {
            access.rollback_first_undo(metadata, undo);
        });
    }

    #[inline]
    fn cancel_owned_cold_row(&mut self, row_id: RowID) {
        let deletion_buffer = self.accessor.lwc_deletion_buffer();
        let status = self.rt.status();
        self.effects.cancel_last_row_undo_lock(|_| {
            assert!(
                deletion_buffer.remove_ref_if_owned(row_id, status),
                "provisional cold-row marker ownership changed before callback cancellation"
            );
        });
    }

    fn validate_unique_driver_update(
        &self,
        lazy_row: &mut LazyRow<'_>,
        candidate: &BoundIndexCandidate<'_>,
        update: &[UpdateCol],
    ) -> Result<()> {
        let index_no = candidate.index_no;
        let index_spec = self
            .accessor
            .metadata()
            .idx
            .require_index_spec(index_no)
            .expect("IndexWrite admission must retain an active index spec");
        let mut key_vals = Vec::with_capacity(index_spec.cols.len());
        for key in &index_spec.cols {
            let column_no = key.col_no as usize;
            let val = update
                .iter()
                .find(|update_col| update_col.idx == column_no)
                .map_or_else(
                    || lazy_row.val_inner(column_no).cloned(),
                    |update_col| Ok(update_col.val.clone()),
                )
                .disclose()?;
            key_vals.push(val);
        }
        if !candidate.matches_key(&key_vals) {
            return Err(Report::new(OperationError::InvalidDmlInput)
                .attach(format!(
                    "operation=table_index_mutate_mvcc, table_id={}, index_no={index_no}, row_id={}, unique driver key must remain unchanged",
                    self.accessor.table_id(),
                    candidate.row_id
                ))
                .disclose());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::tests::table4;
    use crate::error::{DiscloseResultExt, OperationError};
    use crate::row::ops::{DeleteMvcc, RowMutation, TableMutationOutcome, UpdateCol, UpdateMvcc};
    use crate::session::tests::assert_checkpoint_published;
    use crate::table::tests::*;
    use crate::trx::tests::{
        prepare_event_is_installed, prepare_transaction, transaction_status_for_test,
    };
    use crate::value::Val;
    use smol::future::yield_now;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[test]
    fn test_table_index_mutate_mvcc_mixed_cold_hot_actions() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "index_mutate_mixed").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 3, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            let mut callbacks = 0usize;
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        callbacks += 1;
                        Ok(match row.val(0)?.as_i32().unwrap() {
                            0 => RowMutation::Update(vec![UpdateCol {
                                idx: 1,
                                val: Val::from("cold-updated"),
                            }]),
                            1 | 12 => RowMutation::Delete,
                            2 => RowMutation::Skip,
                            10 => RowMutation::Update(vec![UpdateCol {
                                idx: 1,
                                val: Val::from("hot-updated"),
                            }]),
                            11 => RowMutation::Update(Vec::new()),
                            _ => unreachable!(),
                        })
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(callbacks, 6);
            assert_eq!(
                outcome,
                TableMutationOutcome {
                    delete_count: 2,
                    update_count: 3,
                }
            );
            trx.commit().await.unwrap();

            let mut reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut reader, table_id).await,
                vec![
                    (0, "cold-updated".to_owned()),
                    (2, "cold".to_owned()),
                    (10, "hot-updated".to_owned()),
                    (11, "hot".to_owned()),
                ]
            );
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_unique_driver_key_change_rolls_back() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_unique_reject").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "original").await;

            let mut trx = session.begin_trx().unwrap();
            let result = trx
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        Ok(match row.val(0)?.as_i32().unwrap() {
                            0 => RowMutation::Update(vec![UpdateCol {
                                idx: 1,
                                val: Val::from("must-rollback"),
                            }]),
                            1 => RowMutation::Update(vec![UpdateCol {
                                idx: 0,
                                val: Val::from(100i32),
                            }]),
                            _ => RowMutation::Skip,
                        })
                    })
                    .await
                })
                .await;
            let error = result.unwrap_err();
            assert_eq!(
                error.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidDmlInput)
            );
            assert_eq!(
                scan_table_pairs(&mut trx, table_id).await,
                vec![
                    (0, "original".to_owned()),
                    (1, "original".to_owned()),
                    (2, "original".to_owned()),
                ]
            );
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_non_unique_forward_moves_skip_self_entries() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_non_unique_move").await;
            let table_id = table4(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut setup = session.begin_trx().unwrap();
            setup
                .exec(async |stmt| {
                    for id in 0..5i32 {
                        stmt.table_insert_mvcc(table_id, vec![Val::from(id), Val::from(id)])
                            .await?;
                    }
                    Ok(())
                })
                .await
                .unwrap();
            setup.commit().await.unwrap();

            let lower = [Val::from(0i32)];
            let upper = [Val::from(4i32)];
            let mut callbacks = 0usize;
            let mut writer = session.begin_trx().unwrap();
            let outcome = writer
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 1, &lower[..]..=&upper[..], |row| {
                        callbacks += 1;
                        let value = row.val(1)?.as_i32().unwrap();
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from(value + 1),
                        }]))
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(
                callbacks, 5,
                "self-produced forward entries must be skipped"
            );
            assert_eq!(
                outcome,
                TableMutationOutcome {
                    delete_count: 0,
                    update_count: 5,
                }
            );
            writer.commit().await.unwrap();

            let mut reader = session.begin_trx().unwrap();
            let mut rows = Vec::new();
            reader
                .exec(async |stmt| {
                    stmt.table_scan_mvcc(table_id, &[0, 1], |vals| {
                        rows.push((vals[0].as_i32().unwrap(), vals[1].as_i32().unwrap()));
                        true
                    })
                    .await
                })
                .await
                .unwrap();
            rows.sort_unstable();
            assert_eq!(rows, vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]);
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_noop_releases_hot_and_cold_ownership() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "index_mutate_noop_release")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 0, 1, "cold").await;
            assert_freeze_created(
                setup_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut setup_session, table_id).await;
            insert_rows(table_id, &mut setup_session, 10, 1, "hot").await;

            let mut owner_session = engine.new_session().unwrap();
            let mut owner = owner_session.begin_trx().unwrap();
            let outcome = owner
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        Ok(match row.val(0)?.as_i32().unwrap() {
                            0 => RowMutation::Skip,
                            10 => RowMutation::Update(Vec::new()),
                            _ => unreachable!(),
                        })
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(
                outcome,
                TableMutationOutcome {
                    delete_count: 0,
                    update_count: 1,
                }
            );

            let mut competitor_session = engine.new_session().unwrap();
            let mut competitor = competitor_session.begin_trx().unwrap();
            for id in [0, 10] {
                assert!(matches!(
                    trx_update_row_by_id(
                        &mut competitor,
                        table_id,
                        &single_key(id),
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from("competitor"),
                        }],
                    )
                    .await
                    .unwrap(),
                    UpdateMvcc::Updated(_)
                ));
            }
            competitor.commit().await.unwrap();
            owner.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_skips_same_statement_but_not_later_statement() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_stmt_identity").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::from(7i32), Val::from("inserted")])
                        .await?;
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |_| {
                        panic!("a row produced by this statement must not reach the callback")
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome, TableMutationOutcome::default());

            let mut callbacks = 0usize;
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        callbacks += 1;
                        assert_eq!(row.val(0)?.as_i32(), Some(7));
                        Ok(RowMutation::Skip)
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(callbacks, 1);
            assert_eq!(outcome, TableMutationOutcome::default());
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_conflicts_with_active_hot_delete_before_callback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_active_hot_delete").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 1, 1, "original").await;

            let mut owner_session = engine.new_session().unwrap();
            let mut owner = owner_session.begin_trx().unwrap();
            assert_eq!(
                trx_delete_row_by_id(&mut owner, table_id, &single_key(1i32))
                    .await
                    .unwrap(),
                DeleteMvcc::Deleted
            );

            let key = [Val::from(1i32)];
            let mut callbacks = 0usize;
            let mut competitor_session = engine.new_session().unwrap();
            let mut competitor = competitor_session.begin_trx().unwrap();
            let result = competitor
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, &key[..]..=&key[..], |_| {
                        callbacks += 1;
                        Ok(RowMutation::Skip)
                    })
                    .await
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::WriteConflict)
            );
            assert_eq!(callbacks, 0);
            competitor.rollback().await.unwrap();
            owner.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_conflicts_with_active_hot_key_change_before_callback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                lightweight_test_engine(&temp_dir, "index_mutate_active_hot_key_change").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 1, 1, "original").await;

            let mut owner_session = engine.new_session().unwrap();
            let mut owner = owner_session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut owner,
                    table_id,
                    &single_key(1i32),
                    vec![UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::Updated(_)
            ));

            let old_key = [Val::from(1i32)];
            let mut callbacks = 0usize;
            let mut competitor_session = engine.new_session().unwrap();
            let mut competitor = competitor_session.begin_trx().unwrap();
            let result = competitor
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, &old_key[..]..=&old_key[..], |_| {
                        callbacks += 1;
                        Ok(RowMutation::Skip)
                    })
                    .await
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::WriteConflict)
            );
            assert_eq!(callbacks, 0);
            competitor.rollback().await.unwrap();
            owner.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_waits_for_preparing_hot_delete_before_skipping() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                lightweight_test_engine(&temp_dir, "index_mutate_preparing_hot_delete").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 1, 1, "original").await;

            let mut owner_session = engine.new_session().unwrap();
            let mut owner = owner_session.begin_trx().unwrap();
            assert_eq!(
                trx_delete_row_by_id(&mut owner, table_id, &single_key(1i32))
                    .await
                    .unwrap(),
                DeleteMvcc::Deleted
            );
            let owner_status = transaction_status_for_test(&owner);
            let prepared = prepare_transaction(owner).unwrap();

            let callbacks = AtomicUsize::new(0);
            let key = [Val::from(1i32)];
            let mut competitor_session = engine.new_session().unwrap();
            let mut competitor = competitor_session.begin_trx().unwrap();
            let mutate = async {
                competitor
                    .exec(async |stmt| {
                        stmt.table_index_mutate_mvcc(table_id, 0, &key[..]..=&key[..], |_| {
                            callbacks.fetch_add(1, Ordering::SeqCst);
                            Ok(RowMutation::Skip)
                        })
                        .await
                    })
                    .await
            };
            let commit = async {
                while !prepare_event_is_installed(&owner_status) {
                    yield_now().await;
                }
                engine
                    .inner()
                    .trx_sys
                    .commit_prepared(prepared)
                    .await
                    .disclose()
                    .unwrap();
            };
            let (result, ()) = futures::join!(mutate, commit);
            assert_eq!(result.unwrap(), TableMutationOutcome::default());
            assert_eq!(callbacks.load(Ordering::SeqCst), 0);
            competitor.commit().await.unwrap();
        });
    }
}
