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
use crate::table::hot::{DeleteInternal, HotRowMutator, ResumeOwnedRow, UpdateRowInplace};
use crate::table::{DeleteMarker, DeletionClaim, DeletionError, TableRootSnapshot};
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
                .resolve_row_location(self.rt.pool_guards(), candidate.row_id)
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
                    .table
                    .wait_transition_route_or_poison(&self.rt.engine().poisoner, candidate.row_id)
                    .await
                    .disclose()?;
                return Ok(CandidateProgress::RetryLocation);
            }
            LockRowForWrite::Ok(access) => access
                .take()
                .expect("candidate ownership must retain hot-row write access"),
        };
        drop(locked);
        let deleted_index_keys = self
            .mutate_owned_hot_index_candidate(
                candidate,
                &page_guard,
                access,
                value_buffer,
                outcome,
                mutate_row,
            )
            .await?;
        if let Some(index_keys) = deleted_index_keys {
            let proof = accessor.owned_row_page_index_set_proof(
                candidate.row_id,
                index_keys,
                self.root_snapshot,
            );
            // Row undo ownership keeps the reconstructed key set stable;
            // release the page latch before awaiting MemIndex mutations.
            drop(page_guard);
            accessor
                .defer_delete_owned_row_index_set(self.rt, self.effects, proof)
                .await
                .attach("index-driven mutation hot delete index masking")
                .disclose()?;
        }
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
            durable_deleted,
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
            durable_deleted,
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
            durable_deleted,
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
                let defer = candidate.unique
                    && self.unique_driver_key_changed(&mut lazy_row, candidate, &update)?;
                if update.is_empty() {
                    *value_buffer = lazy_row.into_reusable_buffer();
                    self.cancel_owned_cold_row(candidate.row_id);
                    drop(persisted);
                } else if defer {
                    *value_buffer = lazy_row.into_reusable_buffer();
                    drop(persisted);
                    // TODO: evaluate if caching entire cold row is better than rescan.
                    self.effects
                        .defer_index_update(accessor.table_id(), candidate.row_id, update);
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
    ) -> Result<Option<WriteIndexKeySet<'op>>>
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
                Ok(None)
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
                Ok(Some(index_keys))
            }
            RowMutation::Update(update) => {
                outcome.update_count += 1;
                accessor
                    .validate_table_mutation_update(self.validator.as_ref(), &update)
                    .disclose()?;
                let defer = candidate.unique
                    && self.unique_driver_key_changed(&mut lazy_row, candidate, &update)?;
                let (access, reusable) = lazy_row.into_hot_write_reusable_buffer();
                *value_buffer = reusable;
                if update.is_empty() {
                    self.cancel_owned_hot_row(access);
                    return Ok(None);
                }
                if defer {
                    drop(access);
                    self.effects
                        .defer_index_update(accessor.table_id(), candidate.row_id, update);
                } else {
                    self.update_owned_hot_row(candidate.row_id, page_guard, access, update)
                        .await?;
                }
                Ok(None)
            }
        }
    }

    /// Applies every cached key-changing update after index traversal ends.
    pub(super) async fn apply_deferred_index_updates(&mut self) -> Result<()> {
        #[cfg(test)]
        if self.effects.has_deferred_index_updates() {
            tests::maybe_pause_before_deferred_application().await;
        }
        self.effects.begin_deferred_index_update_application();
        while let Some((row_id, update)) = self.effects.activate_next_deferred_index_update() {
            self.apply_deferred_index_update(row_id, update).await?;
        }
        Ok(())
    }

    /// Resumes the exact retained hot or cold ownership for one cached update.
    async fn apply_deferred_index_update(
        &mut self,
        row_id: RowID,
        update: Vec<UpdateCol>,
    ) -> Result<()> {
        let accessor = self.accessor;
        loop {
            match accessor
                .resolve_row_location(self.rt.pool_guards(), row_id)
                .await
                .disclose()?
            {
                RowLocation::NotFound => {
                    panic!(
                        "deferred index update lost its retained row route: table_id={}, row_id={row_id}",
                        accessor.table_id()
                    );
                }
                RowLocation::RowPage(page_id) => {
                    let Some(page_guard) = accessor
                        .mem()
                        .try_get_validated_row_page_shared_result(
                            self.rt.pool_guards(),
                            page_id,
                            row_id,
                        )
                        .await
                        .disclose()?
                    else {
                        continue;
                    };
                    {
                        let resumed = HotRowMutator::new(
                            accessor.table_id(),
                            accessor.metadata(),
                            self.rt,
                            &page_guard,
                            row_id,
                        )
                        .resume_owned_row(self.effects);
                        match resumed {
                            ResumeOwnedRow::Ok(access) => {
                                self.update_owned_hot_row(row_id, &page_guard, access, update)
                                    .await?;
                                return Ok(());
                            }
                            ResumeOwnedRow::RetryInTransition => (),
                        }
                    }
                    // Checkpoint has sealed this hot page while the deferred
                    // update still owns its provisional undo. Release the page
                    // before waiting for either authoritative cold-route
                    // publication or poison; after a normal wake, the loop
                    // re-resolves the row instead of assuming its location.
                    drop(page_guard);
                    accessor
                        .table
                        .wait_transition_route_or_poison(&self.rt.engine().poisoner, row_id)
                        .await
                        .disclose()?;
                }
                RowLocation::LwcBlock(location) => {
                    let old_row = accessor
                        .read_lwc_full_row(
                            self.rt.pool_guards(),
                            location.block_id,
                            location.row_idx,
                            location.row_shape_fingerprint,
                        )
                        .await
                        .disclose()?;
                    let owns_marker = matches!(
                        accessor.lwc_deletion_buffer().get(row_id),
                        Some(DeleteMarker::Ref(status)) if Arc::ptr_eq(&status, self.rt.status())
                    );
                    assert!(
                        owns_marker,
                        "deferred cold index update must retain its transaction-owned deletion marker: table_id={}, row_id={row_id}",
                        accessor.table_id()
                    );
                    accessor
                        .update_owned_cold_row(
                            self.rt,
                            self.effects,
                            row_id,
                            old_row,
                            update,
                            self.root_snapshot,
                        )
                        .await
                        .disclose()?;
                    return Ok(());
                }
            }
        }
    }

    /// Reuses the ordinary owned-hot update, move, and index-maintenance paths.
    async fn update_owned_hot_row(
        &mut self,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        access: RowWriteAccess<'_>,
        update: Vec<UpdateCol>,
    ) -> Result<()> {
        let accessor = self.accessor;
        let result = HotRowMutator::new(
            accessor.table_id(),
            accessor.metadata(),
            self.rt,
            page_guard,
            row_id,
        )
        .update_owned_row(self.effects, RowUpdateInput::Sparse(update), access);
        match result {
            UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                debug_assert_eq!(row_id, new_row_id);
                if !index_change_cols.is_empty() {
                    accessor
                        .update_indexes_only_key_change(
                            self.rt,
                            self.effects,
                            row_id,
                            page_guard,
                            &index_change_cols,
                            self.root_snapshot,
                        )
                        .await
                        .attach("index-driven mutation hot key change")
                        .disclose()?;
                }
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
            }
            UpdateRowInplace::RowDeleted(_)
            | UpdateRowInplace::RowNotFound(_)
            | UpdateRowInplace::RetryInTransition(_) => {
                unreachable!("retained owned hot row changed before physical update")
            }
        }
        Ok(())
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

    fn unique_driver_key_changed(
        &self,
        lazy_row: &mut LazyRow<'_>,
        candidate: &BoundIndexCandidate<'_>,
        update: &[UpdateCol],
    ) -> Result<bool> {
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
        Ok(!candidate.matches_key(&key_vals))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::tests::{table3, table4};
    use crate::error::{DiscloseResultExt, OperationError};
    use crate::index::{IndexInsert, RowLocation};
    use crate::row::ops::{
        DeleteMvcc, RowMutation, SelectMvcc, TableMutationOutcome, UpdateCol, UpdateMvcc,
    };
    use crate::session::tests::{
        SessionTestExt, assert_checkpoint_published, wait_for_session_idle,
    };
    use crate::table::DeleteMarker;
    use crate::table::tests::*;
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::trx::tests::{
        prepare_event_is_installed, prepare_transaction, transaction_status_for_test,
    };
    use crate::value::Val;
    use smol::future::yield_now;
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    thread_local! {
        static DEFERRED_APPLICATION_PAUSE: RefCell<Option<flume::Receiver<()>>> = const { RefCell::new(None) };
        static DEFERRED_APPLICATION_PAUSED: Cell<bool> = const { Cell::new(false) };
    }

    fn pause_next_deferred_application() -> flume::Sender<()> {
        let (resume_tx, resume_rx) = flume::bounded(1);
        DEFERRED_APPLICATION_PAUSED.set(false);
        DEFERRED_APPLICATION_PAUSE.with(|slot| {
            let old = slot.borrow_mut().replace(resume_rx);
            assert!(
                old.is_none(),
                "deferred-application pause already installed"
            );
        });
        resume_tx
    }

    pub(super) async fn maybe_pause_before_deferred_application() {
        let receiver = DEFERRED_APPLICATION_PAUSE.with(|slot| slot.borrow_mut().take());
        if let Some(receiver) = receiver {
            DEFERRED_APPLICATION_PAUSED.set(true);
            receiver.recv_async().await.unwrap();
        }
    }

    async fn poll_until_deferred_application_pauses<F>(mut future: Pin<&mut F>)
    where
        F: Future,
    {
        for _ in 0..512 {
            assert!(futures::poll!(future.as_mut()).is_pending());
            if DEFERRED_APPLICATION_PAUSED.get() {
                return;
            }
            yield_now().await;
        }
        panic!("index mutation did not reach deferred application pause");
    }

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
            let deleted_hot_key = [Val::from(12i32)];
            let deleted_hot_row = reader
                .exec(async |stmt| {
                    stmt.table_lookup_unique_mvcc(table_id, 0, &deleted_hot_key, &[0])
                        .await
                })
                .await
                .unwrap();
            assert!(matches!(deleted_hot_row, SelectMvcc::NotFound));
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_skips_persisted_cold_delete() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "index_mutate_persisted_cold_delete",
            )
            .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let table = table_for_internal_assertion(&engine, table_id);
            let reader = session.begin_trx().unwrap();
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key).await;
            let marker_ts = delete_marker_ts(table.deletion_buffer().get(row_id).unwrap());
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let column_index = snapshot.index(pool_guards.disk_guard());
            let entry = column_index.locate_block(row_id).await.unwrap().unwrap();
            let delete_deltas = column_index.load_delete_deltas(&entry).await.unwrap();
            assert!(delete_deltas.contains(&((row_id - entry.start_row_id) as u32)));

            table.deletion_buffer().remove(row_id);
            let inserted = bound_unique_index(&table, &pool_guards, key.index_no)
                .inject_mem_entry_if_absent(&key.vals, row_id, true, MAX_SNAPSHOT_TS)
                .await
                .unwrap();
            assert!(matches!(inserted, IndexInsert::Ok(_)));
            drop(pool_guards);

            let range_key = [Val::from(1i32)];
            let mut callbacks = 0usize;
            let mut writer = session.begin_trx().unwrap();
            let outcome = writer
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(
                        table_id,
                        0,
                        &range_key[..]..=&range_key[..],
                        |_| {
                            callbacks += 1;
                            Ok(RowMutation::Skip)
                        },
                    )
                    .await
                })
                .await
                .unwrap();
            assert_eq!(callbacks, 0);
            assert_eq!(outcome, TableMutationOutcome::default());
            assert!(table.deletion_buffer().get(row_id).is_none());
            writer.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_unique_driver_key_changes_apply_after_traversal() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_unique_change").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "original").await;

            let mut trx = session.begin_trx().unwrap();
            let mut callbacks = Vec::new();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        callbacks.push(id);
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 0,
                            val: Val::from(id + 100),
                        }]))
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(callbacks, vec![0, 1, 2]);
            assert_eq!(
                outcome,
                TableMutationOutcome {
                    delete_count: 0,
                    update_count: 3,
                }
            );
            trx.commit().await.unwrap();

            let mut reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut reader, table_id).await,
                vec![
                    (100, "original".to_owned()),
                    (101, "original".to_owned()),
                    (102, "original".to_owned()),
                ]
            );
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_unique_driver_change_moves_frozen_row() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                lightweight_test_engine(&temp_dir, "index_mutate_unique_frozen_move").await;
            let table_id = table3(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut setup = session.begin_trx().unwrap();
            setup
                .exec(async |stmt| {
                    stmt.table_insert_mvcc(table_id, vec![Val::from("old")])
                        .await?;
                    Ok(())
                })
                .await
                .unwrap();
            setup.commit().await.unwrap();
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());

            let mut writer = session.begin_trx().unwrap();
            let outcome = writer
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        assert_eq!(row.val(0)?.as_str(), Some("old"));
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 0,
                            val: Val::from("new-variable-length-key"),
                        }]))
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome.update_count, 1);
            writer.commit().await.unwrap();

            let old_key = [Val::from("old")];
            let new_key = [Val::from("new-variable-length-key")];
            let mut reader = session.begin_trx().unwrap();
            let (old_row, new_row) = reader
                .exec(async |stmt| {
                    let old_row = stmt
                        .table_lookup_unique_mvcc(table_id, 0, &old_key, &[0])
                        .await?;
                    let new_row = stmt
                        .table_lookup_unique_mvcc(table_id, 0, &new_key, &[0])
                        .await?;
                    Ok((old_row, new_row))
                })
                .await
                .unwrap();
            assert!(old_row.not_found());
            assert_eq!(
                new_row.unwrap_found(),
                vec![Val::from("new-variable-length-key")]
            );
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_unique_driver_duplicate_settles_pending_locks() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_unique_duplicate").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "original").await;

            let mut trx = session.begin_trx().unwrap();
            let mut callbacks = Vec::new();
            let result = trx
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        callbacks.push(id);
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 0,
                            val: Val::from(if id == 0 { 2 } else { id + 100 }),
                        }]))
                    })
                    .await
                })
                .await;
            assert_eq!(callbacks, vec![0, 1, 2]);
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::DuplicateKey)
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
    fn test_table_index_mutate_mvcc_unique_driver_changes_mixed_cold_hot() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "index_mutate_unique_mixed")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 3, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            let mut callbacks = Vec::new();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        callbacks.push(id);
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 0,
                            val: Val::from(id + 100),
                        }]))
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(callbacks, vec![0, 1, 2, 10, 11, 12]);
            assert_eq!(outcome.update_count, 6);
            trx.commit().await.unwrap();

            let mut reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut reader, table_id).await,
                vec![
                    (100, "cold".to_owned()),
                    (101, "cold".to_owned()),
                    (102, "cold".to_owned()),
                    (110, "hot".to_owned()),
                    (111, "hot".to_owned()),
                    (112, "hot".to_owned()),
                ]
            );
            reader.commit().await.unwrap();
        });
    }

    async fn assert_deferred_hot_lock_resumes_after_cold_publication(commit: bool) {
        let temp_dir = TempDir::new().unwrap();
        let stem = if commit {
            "index_mutate_transition_commit"
        } else {
            "index_mutate_transition_rollback"
        };
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, stem).await;
        let table_id = create_table2_for_test(&engine).await;
        let mut maintenance_session = engine.new_session().unwrap();
        insert_rows(table_id, &mut maintenance_session, 1, 1, "original").await;

        let key = single_key(1i32);
        let probe = maintenance_session.begin_trx().unwrap();
        let row_id = bound_unique_index(
            &table_for_internal_assertion(&engine, table_id),
            &maintenance_session.pool_guards(),
            key.index_no,
        )
        .lookup(&key.vals, probe.sts())
        .await
        .unwrap()
        .unwrap()
        .0;
        probe.commit().await.unwrap();
        assert_freeze_created(
            maintenance_session
                .freeze_table(table_id, usize::MAX)
                .await
                .unwrap(),
        );

        let resume = pause_next_deferred_application();
        let mut writer_session = engine.new_session().unwrap();
        let mut writer = writer_session.begin_trx().unwrap();
        let writer_status = transaction_status_for_test(&writer);
        let mut mutation = Box::pin(writer.exec(async |stmt| {
            stmt.table_index_mutate_mvcc(table_id, 0, .., |row| {
                assert_eq!(row.val(0)?.as_i32(), Some(1));
                Ok(RowMutation::Update(vec![UpdateCol {
                    idx: 0,
                    val: Val::from(101i32),
                }]))
            })
            .await
        }));
        poll_until_deferred_application_pauses(mutation.as_mut()).await;

        assert_checkpoint_published(&mut maintenance_session, table_id).await;
        let table = table_for_internal_assertion(&engine, table_id);
        assert!(matches!(
            table
                .find_row(&maintenance_session.pool_guards(), row_id)
                .await
                .unwrap(),
            RowLocation::LwcBlock(_)
        ));
        assert!(matches!(
            table.deletion_buffer().get(row_id),
            Some(DeleteMarker::Ref(status)) if Arc::ptr_eq(&status, &writer_status)
        ));

        resume.send_async(()).await.unwrap();
        let outcome = mutation.as_mut().await.unwrap();
        assert_eq!(outcome.update_count, 1);
        drop(mutation);
        if commit {
            writer.commit().await.unwrap();
        } else {
            writer.rollback().await.unwrap();
        }

        let mut reader = maintenance_session.begin_trx().unwrap();
        let expected = if commit {
            vec![(101, "original".to_owned())]
        } else {
            vec![(1, "original".to_owned())]
        };
        assert_eq!(scan_table_pairs(&mut reader, table_id).await, expected);
        reader.commit().await.unwrap();
    }

    #[test]
    fn test_table_index_mutate_mvcc_deferred_hot_lock_resumes_after_cold_publication() {
        smol::block_on(async {
            assert_deferred_hot_lock_resumes_after_cold_publication(true).await;
            assert_deferred_hot_lock_resumes_after_cold_publication(false).await;
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_cancellation_retains_deferred_undo_ownership() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_deferred_cancel").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 1, "original").await;

            let resume = pause_next_deferred_application();
            let session_id = session.id();
            let mut writer = session.begin_trx().unwrap();
            let mut mutation = Box::pin(writer.exec(async |stmt| {
                stmt.table_index_mutate_mvcc(table_id, 0, .., |_| {
                    Ok(RowMutation::Update(vec![UpdateCol {
                        idx: 0,
                        val: Val::from(101i32),
                    }]))
                })
                .await
            }));
            poll_until_deferred_application_pauses(mutation.as_mut()).await;
            drop(resume);
            drop(mutation);

            wait_for_session_idle(&engine.inner().session_registry, session_id).await;
            let mut reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut reader, table_id).await,
                vec![(1, "original".to_owned())]
            );
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_index_mutate_mvcc_deferred_update_retains_write_conflict() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "index_mutate_deferred_conflict").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 1, 1, "original").await;

            let resume = pause_next_deferred_application();
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let mut mutation = Box::pin(writer.exec(async |stmt| {
                stmt.table_index_mutate_mvcc(table_id, 0, .., |_| {
                    Ok(RowMutation::Update(vec![UpdateCol {
                        idx: 0,
                        val: Val::from(101i32),
                    }]))
                })
                .await
            }));
            poll_until_deferred_application_pauses(mutation.as_mut()).await;

            let mut competitor_session = engine.new_session().unwrap();
            let mut competitor = competitor_session.begin_trx().unwrap();
            let result = trx_update_row_by_id(
                &mut competitor,
                table_id,
                &single_key(1i32),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("competitor"),
                }],
            )
            .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::WriteConflict)
            );
            competitor.rollback().await.unwrap();

            resume.send_async(()).await.unwrap();
            assert_eq!(mutation.as_mut().await.unwrap().update_count, 1);
            drop(mutation);
            writer.commit().await.unwrap();
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
