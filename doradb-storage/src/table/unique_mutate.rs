//! Unique row selection, retained ownership, and logical action dispatch.
//!
//! Read-current selection is shared with MemTable; snapshot readers never use it.

use super::access::{LazyRow, LazyRowBuffer, LazyRowSource, UserTableAccessor, WriteIndexKeySet};
use super::deletion_buffer::DeletionState;
use super::hot::{DeleteInternal, HotRowLock, HotRowMutator};
use super::{DmlValidator, TableRootSnapshot, validate_page_row_range};
use crate::buffer::guard::PageSharedGuard;
use crate::catalog::IndexRef;
use crate::error::{
    CallbackResult, DataIntegrityError, DiscloseError, DiscloseResultExt, OperationError,
    OperationOrRuntimeResult, OperationResult, RuntimeError,
};
use crate::id::{RowID, TrxID};
use crate::index::{LwcRowLocation, RowLocation, UniqueLookupObservation};
use crate::lwc::PersistedLwcBlock;
use crate::poison::PoisonAwareListener;
use crate::row::ops::{RowUpdateInput, UniqueMutation, UniqueMutationOutcome};
use crate::row::{RowPage, RowRead};
use crate::runtime::yield_now;
use crate::trx::TrxRuntime;
use crate::trx::row::RowWriteAccess;
use crate::trx::stmt::StmtEffects;
use crate::value::Val;
use error_stack::{Report, ResultExt};

#[cfg(test)]
pub(crate) use tests::{
    record_forward_hint, record_lookup_validation, record_unique_disk_lookup, record_unique_lookup,
};

/// Concrete findings when a physical candidate cannot be acquired for this key.
pub(super) enum RowInspection {
    /// The block index no longer resolves the selected RowID.
    MissingRoute,
    /// The newest hot departure of this key supplies the next row to inspect.
    Successor(RowID),
    /// Hot inspection needs original-index validation to establish absence.
    HotUnresolved,
    /// A cold deletion is committed; its CTS may have been compacted away.
    ColdDeleted(Option<TrxID>),
    /// The immutable cold image does not match the selected key.
    ColdKeyMismatch,
    /// An earlier operation of this transaction already consumed the cold row.
    ColdConsumed,
}

/// Outcome before invoking a callback or consuming mutation input.
pub(super) enum CurrentRowDecision {
    Missing,
    Conflict,
    Retry,
    Hint(RowID),
}

/// The current candidate and whether it came from this attempt's index lookup.
enum CurrentRowPosition {
    Index(RowID),
    Forward(RowID),
}

/// Fixed lookup context and explicitly advanced current-row position.
pub(super) struct CurrentRowSelection<'a> {
    // Both remain fixed while following the entire forward chain.
    observation: UniqueLookupObservation<'a>,
    sts: TrxID,
    // Only advance() changes the position after construction.
    position: CurrentRowPosition,
}

impl<'a> CurrentRowSelection<'a> {
    /// Binds fixed lookup evidence and snapshot to the first physical candidate.
    pub(super) fn new(observation: UniqueLookupObservation<'a>, sts: TrxID, row_id: RowID) -> Self {
        Self {
            observation,
            sts,
            position: CurrentRowPosition::Index(row_id),
        }
    }

    /// Returns the physical candidate to inspect next.
    pub(super) fn row_id(&self) -> RowID {
        match self.position {
            CurrentRowPosition::Index(row_id) | CurrentRowPosition::Forward(row_id) => row_id,
        }
    }

    /// Only the index-selected candidate can use the old-deletion shortcut.
    pub(super) fn is_original_candidate(&self) -> bool {
        matches!(self.position, CurrentRowPosition::Index(_))
    }

    /// Applies a forward link without replacing the original lookup evidence.
    pub(super) fn advance(&mut self, successor: RowID) {
        // Revisiting the initial RowID still represents a forward target.
        self.position = CurrentRowPosition::Forward(successor);
    }

    /// Chooses the next action without advancing or changing the lookup context.
    pub(super) fn decide(&self, inspection: RowInspection) -> CurrentRowDecision {
        if self.is_original_candidate()
            && matches!(inspection, RowInspection::ColdDeleted(Some(cts)) if cts < self.sts)
        {
            return CurrentRowDecision::Missing;
        }
        if let RowInspection::Successor(row_id) = inspection {
            return CurrentRowDecision::Hint(row_id);
        }
        // The initial lookup may have captured an uncommitted destination whose
        // claim was rolled back before inspection. Its remaining row history can
        // be empty or contain an older, unrelated departure of this same key.
        // Even a terminal hot chain therefore needs the original index evidence.
        if !self.observation.is_valid() {
            return CurrentRowDecision::Retry;
        }
        // A surviving transfer publishes its index target before another
        // transaction can follow the link. Rollback can expose an older link
        // whose target becomes cold, but it also invalidates the lookup.
        assert!(
            self.is_original_candidate() || matches!(inspection, RowInspection::HotUnresolved),
            "rejected cold or missing forward target requires stale index observation: row_id={}",
            self.row_id()
        );
        if matches!(inspection, RowInspection::ColdDeleted(Some(cts)) if cts > self.sts) {
            CurrentRowDecision::Conflict
        } else {
            CurrentRowDecision::Missing
        }
    }
}

/// Selection waits begin after all attempt-local guards have been released.
enum SelectionWait {
    Preparing(PoisonAwareListener),
    Transition(RowID),
}

/// Cold admission retains either fresh ownership or the reason to retry/reject.
enum ColdRowSelection {
    Owned(PersistedLwcBlock),
    Rejected(RowInspection),
    Preparing(PoisonAwareListener),
}

/// Hot deletes defer index maintenance until the caller releases the row page.
enum HotMutationResult<'op> {
    Completed(UniqueMutationOutcome),
    Deleted(WriteIndexKeySet<'op>),
}

/// Operation-scoped unique mutation executor; guards and roots remain attempt-local.
pub(super) struct UniqueMutator<'a, 'op, 'r> {
    accessor: &'a UserTableAccessor<'op>,
    rt: TrxRuntime<'r>,
    effects: &'a mut StmtEffects,
    index: IndexRef,
    key_vals: &'a [Val],
    validator: Option<DmlValidator<'a>>,
}

impl<'a, 'op, 'r> UniqueMutator<'a, 'op, 'r> {
    pub(super) fn new(
        accessor: &'a UserTableAccessor<'op>,
        rt: TrxRuntime<'r>,
        effects: &'a mut StmtEffects,
        index: IndexRef,
        key_vals: &'a [Val],
        validate: bool,
    ) -> Self {
        Self {
            accessor,
            rt,
            effects,
            index,
            key_vals,
            validator: validate.then(|| DmlValidator::new(accessor.metadata())),
        }
    }

    /// Retries only selection; invoking the callback commits to the selected entry.
    #[inline]
    pub(super) async fn execute<F, E>(
        mut self,
        mutate_row: F,
    ) -> CallbackResult<UniqueMutationOutcome, E>
    where
        F: for<'row> FnOnce(Option<&mut LazyRow<'row>>) -> CallbackResult<UniqueMutation, E>,
    {
        let accessor = self.accessor;
        let rt = self.rt;
        let key_vals = self.key_vals;
        let poisoner = &rt.engine().poisoner;
        'retry: loop {
            // Both prepare and transition waits release the entire attempt first.
            let wait = 'attempt: {
                let root = accessor.root_snapshot(rt.ctx());
                let handle =
                    accessor.snapshot_index_read_handle(rt.pool_guards(), &root, self.index);
                let index = handle.bind_unique().disclose()?;
                let (candidate, observation) = index.lookup_observed(key_vals).await.disclose()?;
                let Some((row_id, _)) = candidate else {
                    if observation.is_valid() {
                        break 'retry;
                    }
                    break 'attempt None;
                };
                let mut selection = CurrentRowSelection::new(observation, rt.sts(), row_id);
                loop {
                    let row_id = selection.row_id();
                    let inspection = match accessor
                        .resolve_row_location(rt.pool_guards(), row_id)
                        .await
                        .disclose()?
                    {
                        RowLocation::NotFound => RowInspection::MissingRoute,
                        RowLocation::RowPage(page_id) => 'inspect: {
                            // This active transaction prevents checkpoint
                            // retirement from reclaiming the selected hot page.
                            let page = accessor
                                .mem()
                                .must_get_row_page_shared(rt.pool_guards(), page_id)
                                .await
                                .disclose()?;
                            assert!(
                                validate_page_row_range(&page, page_id, row_id),
                                "unique mutation row page does not match selected row: table_id={}, page_id={page_id}, row_id={row_id}",
                                accessor.table_id()
                            );
                            let hot = HotRowMutator::new(
                                accessor.table_id(),
                                accessor.metadata(),
                                rt,
                                &page,
                                row_id,
                            );
                            let access = match hot.try_lock_current(
                                self.effects,
                                self.index,
                                key_vals,
                                selection.is_original_candidate(),
                            ) {
                                HotRowLock::Owned(access) => access,
                                HotRowLock::DeletedBeforeSnapshot => break 'retry,
                                HotRowLock::Successor(row_id) => {
                                    break 'inspect RowInspection::Successor(row_id);
                                }
                                HotRowLock::Unresolved => {
                                    break 'inspect RowInspection::HotUnresolved;
                                }
                                HotRowLock::WriteConflict => {
                                    return Err(Report::new(OperationError::WriteConflict)
                                        .attach("unique mutation hot-row ownership")
                                        .disclose()
                                        .into());
                                }
                                HotRowLock::Preparing(listener) => {
                                    break 'attempt Some(SelectionWait::Preparing(listener));
                                }
                                HotRowLock::Transition => {
                                    break 'attempt Some(SelectionWait::Transition(row_id));
                                }
                            };
                            drop(selection);
                            return match self
                                .mutate_owned_hot_row(&page, access, &root, mutate_row)
                                .await?
                            {
                                HotMutationResult::Completed(outcome) => Ok(outcome),
                                HotMutationResult::Deleted(keys) => {
                                    let proof = accessor
                                        .owned_row_page_index_set_proof(row_id, keys, &root);
                                    drop(page);
                                    accessor
                                        .defer_delete_owned_row_index_set(rt, self.effects, proof)
                                        .await
                                        .disclose()?;
                                    Ok(UniqueMutationOutcome::Deleted)
                                }
                            };
                        }
                        RowLocation::LwcBlock(location) => {
                            match self.select_cold_row(row_id, location).await.disclose()? {
                                ColdRowSelection::Rejected(inspection) => inspection,
                                ColdRowSelection::Preparing(listener) => {
                                    break 'attempt Some(SelectionWait::Preparing(listener));
                                }
                                ColdRowSelection::Owned(persisted) => {
                                    drop(selection);
                                    return self
                                        .mutate_owned_cold_row(
                                            row_id, location, persisted, &root, mutate_row,
                                        )
                                        .await;
                                }
                            }
                        }
                    };
                    match selection.decide(inspection) {
                        CurrentRowDecision::Missing => break 'retry,
                        CurrentRowDecision::Conflict => {
                            return Err(Report::new(OperationError::WriteConflict)
                                .attach("stable unique lookup cold deletion")
                                .disclose()
                                .into());
                        }
                        CurrentRowDecision::Retry => break 'attempt None,
                        CurrentRowDecision::Hint(target) => selection.advance(target),
                    }
                    yield_now().await;
                    poisoner.ensure_healthy().disclose()?;
                }
            };
            if let Some(wait) = wait {
                match wait {
                    SelectionWait::Preparing(listener) => {
                        rt.wait_prepare_or_poison(listener).await.disclose()?;
                    }
                    SelectionWait::Transition(row_id) => {
                        accessor
                            .table()
                            .wait_transition_route_or_poison(poisoner, row_id)
                            .await
                            .disclose()?;
                    }
                }
            }
            yield_now().await;
            poisoner.ensure_healthy().disclose()?;
        }
        self.mutate_missing(mutate_row).await
    }

    /// Checks the immutable image between initial inspection and an undo-backed claim.
    async fn select_cold_row(
        &mut self,
        row_id: RowID,
        location: LwcRowLocation,
    ) -> OperationOrRuntimeResult<ColdRowSelection> {
        let accessor = self.accessor;
        let rt = self.rt;
        #[cfg(test)]
        tests::run_current_cold_hook(false);
        match accessor.lwc_deletion_buffer().current_state(
            row_id,
            rt.status(),
            location.durable_deleted,
        ) {
            DeletionState::Available => (),
            DeletionState::Consumed => {
                return Ok(ColdRowSelection::Rejected(RowInspection::ColdConsumed));
            }
            DeletionState::Deleted(cts) => {
                return Ok(ColdRowSelection::Rejected(RowInspection::ColdDeleted(cts)));
            }
            DeletionState::WriteConflict => {
                return Err(Report::new(OperationError::WriteConflict)
                    .attach("unique mutation cold-row ownership")
                    .into());
            }
            DeletionState::Preparing(listener) => return Ok(ColdRowSelection::Preparing(listener)),
            DeletionState::Acquired => unreachable!("inspection cannot acquire a marker"),
        }
        let persisted = accessor
            .column_storage()
            .load_lwc_block(rt.pool_guards().disk_guard(), location.block_id)
            .await
            .change_context(RuntimeError::TableAccess)?;
        let block = persisted.block();
        if block.row_shape_fingerprint() != location.row_shape_fingerprint {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "unique lookup row shape mismatch: block_id={}",
                    location.block_id
                ))
                .change_context(RuntimeError::TableAccess)
                .into());
        }
        let spec = accessor.metadata().idx.expect_index_spec(self.index);
        let actual_key = block
            .decode_index_key_values(accessor.metadata().col.as_ref(), spec, location.row_idx)
            .change_context(RuntimeError::TableAccess)?;
        if actual_key != self.key_vals {
            return Ok(ColdRowSelection::Rejected(RowInspection::ColdKeyMismatch));
        }
        drop(actual_key);
        #[cfg(test)]
        tests::run_current_cold_hook(true);
        match accessor.claim_current_cold_row(rt, self.effects, row_id, location.durable_deleted) {
            DeletionState::Acquired => Ok(ColdRowSelection::Owned(persisted)),
            DeletionState::Consumed => Ok(ColdRowSelection::Rejected(RowInspection::ColdConsumed)),
            DeletionState::Deleted(cts) => {
                Ok(ColdRowSelection::Rejected(RowInspection::ColdDeleted(cts)))
            }
            DeletionState::WriteConflict => Err(Report::new(OperationError::WriteConflict)
                .attach("unique mutation cold-row claim")
                .into()),
            DeletionState::Preparing(listener) => Ok(ColdRowSelection::Preparing(listener)),
            DeletionState::Available => unreachable!("conditional claim resolves vacancy"),
        }
    }

    /// Invokes the callback while retaining the hot row's write access.
    async fn mutate_owned_hot_row<F, E>(
        &mut self,
        page: &PageSharedGuard<RowPage>,
        access: RowWriteAccess<'_>,
        root: &TableRootSnapshot<'_>,
        mutate_row: F,
    ) -> CallbackResult<HotMutationResult<'op>, E>
    where
        F: for<'row> FnOnce(Option<&mut LazyRow<'row>>) -> CallbackResult<UniqueMutation, E>,
    {
        let accessor = self.accessor;
        let rt = self.rt;
        let row_id = access.row().row_id();
        let width = accessor.metadata().col.col_count();
        let mut buffer = LazyRowBuffer::new_deferred(width);
        let source = LazyRowSource::HotWrite {
            access,
            column_layout: accessor.metadata().col.as_ref(),
        };
        let mut row = LazyRow::new(source, &mut buffer, width);
        let action = mutate_row(Some(&mut row))?;
        let access = row.into_hot_write_access();
        self.validate_action(true, &action).disclose()?;
        let outcome = match action {
            UniqueMutation::Skip => {
                accessor.cancel_owned_hot_row(self.effects, access);
                UniqueMutationOutcome::Noop
            }
            UniqueMutation::Update(ref cols) if cols.is_empty() => {
                accessor.cancel_owned_hot_row(self.effects, access);
                UniqueMutationOutcome::Updated(row_id)
            }
            UniqueMutation::Update(input) => {
                let result = accessor
                    .update_owned_hot_row(
                        rt,
                        self.effects,
                        page,
                        access,
                        RowUpdateInput::Sparse(input),
                        root,
                    )
                    .await
                    .disclose()?;
                UniqueMutationOutcome::Updated(result)
            }
            UniqueMutation::Delete => {
                let result =
                    HotRowMutator::new(accessor.table_id(), accessor.metadata(), rt, page, row_id)
                        .delete_owned_row(self.effects, access);
                assert!(
                    matches!(result, DeleteInternal::Ok),
                    "retained unique row must remain deletable: row_id={row_id}"
                );
                let keys = WriteIndexKeySet::from_physical_row(accessor, page, row_id);
                return Ok(HotMutationResult::Deleted(keys));
            }
            UniqueMutation::Insert(_) => unreachable!("validated occupied actions cannot insert"),
        };
        Ok(HotMutationResult::Completed(outcome))
    }

    /// Invokes the callback after a fresh cold claim and applies its action.
    async fn mutate_owned_cold_row<F, E>(
        &mut self,
        row_id: RowID,
        location: LwcRowLocation,
        persisted: PersistedLwcBlock,
        root: &TableRootSnapshot<'_>,
        mutate_row: F,
    ) -> CallbackResult<UniqueMutationOutcome, E>
    where
        F: for<'row> FnOnce(Option<&mut LazyRow<'row>>) -> CallbackResult<UniqueMutation, E>,
    {
        let accessor = self.accessor;
        let rt = self.rt;
        let width = accessor.metadata().col.col_count();
        let mut buffer = LazyRowBuffer::new_deferred(width);
        let block = persisted.block();
        let source = || LazyRowSource::Cold {
            block,
            column_layout: accessor.metadata().col.as_ref(),
            row_idx: location.row_idx,
            file_kind: accessor.column_storage().file().file_kind(),
            block_id: location.block_id,
        };
        let action = {
            let mut row = LazyRow::new(source(), &mut buffer, width);
            mutate_row(Some(&mut row))?
        };
        self.validate_action(true, &action).disclose()?;
        match action {
            UniqueMutation::Skip => {
                accessor.cancel_owned_cold_row(rt, self.effects, row_id);
                Ok(UniqueMutationOutcome::Noop)
            }
            UniqueMutation::Update(ref cols) if cols.is_empty() => {
                accessor.cancel_owned_cold_row(rt, self.effects, row_id);
                Ok(UniqueMutationOutcome::Updated(row_id))
            }
            UniqueMutation::Update(input) => {
                let old = LazyRow::new_prepared(source(), &mut buffer, width)
                    .into_full_row()
                    .change_context(RuntimeError::TableAccess)
                    .disclose()?;
                drop(persisted);
                let result = accessor
                    .update_owned_cold_row(
                        rt,
                        self.effects,
                        row_id,
                        old,
                        RowUpdateInput::Sparse(input),
                        root,
                    )
                    .await
                    .disclose()?;
                Ok(UniqueMutationOutcome::Updated(result.row_id()))
            }
            UniqueMutation::Delete => {
                // Decode indexed columns directly; a constant delete never
                // initializes dense callback scratch.
                let keys = WriteIndexKeySet::from_cold_row(accessor, block, location.row_idx)
                    .change_context(RuntimeError::TableAccess)
                    .disclose()?;
                drop(persisted);
                accessor
                    .finish_owned_cold_delete_effects(rt, self.effects, row_id, keys, root)
                    .await
                    .disclose()?;
                Ok(UniqueMutationOutcome::Deleted)
            }
            UniqueMutation::Insert(_) => unreachable!("validated occupied actions cannot insert"),
        }
    }

    /// Invokes the callback once absence is confirmed by the selection loop.
    async fn mutate_missing<F, E>(
        &mut self,
        mutate_row: F,
    ) -> CallbackResult<UniqueMutationOutcome, E>
    where
        F: for<'row> FnOnce(Option<&mut LazyRow<'row>>) -> CallbackResult<UniqueMutation, E>,
    {
        let action = mutate_row(None)?;
        self.validate_action(false, &action).disclose()?;
        match action {
            UniqueMutation::Skip => Ok(UniqueMutationOutcome::Noop),
            UniqueMutation::Insert(row) => self
                .accessor
                .insert_mvcc(self.rt, self.effects, row)
                .await
                .map(UniqueMutationOutcome::Inserted)
                .disclose()
                .map_err(Into::into),
            _ => unreachable!("validated missing actions can only skip or insert"),
        }
    }

    fn validate_action(&self, occupied: bool, action: &UniqueMutation) -> OperationResult<()> {
        let key = self.key_vals;
        if matches!(
            (occupied, action),
            (true, UniqueMutation::Insert(_))
                | (false, UniqueMutation::Update(_) | UniqueMutation::Delete)
        ) {
            return Err(Report::new(OperationError::InvalidDmlInput)
                .attach("unique mutation action is invalid for the observed entry state"));
        }
        match action {
            UniqueMutation::Insert(row) => {
                if let Some(validator) = &self.validator {
                    validator
                        .validate_full_row(row)
                        .change_context(OperationError::InvalidDmlInput)?;
                }
                let spec = self.accessor.metadata().idx.expect_index_spec(self.index);
                if spec.keys.len() != key.len()
                    || !spec.keys.iter().zip(key).all(|(column, expected)| {
                        row.get(column.column_ordinal.as_usize()) == Some(expected)
                    })
                {
                    return Err(Report::new(OperationError::InvalidDmlInput)
                        .attach("inserted row must match the selected unique key"));
                }
            }
            UniqueMutation::Update(update) => {
                if let Some(validator) = &self.validator {
                    validator
                        .validate_sparse_update(update)
                        .change_context(OperationError::InvalidDmlInput)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CurrentRowDecision, CurrentRowSelection, RowInspection};
    use crate::IndexID;
    use crate::buffer::PoolGuards;
    use crate::buffer::guard::PageGuard;
    use crate::catalog::{
        IndexRef, IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
        StorageIndexKey, StorageIndexSpec, StorageTableSpec, TableCache,
    };
    use crate::error::{
        CallbackError, CallbackResult, ErrorKind, FatalError, LifecycleError, OperationError,
        Validation,
    };
    use crate::id::RowID;
    use crate::index::RowLocation;
    use crate::lock::TableLockMode;
    use crate::lwc::test_decode_counts;
    use crate::row::ops::{UniqueMutation, UniqueMutationOutcome, UpdateCol};
    use crate::session::tests::{
        SessionTestExt, assert_checkpoint_published, wait_for_session_idle,
    };
    use crate::table::Table;
    use crate::table::access::dense_initializations;
    use crate::table::hot::{HotRowLock, HotRowMutator};
    use crate::table::test_hooks::set_test_hot_row_write_before_state_lock_hook;
    use crate::table::tests::{
        assert_freeze_created, bound_unique_index, evictable_test_engine, scan_table_rows,
        table_for_internal_assertion,
    };
    use crate::trx::tests::{
        commit_preparing_shared_trx_status, prepare_event_is_installed, prepare_shared_trx_status,
        prepare_transaction, rollback_preparing_shared_trx_status,
        rollback_production_prepared_for_test, shared_trx_status, transaction_status_for_test,
        with_statement_runtime,
    };
    use crate::trx::undo::{IndexBranchTarget, RowUndoKind, RowUndoRollbackContext, UndoStatus};
    use crate::trx::{MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, MIN_SNAPSHOT_TS};
    use crate::{Engine, Session, TableIndex, Transaction, Val, ValKind};
    use error_stack::Report;
    use futures::FutureExt;
    use smol::future::yield_now;
    use std::cell::{Cell, RefCell};
    use std::panic::AssertUnwindSafe;
    use std::rc::Rc;
    use std::sync::Arc;
    use tempfile::TempDir;

    type CurrentColdHook = (bool, Box<dyn FnOnce()>);

    thread_local! {
        static UNIQUE_DISK_LOOKUPS: Cell<usize> = const { Cell::new(0) };
        static CURRENT_LOOKUP_COUNTS: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 0)) };
        static CURRENT_COLD_HOOK: RefCell<Option<CurrentColdHook>> = const { RefCell::new(None) };
    }

    /// Records entry to the persistent side of a unique lookup.
    pub(crate) fn record_unique_disk_lookup() {
        UNIQUE_DISK_LOOKUPS.set(UNIQUE_DISK_LOOKUPS.get() + 1);
    }

    /// Counts observed unique tree traversals on this test thread.
    pub(crate) fn record_unique_lookup() {
        let (lookups, validations, hints) = CURRENT_LOOKUP_COUNTS.get();
        CURRENT_LOOKUP_COUNTS.set((lookups + 1, validations, hints));
    }

    /// Counts validation of original unique lookup evidence on this test thread.
    pub(crate) fn record_lookup_validation() {
        let (lookups, validations, hints) = CURRENT_LOOKUP_COUNTS.get();
        CURRENT_LOOKUP_COUNTS.set((lookups, validations + 1, hints));
    }

    /// Counts accesses to optional hot successor storage on this test thread.
    pub(crate) fn record_forward_hint() {
        let (lookups, validations, hints) = CURRENT_LOOKUP_COUNTS.get();
        CURRENT_LOOKUP_COUNTS.set((lookups, validations, hints + 1));
    }

    /// Fires a one-shot hook at initial inspection or the late claim boundary.
    pub(super) fn run_current_cold_hook(after_load: bool) {
        let hook = CURRENT_COLD_HOOK.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.as_ref().is_some_and(|(late, _)| *late == after_load) {
                slot.take()
            } else {
                None
            }
        });
        if let Some((_, hook)) = hook {
            hook();
        }
    }

    fn install_selection_hook(state: &str, after_load: bool, hook: impl FnOnce() + 'static) {
        if state == "cold" {
            CURRENT_COLD_HOOK.with(|slot| {
                assert!(
                    slot.borrow_mut()
                        .replace((after_load, Box::new(hook)))
                        .is_none()
                );
            });
        } else {
            set_test_hot_row_write_before_state_lock_hook(hook);
        }
    }

    /// Returns unique DiskTree lookups performed by this test thread.
    fn test_unique_disk_lookups() -> usize {
        UNIQUE_DISK_LOOKUPS.get()
    }

    fn values(id: i32) -> Vec<Val> {
        vec![
            Val::from(id),
            Val::from(id),
            Val::from(10i32),
            Val::from("original"),
        ]
    }

    fn assignment(idx: usize, val: impl Into<Val>) -> UniqueMutation {
        UniqueMutation::Update(vec![UpdateCol {
            idx,
            val: val.into(),
        }])
    }

    async fn fixture(
        state: &str,
        count: i32,
    ) -> (TempDir, Engine, Session, TableIndex, Vec<RowID>) {
        let root = TempDir::new().unwrap();
        let engine = evictable_test_engine(&root, 64 * 1024 * 1024, "unique_callback").await;
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                StorageTableSpec::new(vec![
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::NULLABLE),
                    StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                ]),
                vec![
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(3)],
                        StorageIndexFlags::empty(),
                    ),
                ],
            )
            .await
            .unwrap()
            .table_id();
        let mut trx = session.begin_trx().unwrap();
        let ids = trx
            .table_insert_batch_mvcc(table_id, (0..count).map(values).collect())
            .await
            .unwrap();
        trx.commit().await.unwrap();
        if state != "hot" {
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
        }
        if state == "cold" {
            assert_checkpoint_published(&mut session, table_id).await;
        }
        (
            root,
            engine,
            session,
            TableIndex(table_id, IndexID::new(0)),
            ids,
        )
    }

    async fn apply(
        trx: &mut Transaction,
        index: TableIndex,
        id: i32,
        action: UniqueMutation,
    ) -> CallbackResult<UniqueMutationOutcome> {
        trx.table_unique_mutate_mvcc(index, &[Val::from(id)], |_| Ok(action))
            .await
    }

    async fn read(trx: &mut Transaction, index: TableIndex, id: i32) -> Vec<Val> {
        trx.table_lookup_unique_mvcc(index, &[Val::from(id)], &[0, 1, 2, 3])
            .await
            .unwrap()
            .unwrap_found()
    }

    async fn rekey_in_place(
        trx: &mut Transaction,
        index: TableIndex,
        row_id: RowID,
        from: i32,
        to: i32,
    ) {
        assert_eq!(
            apply(trx, index, from, assignment(0, to)).await.unwrap(),
            UniqueMutationOutcome::Updated(row_id),
            "fixture must retain its physical row"
        );
    }

    fn invalid(result: CallbackResult<UniqueMutationOutcome>) {
        let CallbackError::Engine(error) = result.unwrap_err();
        assert_eq!(
            error.operation_error(),
            Some(OperationError::InvalidDmlInput),
            "{error:?}"
        );
    }

    /// Replaces key zero and proves that the chosen schedule retires its RowID.
    async fn replace_zero(
        writer: &mut Transaction,
        index: TableIndex,
        old_id: RowID,
        delete_insert: bool,
    ) -> RowID {
        let outcome = if delete_insert {
            apply(writer, index, 0, UniqueMutation::Delete)
                .await
                .unwrap();
            let mut row = values(0);
            row[2] = Val::from(20i32);
            apply(writer, index, 0, UniqueMutation::Insert(row))
                .await
                .unwrap()
        } else {
            apply(
                writer,
                index,
                0,
                UniqueMutation::Update(vec![
                    UpdateCol {
                        idx: 2,
                        val: Val::from(20i32),
                    },
                    UpdateCol {
                        idx: 3,
                        val: Val::from(vec![b'r'; 48_000]),
                    },
                ]),
            )
            .await
            .unwrap()
        };
        let new_id = match outcome {
            UniqueMutationOutcome::Inserted(row_id) | UniqueMutationOutcome::Updated(row_id) => {
                row_id
            }
            _ => panic!("replacement must insert or update: {outcome:?}"),
        };
        assert_ne!(new_id, old_id, "fixture must physically replace key zero");
        new_id
    }

    #[test]
    fn test_unique_current_decide_preserves_position_and_advance_preserves_evidence() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("hot", 2).await;
            let reader = session.begin_trx().unwrap();
            assert!(MIN_SNAPSHOT_TS < reader.sts());
            let table = table_for_internal_assertion(&engine, index.0);
            let guards = session.pool_guards();
            let layout = table.layout_snapshot();
            let secondary = layout.secondary_index(IndexSlot::new(0)).unwrap();
            let binding = secondary.unique_mem().unwrap().bind(guards.index_guard());
            let key = [Val::from(0i32)];
            let (candidate, observation) = binding.lookup_observed(&key).await.unwrap();
            assert_eq!(candidate.unwrap().0, ids[0]);
            let mut selection = CurrentRowSelection::new(observation, reader.sts(), ids[0]);
            let rejected_targets = || {
                [
                    RowInspection::MissingRoute,
                    RowInspection::ColdDeleted(None),
                    RowInspection::ColdDeleted(Some(MIN_SNAPSHOT_TS)),
                    RowInspection::ColdDeleted(Some(reader.sts() + 1)),
                    RowInspection::ColdKeyMismatch,
                    RowInspection::ColdConsumed,
                ]
            };

            // Deciding through a shared reference must not consume the original
            // candidate's timestamp authority or advance to the returned RowID.
            for _ in 0..2 {
                let context = &selection;
                assert!(matches!(
                    context.decide(RowInspection::Successor(ids[1])),
                    CurrentRowDecision::Hint(row_id) if row_id == ids[1]
                ));
                assert_eq!(context.row_id(), ids[0]);
                assert!(context.is_original_candidate());
                assert!(matches!(
                    context.decide(RowInspection::HotUnresolved),
                    CurrentRowDecision::Missing
                ));
            }

            let (_, stable_observation) = binding.lookup_observed(&key).await.unwrap();
            let mut forwarded = CurrentRowSelection::new(stable_observation, reader.sts(), ids[0]);
            forwarded.advance(ids[1]);
            assert!(matches!(
                forwarded.decide(RowInspection::HotUnresolved),
                CurrentRowDecision::Missing
            ));
            for inspection in rejected_targets() {
                // An unchanged index and a rejected non-hot forward target
                // contradict the transfer contract; retrying cannot fix it.
                let result =
                    std::panic::catch_unwind(AssertUnwindSafe(|| forwarded.decide(inspection)));
                assert!(
                    result.is_err(),
                    "stable lookup must not retry a rejected forward target"
                );
                assert_eq!(forwarded.row_id(), ids[1]);
            }
            drop(forwarded);

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            apply(&mut writer, index, 0, UniqueMutation::Delete)
                .await
                .unwrap();
            writer.commit().await.unwrap();
            let old_delete = || RowInspection::ColdDeleted(Some(MIN_SNAPSHOT_TS));
            // A real index write invalidated the retained observation. Only an
            // original cold candidate can still use an older confirmed deletion.
            // Hot deletion timestamps are classified by try_lock_current().
            assert!(matches!(
                selection.decide(RowInspection::HotUnresolved),
                CurrentRowDecision::Retry
            ));
            assert!(matches!(
                selection.decide(old_delete()),
                CurrentRowDecision::Missing
            ));
            for successor in [ids[1], ids[0]] {
                selection.advance(successor);
                assert_eq!(selection.row_id(), successor);
                assert!(!selection.is_original_candidate());
                // Advancing must retain the invalidated original evidence,
                // including when traversal returns to the initial RowID.
                assert!(matches!(
                    selection.decide(RowInspection::HotUnresolved),
                    CurrentRowDecision::Retry
                ));
                assert!(matches!(
                    selection.decide(old_delete()),
                    CurrentRowDecision::Retry
                ));
                for inspection in rejected_targets() {
                    assert!(matches!(
                        selection.decide(inspection),
                        CurrentRowDecision::Retry
                    ));
                }
            }
            drop(selection);
            reader.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_current_row_page_range_mismatch_panics_before_callback() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("hot", 1).await;
            // Drain insert undo before deliberately corrupting a page that
            // background purge could otherwise inspect.
            session
                .wait_for_purge_completion_after(session.last_cts())
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, index.0);
            let guards = session.pool_guards();
            let row_id = ids[0];
            let RowLocation::RowPage(page_id) = table.find_row(&guards, row_id).await.unwrap()
            else {
                panic!("fixture row must have a hot route");
            };
            let mut trx = session.begin_trx().unwrap();
            let mut page = table
                .mem
                .must_get_row_page_exclusive(&guards, page_id)
                .await
                .unwrap();
            let original_start = page.page_mut().header.start_row_id;
            // Both index routes stay unchanged, so a retry would encounter
            // exactly the same range mismatch indefinitely.
            page.page_mut().header.start_row_id = row_id + 1;
            drop(page);

            let called = Cell::new(false);
            let result = AssertUnwindSafe(trx.table_unique_mutate_mvcc(
                index,
                &[Val::from(0i32)],
                |_| -> CallbackResult<_> {
                    called.set(true);
                    Ok(UniqueMutation::Skip)
                },
            ))
            .catch_unwind()
            .await;

            let mut page = table
                .mem
                .must_get_row_page_exclusive(&guards, page_id)
                .await
                .unwrap();
            page.page_mut().header.start_row_id = original_start;
            drop(page);

            let panic = result.expect_err("a persistent range mismatch must fail immediately");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            assert!(
                message.contains("unique mutation row page does not match selected row"),
                "unexpected panic: {message}"
            );
            assert!(
                !called.get(),
                "invalid row routing must not invoke the callback"
            );
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            wait_for_session_idle(&engine.inner().session_registry, session.id()).await;
            let mut reader = session.begin_trx().unwrap();
            assert_eq!(read(&mut reader, index, 0).await, values(0));
            reader.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_current_rollback_exposes_forward_target_that_becomes_cold() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("hot", 1).await;
            let target = ids[0];
            let mut create = session.begin_trx().unwrap();
            let mut source_values = values(10);
            source_values[3] = Val::from(vec![b'x'; 48_000]);
            let UniqueMutationOutcome::Inserted(source) = apply(
                &mut create,
                index,
                10,
                UniqueMutation::Insert(source_values),
            )
            .await
            .unwrap() else {
                panic!("source must be inserted")
            };
            create.commit().await.unwrap();
            let table = table_for_internal_assertion(&engine, index.0);
            let guards = session.pool_guards();
            let RowLocation::RowPage(target_page) = table.find_row(&guards, target).await.unwrap()
            else {
                panic!("target must be hot")
            };
            let RowLocation::RowPage(source_page) = table.find_row(&guards, source).await.unwrap()
            else {
                panic!("source must be hot")
            };
            assert_ne!(target_page, source_page);
            assert!(target < source);
            let mut anchor_session = engine.new_session().unwrap();
            let anchor = anchor_session.begin_trx().unwrap();
            let mut old = session.begin_trx().unwrap();
            rekey_in_place(&mut old, index, source, 10, 11).await;
            rekey_in_place(&mut old, index, target, 0, 10).await;
            let transfer_cts = old.commit().await.unwrap();
            let mut remove = session.begin_trx().unwrap();
            rekey_in_place(&mut remove, index, target, 10, 0).await;
            let remove_cts = remove.commit().await.unwrap();

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            rekey_in_place(&mut writer, index, source, 11, 10).await;
            let mut reader = session.begin_trx().unwrap();
            assert!(transfer_cts < reader.sts());
            assert!(remove_cts < reader.sts());
            let layout = table.layout_snapshot();
            let secondary = layout.secondary_index(IndexSlot::new(0)).unwrap();
            let binding = secondary.unique_mem().unwrap().bind(guards.index_guard());
            let key = [Val::from(10i32)];
            let (candidate, observation) = binding.lookup_observed(&key).await.unwrap();
            assert_eq!(candidate.unwrap().0, source);
            let mut selection = CurrentRowSelection::new(observation, reader.sts(), source);
            writer.rollback().await.unwrap();

            // Match the production reader after ownership admission rejects the
            // source's restored current key and exposes its old departure.
            let page = table
                .mem
                .must_get_row_page_shared(&guards, source_page)
                .await
                .unwrap();
            let index_ref = IndexRef::new(index.1, IndexSlot::new(0));
            let successor =
                page.write_row_by_id(source)
                    .current_successor(layout.metadata(), index_ref, &key);
            assert_eq!(successor, Some(target));
            assert!(matches!(
                selection.decide(RowInspection::Successor(successor.unwrap())),
                CurrentRowDecision::Hint(row_id) if row_id == target
            ));
            selection.advance(target);
            drop(page);

            // The reader keeps the copied RowID, but the older anchor no longer
            // keeps the transfer's image CTS above checkpoint's cutoff.
            anchor.rollback().await.unwrap();
            session
                .wait_for_purge_completion_after(remove_cts)
                .await
                .unwrap();
            assert_freeze_created(writer_session.freeze_table(index.0, 1).await.unwrap());
            assert_eq!(
                table.checkpoint_workflow.frozen_page_ids().unwrap(),
                vec![target_page]
            );
            assert_checkpoint_published(&mut writer_session, index.0).await;
            assert!(matches!(
                table.find_row(&guards, source).await.unwrap(),
                RowLocation::RowPage(_)
            ));
            let RowLocation::LwcBlock(location) =
                table.find_row(&guards, selection.row_id()).await.unwrap()
            else {
                panic!("captured forward target should now be cold")
            };
            let accessor = table.accessor_with_layout(&layout);
            let persisted = accessor
                .column_storage()
                .load_lwc_block(guards.disk_guard(), location.block_id)
                .await
                .unwrap();
            let spec = layout.metadata().idx.index_spec(IndexSlot::new(0)).unwrap();
            let actual = persisted
                .block()
                .decode_index_key_values(layout.metadata().col.as_ref(), spec, location.row_idx)
                .unwrap();
            assert_ne!(actual, key);
            drop(persisted);
            assert!(!selection.observation.is_valid());
            assert!(matches!(
                selection.decide(RowInspection::ColdKeyMismatch),
                CurrentRowDecision::Retry
            ));
            drop(selection);
            reader
                .table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                    assert!(row.is_none());
                    Ok(UniqueMutation::Skip)
                })
                .await
                .unwrap();
            reader.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_current_replacement_before_inspection() {
        smol::block_on(async {
            for (state, after_load, delete_insert) in [
                ("hot", false, false),
                ("frozen", false, false),
                ("hot", false, true),
                ("cold", false, false),
                ("cold", true, false),
            ] {
                for insert_missing in [false, true] {
                    let (_root, engine, mut session, index, ids) = fixture(state, 300).await;
                    if state == "cold" {
                        let cleanup = session
                            .cleanup_secondary_mem_indexes(index.0, true)
                            .await
                            .unwrap();
                        assert_eq!(cleanup.live_delay, None);
                        assert!(cleanup.stats.indexes.iter().all(|stats| stats.removed > 0));
                    }
                    let mut writer_session = engine.new_session().unwrap();
                    let mut writer = writer_session.begin_trx().unwrap();
                    let mut reader = session.begin_trx().unwrap();
                    let mut snapshot_session = engine.new_session().unwrap();
                    let mut snapshot = snapshot_session.begin_trx().unwrap();
                    let old_id = ids[0];
                    let baseline = Rc::new(Cell::new((0, 0, 0)));
                    let captured = Rc::clone(&baseline);
                    install_selection_hook(state, after_load, move || {
                        smol::block_on(async {
                            replace_zero(&mut writer, index, old_id, delete_insert).await;
                            writer.commit().await.unwrap();
                        });
                        captured.set(CURRENT_LOOKUP_COUNTS.get());
                    });
                    let mut calls = 0;
                    let result = reader.table_unique_mutate_mvcc(index, &[Val::from(0i32)], |row| -> CallbackResult<_> {
                        calls += 1;
                        let Some(row) = row else {
                            return Ok(if insert_missing { UniqueMutation::Insert(values(0)) } else { UniqueMutation::Skip });
                        };
                        let value = row.val(2)?.as_i32().unwrap();
                        assert_eq!(value, 20, "state={state}, late={after_load}, delete_insert={delete_insert}");
                        if state == "hot" || state == "frozen" {
                            let now = CURRENT_LOOKUP_COUNTS.get();
                            let before = baseline.get();
                            assert_eq!((now.0 - before.0, now.1 - before.1, now.2 - before.2), (0, 0, 1), "one successful hot hint saves relookup");
                        }
                        Ok(assignment(2, value + 1))
                    }).await.unwrap();
                    assert_eq!(calls, 1);
                    assert!(
                        matches!(result, UniqueMutationOutcome::Updated(_)),
                        "{result:?}"
                    );
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    let rows = scan_table_rows(&mut snapshot, index.0, &[0, 2]).await;
                    assert_eq!(
                        rows.len(),
                        300,
                        "snapshot scans must not duplicate backward branches"
                    );
                    assert_eq!(
                        rows.iter()
                            .filter(|row| row[0] == Val::from(0i32))
                            .collect::<Vec<_>>(),
                        vec![&vec![Val::from(0i32), Val::from(10i32)]]
                    );
                    reader.commit().await.unwrap();
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    snapshot.rollback().await.unwrap();
                    let mut verify = session.begin_trx().unwrap();
                    assert_eq!(read(&mut verify, index, 0).await[2], Val::from(21i32));
                    verify.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_current_replacement_preparing_and_active() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                for settle in ["commit", "rollback", "active"] {
                    let (_root, engine, mut session, index, ids) = fixture(state, 300).await;
                    let mut writer_session = engine.new_session().unwrap();
                    let mut writer = writer_session.begin_trx().unwrap();
                    let status = transaction_status_for_test(&writer);
                    let mut reader = session.begin_trx().unwrap();
                    let prepared = Rc::new(RefCell::new(None));
                    let active = Rc::new(RefCell::new(None));
                    let saved_prepared = Rc::clone(&prepared);
                    let saved_active = Rc::clone(&active);
                    let old_id = ids[0];
                    install_selection_hook(state, false, move || {
                        smol::block_on(replace_zero(&mut writer, index, old_id, false));
                        if settle == "active" {
                            *saved_active.borrow_mut() = Some(writer);
                        } else {
                            *saved_prepared.borrow_mut() =
                                Some(prepare_transaction(writer).unwrap());
                        }
                    });
                    let calls = Cell::new(0);
                    let key = [Val::from(0i32)];
                    let mutation =
                        reader.table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                            calls.set(calls.get() + 1);
                            let value = row
                                .expect("settled replacement retains the key")
                                .val(2)?
                                .as_i32()
                                .unwrap();
                            assert_eq!(value, if settle == "rollback" { 10 } else { 20 });
                            Ok(assignment(2, value + 1))
                        });
                    let release = async {
                        if settle == "active" {
                            return;
                        }
                        while !prepare_event_is_installed(&status) {
                            yield_now().await;
                        }
                        assert_eq!(calls.get(), 0);
                        let prepared = prepared.borrow_mut().take().unwrap();
                        if settle == "commit" {
                            engine
                                .inner()
                                .trx_sys
                                .commit_prepared(prepared)
                                .await
                                .unwrap();
                        } else {
                            rollback_production_prepared_for_test(prepared).await;
                        }
                    };
                    let (result, ()) = futures::join!(mutation, release);
                    if settle == "active" {
                        let CallbackError::Engine(error) = result.unwrap_err();
                        assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                        assert_eq!(calls.get(), 0);
                        let writer = active.borrow_mut().take().unwrap();
                        writer.rollback().await.unwrap();
                    } else {
                        assert!(matches!(result.unwrap(), UniqueMutationOutcome::Updated(_)));
                        assert_eq!(calls.get(), 1);
                    }
                    reader.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_current_successor_changes_follow_complete_hot_chain() {
        smol::block_on(async {
            for change in ["move", "rekey", "delete", "cold"] {
                let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                let mut writer_session = engine.new_session().unwrap();
                let mut writer = writer_session.begin_trx().unwrap();
                let mut reader = session.begin_trx().unwrap();
                let old_id = ids[0];
                install_selection_hook("hot", false, move || {
                    smol::block_on(async {
                        let b = replace_zero(&mut writer, index, old_id, true).await;
                        match change {
                            "move" => {
                                replace_zero(&mut writer, index, b, false).await;
                            }
                            "rekey" => {
                                apply(&mut writer, index, 0, assignment(0, 900i32))
                                    .await
                                    .unwrap();
                                let mut replacement = values(0);
                                replacement[1] = Val::from(901i32);
                                replacement[2] = Val::from(20i32);
                                apply(&mut writer, index, 0, UniqueMutation::Insert(replacement))
                                    .await
                                    .unwrap();
                            }
                            "delete" => {
                                apply(&mut writer, index, 0, UniqueMutation::Delete)
                                    .await
                                    .unwrap();
                            }
                            "cold" => (),
                            _ => unreachable!(),
                        }
                        writer.commit().await.unwrap();
                        if change == "cold" {
                            assert_freeze_created(
                                writer_session
                                    .freeze_table(index.0, usize::MAX)
                                    .await
                                    .unwrap(),
                            );
                            let outcome = writer_session.checkpoint_table(index.0).await.unwrap();
                            assert!(
                                matches!(
                                    outcome,
                                    crate::table::CheckpointOutcome::Delayed {
                                        reason:
                                            crate::table::CheckpointDelayReason::FrozenPageCutoff { .. }
                                    }
                                ),
                                "retained reader must protect replacement history: {outcome:?}"
                            );
                        }
                    });
                    CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                });
                let mut calls = 0;
                reader
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls += 1;
                            assert_eq!(row.is_none(), change == "delete");
                            if let Some(row) = row {
                                assert_eq!(row.val(2)?, &Val::from(20i32));
                            }
                            let expected = match change {
                                "cold" => (0, 0, 1),
                                "delete" => (1, 2, 3),
                                _ => (0, 0, 2),
                            };
                            assert_eq!(
                                CURRENT_LOOKUP_COUNTS.get(),
                                expected,
                                "successful chains skip index validation; terminal chains validate: {change}"
                            );
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(calls, 1);
                reader.rollback().await.unwrap();
                if change == "cold" {
                    // Checkpoint's FrozenPageCutoff protects the retained reader.
                    // Publication becomes legal only after that reader releases STS.
                    assert_checkpoint_published(&mut session, index.0).await;
                    let mut current = session.begin_trx().unwrap();
                    assert_eq!(read(&mut current, index, 0).await[2], Val::from(20i32));
                    apply(&mut current, index, 0, assignment(2, 21i32))
                        .await
                        .unwrap();
                    current.commit().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_current_rollback_restores_surviving_source_links() {
        smol::block_on(async {
            for (source_update, rekey, target_owner, claim_again) in
                [false, true].into_iter().flat_map(|source_update| {
                    [
                        (false, "none", false),
                        (false, "none", true),
                        (true, "none", false),
                        (true, "active", false),
                        (true, "preparing", false),
                    ]
                    .into_iter()
                    .map(move |(rekey, target_owner, claim_again)| {
                        (source_update, rekey, target_owner, claim_again)
                    })
                })
            {
                let (_root, engine, mut session, index, _) = fixture("hot", 300).await;
                let mut writer_session = engine.new_session().unwrap();
                let mut writer = writer_session.begin_trx().unwrap();
                let mut other_session = engine.new_session().unwrap();
                let mut other = other_session.begin_trx().unwrap();
                let mut reader = session.begin_trx().unwrap();
                let active = Rc::new(RefCell::new(None));
                let prepared = Rc::new(RefCell::new(None));
                let saved_active = Rc::clone(&active);
                let saved_prepared = Rc::clone(&prepared);
                install_selection_hook("hot", false, move || {
                    smol::block_on(async {
                        apply(
                            &mut writer,
                            index,
                            0,
                            if source_update {
                                assignment(0, 900i32)
                            } else {
                                UniqueMutation::Delete
                            },
                        )
                        .await
                        .unwrap();
                        // The first unique index publishes its hint before index one fails.
                        let failed = if rekey {
                            apply(
                                &mut writer,
                                index,
                                2,
                                UniqueMutation::Update(vec![
                                    UpdateCol {
                                        idx: 0,
                                        val: Val::from(0i32),
                                    },
                                    UpdateCol {
                                        idx: 1,
                                        val: Val::from(1i32),
                                    },
                                ]),
                            )
                            .await
                        } else {
                            let mut row = values(0);
                            row[1] = Val::from(1i32);
                            apply(&mut writer, index, 0, UniqueMutation::Insert(row)).await
                        };
                        let CallbackError::Engine(error) = failed.unwrap_err();
                        assert_eq!(error.operation_error(), Some(OperationError::DuplicateKey));
                        if claim_again {
                            let mut row = values(0);
                            if source_update {
                                row[1] = Val::from(901i32);
                            }
                            row[2] = Val::from(20i32);
                            apply(&mut writer, index, 0, UniqueMutation::Insert(row))
                                .await
                                .unwrap();
                        }
                        writer.commit().await.unwrap();
                        if target_owner != "none" {
                            apply(&mut other, index, 2, assignment(2, 99i32))
                                .await
                                .unwrap();
                        }
                        if target_owner == "preparing" {
                            *saved_prepared.borrow_mut() =
                                Some(prepare_transaction(other).unwrap());
                        } else {
                            *saved_active.borrow_mut() = Some(other);
                        }
                    });
                });
                let mut calls = 0;
                reader
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls += 1;
                            assert_eq!(row.is_some(), claim_again);
                            if let Some(row) = row {
                                assert_eq!(row.val(2)?, &Val::from(20i32));
                            }
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    calls, 1,
                    "restored links cannot reach a rolled-back destination"
                );
                reader.rollback().await.unwrap();
                let active = active.borrow_mut().take();
                if let Some(other) = active {
                    other.rollback().await.unwrap();
                }
                let prepared = prepared.borrow_mut().take();
                if let Some(other) = prepared {
                    rollback_production_prepared_for_test(other).await;
                }
            }
        });
    }

    #[test]
    fn test_unique_current_statement_rollback_restores_multiple_update_sources() {
        smol::block_on(async {
            for selected in [0, 1] {
                for existing_destination in [false, true] {
                    let (_root, engine, mut session, index, _) = fixture("hot", 3).await;
                    let mut setup = session.begin_trx().unwrap();
                    for key in 0..3 {
                        apply(&mut setup, index, key, assignment(2, key))
                            .await
                            .unwrap();
                    }
                    setup.commit().await.unwrap();
                    session
                        .create_index(
                            index.0,
                            StorageIndexSpec::new(
                                vec![StorageIndexKey::new(2)],
                                StorageIndexFlags::UK,
                            ),
                        )
                        .await
                        .unwrap();
                    let mut writer_session = engine.new_session().unwrap();
                    let mut writer = writer_session.begin_trx().unwrap();
                    let mut other_session = engine.new_session().unwrap();
                    let mut other = other_session.begin_trx().unwrap();
                    let mut reader = session.begin_trx().unwrap();
                    let active = Rc::new(RefCell::new(None));
                    let retained = Rc::clone(&active);
                    install_selection_hook("hot", false, move || {
                        smol::block_on(async {
                            apply(&mut writer, index, 0, assignment(0, 900i32))
                                .await
                                .unwrap();
                            apply(&mut writer, index, 1, assignment(1, 901i32))
                                .await
                                .unwrap();
                            // Two unique indexes publish links from different
                            // source Updates before the third unique index fails.
                            let failed = if existing_destination {
                                apply(
                                    &mut writer,
                                    index,
                                    2,
                                    UniqueMutation::Update(vec![
                                        UpdateCol {
                                            idx: 0,
                                            val: Val::from(0i32),
                                        },
                                        UpdateCol {
                                            idx: 1,
                                            val: Val::from(1i32),
                                        },
                                        UpdateCol {
                                            idx: 2,
                                            val: Val::from(0i32),
                                        },
                                    ]),
                                )
                                .await
                            } else {
                                let mut row = values(0);
                                row[1] = Val::from(1i32);
                                row[2] = Val::from(0i32);
                                apply(&mut writer, index, 0, UniqueMutation::Insert(row)).await
                            };
                            let CallbackError::Engine(error) = failed.unwrap_err();
                            assert_eq!(error.operation_error(), Some(OperationError::DuplicateKey));
                            writer.commit().await.unwrap();
                            apply(&mut other, index, 2, assignment(3, "unrelated"))
                                .await
                                .unwrap();
                            *retained.borrow_mut() = Some(other);
                        });
                        CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                    });
                    let mut calls = 0;
                    reader
                        .table_unique_mutate_mvcc(
                            TableIndex(index.0, IndexID::new(selected)),
                            &[Val::from(selected as i32)],
                            |row| -> CallbackResult<_> {
                                calls += 1;
                                assert!(
                                    row.is_none(),
                                    "failed statement must restore both source slots"
                                );
                                assert_eq!(CURRENT_LOOKUP_COUNTS.get(), (1, 2, 2));
                                Ok(UniqueMutation::Skip)
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(calls, 1);
                    let other = active.borrow_mut().take().unwrap();
                    other.rollback().await.unwrap();
                    reader.rollback().await.unwrap();
                    let mut verify = session.begin_trx().unwrap();
                    assert_eq!(read(&mut verify, index, 900).await[1], Val::from(0i32));
                    assert_eq!(read(&mut verify, index, 1).await[1], Val::from(901i32));
                    assert_eq!(read(&mut verify, index, 2).await[2], Val::from(2i32));
                    verify.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_current_cancelled_forward_restore_retains_destination_undo() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("frozen", 300).await;
            let mut writer = session.begin_trx().unwrap();
            let UniqueMutationOutcome::Updated(destination) =
                apply(&mut writer, index, 0, assignment(2, 20i32))
                    .await
                    .unwrap()
            else {
                panic!("frozen update must move its row")
            };
            assert_ne!(destination, ids[0]);
            let table = table_for_internal_assertion(&engine, index.0);
            let mut checkout = writer.checkout().unwrap();
            let sts = checkout.inner().sts();
            let guards = checkout.attachment().pool_guards().clone();
            let undo = checkout.inner_mut().effects_mut().row_undo_mut();
            let count = undo.len();
            let source_page = undo
                .iter()
                .find(|undo| undo.row_id == ids[0])
                .unwrap()
                .page_id
                .unwrap();
            let destination_page = undo.last().unwrap().page_id.unwrap();
            assert_ne!(
                source_page, destination_page,
                "only source restoration may block before the destination is undone"
            );
            let exclusive = table
                .mem
                .get_row_page_exclusive(&guards, source_page.page_id)
                .await
                .unwrap()
                .unwrap();
            let mut cache = TableCache::new(engine.inner().core.catalog());
            checkout
                .inner_mut()
                .effects_mut()
                .index_undo_mut()
                .rollback(&mut cache, &guards, sts)
                .await
                .unwrap();
            let context = RowUndoRollbackContext::new(&guards, &engine.inner().poisoner);
            let mut rollback = Box::pin(
                checkout
                    .inner_mut()
                    .effects_mut()
                    .row_undo_mut()
                    .rollback(&mut cache, context),
            );
            assert!(futures::poll!(rollback.as_mut()).is_pending());
            drop(rollback);
            let undo = checkout.inner_mut().effects_mut().row_undo_mut();
            assert_eq!(
                undo.len(),
                count,
                "cancellation must retain the destination and its unfinished restoration records"
            );
            assert_eq!(undo.last().unwrap().row_id, destination);
            drop(exclusive);
            undo.rollback(&mut cache, context).await.unwrap();
            assert!(undo.is_empty());
            drop(checkout);
            writer.rollback().await.unwrap();
            let mut verify = session.begin_trx().unwrap();
            assert_eq!(read(&mut verify, index, 0).await, values(0));
            assert_eq!(
                apply(&mut verify, index, 0, UniqueMutation::Update(vec![]))
                    .await
                    .unwrap(),
                UniqueMutationOutcome::Updated(ids[0])
            );
            verify.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_current_captured_uncommitted_destination_survives_writer_rollback() {
        smol::block_on(async {
            for history in ["none", "terminal", "forward"] {
                let existing_destination = history != "none";
                let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                let mut anchor_session = engine.new_session().unwrap();
                let anchor = anchor_session.begin_trx().unwrap();
                if existing_destination {
                    // Retain an older, unrelated departure of this same key on
                    // the destination. Rollback of the new claim exposes that
                    // old terminal history, which cannot prove current absence.
                    let mut old = session.begin_trx().unwrap();
                    rekey_in_place(&mut old, index, ids[0], 0, 800).await;
                    rekey_in_place(&mut old, index, ids[2], 2, 0).await;
                    old.commit().await.unwrap();
                    let mut remove = session.begin_trx().unwrap();
                    rekey_in_place(&mut remove, index, ids[2], 0, 2).await;
                    if history == "forward" {
                        rekey_in_place(&mut remove, index, ids[1], 1, 0).await;
                    }
                    remove.commit().await.unwrap();
                    if history == "forward" {
                        let mut terminal = session.begin_trx().unwrap();
                        rekey_in_place(&mut terminal, index, ids[1], 0, 1).await;
                        terminal.commit().await.unwrap();
                    }
                    let mut restore = session.begin_trx().unwrap();
                    rekey_in_place(&mut restore, index, ids[0], 800, 0).await;
                    restore.commit().await.unwrap();
                }
                let mut writer_session = engine.new_session().unwrap();
                let mut writer = writer_session.begin_trx().unwrap();
                rekey_in_place(&mut writer, index, ids[0], 0, 900).await;
                if existing_destination {
                    rekey_in_place(&mut writer, index, ids[2], 2, 0).await;
                } else {
                    let mut row = values(0);
                    row[1] = Val::from(901i32);
                    apply(&mut writer, index, 0, UniqueMutation::Insert(row))
                        .await
                        .unwrap();
                }
                let mut reader = session.begin_trx().unwrap();
                // The index already selects the uncommitted destination. Roll
                // back before its ownership is inspected, restoring key 0 to
                // the original row and unlinking the destination's claim undo.
                install_selection_hook("hot", false, move || {
                    smol::block_on(writer.rollback()).unwrap();
                    CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                });
                let mut calls = 0;
                let result = reader
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls += 1;
                            assert!(row.is_some(), "rollback must not hide the restored committed key owner: history={history}");
                            assert_eq!(CURRENT_LOOKUP_COUNTS.get(), (1, 1, match history {
                                "none" => 0,
                                "terminal" => 1,
                                "forward" => 2,
                                _ => unreachable!(),
                            }));
                            Ok(UniqueMutation::Update(vec![]))
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(result, UniqueMutationOutcome::Updated(ids[0]));
                assert_eq!(calls, 1);
                reader.rollback().await.unwrap();
                anchor.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_current_successors_are_per_index() {
        smol::block_on(async {
            for selected in [0, 1] {
                let (_root, engine, mut session, index, _) = fixture("hot", 300).await;
                let mut writer_session = engine.new_session().unwrap();
                let mut writer = writer_session.begin_trx().unwrap();
                let mut reader = session.begin_trx().unwrap();
                install_selection_hook("hot", false, move || {
                    smol::block_on(async {
                        apply(&mut writer, index, 0, UniqueMutation::Delete)
                            .await
                            .unwrap();
                        let mut b = values(0);
                        b[1] = Val::from(900i32);
                        b[2] = Val::from(20i32);
                        apply(&mut writer, index, 0, UniqueMutation::Insert(b))
                            .await
                            .unwrap();
                        let mut c = values(901);
                        c[1] = Val::from(0i32);
                        c[2] = Val::from(30i32);
                        apply(&mut writer, index, 901, UniqueMutation::Insert(c))
                            .await
                            .unwrap();
                        writer.commit().await.unwrap();
                    });
                    CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                });
                let mut calls = 0;
                reader
                    .table_unique_mutate_mvcc(
                        TableIndex(index.0, IndexID::new(selected)),
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls += 1;
                            assert_eq!(
                                row.unwrap().val(2)?,
                                &Val::from(if selected == 0 { 20i32 } else { 30i32 })
                            );
                            assert_eq!(CURRENT_LOOKUP_COUNTS.get(), (0, 0, 1));
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(calls, 1);
                reader.rollback().await.unwrap();
            }
        });
    }

    fn moved_key_update(key: i32, other_key: Option<i32>) -> UniqueMutation {
        let mut update = vec![UpdateCol {
            idx: 0,
            val: Val::from(key),
        }];
        if let Some(other_key) = other_key {
            update.push(UpdateCol {
                idx: 1,
                val: Val::from(other_key),
            });
        }
        update.push(UpdateCol {
            idx: 3,
            val: Val::from(vec![b'm'; 48_000]),
        });
        UniqueMutation::Update(update)
    }

    async fn assert_moved_key_branches(
        table: &Table,
        guards: &PoolGuards,
        destination: RowID,
        expected: &[(u16, RowID, &str)],
    ) {
        let RowLocation::RowPage(page_id) = table.mem.find_row(guards, destination).await.unwrap()
        else {
            panic!("move destination must remain hot");
        };
        let page = table
            .mem
            .must_get_row_page_shared(guards, page_id)
            .await
            .unwrap();
        let access = page.read_row_by_id(destination);
        let branches = &access.undo_head().unwrap().next.indexes;
        assert_eq!(
            branches.len(),
            expected.len(),
            "one branch per retained unique-key history"
        );
        for &(slot, source, kind) in expected {
            let branch = branches
                .iter()
                .find(|branch| branch.key.index.slot() == IndexSlot::new(slot))
                .unwrap();
            let IndexBranchTarget::Hot { entry, .. } = &branch.target else {
                panic!("fixture requires a hot predecessor");
            };
            let entry = entry.as_ref();
            assert_eq!(entry.row_id, source, "index_slot={slot}");
            let links = match (&entry.kind, kind) {
                (RowUndoKind::Delete(links), "delete") => links,
                (RowUndoKind::Update(update), "update") => update.forward(),
                _ => panic!("wrong departure kind for index_slot={slot}: expected={kind}"),
            };
            assert_eq!(
                links.successor(branch.key.index),
                Some(destination),
                "index_slot={slot}"
            );
        }
    }

    #[test]
    fn test_unique_moved_key_reuse_follows_earlier_update() {
        smol::block_on(async {
            for freeze in [false, true] {
                let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                let table = table_for_internal_assertion(&engine, index.0);
                let mut writer_session = engine.new_session().unwrap();
                let mut writer = writer_session.begin_trx().unwrap();
                let mut freezer = engine.new_session().unwrap();
                let mut snapshot_session = engine.new_session().unwrap();
                let mut snapshot = snapshot_session.begin_trx().unwrap();
                let mut reader = session.begin_trx().unwrap();
                let source = ids[0];
                let destination = Rc::new(Cell::new(source));
                let selected = Rc::clone(&destination);
                install_selection_hook("hot", false, move || {
                    smol::block_on(async {
                        rekey_in_place(&mut writer, index, source, 0, 900).await;
                        if freeze {
                            assert_freeze_created(
                                freezer.freeze_table(index.0, usize::MAX).await.unwrap(),
                            );
                        }
                        let UniqueMutationOutcome::Updated(moved) =
                            apply(&mut writer, index, 900, moved_key_update(0, None))
                                .await
                                .unwrap()
                        else {
                            panic!("reused key must move its row");
                        };
                        assert_ne!(moved, source);
                        assert_moved_key_branches(
                            &table,
                            &writer_session.pool_guards(),
                            moved,
                            &[(0, source, "update"), (1, source, "delete")],
                        )
                        .await;
                        selected.set(moved);
                        writer.commit().await.unwrap();
                    });
                    CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                });
                let result = reader
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            assert_eq!(row.unwrap().val(3)?, &Val::from(vec![b'm'; 48_000]));
                            assert_eq!(
                                CURRENT_LOOKUP_COUNTS.get(),
                                (0, 0, 1),
                                "the earlier Update must supply the successor: freeze={freeze}"
                            );
                            Ok(UniqueMutation::Update(vec![]))
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(result, UniqueMutationOutcome::Updated(destination.get()));
                assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                reader.rollback().await.unwrap();
                snapshot.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_moved_key_history_uses_actual_previous_owner() {
        smol::block_on(async {
            for previous in ["absent", "deleted", "updated"] {
                for commit in [false, true] {
                    let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                    let table = table_for_internal_assertion(&engine, index.0);
                    let mut snapshot_session = engine.new_session().unwrap();
                    let mut snapshot = snapshot_session.begin_trx().unwrap();
                    let mut writer = session.begin_trx().unwrap();
                    let new_key = if previous == "absent" { 900 } else { 1 };
                    match previous {
                        "deleted" => {
                            apply(&mut writer, index, 1, UniqueMutation::Delete)
                                .await
                                .unwrap();
                        }
                        "updated" => {
                            rekey_in_place(&mut writer, index, ids[1], 1, 901).await;
                        }
                        _ => (),
                    }
                    let UniqueMutationOutcome::Updated(destination) =
                        apply(&mut writer, index, 0, moved_key_update(new_key, None))
                            .await
                            .unwrap()
                    else {
                        panic!("new key must move its row");
                    };
                    assert_ne!(destination, ids[0]);
                    let mut expected = vec![(1, ids[0], "delete")];
                    if previous != "absent" {
                        expected.push((
                            0,
                            ids[1],
                            if previous == "deleted" {
                                "delete"
                            } else {
                                "update"
                            },
                        ));
                    }
                    assert_moved_key_branches(
                        &table,
                        &session.pool_guards(),
                        destination,
                        &expected,
                    )
                    .await;
                    let mut updated = values(0);
                    updated[0] = Val::from(new_key);
                    updated[3] = Val::from(vec![b'm'; 48_000]);
                    assert_eq!(read(&mut writer, index, new_key).await, updated);
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    if previous == "absent" {
                        assert!(
                            snapshot
                                .table_lookup_unique_mvcc(index, &[Val::from(new_key)], &[0])
                                .await
                                .unwrap()
                                .not_found()
                        );
                    } else {
                        assert_eq!(read(&mut snapshot, index, new_key).await, values(1));
                    }
                    if commit {
                        writer.commit().await.unwrap();
                    } else {
                        writer.rollback().await.unwrap();
                    }
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    assert_eq!(read(&mut snapshot, index, 1).await, values(1));
                    snapshot.rollback().await.unwrap();
                    let mut verify = session.begin_trx().unwrap();
                    assert_eq!(
                        read(&mut verify, index, if commit { new_key } else { 0 }).await,
                        if commit { updated } else { values(0) }
                    );
                    if !commit {
                        assert_eq!(read(&mut verify, index, 1).await, values(1));
                    }
                    verify.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_moved_key_failure_restores_update_successor() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                let table = table_for_internal_assertion(&engine, index.0);
                let mut snapshot_session = engine.new_session().unwrap();
                let mut snapshot = snapshot_session.begin_trx().unwrap();
                let mut writer = session.begin_trx().unwrap();
                rekey_in_place(&mut writer, index, ids[0], 0, 900).await;
                // Index zero transfers the reused key; index one then rejects its live owner.
                let CallbackError::Engine(error) =
                    apply(&mut writer, index, 900, moved_key_update(0, Some(1)))
                        .await
                        .unwrap_err();
                assert_eq!(error.operation_error(), Some(OperationError::DuplicateKey));
                let mut earlier = values(0);
                earlier[0] = Val::from(900i32);
                assert_eq!(read(&mut writer, index, 900).await, earlier);
                assert!(
                    writer
                        .table_lookup_unique_mvcc(index, &[Val::from(0i32)], &[0])
                        .await
                        .unwrap()
                        .not_found()
                );
                let guards = session.pool_guards();
                let RowLocation::RowPage(page_id) =
                    table.mem.find_row(&guards, ids[0]).await.unwrap()
                else {
                    panic!("source must remain hot")
                };
                {
                    let page = table
                        .mem
                        .must_get_row_page_shared(&guards, page_id)
                        .await
                        .unwrap();
                    let access = page.write_row_by_id(ids[0]);
                    let layout = table.layout_snapshot();
                    let index_ref = layout.resolve_index_id(index.1).unwrap();
                    assert_eq!(
                        access.current_successor(layout.metadata(), index_ref, &[Val::from(0i32)]),
                        None,
                        "statement rollback must remove the failed transfer"
                    );
                }
                let UniqueMutationOutcome::Updated(destination) =
                    apply(&mut writer, index, 900, moved_key_update(0, None))
                        .await
                        .unwrap()
                else {
                    panic!("valid retry must move")
                };
                assert_ne!(destination, ids[0]);
                assert_moved_key_branches(
                    &table,
                    &guards,
                    destination,
                    &[(0, ids[0], "update"), (1, ids[0], "delete")],
                )
                .await;
                if commit {
                    writer.commit().await.unwrap();
                } else {
                    writer.rollback().await.unwrap();
                }
                assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                snapshot.rollback().await.unwrap();
                let mut verify = session.begin_trx().unwrap();
                let mut expected = values(0);
                if commit {
                    expected[3] = Val::from(vec![b'm'; 48_000]);
                }
                assert_eq!(read(&mut verify, index, 0).await, expected);
                assert_eq!(read(&mut verify, index, 1).await, values(1));
                verify.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_current_update_departures_forward_to_insert_and_update() {
        smol::block_on(async {
            for buried in [false, true] {
                for existing_destination in [false, true] {
                    let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                    let mut writer_session = engine.new_session().unwrap();
                    let mut writer = writer_session.begin_trx().unwrap();
                    let mut snapshot_session = engine.new_session().unwrap();
                    let mut snapshot = snapshot_session.begin_trx().unwrap();
                    let mut reader = session.begin_trx().unwrap();
                    let expected = Rc::new(Cell::new(ids[2]));
                    let selected = Rc::clone(&expected);
                    install_selection_hook("hot", false, move || {
                        smol::block_on(async {
                            rekey_in_place(&mut writer, index, ids[0], 0, 900).await;
                            if buried {
                                apply(&mut writer, index, 900, assignment(2, 11i32))
                                    .await
                                    .unwrap();
                                rekey_in_place(&mut writer, index, ids[0], 900, 901).await;
                            }
                            if existing_destination {
                                rekey_in_place(&mut writer, index, ids[2], 2, 0).await;
                                apply(&mut writer, index, 0, assignment(2, 20i32))
                                    .await
                                    .unwrap();
                            } else {
                                let mut row = values(0);
                                row[1] = Val::from(902i32);
                                row[2] = Val::from(20i32);
                                let UniqueMutationOutcome::Inserted(id) =
                                    apply(&mut writer, index, 0, UniqueMutation::Insert(row))
                                        .await
                                        .unwrap()
                                else {
                                    panic!("replacement must insert a new physical row")
                                };
                                selected.set(id);
                            }
                            writer.commit().await.unwrap();
                        });
                        CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                    });
                    let mut calls = 0;
                    let result = reader
                        .table_unique_mutate_mvcc(
                            index,
                            &[Val::from(0i32)],
                            |row| -> CallbackResult<_> {
                                calls += 1;
                                assert_eq!(row.unwrap().val(2)?, &Val::from(20i32));
                                assert_eq!(
                                    CURRENT_LOOKUP_COUNTS.get(),
                                    (0, 0, 1),
                                    "buried={buried}, existing={existing_destination}"
                                );
                                Ok(assignment(2, 21i32))
                            },
                        )
                        .await
                        .unwrap();
                    assert_eq!(calls, 1);
                    assert_eq!(result, UniqueMutationOutcome::Updated(expected.get()));
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    reader.rollback().await.unwrap();
                    snapshot.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_current_forward_chain_revisits_row_id() {
        smol::block_on(async {
            for finish in ["return", "transfer", "remove"] {
                let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                let mut first_session = engine.new_session().unwrap();
                let mut first = first_session.begin_trx().unwrap();
                let mut second_session = engine.new_session().unwrap();
                let mut second = second_session.begin_trx().unwrap();
                let mut reader = session.begin_trx().unwrap();
                let expected_id = if finish == "transfer" { ids[2] } else { ids[0] };
                install_selection_hook("hot", false, move || {
                    smol::block_on(async {
                        rekey_in_place(&mut first, index, ids[0], 0, 900).await;
                        rekey_in_place(&mut first, index, ids[1], 1, 0).await;
                        first.commit().await.unwrap();
                    });
                    CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                    // Run only after the reader has copied 100 -> successor.
                    // The next committed transfer brings the key back to 100.
                    install_selection_hook("hot", false, move || {
                        let before = CURRENT_LOOKUP_COUNTS.get();
                        smol::block_on(async {
                            rekey_in_place(&mut second, index, ids[1], 0, 901).await;
                            rekey_in_place(&mut second, index, ids[0], 900, 0).await;
                            if finish != "return" {
                                rekey_in_place(&mut second, index, ids[0], 0, 902).await;
                                if finish == "transfer" {
                                    rekey_in_place(&mut second, index, ids[2], 2, 0).await;
                                }
                            }
                            second.commit().await.unwrap();
                        });
                        CURRENT_LOOKUP_COUNTS.set(before);
                    });
                });
                let mut calls = 0;
                let result = reader
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls += 1;
                            assert_eq!(row.is_none(), finish == "remove");
                            assert_eq!(
                                CURRENT_LOOKUP_COUNTS.get(),
                                match finish {
                                    "return" => (0, 0, 2),
                                    "transfer" => (0, 0, 3),
                                    "remove" => (1, 2, 4),
                                    _ => unreachable!(),
                                }
                            );
                            Ok(if row.is_some() {
                                UniqueMutation::Update(vec![])
                            } else {
                                UniqueMutation::Skip
                            })
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(calls, 1);
                assert_eq!(
                    result,
                    if finish == "remove" {
                        UniqueMutationOutcome::Noop
                    } else {
                        UniqueMutationOutcome::Updated(expected_id)
                    }
                );
                reader.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_current_live_wrong_key_cannot_use_head_timestamp() {
        smol::block_on(async {
            let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
            let mut anchor_session = engine.new_session().unwrap();
            let anchor = anchor_session.begin_trx().unwrap();
            let mut old = session.begin_trx().unwrap();
            apply(&mut old, index, 0, assignment(0, 9i32))
                .await
                .unwrap();
            let old_cts = old.commit().await.unwrap();
            let mut advance = session.begin_trx().unwrap();
            apply(&mut advance, index, 8, UniqueMutation::Insert(values(8)))
                .await
                .unwrap();
            advance.commit().await.unwrap();
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let mut reader = session.begin_trx().unwrap();
            assert!(reader.sts() > old_cts);
            install_selection_hook("hot", false, move || {
                smol::block_on(async {
                    let mut replacement = values(0);
                    replacement[1] = Val::from(1i32);
                    replacement[2] = Val::from(20i32);
                    apply(&mut writer, index, 0, UniqueMutation::Insert(replacement))
                        .await
                        .unwrap();
                    writer.commit().await.unwrap();
                });
                CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
            });
            reader
                .table_unique_mutate_mvcc(index, &[Val::from(0i32)], |row| -> CallbackResult<_> {
                    assert_eq!(row.unwrap().val(2)?, &Val::from(20i32));
                    assert_eq!(
                        CURRENT_LOOKUP_COUNTS.get(),
                        (1, 1, 1),
                        "a terminal Update still needs original lookup validation"
                    );
                    Ok(UniqueMutation::Skip)
                })
                .await
                .unwrap();
            reader.rollback().await.unwrap();
            anchor.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_current_hot_admission_classifies_deletion_and_successor() {
        smol::block_on(async {
            for has_successor in [false, true] {
                let (_root, engine, mut session, index, ids) = fixture("hot", 1).await;
                let mut anchor_session = engine.new_session().unwrap();
                let anchor = anchor_session.begin_trx().unwrap();
                let mut writer = session.begin_trx().unwrap();
                let successor = if has_successor {
                    Some(replace_zero(&mut writer, index, ids[0], true).await)
                } else {
                    apply(&mut writer, index, 0, UniqueMutation::Delete)
                        .await
                        .unwrap();
                    None
                };
                let delete_cts = writer.commit().await.unwrap();
                let mut reader = session.begin_trx().unwrap();
                assert!(anchor.sts() < delete_cts && delete_cts < reader.sts());
                reader
                    .lock_table(index.0, TableLockMode::Exclusive)
                    .await
                    .unwrap();
                let table = table_for_internal_assertion(&engine, index.0);
                let layout = table.layout_snapshot();
                let guards = session.pool_guards();
                let RowLocation::RowPage(page_id) =
                    table.mem.find_row(&guards, ids[0]).await.unwrap()
                else {
                    panic!("deleted source must remain hot");
                };
                let page = table
                    .mem
                    .must_get_row_page_shared(&guards, page_id)
                    .await
                    .unwrap();
                let row_idx = page.page().row_idx(ids[0]);
                let index_ref = IndexRef::new(index.1, IndexSlot::new(0));

                for original_candidate in [true, false] {
                    let sts = reader.sts();
                    let own_status = transaction_status_for_test(&reader);
                    let foreign_status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID));
                    for (label, status, old_delete, foreign) in [
                        ("older", UndoStatus::Committed(delete_cts), true, false),
                        ("equal", UndoStatus::Committed(sts), false, false),
                        ("newer", UndoStatus::Committed(sts + 1), false, false),
                        ("own active", UndoStatus::Ref(own_status), false, false),
                        (
                            "foreign active",
                            UndoStatus::Ref(foreign_status),
                            false,
                            true,
                        ),
                    ] {
                        let shortcut = original_candidate && old_delete;
                        let expected = if foreign {
                            ("conflict", None)
                        } else if shortcut {
                            ("deleted before snapshot", None)
                        } else if has_successor {
                            ("successor", successor)
                        } else {
                            ("unresolved", None)
                        };
                        let observed = with_statement_runtime(&mut reader, |rt, effects| {
                            // Keep the real Delete and its links. The older anchor
                            // retains this undo while synthetic statuses exercise
                            // the exact timestamp and ownership boundaries.
                            let saved_status = {
                                let mut head = page.unwrap_vmap().write_latch(row_idx);
                                std::mem::replace(
                                    &mut head.as_mut().unwrap().next.main.status,
                                    status,
                                )
                            };
                            let before = CURRENT_LOOKUP_COUNTS.get();
                            let hot =
                                HotRowMutator::new(index.0, layout.metadata(), rt, &page, ids[0]);
                            let observed = match hot.try_lock_current(
                                effects,
                                index_ref,
                                &[Val::from(0i32)],
                                original_candidate,
                            ) {
                                HotRowLock::DeletedBeforeSnapshot => {
                                    ("deleted before snapshot", None)
                                }
                                HotRowLock::Successor(row_id) => ("successor", Some(row_id)),
                                HotRowLock::Unresolved => ("unresolved", None),
                                HotRowLock::WriteConflict => ("conflict", None),
                                HotRowLock::Owned(_) => ("owned", None),
                                HotRowLock::Preparing(_) => ("preparing", None),
                                HotRowLock::Transition => ("transition", None),
                            };
                            page.unwrap_vmap()
                                .write_latch(row_idx)
                                .as_mut()
                                .unwrap()
                                .next
                                .main
                                .status = saved_status;
                            let after = CURRENT_LOOKUP_COUNTS.get();
                            assert_eq!(after.0 - before.0, 0);
                            assert_eq!(after.1 - before.1, 0);
                            assert_eq!(
                                after.2 - before.2,
                                usize::from(!foreign && !shortcut),
                                "{label}, original_candidate={original_candidate}"
                            );
                            observed
                        })
                        .await
                        .unwrap();
                        assert_eq!(
                            observed, expected,
                            "{label}, original_candidate={original_candidate}, has_successor={has_successor}"
                        );
                    }
                }
                drop(page);
                reader.rollback().await.unwrap();
                anchor.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_current_timestamp_shortcut_and_success_cost() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, engine, mut session, index, _) = fixture(state, 1).await;
                let mut anchor_session = engine.new_session().unwrap();
                let anchor = anchor_session.begin_trx().unwrap();
                let mut writer = session.begin_trx().unwrap();
                let before = CURRENT_LOOKUP_COUNTS.get();
                apply(&mut writer, index, 0, UniqueMutation::Skip)
                    .await
                    .unwrap();
                let after = CURRENT_LOOKUP_COUNTS.get();
                assert_eq!(
                    (after.0 - before.0, after.1 - before.1, after.2 - before.2),
                    (1, 0, 0)
                );
                apply(&mut writer, index, 0, UniqueMutation::Delete)
                    .await
                    .unwrap();
                let delete_cts = writer.commit().await.unwrap();
                let mut advance = session.begin_trx().unwrap();
                apply(&mut advance, index, 9, UniqueMutation::Insert(values(9)))
                    .await
                    .unwrap();
                advance.commit().await.unwrap();
                let mut reader = session.begin_trx().unwrap();
                assert!(reader.sts() > delete_cts);
                let before = CURRENT_LOOKUP_COUNTS.get();
                reader
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            assert!(row.is_none());
                            let after = CURRENT_LOOKUP_COUNTS.get();
                            assert_eq!(
                                (after.0 - before.0, after.1 - before.1, after.2 - before.2),
                                (1, 0, 0),
                                "older deletion skips hint storage and post-row validation"
                            );
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await
                    .unwrap();
                reader.rollback().await.unwrap();
                anchor.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_entry_state_matrix_and_trusted_contracts() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, _engine, mut session, index, ids) = fixture(state, 1).await;
                for trusted in [false, true] {
                    for occupied in [false, true] {
                        for action_no in 0..5 {
                            let mut trx = session.begin_trx().unwrap();
                            trx.disable_dml_validation(trusted);
                            let id = if occupied { 0 } else { 9 };
                            let action = match action_no {
                                0 => UniqueMutation::Skip,
                                1 => UniqueMutation::Insert(values(id)),
                                2 => assignment(2, 20i32),
                                3 => UniqueMutation::Delete,
                                _ => UniqueMutation::Update(vec![]),
                            };
                            let mut calls = 0;
                            let result = trx
                                .table_unique_mutate_mvcc(
                                    index,
                                    &[Val::from(id)],
                                    |row| -> CallbackResult<_> {
                                        calls += 1;
                                        assert_eq!(row.is_some(), occupied);
                                        Ok(action)
                                    },
                                )
                                .await;
                            assert_eq!(calls, 1);
                            match (occupied, action_no) {
                                (_, 0) => assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop),
                                (false, 1) => assert!(matches!(
                                    result.unwrap(),
                                    UniqueMutationOutcome::Inserted(_)
                                )),
                                (true, 2) => assert!(matches!(
                                    result.unwrap(),
                                    UniqueMutationOutcome::Updated(_)
                                )),
                                (true, 3) => {
                                    assert_eq!(result.unwrap(), UniqueMutationOutcome::Deleted)
                                }
                                (true, 4) => assert_eq!(
                                    result.unwrap(),
                                    UniqueMutationOutcome::Updated(ids[0])
                                ),
                                _ => invalid(result),
                            }
                            trx.rollback().await.unwrap();
                        }
                    }
                    let mut trx = session.begin_trx().unwrap();
                    trx.disable_dml_validation(trusted);
                    invalid(apply(&mut trx, index, 9, UniqueMutation::Insert(values(8))).await);
                    trx.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_callback_payload_validation_and_index_arguments() {
        smol::block_on(async {
            let (_root, _engine, mut session, index, _) = fixture("hot", 1).await;
            let mut trx = session.begin_trx().unwrap();
            let resolved = trx.resolve_table_index(index).await.unwrap();
            assert_eq!(
                trx.table_unique_mutate_mvcc(
                    resolved,
                    &[Val::from(0i32)],
                    |_| -> CallbackResult<_> { Ok(UniqueMutation::Skip) }
                )
                .await
                .unwrap(),
                UniqueMutationOutcome::Noop
            );
            let mut wrong_kind = values(9);
            wrong_kind[2] = Val::from("wrong");
            let mut wrong_null = values(9);
            wrong_null[1] = Val::Null;
            for row in [vec![], wrong_kind, wrong_null] {
                invalid(apply(&mut trx, index, 9, UniqueMutation::Insert(row)).await);
            }
            for update in [
                vec![UpdateCol {
                    idx: 4,
                    val: Val::from(1i32),
                }],
                vec![UpdateCol {
                    idx: 2,
                    val: Val::from("bad"),
                }],
                vec![
                    UpdateCol {
                        idx: 2,
                        val: Val::from(1i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from(1i32),
                    },
                ],
                vec![
                    UpdateCol {
                        idx: 2,
                        val: Val::from(1i32),
                    },
                    UpdateCol {
                        idx: 2,
                        val: Val::from(1i32),
                    },
                ],
            ] {
                invalid(apply(&mut trx, index, 0, UniqueMutation::Update(update)).await);
            }
            for bad_index in [
                TableIndex(index.0, IndexID::new(2)),
                TableIndex(index.0, IndexID::new(99)),
            ] {
                let calls = Cell::new(0);
                assert!(
                    trx.table_unique_mutate_mvcc(
                        bad_index,
                        &[Val::from(0i32)],
                        |_| -> CallbackResult<_> {
                            calls.set(calls.get() + 1);
                            Ok(UniqueMutation::Skip)
                        }
                    )
                    .await
                    .is_err()
                );
                assert_eq!(calls.get(), 0);
            }
            assert_eq!(read(&mut trx, index, 0).await, values(0));
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_current_read_and_branch_local_payload() {
        smol::block_on(async {
            let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
            let mut writer = session.begin_trx().unwrap();
            let mut concurrent = engine.new_session().unwrap();
            let mut update = concurrent.begin_trx().unwrap();
            apply(&mut update, index, 0, assignment(2, 25i32))
                .await
                .unwrap();
            update.commit().await.unwrap();
            let mut inserts = 0;
            let mut updates = 0;
            for id in [0i32, 9] {
                let mut missing = false;
                writer
                    .table_unique_mutate_mvcc(index, &[Val::from(id)], |row| -> CallbackResult<_> {
                        match row {
                            Some(row) => {
                                updates += 1;
                                let old = row.val(2)?.as_i32().unwrap();
                                assert_eq!(old, 25);
                                Ok(assignment(2, old.checked_add(1).unwrap()))
                            }
                            None => {
                                missing = true;
                                inserts += 1;
                                Ok(UniqueMutation::Insert(values(id)))
                            }
                        }
                    })
                    .await
                    .unwrap();
                assert_eq!(missing, id == 9);
            }
            assert_eq!((inserts, updates), (1, 1));
            assert_eq!(read(&mut writer, index, 0).await[2], Val::from(26i32));
            // FnOnce consumes a non-Clone payload and may capture non-Send state.
            struct Payload {
                row: Vec<Val>,
                _local: Rc<()>,
            }
            let payload = Payload {
                row: values(10),
                _local: Rc::new(()),
            };
            writer
                .table_unique_mutate_mvcc(
                    index,
                    &[Val::from(10i32)],
                    move |_| -> CallbackResult<_> {
                        let Payload { row, _local } = payload;
                        drop(_local);
                        Ok(UniqueMutation::Insert(row))
                    },
                )
                .await
                .unwrap();
            writer
                .table_unique_mutate_mvcc(index, &[Val::from(0i32)], |row| -> CallbackResult<_> {
                    Ok(if row.unwrap().val(2)?.as_i32().unwrap() > 20 {
                        UniqueMutation::Delete
                    } else {
                        UniqueMutation::Skip
                    })
                })
                .await
                .unwrap();
            writer.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_cancellation_releases_only_new_ownership() {
        smol::block_on(async {
            for state in ["hot", "frozen", "cold"] {
                let (_root, engine, mut session, index, ids) = fixture(state, 2).await;
                let mut trx = session.begin_trx().unwrap();
                for action in [UniqueMutation::Skip, UniqueMutation::Update(vec![])] {
                    apply(&mut trx, index, 0, action).await.unwrap();
                    let mut other = engine.new_session().unwrap();
                    let mut competing = other.begin_trx().unwrap();
                    assert!(matches!(
                        apply(&mut competing, index, 0, assignment(2, 20i32))
                            .await
                            .unwrap(),
                        UniqueMutationOutcome::Updated(_)
                    ));
                    competing.rollback().await.unwrap();
                }
                let first = apply(&mut trx, index, 0, assignment(2, 30i32))
                    .await
                    .unwrap();
                for action in [UniqueMutation::Skip, UniqueMutation::Update(vec![])] {
                    apply(&mut trx, index, 0, action).await.unwrap();
                    assert_eq!(read(&mut trx, index, 0).await[2], Val::from(30i32));
                }
                invalid(apply(&mut trx, index, 0, UniqueMutation::Insert(values(0))).await);
                let mut other = engine.new_session().unwrap();
                let mut competing = other.begin_trx().unwrap();
                let error = apply(&mut competing, index, 0, UniqueMutation::Skip)
                    .await
                    .unwrap_err();
                let CallbackError::Engine(error) = error;
                assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                competing.rollback().await.unwrap();
                if state == "cold" {
                    let table = table_for_internal_assertion(&engine, index.0);
                    assert!(table.deletion_buffer().get(ids[0]).is_some());
                }
                assert!(matches!(first, UniqueMutationOutcome::Updated(_)));
                trx.rollback().await.unwrap();
                let mut reader = session.begin_trx().unwrap();
                assert_eq!(read(&mut reader, index, 0).await, values(0));
                reader.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_moves_keys_and_rolls_back_index_conflicts() {
        smol::block_on(async {
            for commit in [false, true] {
                for state in ["hot", "frozen", "cold", "space"] {
                    let (_root, engine, mut session, index, ids) =
                        fixture(if state == "space" { "hot" } else { state }, 300).await;
                    let mut snapshot_session = engine.new_session().unwrap();
                    let mut snapshot = snapshot_session.begin_trx().unwrap();
                    let mut trx = session.begin_trx().unwrap();
                    let payload = if state == "space" {
                        Val::from(vec![b'x'; 48000])
                    } else {
                        Val::from("changed")
                    };
                    let result = apply(
                        &mut trx,
                        index,
                        0,
                        UniqueMutation::Update(vec![
                            UpdateCol {
                                idx: 0,
                                val: Val::from(500i32),
                            },
                            UpdateCol {
                                idx: 1,
                                val: Val::from(500i32),
                            },
                            UpdateCol {
                                idx: 3,
                                val: payload.clone(),
                            },
                        ]),
                    )
                    .await
                    .unwrap();
                    let UniqueMutationOutcome::Updated(result_id) = result else {
                        panic!("move must be a logical update")
                    };
                    if state == "hot" {
                        assert_eq!(result_id, ids[0]);
                    } else {
                        assert_ne!(result_id, ids[0]);
                    }
                    assert!(
                        trx.table_lookup_unique_mvcc(index, &[Val::from(0i32)], &[0])
                            .await
                            .unwrap()
                            .not_found()
                    );
                    assert_eq!(read(&mut trx, index, 500).await[3], payload);
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    let by_other = trx
                        .table_lookup_unique_mvcc(
                            TableIndex(index.0, IndexID::new(1)),
                            &[Val::from(500i32)],
                            &[0],
                        )
                        .await
                        .unwrap()
                        .unwrap_found();
                    assert_eq!(by_other, vec![Val::from(500i32)]);
                    assert_eq!(
                        trx.table_index_lookup_mvcc(
                            TableIndex(index.0, IndexID::new(2)),
                            &[payload],
                            &[0]
                        )
                        .await
                        .unwrap()
                        .unwrap_rows(),
                        vec![vec![Val::from(500i32)]]
                    );
                    assert!(
                        apply(&mut trx, index, 1, assignment(1, 2i32))
                            .await
                            .is_err()
                    );
                    assert_eq!(read(&mut trx, index, 1).await, values(1));
                    assert_eq!(read(&mut trx, index, 500).await[0], Val::from(500i32));
                    if commit {
                        trx.commit().await.unwrap();
                    } else {
                        trx.rollback().await.unwrap();
                    }
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    snapshot.rollback().await.unwrap();
                    let mut reader = session.begin_trx().unwrap();
                    if commit {
                        assert_eq!(read(&mut reader, index, 500).await[0], Val::from(500i32));
                    } else {
                        assert_eq!(read(&mut reader, index, 0).await, values(0));
                    }
                    reader.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_callback_errors_preserve_earlier_statements() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, _engine, mut session, index, _) = fixture(state, 2).await;
                let mut trx = session.begin_trx().unwrap();
                apply(&mut trx, index, 0, assignment(2, 30i32))
                    .await
                    .unwrap();
                struct Failure<'a> {
                    message: &'a str,
                    identity: Rc<()>,
                }
                let message = String::from("borrowed failure");
                let identity = Rc::new(());
                let error = trx
                    .table_unique_mutate_mvcc(index, &[Val::from(1i32)], |row| {
                        assert_eq!(row.unwrap().val(2)?.as_i32(), Some(10));
                        Err(CallbackError::User(Failure {
                            message: &message,
                            identity: Rc::clone(&identity),
                        }))
                    })
                    .await;
                match error {
                    Err(CallbackError::User(error)) => {
                        assert_eq!(error.message, message);
                        assert!(Rc::ptr_eq(&error.identity, &identity));
                    }
                    _ => panic!("callback application error must retain its original payload"),
                }
                let error = trx
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(1i32)],
                        |row| -> CallbackResult<_> {
                            row.unwrap().val(99)?;
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await;
                invalid(error);
                assert_eq!(read(&mut trx, index, 0).await[2], Val::from(30i32));
                assert_eq!(read(&mut trx, index, 1).await, values(1));
                trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_cold_marker_authority_and_consumed_images() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("cold", 1).await;
            let table = table_for_internal_assertion(&engine, index.0);
            let mut trx = session.begin_trx().unwrap();
            let status = transaction_status_for_test(&trx);
            table
                .deletion_buffer()
                .put_ref(ids[0], Arc::clone(&status), trx.sts())
                .unwrap();
            for action in [
                UniqueMutation::Skip,
                UniqueMutation::Update(vec![]),
                UniqueMutation::Delete,
                UniqueMutation::Insert(values(1)),
            ] {
                let result = trx
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            assert!(row.is_none(), "a consumed cold image is missing");
                            Ok(action)
                        },
                    )
                    .await;
                assert!(matches!(result, Ok(UniqueMutationOutcome::Noop) | Err(_)));
                assert!(
                    matches!(table.deletion_buffer().get(ids[0]), Some(crate::table::DeleteMarker::Ref(owner)) if Arc::ptr_eq(&owner, &status))
                );
            }
            table.deletion_buffer().remove(ids[0]);
            for newer in [false, true] {
                let cts = if newer { trx.sts() + 1 } else { trx.sts() };
                table.deletion_buffer().put_committed(ids[0], cts).unwrap();
                let calls = Cell::new(0);
                let result = trx
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls.set(calls.get() + 1);
                            assert!(row.is_none());
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await;
                if newer {
                    let CallbackError::Engine(error) = result.unwrap_err();
                    assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                    assert_eq!(calls.get(), 0);
                } else {
                    assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop);
                    assert_eq!(calls.get(), 1);
                }
                table.deletion_buffer().remove(ids[0]);
            }
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_cold_preparing_retry_invokes_once() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, ids) = fixture("cold", 1).await;
                let table = table_for_internal_assertion(&engine, index.0);
                let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 100));
                table
                    .deletion_buffer()
                    .put_ref(ids[0], Arc::clone(&owner), MAX_SNAPSHOT_TS)
                    .unwrap();
                prepare_shared_trx_status(&owner);
                let mut trx = session.begin_trx().unwrap();
                let cts = trx.sts();
                let calls = Cell::new(0);
                let key = [Val::from(0i32)];
                let mutation =
                    trx.table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                        calls.set(calls.get() + 1);
                        assert_eq!(row.is_none(), commit);
                        Ok(UniqueMutation::Skip)
                    });
                let release = async {
                    while !prepare_event_is_installed(&owner) {
                        yield_now().await;
                    }
                    assert_eq!(calls.get(), 0);
                    if commit {
                        commit_preparing_shared_trx_status(&owner, cts);
                    } else {
                        table.deletion_buffer().remove(ids[0]);
                        rollback_preparing_shared_trx_status(&owner);
                    }
                };
                let (result, ()) = futures::join!(mutation, release);
                assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop);
                assert_eq!(calls.get(), 1);
                table.deletion_buffer().remove(ids[0]);
                trx.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_insert_race_is_not_retried() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
                let mut winner_session = engine.new_session().unwrap();
                let winner = winner_session.begin_trx().unwrap();
                let mut loser = session.begin_trx().unwrap();
                let calls = Cell::new(0);
                let mut winner = Some(winner);
                let result = loser
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(9i32)],
                        |row| -> CallbackResult<_> {
                            assert!(row.is_none());
                            calls.set(calls.get() + 1);
                            // The synchronous callback is the precise missing-observation
                            // barrier. Run the independent writer before returning Insert.
                            smol::block_on(
                                winner
                                    .as_mut()
                                    .unwrap()
                                    .table_insert_mvcc(index.0, values(9)),
                            )
                            .unwrap();
                            if commit {
                                smol::block_on(winner.take().unwrap().commit()).unwrap();
                            }
                            Ok(UniqueMutation::Insert(values(9)))
                        },
                    )
                    .await;
                let CallbackError::Engine(error) = result.unwrap_err();
                assert!(
                    matches!(
                        error.operation_error(),
                        Some(OperationError::WriteConflict | OperationError::DuplicateKey)
                    ),
                    "{error:?}"
                );
                assert_eq!(calls.get(), 1);
                loser.rollback().await.unwrap();
                if let Some(winner) = winner {
                    winner.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_current_yield_allows_poison_and_cancellation() {
        smol::block_on(async {
            for path in ["forward", "retry"] {
                for finish in ["resume", "poison", "cancel"] {
                    let (_root, engine, mut session, index, ids) = fixture("hot", 300).await;
                    // Drain setup purge so the page-latch assertion below
                    // observes only the suspended selector's guards.
                    session
                        .wait_for_purge_completion_after(session.last_cts())
                        .await
                        .unwrap();
                    let table = table_for_internal_assertion(&engine, index.0);
                    let guards = session.pool_guards();
                    let RowLocation::RowPage(source_page) =
                        table.find_row(&guards, ids[0]).await.unwrap()
                    else {
                        panic!("fixture row must be hot");
                    };
                    let mut writer_session = engine.new_session().unwrap();
                    let mut writer = writer_session.begin_trx().unwrap();
                    let mut reader = session.begin_trx().unwrap();
                    // Cancellation must also roll back an earlier statement.
                    apply(&mut reader, index, 1, assignment(2, 30i32))
                        .await
                        .unwrap();
                    let changed = Rc::new(Cell::new(false));
                    let hook_changed = Rc::clone(&changed);
                    install_selection_hook("hot", false, move || {
                        smol::block_on(async {
                            if path == "forward" {
                                replace_zero(&mut writer, index, ids[0], false).await;
                            } else {
                                apply(&mut writer, index, 0, UniqueMutation::Delete)
                                    .await
                                    .unwrap();
                            }
                            writer.commit().await.unwrap();
                        });
                        CURRENT_LOOKUP_COUNTS.set((0, 0, 0));
                        hook_changed.set(true);
                    });
                    let calls = Cell::new(0);
                    let key = [Val::from(0i32)];
                    let mut operation = Box::pin(reader.table_unique_mutate_mvcc(
                        index,
                        &key,
                        |row| -> CallbackResult<_> {
                            calls.set(calls.get() + 1);
                            assert_eq!(row.is_some(), path == "forward");
                            if let Some(row) = row {
                                assert_eq!(row.val(2)?, &Val::from(20i32));
                            }
                            Ok(UniqueMutation::Skip)
                        },
                    ));
                    loop {
                        let poll = futures::poll!(operation.as_mut());
                        assert!(
                            poll.is_pending(),
                            "selection must yield before continuing: path={path}, finish={finish}, poll={poll:?}"
                        );
                        if changed.get() {
                            break;
                        }
                        yield_now().await;
                    }
                    assert_eq!(calls.get(), 0);
                    assert_eq!(
                        CURRENT_LOOKUP_COUNTS.get(),
                        if path == "forward" {
                            (0, 0, 1)
                        } else {
                            (0, 1, 1)
                        },
                        "yield must follow the first rejection before another lookup or row inspection"
                    );
                    let page = table
                        .mem
                        .must_get_row_page_shared(&guards, source_page)
                        .await
                        .unwrap();
                    assert!(
                        matches!(page.downgrade().try_exclusive(), Validation::Valid(_)),
                        "selection must release the rejected row page before yielding"
                    );
                    match finish {
                        "resume" => {
                            assert_eq!(operation.await.unwrap(), UniqueMutationOutcome::Noop);
                            assert_eq!(calls.get(), 1);
                            reader.rollback().await.unwrap();
                        }
                        "poison" => {
                            engine.inner().poisoner.poison(
                                Report::new(FatalError::StorageIo)
                                    .attach("poison during unique selection yield"),
                            );
                            let CallbackError::Engine(error) = operation.await.unwrap_err();
                            assert_eq!(error.kind(), ErrorKind::Fatal);
                            assert_eq!(
                                error.report().downcast_ref::<FatalError>().copied(),
                                Some(FatalError::StorageIo)
                            );
                            assert!(
                                format!("{error:?}")
                                    .contains("poison during unique selection yield")
                            );
                            assert_eq!(calls.get(), 0);
                            reader.rollback().await.unwrap();
                        }
                        "cancel" => {
                            drop(operation);
                            drop(reader);
                            assert_eq!(calls.get(), 0);
                            wait_for_session_idle(&engine.inner().session_registry, session.id())
                                .await;
                        }
                        _ => unreachable!(),
                    }
                    if finish != "poison" {
                        let mut verify = session.begin_trx().unwrap();
                        assert_eq!(read(&mut verify, index, 1).await, values(1));
                        verify.rollback().await.unwrap();
                    }
                }
            }
        });
    }

    #[test]
    fn test_unique_callback_drop_waiting_future_cleans_transaction() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("cold", 2).await;
            let table = table_for_internal_assertion(&engine, index.0);
            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 101));
            table
                .deletion_buffer()
                .put_ref(ids[1], Arc::clone(&owner), MAX_SNAPSHOT_TS)
                .unwrap();
            prepare_shared_trx_status(&owner);
            let mut trx = session.begin_trx().unwrap();
            apply(&mut trx, index, 0, assignment(2, 30i32))
                .await
                .unwrap();
            let key = [Val::from(1i32)];
            let mut operation = Box::pin(trx.table_unique_mutate_mvcc(
                index,
                &key,
                |_| -> CallbackResult<_> { panic!("callback must not run while owner prepares") },
            ));
            while !prepare_event_is_installed(&owner) {
                assert!(futures::poll!(operation.as_mut()).is_pending());
                yield_now().await;
            }
            drop(operation);
            drop(trx);
            table.deletion_buffer().remove(ids[1]);
            rollback_preparing_shared_trx_status(&owner);
            wait_for_session_idle(&engine.inner().session_registry, session.id()).await;
            let mut reader = session.begin_trx().unwrap();
            assert_eq!(read(&mut reader, index, 0).await, values(0));
            reader.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_empty_update_preserves_row_before_nonempty_move() {
        smol::block_on(async {
            for state in ["frozen", "cold"] {
                let (_root, engine, mut session, index, ids) = fixture(state, 1).await;
                let mut trx = session.begin_trx().unwrap();
                assert_eq!(
                    apply(&mut trx, index, 0, UniqueMutation::Update(vec![]))
                        .await
                        .unwrap(),
                    UniqueMutationOutcome::Updated(ids[0])
                );
                let UniqueMutationOutcome::Updated(new_id) =
                    apply(&mut trx, index, 0, assignment(2, 42i32))
                        .await
                        .unwrap()
                else {
                    panic!("nonempty update must find row")
                };
                assert_ne!(ids[0], new_id);
                let table = table_for_internal_assertion(&engine, index.0);
                assert!(matches!(
                    table
                        .find_row(&session.pool_guards(), new_id)
                        .await
                        .unwrap(),
                    RowLocation::RowPage(_)
                ));
                trx.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_zero_read_actions_avoid_dense_cache() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, _engine, mut session, index, _) = fixture(state, 1).await;
                for action in [
                    UniqueMutation::Skip,
                    UniqueMutation::Update(vec![]),
                    assignment(2, 20i32),
                    UniqueMutation::Delete,
                ] {
                    let mut trx = session.begin_trx().unwrap();
                    let before = dense_initializations();
                    apply(&mut trx, index, 0, action).await.unwrap();
                    assert_eq!(dense_initializations(), before);
                    trx.rollback().await.unwrap();
                }
                let mut trx = session.begin_trx().unwrap();
                let before = dense_initializations();
                trx.table_unique_mutate_mvcc(
                    index,
                    &[Val::from(0i32)],
                    |row| -> CallbackResult<_> {
                        let row = row.unwrap();
                        row.val(0)?;
                        row.val(0)?;
                        row.val(1)?;
                        row.val(2)?;
                        Ok(assignment(2, 30i32))
                    },
                )
                .await
                .unwrap();
                assert_eq!(dense_initializations(), before + 1);
                trx.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_active_hot_delete_and_preparing_settlement() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
                let mut owner = session.begin_trx().unwrap();
                apply(&mut owner, index, 0, UniqueMutation::Delete)
                    .await
                    .unwrap();
                let mut competitor_session = engine.new_session().unwrap();
                let mut competitor = competitor_session.begin_trx().unwrap();
                let calls = Cell::new(0);
                let result = competitor
                    .table_unique_mutate_mvcc(index, &[Val::from(0i32)], |_| -> CallbackResult<_> {
                        calls.set(calls.get() + 1);
                        Ok(UniqueMutation::Skip)
                    })
                    .await;
                let CallbackError::Engine(error) = result.unwrap_err();
                assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                assert_eq!(calls.get(), 0);
                let status = transaction_status_for_test(&owner);
                let prepared = prepare_transaction(owner).unwrap();
                let key = [Val::from(0i32)];
                let mutate =
                    competitor.table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                        calls.set(calls.get() + 1);
                        assert_eq!(row.is_none(), commit);
                        Ok(UniqueMutation::Skip)
                    });
                let settle = async {
                    while !prepare_event_is_installed(&status) {
                        yield_now().await;
                    }
                    assert_eq!(calls.get(), 0);
                    if commit {
                        engine
                            .inner()
                            .trx_sys
                            .commit_prepared(prepared)
                            .await
                            .unwrap();
                    } else {
                        rollback_production_prepared_for_test(prepared).await;
                    }
                };
                let (result, ()) = futures::join!(mutate, settle);
                assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop);
                assert_eq!(calls.get(), 1);
                competitor.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_cold_delete_only_decodes_index_columns() {
        smol::block_on(async {
            let (_root, _engine, mut session, index, _) = fixture("cold", 1).await;
            let mut trx = session.begin_trx().unwrap();
            let before = test_decode_counts();
            apply(&mut trx, index, 0, UniqueMutation::Delete)
                .await
                .unwrap();
            let after = test_decode_counts();
            assert!(after[0] > before[0]);
            assert!(after[1] > before[1]);
            assert_eq!(
                after[2], before[2],
                "unindexed counter must remain undecoded"
            );
            assert!(after[3] > before[3]);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_mem_hit_short_circuits_disk() {
        smol::block_on(async {
            let (_root, engine, mut session, index, _) = fixture("cold", 1).await;
            let mut trx = session.begin_trx().unwrap();
            apply(&mut trx, index, 0, assignment(2, 20i32))
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, index.0);
            assert!(
                bound_unique_index(&table, &session.pool_guards(), IndexSlot::new(0))
                    .lookup(&[Val::from(0i32)], MAX_SNAPSHOT_TS)
                    .await
                    .unwrap()
                    .is_some()
            );
            let before = test_unique_disk_lookups();
            apply(&mut trx, index, 0, UniqueMutation::Skip)
                .await
                .unwrap();
            assert_eq!(test_unique_disk_lookups(), before);
            trx.rollback().await.unwrap();
        });
    }
}
