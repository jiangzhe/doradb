use crate::buffer::PoolGuards;
use crate::id::{RowID, TableID, TrxID};

use crate::catalog::{
    CatalogIndexNo, CatalogSelectKey, CatalogTable, IndexID, IndexRef, ResolvedUserIndex,
    ResolvedUserIndexKey, TableCache,
};
use crate::error::{
    DiscloseResultExt, FatalError, FatalResult, MultiDomainResultExt, OperationError,
    OperationOrFatalError, OperationOrFatalResult, OperationOrRuntimeError,
    OperationOrRuntimeResult, OperationResult, QuadError, QuadResult, Result, RuntimeError,
    RuntimeOrFatalError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::lock::{LockMode, LockResource};
use crate::log::redo::{RedoLogs, RowRedo};
use crate::obs;
use crate::row::ops::{
    DeleteMvcc, RowMutation, ScanMvcc, SelectMvcc, TableMutationOutcome, UpdateCol, UpdateMvcc,
    UpsertMvcc,
};
use crate::session::TrxAttachment;
use crate::table::{DmlValidator, LazyRow};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
    RowUndoRollbackContext,
};
use crate::trx::{
    FatalRollbackRetention, NON_FOREGROUND_STMT_NO, SessionOperationCheckout, StmtNo, TrxEffects,
    TrxInner, TrxRuntime,
};
use crate::value::Val;
use error_stack::ResultExt;
use std::mem;
use std::ops::RangeBounds;

use super::admission::{
    AdmittedUserIndex, AdmittedUserTable, UserIndexSelector, admit_user_index, admit_user_table,
};

/// Cached unique-driver update whose provisional row lock remains installed.
struct DeferredIndexUpdate {
    index: IndexRef,
    row_id: RowID,
    update: Vec<UpdateCol>,
    undo: OwnedRowUndo,
}

/// Mutable effects accumulated by one statement before success or rollback.
///
/// These effects merge into transaction-level `TrxEffects` when the statement
/// succeeds. If the statement fails, index effects roll back before row effects
/// and redo is discarded.
pub(crate) struct StmtEffects {
    stmt_no: StmtNo,
    row_undo: RowUndoLogs,
    deferred_index_updates: Vec<DeferredIndexUpdate>,
    index_undo: IndexUndoLogs,
    redo: RedoLogs,
}

impl StmtEffects {
    /// Create an empty effect accumulator for one checked-out statement.
    #[inline]
    pub(crate) fn new(stmt_no: StmtNo) -> Self {
        assert!(
            stmt_no != NON_FOREGROUND_STMT_NO,
            "foreground statement effects require a non-sentinel statement number"
        );
        StmtEffects {
            stmt_no,
            row_undo: RowUndoLogs::empty(),
            deferred_index_updates: Vec::new(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    /// Returns this transaction-local statement identity.
    #[inline]
    pub(crate) fn stmt_no(&self) -> StmtNo {
        self.stmt_no
    }

    /// Push one row undo entry into this statement.
    #[inline]
    pub(crate) fn push_row_undo(&mut self, undo: OwnedRowUndo) {
        debug_assert!(
            self.stmt_no == NON_FOREGROUND_STMT_NO || undo.stmt_no == self.stmt_no,
            "foreground row undo statement tag mismatch: effects_stmt_no={}, undo_stmt_no={}",
            self.stmt_no,
            undo.stmt_no
        );
        self.row_undo.push(undo);
    }

    /// Moves the newest provisional lock into statement-owned deferred storage.
    #[inline]
    pub(crate) fn defer_index_update(
        &mut self,
        table_id: TableID,
        index: IndexRef,
        row_id: RowID,
        update: Vec<UpdateCol>,
    ) {
        let undo = self
            .row_undo
            .last()
            .expect("deferred index update requires a provisional row lock");
        assert!(
            undo.table_id == table_id
                && undo.row_id == row_id
                && undo.stmt_no == self.stmt_no
                && matches!(undo.kind, RowUndoKind::Lock),
            "deferred index update must capture the newest matching provisional lock: effects_stmt_no={}, undo_stmt_no={}, table_id={table_id}, undo_table_id={}, row_id={row_id}, undo_row_id={}, undo_kind={:?}",
            self.stmt_no,
            undo.stmt_no,
            undo.table_id,
            undo.row_id,
            undo.kind
        );
        let undo = self
            .row_undo
            .pop()
            .expect("validated deferred index update lock must remain newest");
        self.deferred_index_updates.push(DeferredIndexUpdate {
            index,
            row_id,
            update,
            undo,
        });
    }

    /// Prepares deferred updates for callback-order activation through `pop`.
    #[inline]
    pub(crate) fn begin_deferred_index_update_application(&mut self) {
        self.deferred_index_updates.reverse();
    }

    /// Restores the next deferred lock before returning its cached update.
    #[inline]
    pub(crate) fn activate_next_deferred_index_update(
        &mut self,
    ) -> Option<(IndexRef, RowID, Vec<UpdateCol>)> {
        let DeferredIndexUpdate {
            index,
            row_id,
            update,
            undo,
        } = self.deferred_index_updates.pop()?;
        // Restore the stable box owner before any assertion or other panic can
        // unwind this synchronous activation path.
        self.row_undo.push(undo);
        let undo = self
            .row_undo
            .last()
            .expect("activated deferred index update must restore row undo ownership");
        assert!(
            undo.row_id == row_id
                && undo.stmt_no == self.stmt_no
                && matches!(undo.kind, RowUndoKind::Lock),
            "deferred index update activation must restore its exact provisional lock: effects_stmt_no={}, undo_stmt_no={}, row_id={}, undo_row_id={}, undo_kind={:?}",
            self.stmt_no,
            undo.stmt_no,
            row_id,
            undo.row_id,
            undo.kind
        );
        Some((index, row_id, update))
    }

    /// Returns the newest ordinary row undo restored for physical mutation.
    #[inline]
    pub(crate) fn last_row_undo(&self) -> &OwnedRowUndo {
        self.row_undo
            .last()
            .expect("owned row mutation requires a newest ordinary row undo")
    }

    /// Requires that no operation-local deferred ownership remains.
    #[inline]
    pub(crate) fn assert_no_deferred_index_updates(&self) {
        assert!(
            self.deferred_index_updates.is_empty(),
            "statement boundary requires every deferred index update to be settled"
        );
    }

    /// Restores every pending lock to ordinary row rollback ownership.
    #[inline]
    pub(crate) fn settle_deferred_index_updates(&mut self) {
        for deferred in self.deferred_index_updates.drain(..) {
            self.row_undo.push(deferred.undo);
        }
    }

    /// Rewrite the latest provisional row undo lock into its final operation.
    #[inline]
    pub(crate) fn update_last_row_undo(&mut self, kind: RowUndoKind) {
        let last_undo = self.row_undo.last_mut().unwrap();
        // Currently the update can only be applied on LOCK entry.
        debug_assert!(matches!(last_undo.kind, RowUndoKind::Lock));
        last_undo.kind = kind;
    }

    /// Unlink and discard the newest provisional row lock without retaining an effect.
    #[inline]
    pub(crate) fn cancel_last_row_undo_lock(&mut self, unlink: impl FnOnce(&mut OwnedRowUndo)) {
        let last_undo = self
            .row_undo
            .last_mut()
            .expect("provisional row lock cancellation requires a row undo entry");
        assert!(
            matches!(last_undo.kind, RowUndoKind::Lock),
            "provisional row lock cancellation requires the newest undo to be Lock"
        );
        assert!(
            last_undo.stmt_no == self.stmt_no,
            "provisional row lock cancellation statement mismatch: effects_stmt_no={}, undo_stmt_no={}",
            self.stmt_no,
            last_undo.stmt_no
        );
        unlink(last_undo);
        let removed = self
            .row_undo
            .pop()
            .expect("unlinked provisional row lock must remain statement-owned");
        debug_assert!(matches!(removed.kind, RowUndoKind::Lock));
    }

    /// Push an inserted unique-index claim into statement rollback state.
    #[inline]
    pub(crate) fn push_catalog_insert_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: CatalogSelectKey,
        merge_old_deleted: bool,
    ) {
        self.index_undo.push_catalog(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertUnique(key, merge_old_deleted),
        });
    }

    /// Push an inserted non-unique-index claim into statement rollback state.
    #[inline]
    pub(crate) fn push_catalog_insert_non_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: CatalogSelectKey,
        merge_old_deleted: bool,
    ) {
        self.index_undo.push_catalog(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertNonUnique(key, merge_old_deleted),
        });
    }

    /// Push a deferred index delete into statement rollback and GC state.
    #[inline]
    pub(crate) fn push_catalog_delete_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: CatalogSelectKey,
        unique: bool,
    ) {
        self.index_undo.push_catalog(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::DeferDelete(key, unique),
        });
    }

    /// Push a unique-index update into statement rollback state.
    #[inline]
    pub(crate) fn push_catalog_update_unique_index_undo(
        &mut self,
        table_id: TableID,
        old_row_id: RowID,
        new_row_id: RowID,
        key: CatalogSelectKey,
        old_deleted: bool,
    ) {
        self.index_undo.push_catalog(IndexUndo {
            table_id,
            row_id: new_row_id,
            kind: IndexUndoKind::UpdateUnique(key, old_row_id, old_deleted),
        });
    }

    /// Push an inserted generation-qualified unique user-index claim.
    #[inline]
    pub(crate) fn push_user_insert_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: ResolvedUserIndexKey,
        merge_old_deleted: bool,
    ) {
        self.index_undo.push_user(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertUnique(key, merge_old_deleted),
        });
    }

    /// Push an inserted generation-qualified non-unique user-index claim.
    #[inline]
    pub(crate) fn push_user_insert_non_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: ResolvedUserIndexKey,
        merge_old_deleted: bool,
    ) {
        self.index_undo.push_user(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertNonUnique(key, merge_old_deleted),
        });
    }

    /// Push a generation-qualified deferred user-index delete.
    #[inline]
    pub(crate) fn push_user_delete_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: ResolvedUserIndexKey,
        unique: bool,
    ) {
        self.index_undo.push_user(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::DeferDelete(key, unique),
        });
    }

    /// Push a generation-qualified unique user-index update.
    #[inline]
    pub(crate) fn push_user_update_unique_index_undo(
        &mut self,
        table_id: TableID,
        old_row_id: RowID,
        new_row_id: RowID,
        key: ResolvedUserIndexKey,
        old_deleted: bool,
    ) {
        self.index_undo.push_user(IndexUndo {
            table_id,
            row_id: new_row_id,
            kind: IndexUndoKind::UpdateUnique(key, old_row_id, old_deleted),
        });
    }

    /// Insert one row redo entry into this statement's redo buffer.
    #[inline]
    pub(crate) fn insert_row_redo(&mut self, table_id: TableID, entry: RowRedo) {
        self.redo.insert_dml(table_id, entry);
    }

    /// Moves successful statement effects into the active transaction effects.
    #[inline]
    pub(crate) fn merge_into_trx_effects(&mut self, trx_effects: &mut TrxEffects) {
        self.assert_no_deferred_index_updates();
        trx_effects.merge_statement_effects(
            &mut self.row_undo,
            &mut self.index_undo,
            mem::take(&mut self.redo),
        );
    }

    /// Folds residual incomplete-statement undo into whole-transaction rollback.
    ///
    /// Redo from a statement that did not complete is never commit-visible.
    /// Undo remains ordered after prior successful statements so whole-
    /// transaction rollback unwinds this statement first. Public cancellation
    /// and private mandatory panic settlement share this mechanical operation.
    #[inline]
    pub(crate) fn fold_cancelled_into_trx_effects(&mut self, trx_effects: &mut TrxEffects) {
        self.settle_deferred_index_updates();
        self.redo.clear();
        trx_effects.row_undo_mut().merge(&mut self.row_undo);
        trx_effects.index_undo_mut().merge(&mut self.index_undo);
    }

    /// Rolls back statement-local row effects in reverse effect order.
    #[inline]
    pub(crate) async fn rollback_row(
        &mut self,
        table_cache: &mut TableCache<'_>,
        context: RowUndoRollbackContext<'_>,
    ) -> RuntimeOrFatalResult<()> {
        self.settle_deferred_index_updates();
        self.row_undo.rollback(table_cache, context).await
    }

    /// Rolls back statement-local secondary-index effects in reverse effect order.
    #[inline]
    pub(crate) async fn rollback_index(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
        sts: TrxID,
    ) -> RuntimeResult<()> {
        #[cfg(test)]
        tests::maybe_force_stmt_index_rollback_error()?;
        self.index_undo
            .rollback(table_cache, pool_guards, sts)
            .await
    }

    /// Discards statement-local redo after ordinary statement rollback succeeds.
    #[inline]
    pub(crate) fn clear_redo(&mut self) {
        self.redo.clear();
    }

    /// Discards every statement-local effect after fatal transaction cleanup.
    #[inline]
    fn take_for_fatal_retention(&mut self) -> FatalRollbackRetention {
        self.settle_deferred_index_updates();
        self.redo.clear();
        FatalRollbackRetention::Statement {
            row_undo: mem::take(&mut self.row_undo),
            index_undo: mem::take(&mut self.index_undo),
        }
    }
}

#[derive(Clone, Copy)]
enum StmtDropAction {
    CancelPublicTransaction,
    Settled,
}

/// Lifetime-free owner of one checked-out statement operation.
///
/// The carrier keeps the transaction core and statement effects together
/// across owned-operation await points. It lends direct disjoint borrows to
/// [`Statement`] and owns the final policy when that operation future is dropped.
pub(super) struct StmtState {
    effects: StmtEffects,
    dml_validation_disabled: bool,
    drop_action: StmtDropAction,
    checkout: Option<SessionOperationCheckout>,
}

impl StmtState {
    /// Arms public statement cancellation after a successful checkout.
    #[inline]
    pub(super) fn public(
        mut checkout: SessionOperationCheckout,
        dml_validation_disabled: bool,
    ) -> Self {
        let stmt_no = checkout.inner_mut().next_stmt_no();
        Self {
            effects: StmtEffects::new(stmt_no),
            dml_validation_disabled,
            drop_action: StmtDropAction::CancelPublicTransaction,
            checkout: Some(checkout),
        }
    }

    /// Lends one owned statement facade for the selected operation.
    #[inline]
    pub(super) fn statement(&mut self) -> Statement<'_> {
        let Self {
            effects,
            dml_validation_disabled,
            checkout,
            ..
        } = self;
        let checkout = checkout
            .as_mut()
            .expect("active statement state must own its transaction checkout");
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        Statement {
            inner,
            attachment,
            effects,
            dml_validation_disabled: *dml_validation_disabled,
        }
    }

    /// Merge a successful statement into the checked-out transaction.
    #[inline]
    pub(super) fn merge_effects(&mut self) {
        let Self {
            effects, checkout, ..
        } = self;
        let checkout = checkout
            .as_mut()
            .expect("active statement state must own its transaction checkout");
        effects.merge_into_trx_effects(checkout.inner_mut().effects_mut());
    }

    /// Roll back a failed statement before its initiating error is returned.
    #[inline]
    pub(super) async fn rollback_effects(&mut self) -> FatalResult<()> {
        let Self {
            effects, checkout, ..
        } = self;
        let checkout = checkout
            .as_mut()
            .expect("active statement state must own its transaction checkout");
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        rollback_effects(inner, attachment, effects).await
    }

    /// Ordinarily checks the core back in.
    #[inline]
    pub(super) fn return_ordinary(mut self) {
        self.drop_action = StmtDropAction::Settled;
        self.checkout = None;
    }

    /// Publishes fatal rollback retention after statement effects were retained.
    #[inline]
    pub(super) fn discard_after_fatal_rollback(mut self) {
        self.drop_action = StmtDropAction::Settled;
        if let Some(checkout) = self.checkout.as_mut() {
            checkout.discard_after_fatal_rollback();
        }
        self.checkout = None;
    }

    #[cold]
    #[inline(never)]
    fn settle_armed_drop(&mut self) {
        match self.drop_action {
            StmtDropAction::CancelPublicTransaction => {
                let Some(mut checkout) = self.checkout.take() else {
                    return;
                };
                self.effects
                    .fold_cancelled_into_trx_effects(checkout.inner_mut().effects_mut());
                checkout.return_cancelled();
            }
            StmtDropAction::Settled => {}
        }
    }
}

impl Drop for StmtState {
    #[inline]
    fn drop(&mut self) {
        if !matches!(self.drop_action, StmtDropAction::Settled) {
            self.settle_armed_drop();
        }
    }
}

/// Carrier for one private statement over a continuously held checkout.
pub(super) struct PrivateStmtState<'checkout> {
    effects: StmtEffects,
    checkout: &'checkout mut SessionOperationCheckout,
    settled: bool,
}

impl<'checkout> PrivateStmtState<'checkout> {
    /// Create one private statement carrier and allocate its statement number.
    #[inline]
    pub(super) fn new(checkout: &'checkout mut SessionOperationCheckout) -> Self {
        let stmt_no = checkout.inner_mut().next_stmt_no();
        Self {
            effects: StmtEffects::new(stmt_no),
            checkout,
            settled: false,
        }
    }

    /// Lend the one owned statement operation.
    #[inline]
    pub(super) fn statement(&mut self) -> Statement<'_> {
        let (inner, attachment) = self.checkout.inner_and_attachment_mut();
        Statement {
            inner,
            attachment,
            effects: &mut self.effects,
            dml_validation_disabled: false,
        }
    }

    /// Merge one successful private statement into transaction effects.
    #[inline]
    pub(super) fn merge_effects(mut self) {
        self.effects
            .merge_into_trx_effects(self.checkout.inner_mut().effects_mut());
        self.settled = true;
    }

    /// Roll back one failed private statement before returning its Runtime error.
    #[inline]
    pub(super) async fn rollback_effects(mut self) -> FatalResult<()> {
        let result = {
            let (inner, attachment) = self.checkout.inner_and_attachment_mut();
            rollback_effects(inner, attachment, &mut self.effects).await
        };
        self.settled = true;
        result
    }

    /// Preserve residual undo and discard redo before resuming a private panic.
    #[inline]
    pub(super) fn fold_cancelled_into_transaction(mut self) {
        self.effects
            .fold_cancelled_into_trx_effects(self.checkout.inner_mut().effects_mut());
        self.settled = true;
    }
}

impl Drop for PrivateStmtState<'_> {
    #[inline]
    fn drop(&mut self) {
        if !self.settled {
            self.effects
                .fold_cancelled_into_trx_effects(self.checkout.inner_mut().effects_mut());
        }
    }
}

/// Owned one-shot facade for one internal transaction operation.
pub(super) struct Statement<'stmt> {
    inner: &'stmt mut TrxInner,
    attachment: &'stmt TrxAttachment,
    effects: &'stmt mut StmtEffects,
    dml_validation_disabled: bool,
}

impl<'stmt> Statement<'stmt> {
    /// Returns this statement's operation-local transaction runtime.
    #[inline]
    pub(crate) fn runtime(&self) -> TrxRuntime<'_> {
        TrxRuntime::new(
            self.inner.ctx(),
            self.attachment,
            self.inner.checked_lock_state(),
        )
    }

    #[inline]
    fn runtime_and_effects_mut(&mut self) -> (TrxRuntime<'_>, &mut StmtEffects) {
        let runtime = TrxRuntime::new(
            self.inner.ctx(),
            self.attachment,
            self.inner.checked_lock_state(),
        );
        (runtime, self.effects)
    }

    /// Acquires transaction-lifetime metadata protection for a table write.
    #[inline]
    pub(crate) async fn acquire_table_write_metadata_lock(
        &mut self,
        table_id: TableID,
    ) -> OperationOrFatalResult<()> {
        let engine = self.attachment.engine();
        let lock_manager = engine.lock_manager();
        self.inner
            .checked_lock_state_mut()
            .acquire(
                lock_manager,
                &engine.poisoner,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            )
            .await
            .map(|_| ())
    }

    /// Acquires transaction-lifetime table-data intent for a point write.
    #[inline]
    pub(crate) async fn acquire_table_write_data_lock(
        &mut self,
        table_id: TableID,
    ) -> OperationOrFatalResult<()> {
        let engine = self.attachment.engine();
        let lock_manager = engine.lock_manager();
        self.inner
            .checked_lock_state_mut()
            .acquire(
                lock_manager,
                &engine.poisoner,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
            )
            .await
            .map(|_| ())
    }

    /// Acquires transaction-lifetime exclusive table-data protection.
    #[inline]
    async fn acquire_table_exclusive_data_lock(
        &mut self,
        table_id: TableID,
    ) -> OperationOrFatalResult<()> {
        let engine = self.attachment.engine();
        let lock_manager = engine.lock_manager();
        self.inner
            .checked_lock_state_mut()
            .acquire(
                lock_manager,
                &engine.poisoner,
                LockResource::TableData(table_id),
                LockMode::Exclusive,
            )
            .await
            .map(|_| ())
    }

    /// Admits one user-table operation under transaction-owned metadata protection.
    ///
    /// Once a first-touch metadata claim is accepted, a later resolution or
    /// validation error does not release it. The transaction remains reusable,
    /// but the retained claim can delay metadata-exclusive DDL and subsequent
    /// FIFO waiters until terminal cleanup. A caller that does not deliberately
    /// continue after such an error should roll back the transaction promptly.
    #[inline]
    async fn admit_user_table(
        &mut self,
        table_id: TableID,
        write: bool,
        operation: &'static str,
    ) -> OperationOrFatalResult<AdmittedUserTable> {
        admit_user_table(self.inner, self.attachment, table_id, write, operation).await
    }

    /// Admits one indexed operation under transaction-owned metadata protection.
    #[inline]
    async fn admit_user_index(
        &mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        write: bool,
        operation: &'static str,
    ) -> OperationOrFatalResult<AdmittedUserIndex> {
        admit_user_index(
            self.inner,
            self.attachment,
            table_id,
            selector,
            write,
            operation,
        )
        .await
    }

    /// Sequentially mutate callback-selected rows using latest modification reads.
    ///
    /// A cold persisted image is exposed only while it remains the current
    /// logical row. Hot-row values come from the latest physical page image
    /// rather than an older version reconstructed for the transaction snapshot.
    /// Another active row owner causes a write conflict; this transaction's own
    /// active state is followed to its latest hot image. The callback may skip,
    /// delete, or sparsely update each exposed row. An empty update still counts
    /// as an update decision without creating physical row, index, undo, or redo
    /// work. Each eligible original row is exposed at most once, and replacement
    /// rows inserted by updates are excluded from the same traversal. The result
    /// reports delete and update decisions independently after all actions
    /// succeed.
    #[inline]
    pub(super) async fn table_mutate_mvcc<F>(
        mut self,
        table_id: TableID,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        const OPERATION: &str = "table_mutate_mvcc";
        let AdmittedUserTable { table, layout } = self
            .admit_user_table(table_id, true, OPERATION)
            .await
            .disclose()?;
        self.acquire_table_exclusive_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let validate_updates = !self.dml_validation_disabled;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .table_mutate_mvcc(rt, effects, validate_updates, mutate_row)
            .await
    }

    /// Sequentially mutates latest rows selected by a secondary-index range.
    ///
    /// The traversal is a weak monotonic current read: mutable index state is
    /// resumed strictly after its last consumed exact key, while the captured
    /// DiskTree cursor advances incrementally. Each callback runs only after
    /// exact candidate revalidation and row ownership acquisition. Updates
    /// driven by a unique index that change its encoded logical key retain row
    /// ownership and apply only after candidate traversal is exhausted. This
    /// keeps old index entries discoverable and invokes each callback at most
    /// once, but it can report a later uniqueness or storage error after all
    /// callbacks have run. Deferred updates are memory-only and intentionally
    /// uncapped; callbacks must not depend on candidate-order physical effects.
    #[inline]
    pub(super) async fn table_index_mutate_mvcc<'r, R, F>(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        range: R,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        R: RangeBounds<&'r [Val]>,
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        const OPERATION: &str = "table_index_mutate_mvcc";
        self.effects.assert_no_deferred_index_updates();
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, true, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_index_range(index.slot(), &range)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let validate_updates = !self.dml_validation_disabled;
        let result = {
            let (rt, effects) = self.runtime_and_effects_mut();
            table
                .accessor_with_layout(&layout)
                .table_index_mutate_mvcc(rt, effects, index, range, validate_updates, mutate_row)
                .await
        };
        if result.is_err() {
            self.effects.settle_deferred_index_updates();
        } else {
            self.effects.assert_no_deferred_index_updates();
        }
        result
    }

    /// Looks up one unique-key row in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn resolve_user_index(
        mut self,
        table_id: TableID,
        index_id: IndexID,
    ) -> Result<ResolvedUserIndex> {
        const OPERATION: &str = "resolve_user_index";
        let admitted = self
            .admit_user_index(table_id, UserIndexSelector::ID(index_id), false, OPERATION)
            .await
            .disclose()?;
        Ok(ResolvedUserIndex::from_admitted(table_id, admitted.index))
    }

    /// Looks up one unique-key row in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_lookup_unique_mvcc(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        const OPERATION: &str = "table_lookup_unique_mvcc";
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, false, OPERATION)
            .await
            .disclose()?;
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_unique_mvcc(rt, index, key_vals, user_read_set)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}, index={index}"))
            .disclose()
    }

    /// Looks up one secondary-index key in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_index_lookup_mvcc(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        const OPERATION: &str = "table_index_lookup_mvcc";
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, false, OPERATION)
            .await
            .disclose()?;
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_mvcc(rt, index, key_vals, user_read_set)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}, index={index}"))
            .disclose()
    }

    /// Scans one secondary-index range in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_index_scan_mvcc<'r, R>(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        range: R,
        read_set: &[usize],
    ) -> Result<ScanMvcc>
    where
        R: RangeBounds<&'r [Val]>,
    {
        const OPERATION: &str = "table_index_scan_mvcc";
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, false, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_index_scan(index.slot(), &range, read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_scan_mvcc(rt, index, range, read_set)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}, index={index}"))
            .disclose()
    }

    /// Inserts one row into a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_insert_mvcc(
        mut self,
        table_id: TableID,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        const OPERATION: &str = "table_insert_mvcc";
        let AdmittedUserTable { table, layout } = self
            .admit_user_table(table_id, true, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_full_row(&cols)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .insert_mvcc(rt, effects, cols)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Atomically inserts one validated batch into a catalog-owned user table.
    #[inline]
    pub(super) async fn table_insert_batch_mvcc(
        mut self,
        table_id: TableID,
        rows: Vec<Vec<Val>>,
    ) -> Result<Vec<RowID>> {
        const OPERATION: &str = "table_insert_batch_mvcc";
        let AdmittedUserTable { table, layout } = self
            .admit_user_table(table_id, true, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            let validator = DmlValidator::new(layout.metadata());
            for (batch_index, row) in rows.iter().enumerate() {
                validator
                    .validate_full_row(row)
                    .change_context(OperationError::InvalidDmlInput)
                    .attach_with(|| {
                        format!(
                            "operation={OPERATION}, table_id={table_id}, batch_index={batch_index}"
                        )
                    })
                    .disclose()?;
            }
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let mut row_ids = Vec::with_capacity(rows.len());
        let (rt, effects) = self.runtime_and_effects_mut();
        let accessor = table.accessor_with_layout(&layout);
        for (batch_index, row) in rows.into_iter().enumerate() {
            let row_id = accessor
                .insert_mvcc(rt, effects, row)
                .await
                .attach_with(|| {
                    format!("operation={OPERATION}, table_id={table_id}, batch_index={batch_index}")
                })
                .disclose()?;
            row_ids.push(row_id);
        }
        Ok(row_ids)
    }

    /// Inserts or replaces one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_upsert_unique_mvcc(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        cols: Vec<Val>,
    ) -> Result<UpsertMvcc> {
        const OPERATION: &str = "table_upsert_unique_mvcc";
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, true, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            let validator = DmlValidator::new(layout.metadata());
            validator
                .validate_full_row(&cols)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
            validator
                .validate_unique_index(index.slot())
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .upsert_unique_mvcc(rt, effects, index, cols, false)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Updates one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_update_unique_mvcc(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        const OPERATION: &str = "table_update_unique_mvcc";
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, true, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            let validator = DmlValidator::new(layout.metadata());
            validator
                .validate_unique_key(index.slot(), key_vals)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
            validator
                .validate_sparse_update(&update)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .update_unique_mvcc(rt, effects, index, key_vals, update, false)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Deletes one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub(super) async fn table_delete_unique_mvcc(
        mut self,
        table_id: TableID,
        selector: UserIndexSelector,
        key_vals: &[Val],
    ) -> Result<DeleteMvcc> {
        const OPERATION: &str = "table_delete_unique_mvcc";
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self
            .admit_user_index(table_id, selector, true, OPERATION)
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_unique_key(index.slot(), key_vals)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .delete_unique_mvcc(rt, effects, index, key_vals)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Inserts one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(super) async fn catalog_insert_mvcc(
        mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> RuntimeOrFatalResult<RowID> {
        self.catalog_insert_mvcc_inner(table, cols).await
    }

    /// Performs one catalog insert while narrowing each native error carrier at
    /// its owning boundary.
    #[inline]
    async fn catalog_insert_mvcc_inner(
        &mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> RuntimeOrFatalResult<RowID> {
        const OPERATION: &str = "catalog_insert_mvcc";
        let table_id = table.table_id();
        let metadata_lock = self
            .acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, metadata_lock)?;
        let validation = DmlValidator::new(table.metadata())
            .validate_full_row(&cols)
            .change_context(OperationError::InvalidDmlInput)
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        assert_catalog_operation_invariant(table_id, validation);
        let data_lock = self
            .acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, data_lock)?;
        let (rt, effects) = self.runtime_and_effects_mut();
        let result = table.insert_mvcc(rt, effects, cols).await;
        Ok(narrow_catalog_operation_or_runtime(
            table_id,
            result,
            || format!("operation={OPERATION}, table_id={table_id}"),
        )?)
    }

    /// Inserts an ordered catalog batch through one consumed statement.
    #[inline]
    pub(super) async fn catalog_insert_batch_mvcc(
        mut self,
        table: &CatalogTable,
        rows: Vec<Vec<Val>>,
    ) -> RuntimeOrFatalResult<()> {
        const OPERATION: &str = "catalog_insert_batch_mvcc";
        let table_id = table.table_id();
        let metadata_lock = self
            .acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, metadata_lock)?;
        let validator = DmlValidator::new(table.metadata());
        for (batch_index, row) in rows.iter().enumerate() {
            let validation = validator
                .validate_full_row(row)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={OPERATION}, table_id={table_id}, batch_index={batch_index}")
                });
            assert_catalog_operation_invariant(table_id, validation);
        }
        let data_lock = self
            .acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, data_lock)?;
        let (rt, effects) = self.runtime_and_effects_mut();
        for (batch_index, row) in rows.into_iter().enumerate() {
            let result = table.insert_mvcc(rt, effects, row).await;
            narrow_catalog_operation_or_runtime(table_id, result, || {
                format!("operation={OPERATION}, table_id={table_id}, batch_index={batch_index}")
            })?;
        }
        Ok(())
    }

    /// Deletes one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(super) async fn catalog_delete_primary_key_mvcc(
        mut self,
        table: &CatalogTable,
        index_slot: CatalogIndexNo,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> RuntimeOrFatalResult<DeleteMvcc> {
        self.catalog_delete_primary_key_mvcc_inner(table, index_slot, key_vals, log_by_key)
            .await
    }

    /// Performs one catalog delete while narrowing each native error carrier at
    /// its owning boundary.
    #[inline]
    async fn catalog_delete_primary_key_mvcc_inner(
        &mut self,
        table: &CatalogTable,
        index_slot: CatalogIndexNo,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> RuntimeOrFatalResult<DeleteMvcc> {
        const OPERATION: &str = "catalog_delete_primary_key_mvcc";
        let table_id = table.table_id();
        let metadata_lock = self
            .acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, metadata_lock)?;
        let validation = DmlValidator::new(table.metadata())
            .validate_primary_key(index_slot, key_vals)
            .change_context(OperationError::InvalidDmlInput)
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        assert_catalog_operation_invariant(table_id, validation);
        let data_lock = self
            .acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, data_lock)?;
        let (rt, effects) = self.runtime_and_effects_mut();
        let result = table
            .delete_unique_mvcc(rt, effects, index_slot, key_vals, log_by_key)
            .await;
        narrow_catalog_quad_result(table_id, result, || {
            format!("operation={OPERATION}, table_id={table_id}, index_slot={index_slot}")
        })
    }

    /// Deletes an ordered catalog primary-key batch through one consumed statement.
    #[inline]
    pub(super) async fn catalog_delete_primary_key_batch_mvcc(
        mut self,
        table: &CatalogTable,
        index_slot: CatalogIndexNo,
        keys: Vec<Vec<Val>>,
    ) -> RuntimeOrFatalResult<usize> {
        const OPERATION: &str = "catalog_delete_primary_key_batch_mvcc";
        let table_id = table.table_id();
        let metadata_lock = self
            .acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, metadata_lock)?;
        let validator = DmlValidator::new(table.metadata());
        for (batch_index, key_vals) in keys.iter().enumerate() {
            let validation = validator
                .validate_primary_key(index_slot, key_vals)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={OPERATION}, table_id={table_id}, batch_index={batch_index}")
                });
            assert_catalog_operation_invariant(table_id, validation);
        }
        let data_lock = self
            .acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, data_lock)?;
        let (rt, effects) = self.runtime_and_effects_mut();
        let mut deleted = 0;
        for (batch_index, key_vals) in keys.iter().enumerate() {
            let result = table
                .delete_unique_mvcc(rt, effects, index_slot, key_vals, true)
                .await;
            let result = narrow_catalog_quad_result(table_id, result, || {
                format!(
                    "operation={OPERATION}, table_id={table_id}, index_slot={index_slot}, batch_index={batch_index}"
                )
            })?;
            deleted += usize::from(matches!(result, DeleteMvcc::Deleted));
        }
        Ok(deleted)
    }

    /// Replaces one catalog row through one delete-then-insert statement.
    #[inline]
    pub(super) async fn catalog_replace_primary_key_mvcc(
        mut self,
        table: &CatalogTable,
        index_slot: CatalogIndexNo,
        key_vals: &[Val],
        cols: Vec<Val>,
    ) -> RuntimeOrFatalResult<DeleteMvcc> {
        const OPERATION: &str = "catalog_replace_primary_key_mvcc";
        let table_id = table.table_id();
        let metadata_lock = self
            .acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, metadata_lock)?;
        let validator = DmlValidator::new(table.metadata());
        let key_validation = validator
            .validate_primary_key(index_slot, key_vals)
            .change_context(OperationError::InvalidDmlInput)
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        assert_catalog_operation_invariant(table_id, key_validation);
        let row_validation = validator
            .validate_full_row(&cols)
            .change_context(OperationError::InvalidDmlInput)
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        assert_catalog_operation_invariant(table_id, row_validation);
        let data_lock = self
            .acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"));
        narrow_catalog_operation_or_fatal(table_id, data_lock)?;
        let (rt, effects) = self.runtime_and_effects_mut();
        let delete_result = table
            .delete_unique_mvcc(rt, effects, index_slot, key_vals, true)
            .await;
        let deleted = narrow_catalog_quad_result(table_id, delete_result, || {
            format!("operation={OPERATION}, table_id={table_id}, phase=delete")
        })?;
        let insert_result = table.insert_mvcc(rt, effects, cols).await;
        narrow_catalog_operation_or_runtime(table_id, insert_result, || {
            format!("operation={OPERATION}, table_id={table_id}, phase=insert")
        })?;
        Ok(deleted)
    }
}

/// Roll back one statement's effects using shared public/private mechanics.
#[inline]
async fn rollback_effects(
    inner: &mut TrxInner,
    attachment: &TrxAttachment,
    effects: &mut StmtEffects,
) -> FatalResult<()> {
    let sts = inner.sts();
    let engine = attachment.engine();
    let pool_guards = attachment.pool_guards();
    let rollback_context = RowUndoRollbackContext::new(pool_guards, &engine.poisoner);
    let mut table_cache = TableCache::new(engine.catalog());
    if let Err(err) = effects
        .rollback_index(&mut table_cache, pool_guards, sts)
        .await
    {
        let retention = effects.take_for_fatal_retention();
        engine.trx_sys.retain_fatal_rollback(retention);
        let report = err
            .change_context(FatalError::RollbackAccess)
            .attach("statement index rollback failed");
        obs::error!(
            "event=engine_poison component=trx action=poison result=error error={:?}",
            report
        );
        return Err(engine.poisoner.poison(report).into_report());
    }
    if let Err(err) = effects
        .rollback_row(&mut table_cache, rollback_context)
        .await
    {
        let retention = effects.take_for_fatal_retention();
        engine.trx_sys.retain_fatal_rollback(retention);
        return match err {
            RuntimeOrFatalError::Runtime(report) => {
                let report = report
                    .change_context(FatalError::RollbackAccess)
                    .attach("statement row rollback failed");
                obs::error!(
                    "event=engine_poison component=trx action=poison result=error error={:?}",
                    report
                );
                Err(engine.poisoner.poison(report).into_report())
            }
            RuntimeOrFatalError::Fatal(report) => {
                Err(report.attach("statement row rollback failed"))
            }
        };
    }
    effects.clear_redo();
    Ok(())
}

/// Assert one catalog-only Operation result at its immediate owning boundary.
#[inline]
fn assert_catalog_operation_invariant<T>(table_id: TableID, result: OperationResult<T>) -> T {
    match result {
        Ok(value) => value,
        Err(report) => {
            panic!("catalog mutation invariant violated: table_id={table_id}, error={report:?}")
        }
    }
}

/// Assert the impossible Operation arm of a catalog lock result and preserve
/// Fatal without widening through a synthetic carrier.
#[inline]
fn narrow_catalog_operation_or_fatal<T>(
    table_id: TableID,
    result: OperationOrFatalResult<T>,
) -> FatalResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(OperationOrFatalError::Operation(report)) => {
            panic!("catalog mutation invariant violated: table_id={table_id}, error={report:?}")
        }
        Err(OperationOrFatalError::Fatal(report)) => Err(report),
    }
}

/// Assert the impossible Operation arm of a catalog insert result and assign
/// catalog Runtime ownership before returning it.
#[inline]
fn narrow_catalog_operation_or_runtime<T, F>(
    table_id: TableID,
    result: OperationOrRuntimeResult<T>,
    attachment: F,
) -> RuntimeResult<T>
where
    F: FnOnce() -> String,
{
    match result {
        Ok(value) => Ok(value),
        Err(OperationOrRuntimeError::Operation(report)) => {
            let report = report.attach(attachment());
            panic!("catalog mutation invariant violated: table_id={table_id}, error={report:?}")
        }
        Err(OperationOrRuntimeError::Runtime(report)) => Err(report
            .change_context(RuntimeError::CatalogAccess)
            .attach(attachment())),
    }
}

/// Narrow the generic table-delete carrier immediately at the catalog boundary.
#[inline]
fn narrow_catalog_quad_result<T, F>(
    table_id: TableID,
    result: QuadResult<T>,
    attachment: F,
) -> RuntimeOrFatalResult<T>
where
    F: FnOnce() -> String,
{
    match result {
        Ok(value) => Ok(value),
        Err(QuadError::Operation(report)) => {
            let report = report.attach(attachment());
            panic!("catalog mutation invariant violated: table_id={table_id}, error={report:?}")
        }
        Err(QuadError::Runtime(report)) => Err(RuntimeOrFatalError::Runtime(
            report
                .change_context(RuntimeError::CatalogAccess)
                .attach(attachment()),
        )),
        Err(QuadError::Lifecycle(report)) => {
            let report = report.attach(attachment());
            panic!(
                "catalog mutation lifecycle invariant violated: table_id={table_id}, error={report:?}"
            )
        }
        Err(QuadError::Fatal(report)) => {
            Err(RuntimeOrFatalError::Fatal(report.attach(attachment())))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::EvictableBufferPool;
    use crate::buffer::guard::PageSharedGuard;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::catalog::storage::tests::begin_catalog_test_trx;
    use crate::catalog::{IndexSlot, user_key_from_active_slot};
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{
        DiscloseError, DiscloseResultExt, FatalError, InternalError, LifecycleError,
        OperationError, ResourceError,
    };
    use crate::id::TrxID;
    use crate::lock::LockOwner;
    use crate::lock::tests::debug_snapshot;
    use crate::log::redo::RowRedoKind;
    use crate::row::RowPage;
    use crate::row::ops::SelectKey;
    use crate::session::{SessionState, tests as session_tests};
    use crate::table::tests::{
        lock_hot_row_then_wait_and_error_operation, transition_delete_operation,
        transition_insert_update_operation,
    };
    use crate::table::{MemTable, Table};
    use crate::trx::sys::tests as sys_tests;
    use crate::trx::undo::tests::{pause_next_index_rollback, pause_next_row_rollback};
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
    use crate::trx::{MIN_ACTIVE_TRX_ID, Transaction};
    use error_stack::Report;
    use futures::FutureExt;
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::ptr::from_ref;
    use std::sync::Arc;
    use tempfile::TempDir;

    thread_local! {
        static TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR: Cell<bool> = const { Cell::new(false) };
    }

    /// Return whether one statement retains deferred index updates.
    #[inline]
    pub(crate) fn has_deferred_index_updates(effects: &StmtEffects) -> bool {
        !effects.deferred_index_updates.is_empty()
    }

    pub(super) fn set_test_force_stmt_index_rollback_error(enabled: bool) {
        TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR.with(|flag| flag.set(enabled));
    }

    pub(super) fn maybe_force_stmt_index_rollback_error() -> RuntimeResult<()> {
        if TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR.with(|flag| flag.get()) {
            return Err(Report::new(RuntimeError::IndexAccess)
                .attach("operation=test_statement_index_rollback"));
        }
        Ok(())
    }

    #[inline]
    pub(in crate::trx) fn transaction_lock_owner(stmt: &Statement<'_>) -> LockOwner {
        stmt.inner.checked_lock_state().owner()
    }

    #[inline]
    pub(in crate::trx) async fn acquire_transaction_lock(
        stmt: &mut Statement<'_>,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        let engine = stmt.attachment.engine();
        let lock_manager = engine.lock_manager();
        stmt.inner
            .checked_lock_state_mut()
            .acquire(lock_manager, &engine.poisoner, resource, mode)
            .await
            .map(|_| ())
            .disclose()
    }

    #[inline]
    pub(in crate::trx) fn statement_effects_mut<'borrow>(
        stmt: &'borrow mut Statement<'_>,
    ) -> &'borrow mut StmtEffects {
        stmt.effects
    }

    #[inline]
    fn empty_stmt_effects() -> StmtEffects {
        StmtEffects {
            stmt_no: NON_FOREGROUND_STMT_NO,
            row_undo: RowUndoLogs::empty(),
            deferred_index_updates: Vec::new(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    #[inline]
    async fn prepare_raw_table_write(stmt: &mut Statement<'_>, table_id: TableID) -> Result<()> {
        stmt.acquire_table_write_metadata_lock(table_id)
            .await
            .disclose()?;
        stmt.acquire_table_write_data_lock(table_id)
            .await
            .disclose()
    }

    /// Insert through a standalone MemTable using production statement settlement.
    pub(in crate::trx) async fn mem_table_insert_mvcc(
        mut stmt: Statement<'_>,
        mem_table: &MemTable<EvictableBufferPool, EvictableBufferPool>,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        let table_id = mem_table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        mem_table.insert_mvcc(rt, effects, cols).await.disclose()
    }

    /// Upsert through a standalone MemTable using production statement settlement.
    pub(in crate::trx) async fn mem_table_upsert_unique_mvcc(
        mut stmt: Statement<'_>,
        mem_table: &MemTable<EvictableBufferPool, EvictableBufferPool>,
        cols: Vec<Val>,
    ) -> Result<UpsertMvcc> {
        let table_id = mem_table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        mem_table
            .upsert_unique_mvcc(rt, effects, IndexSlot::new(0), cols, false)
            .await
            .disclose()
    }

    /// Update through a standalone MemTable using production statement settlement.
    pub(in crate::trx) async fn mem_table_update_unique_mvcc(
        mut stmt: Statement<'_>,
        mem_table: &MemTable<EvictableBufferPool, EvictableBufferPool>,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        let table_id = mem_table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        mem_table
            .update_unique_mvcc(rt, effects, key.index_slot, &key.vals, update, false)
            .await
            .disclose()
    }

    /// Delete through a standalone MemTable using production statement settlement.
    pub(in crate::trx) async fn mem_table_delete_unique_mvcc(
        mut stmt: Statement<'_>,
        mem_table: &MemTable<EvictableBufferPool, EvictableBufferPool>,
        key: &SelectKey,
    ) -> Result<DeleteMvcc> {
        let table_id = mem_table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        mem_table
            .delete_unique_mvcc(rt, effects, key.index_slot, &key.vals, false)
            .await
            .disclose()
    }

    /// Apply one standalone MemTable index-only key change.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::trx) async fn mem_table_duplicate_index_key_change(
        mut stmt: Statement<'_>,
        mem_table: &MemTable<EvictableBufferPool, EvictableBufferPool>,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        old_key: SelectKey,
        new_key: SelectKey,
    ) -> Result<()> {
        let table_id = mem_table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        mem_table
            .update_unique_index_only_key_change(rt, effects, old_key, new_key, row_id, &page_guard)
            .await
            .disclose()
    }

    /// Run the focused transition-page insert/update operation.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::trx) async fn transition_insert_update(
        mut stmt: Statement<'_>,
        table: &Table,
        insert_page_guard: PageSharedGuard<RowPage>,
        insert: Vec<Val>,
        page_guard: &PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<(bool, bool)> {
        let table_id = table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        transition_insert_update_operation(
            rt,
            effects,
            table,
            insert_page_guard,
            insert,
            page_guard,
            row_id,
            key,
            update,
        )
        .await
    }

    /// Run the focused transition-page delete operation.
    pub(in crate::trx) async fn transition_delete(
        mut stmt: Statement<'_>,
        table: &Table,
        page_guard: &PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
    ) -> Result<bool> {
        let table_id = table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        transition_delete_operation(rt, effects, table, page_guard, row_id, key).await
    }

    /// Install one hot-row lock and pause before forcing operation rollback.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::trx) async fn lock_hot_row_then_wait_and_error(
        mut stmt: Statement<'_>,
        table: &Table,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        key: &SelectKey,
        lock_installed: flume::Sender<bool>,
        return_error: flume::Receiver<()>,
    ) -> Result<()> {
        let table_id = table.table_id();
        prepare_raw_table_write(&mut stmt, table_id).await?;
        let (rt, effects) = stmt.runtime_and_effects_mut();
        lock_hot_row_then_wait_and_error_operation(
            rt,
            effects,
            table,
            page_guard,
            row_id,
            key,
            lock_installed,
            return_error,
        )
        .await
    }

    /// Insert a catalog prefix before injecting one private Runtime error.
    async fn catalog_insert_prefix_then_runtime_error(
        mut stmt: Statement<'_>,
        table: &CatalogTable,
        rows: Vec<Vec<Val>>,
    ) -> RuntimeOrFatalResult<()> {
        for row in rows {
            stmt.catalog_insert_mvcc_inner(table, row).await?;
        }
        Err(Report::new(RuntimeError::CatalogAccess)
            .attach("operation=test_catalog_insert_prefix_then_runtime_error")
            .into())
    }

    #[inline]
    fn trx_lock_owner(trx: &mut Transaction) -> Result<LockOwner> {
        let checkout = trx.checkout().disclose()?;
        Ok(checkout.inner().checked_lock_state().owner())
    }

    #[inline]
    fn acquire_transaction_lock_immediate(
        trx: &mut Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        let mut checkout = trx.checkout().disclose()?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let engine = attachment.engine();
        let lock_manager = engine.lock_manager();
        inner
            .checked_lock_state_mut()
            .acquire(lock_manager, &engine.poisoner, resource, mode)
            .now_or_never()
            .expect("test transaction lock acquisition unexpectedly waited")
            .map(|_| ())
            .disclose()
    }

    async fn test_engine(log_file_stem: &str) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = Engine::bootstrap(
            EngineConfig::default()
                .storage_root(temp_dir.path().to_path_buf())
                .data_buffer(
                    EvictableBufferPoolConfig::default()
                        .max_mem_size(64usize * 1024 * 1024)
                        .max_file_size(128usize * 1024 * 1024),
                )
                .trx(
                    TrxSysConfig::default()
                        .purge_threads(1)
                        .log_file_stem(log_file_stem),
                ),
        )
        .await
        .unwrap();
        (temp_dir, engine)
    }

    fn test_trx(engine: &Engine, sts: TrxID) -> (Transaction, Arc<SessionState>) {
        let session_id = engine.inner().next_session_id();
        session_tests::create_test_transaction(
            engine,
            session_id,
            MIN_ACTIVE_TRX_ID + sts.as_u64(),
            sts,
            0,
        )
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        debug_snapshot(engine.inner().core.lock_manager())
            .entries
            .iter()
            .filter(|entry| entry.family == owner.family())
            .count()
    }

    fn assert_stmt_effects_empty(effects: &StmtEffects) {
        assert!(effects.row_undo.is_empty());
        assert!(effects.deferred_index_updates.is_empty());
        assert!(effects.index_undo.is_empty());
        assert!(effects.redo.is_empty());
    }

    fn assert_catalog_runtime_stack(err: &Report<RuntimeError>, operation: &str) {
        assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
        assert_eq!(
            err.downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        let rendered = format!("{err:?}");
        assert!(rendered.contains("pool_role=Meta"));
        assert!(rendered.contains(operation));
    }

    #[test]
    fn test_stmt_effects_empty() {
        let effects = empty_stmt_effects();
        assert_stmt_effects_empty(&effects);
    }

    // Owned-runner coverage: exact statement-number allocation is inspected
    // through raw statement effects across success and failure.
    #[test]
    fn test_public_statements_consume_monotonic_statement_numbers() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("stmt_number_sequence").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let first = trx
                .exec(async |mut stmt| Ok(statement_effects_mut(&mut stmt).stmt_no()))
                .await
                .unwrap();
            let failed: Result<u64> = trx
                .exec(async |mut stmt| {
                    let stmt_no = statement_effects_mut(&mut stmt).stmt_no();
                    Err(Report::new(OperationError::InvalidDmlInput)
                        .attach(format!("stmt_no={stmt_no}"))
                        .disclose())
                })
                .await;
            let failed = failed.unwrap_err();
            let second = format!("{failed:?}");
            let third = trx
                .exec(async |mut stmt| Ok(statement_effects_mut(&mut stmt).stmt_no()))
                .await
                .unwrap();
            assert_eq!(first, 1);
            assert!(second.contains("stmt_no=2"));
            assert_eq!(third, 3);
            trx.commit().await.unwrap();

            let mut next = session.begin_trx().unwrap();
            let first = next
                .exec(async |mut stmt| Ok(statement_effects_mut(&mut stmt).stmt_no()))
                .await
                .unwrap();
            assert_eq!(first, 1);
            next.commit().await.unwrap();
        });
    }

    #[test]
    fn test_cancelled_stmt_effects_fold_undo_and_discard_redo() {
        let mut trx_effects = TrxEffects::empty();
        trx_effects.row_undo_mut().push(OwnedRowUndo::new(
            NON_FOREGROUND_STMT_NO,
            TableID::new(41),
            None,
            RowID::new(1),
            RowUndoKind::Delete,
        ));
        trx_effects.index_undo_mut().push_user(IndexUndo {
            table_id: TableID::new(41),
            row_id: RowID::new(1),
            kind: IndexUndoKind::DeferDelete(
                user_key_from_active_slot(IndexSlot::new(0), vec![]),
                true,
            ),
        });

        let mut effects = empty_stmt_effects();
        effects.push_row_undo(OwnedRowUndo::new(
            NON_FOREGROUND_STMT_NO,
            TableID::new(42),
            None,
            RowID::new(2),
            RowUndoKind::Insert,
        ));
        effects.push_user_delete_index_undo(
            TableID::new(42),
            RowID::new(2),
            user_key_from_active_slot(IndexSlot::new(0), vec![]),
            true,
        );
        effects.insert_row_redo(
            TableID::new(42),
            RowRedo {
                row_id: RowID::new(2),
                kind: RowRedoKind::Delete(None),
            },
        );
        let cancelled_row_undo = from_ref(&*effects.row_undo[0]);

        effects.fold_cancelled_into_trx_effects(&mut trx_effects);

        assert_stmt_effects_empty(&effects);
        assert_eq!(trx_effects.row_undo.len(), 2);
        assert_eq!(trx_effects.row_undo[0].table_id, TableID::new(41));
        assert_eq!(trx_effects.row_undo[1].table_id, TableID::new(42));
        assert_eq!(from_ref(&*trx_effects.row_undo[1]), cancelled_row_undo);
        assert_eq!(trx_effects.index_undo.len(), 2);
        assert!(trx_effects.redo.is_empty());
        trx_effects.clear_for_rollback();
    }

    #[test]
    fn test_cancelled_undo_rollback_retains_current_entries() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_cancelled_undo_rollback").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let checkout = trx
                .checkout()
                .expect("test transaction should be available for checkout");
            let sts = checkout.inner().sts();
            let pool_guards = checkout.attachment().pool_guards().clone();
            let mut table_cache = TableCache::new(engine.inner().core.catalog());
            let table_id = TableID::new(99_999_998);
            let row_id = RowID::new(23);
            let mut effects = empty_stmt_effects();
            effects.push_user_delete_index_undo(
                table_id,
                row_id,
                user_key_from_active_slot(IndexSlot::new(0), vec![]),
                true,
            );
            effects.push_row_undo(OwnedRowUndo::new(
                NON_FOREGROUND_STMT_NO,
                table_id,
                None,
                row_id,
                RowUndoKind::Delete,
            ));

            pause_next_index_rollback();
            let mut index_rollback =
                Box::pin(effects.rollback_index(&mut table_cache, &pool_guards, sts));
            assert!(futures::poll!(index_rollback.as_mut()).is_pending());
            drop(index_rollback);
            assert_eq!(effects.index_undo.len(), 1);

            pause_next_row_rollback();
            let rollback_context =
                RowUndoRollbackContext::new(&pool_guards, &engine.inner().poisoner);
            let mut row_rollback =
                Box::pin(effects.rollback_row(&mut table_cache, rollback_context));
            assert!(futures::poll!(row_rollback.as_mut()).is_pending());
            drop(row_rollback);
            assert_eq!(effects.row_undo.len(), 1);

            drop(effects.take_for_fatal_retention());
            drop(checkout);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_catalog_native_impossible_domains_violate_invariant() {
        let table_id = TableID::new(42);

        let operation: OperationResult<()> = Err(Report::new(OperationError::InvalidDmlInput));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                assert_catalog_operation_invariant(table_id, operation)
            }))
            .is_err()
        );

        let lock: OperationOrFatalResult<()> =
            Err(Report::new(OperationError::LockFamilyConflict).into());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = narrow_catalog_operation_or_fatal(table_id, lock);
            }))
            .is_err()
        );

        let insert: OperationOrRuntimeResult<()> =
            Err(Report::new(OperationError::DuplicateKey).into());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = narrow_catalog_operation_or_runtime(table_id, insert, || {
                    "operation=test_catalog_insert".to_owned()
                });
            }))
            .is_err()
        );

        let delete_operation: QuadResult<()> =
            Err(Report::new(OperationError::WriteConflict).into());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = narrow_catalog_quad_result(table_id, delete_operation, || {
                    "operation=test_catalog_delete".to_owned()
                });
            }))
            .is_err()
        );

        let delete_lifecycle: QuadResult<()> = Err(Report::new(LifecycleError::Shutdown).into());
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = narrow_catalog_quad_result(table_id, delete_lifecycle, || {
                    "operation=test_catalog_delete".to_owned()
                });
            }))
            .is_err()
        );
    }

    #[test]
    fn test_catalog_native_runtime_errors_preserve_stack() {
        let table_id = TableID::new(42);
        let insert: OperationOrRuntimeResult<()> = Err(OperationOrRuntimeError::Runtime(
            Report::new(ResourceError::BufferPoolFull)
                .attach("pool_role=Meta")
                .change_context(RuntimeError::TableAccess),
        ));
        let err = narrow_catalog_operation_or_runtime(table_id, insert, || {
            "operation=test_catalog_insert".to_owned()
        })
        .unwrap_err();
        assert_catalog_runtime_stack(&err, "operation=test_catalog_insert");

        let delete: QuadResult<()> = Err(Report::new(ResourceError::BufferPoolFull)
            .attach("pool_role=Meta")
            .change_context(RuntimeError::TableAccess)
            .into());
        let err = narrow_catalog_quad_result(table_id, delete, || {
            "operation=test_catalog_delete".to_owned()
        })
        .unwrap_err();
        let RuntimeOrFatalError::Runtime(err) = err else {
            panic!("runtime catalog failure changed domain")
        };
        assert_catalog_runtime_stack(&err, "operation=test_catalog_delete");
    }

    #[test]
    fn test_catalog_native_fatal_errors_preserve_first_source() {
        let table_id = TableID::new(42);
        let lock: OperationOrFatalResult<()> = Err(OperationOrFatalError::Fatal(
            Report::new(FatalError::StorageIo).attach("first catalog mutation poison source"),
        ));
        let err = narrow_catalog_operation_or_fatal(table_id, lock).unwrap_err();
        assert_eq!(*err.current_context(), FatalError::StorageIo);
        assert!(format!("{err:?}").contains("first catalog mutation poison source"));

        let delete: QuadResult<()> = Err(Report::new(FatalError::StorageIo)
            .attach("first catalog mutation poison source")
            .into());
        let err = narrow_catalog_quad_result(table_id, delete, || {
            "operation=test_catalog_delete".to_owned()
        })
        .unwrap_err();
        let RuntimeOrFatalError::Fatal(err) = err else {
            panic!("fatal catalog failure changed domain")
        };
        assert_eq!(*err.current_context(), FatalError::StorageIo);
        assert!(format!("{err:?}").contains("first catalog mutation poison source"));
        assert!(format!("{err:?}").contains("operation=test_catalog_delete"));
    }

    #[test]
    fn test_private_statement_runtime_error_rolls_back_current_catalog_prefix() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_private_stmt_prefix_rollback").await;
            let storage = &engine.inner().core.catalog().storage;
            let table = storage.get_catalog_table(TABLE_ID_TABLES).unwrap();
            let session = engine.new_session().unwrap();
            let mut trx = begin_catalog_test_trx(&session);

            trx.trx()
                .catalog_insert_mvcc(
                    table.as_ref(),
                    vec![Val::from(TableID::new(42)), Val::from(0u16)],
                )
                .await
                .unwrap();
            let err = trx
                .trx()
                .exec(async move |stmt| {
                    catalog_insert_prefix_then_runtime_error(
                        stmt,
                        table.as_ref(),
                        vec![
                            vec![Val::from(TableID::new(43)), Val::from(0u16)],
                            vec![Val::from(TableID::new(44)), Val::from(0u16)],
                        ],
                    )
                    .await
                })
                .await
                .unwrap_err();
            let RuntimeOrFatalError::Runtime(err) = err else {
                panic!("private statement Runtime error changed domain")
            };
            assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);

            let table_ids = storage
                .tables()
                .list_uncommitted(trx.trx().pool_guards())
                .await
                .unwrap()
                .into_iter()
                .map(|table| table.table_id)
                .collect::<Vec<_>>();
            assert!(table_ids.contains(&TableID::new(42)));
            assert!(!table_ids.contains(&TableID::new(43)));
            assert!(!table_ids.contains(&TableID::new(44)));

            trx.rollback().await;
            engine.shutdown();
        });
    }

    #[test]
    fn test_catalog_delete_primary_key_mvcc_rejects_non_primary_key() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_catalog_delete_pk_mismatch").await;
            let catalog_table = engine
                .inner()
                .core
                .catalog()
                .storage
                .get_catalog_table(TABLE_ID_TABLES)
                .unwrap();
            let session = engine.new_session().unwrap();
            let mut trx = begin_catalog_test_trx(&session);
            let key = SelectKey::new(IndexSlot::new(1), vec![Val::from(TableID::new(42))]);

            let panic = AssertUnwindSafe(trx.trx().catalog_delete_primary_key_mvcc(
                catalog_table.as_ref(),
                key.index_slot,
                key.vals,
            ))
            .catch_unwind()
            .await
            .expect_err("non-primary catalog delete must violate the catalog invariant");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown panic");
            assert!(
                message.contains("catalog mutation invariant violated"),
                "unexpected panic: {message}"
            );
            trx.rollback().await;
            engine.shutdown();
        });
    }

    #[test]
    fn test_statement_index_rollback_failure_poisons_and_discards_transaction() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_index_rollback_fail").await;
            let (mut trx, _session_state) = test_trx(&engine, TrxID::new(52));
            let session_id = trx.operation_key.session_id();
            let trx_owner = trx_lock_owner(&mut trx).unwrap();
            acquire_transaction_lock_immediate(
                &mut trx,
                LockResource::TableData(TableID::new(91_250)),
                LockMode::IntentExclusive,
            )
            .unwrap();
            // Owned-runner raw-effect injection forces index-before-row
            // rollback and fatal residual retention.
            set_test_force_stmt_index_rollback_error(true);
            let res: Result<()> = trx
                .exec(async |mut stmt| {
                    acquire_transaction_lock(
                        &mut stmt,
                        LockResource::TableMetadata(TableID::new(91_250)),
                        LockMode::Shared,
                    )
                    .await?;
                    // This row undo references a table that does not exist. If
                    // statement rollback ever runs row rollback before index
                    // rollback, this test fails before the injected index
                    // rollback error can discard the statement safely.
                    let effects = statement_effects_mut(&mut stmt);
                    effects.push_row_undo(OwnedRowUndo::new(
                        effects.stmt_no(),
                        TableID::new(99_999_999),
                        None,
                        RowID::new(24),
                        RowUndoKind::Delete,
                    ));
                    effects.push_user_delete_index_undo(
                        TableID::new(12),
                        RowID::new(23),
                        user_key_from_active_slot(IndexSlot::new(0), vec![]),
                        true,
                    );
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            set_test_force_stmt_index_rollback_error(false);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RollbackAccess)
            );
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::IndexAccess)
            );
            assert!(sys_tests::retains_statement_row_undo(
                &engine.inner().trx_sys,
                TableID::new(99_999_999),
                RowID::new(24)
            ));
            assert_eq!(lock_entry_count(&engine, trx_owner), 0);
            assert!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RollbackAccess)
            );

            let err = trx.commit().await.unwrap_err();
            assert!(err.report().downcast_ref::<InternalError>().is_none());
            session_tests::remove_session_for_test(&engine.inner().session_registry, session_id);
        });
    }
}
