use crate::buffer::PoolGuards;
use crate::id::{RowID, TableID, TrxID};

use crate::catalog::{CatalogTable, TableCache};
use crate::error::{
    DiscloseResultExt, FatalError, FatalResult, MultiDomainResultExt, OperationError,
    OperationOrFatalResult, QuadError, QuadResult, Result, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::lock::{LockMode, LockResource};
use crate::log::redo::{RedoLogs, RowRedo};
use crate::obs;
use crate::row::ops::{
    DeleteMvcc, RowMutation, ScanMvcc, SelectKey, SelectMvcc, TableMutationOutcome, UpdateCol,
    UpdateMvcc, UpsertMvcc,
};
use crate::session::TrxAttachment;
use crate::table::{DmlValidator, LazyRow, Table, TableRuntimeLayout};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{
    FatalRollbackRetention, NON_FOREGROUND_STMT_NO, SessionOperationCheckout, StmtNo,
    TableAdmissionRequest, TrxEffects, TrxInner, TrxRuntime,
};
use crate::value::Val;
use error_stack::ResultExt;
use std::mem;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::admission::admit_user_table;

/// Catalog statement adapters preserve semantic Operation errors while adding
/// a catalog integration context only to Runtime failures.
///
/// `change_runtime_context` is a domain-preserving carrier primitive and owns
/// no operation identity. Its semantic callers chain `attach_with` immediately
/// after reclassification.
trait QuadResultExt<T>: MultiDomainResultExt {
    fn change_runtime_context(self, context: RuntimeError) -> Self;
}

impl<T> QuadResultExt<T> for QuadResult<T> {
    #[inline]
    fn change_runtime_context(self, context: RuntimeError) -> Self {
        self.map_err(|error| match error {
            QuadError::Operation(report) => QuadError::Operation(report),
            QuadError::Runtime(report) => QuadError::Runtime(report.change_context(context)),
            QuadError::Lifecycle(report) => QuadError::Lifecycle(report),
            QuadError::Fatal(report) => QuadError::Fatal(report),
        })
    }
}

/// Mutable effects accumulated by one statement before success or rollback.
///
/// These effects merge into transaction-level `TrxEffects` when the statement
/// succeeds. If the statement fails, index effects roll back before row effects
/// and redo is discarded.
pub(crate) struct StmtEffects {
    stmt_no: StmtNo,
    row_undo: RowUndoLogs,
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
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    /// Create an empty synthetic accumulator for unit tests.
    #[cfg(test)]
    #[inline]
    pub(crate) fn empty() -> Self {
        StmtEffects {
            stmt_no: NON_FOREGROUND_STMT_NO,
            row_undo: RowUndoLogs::empty(),
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
    pub(crate) fn push_insert_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.push_index_undo(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertUnique(key, merge_old_deleted),
        });
    }

    /// Push an inserted non-unique-index claim into statement rollback state.
    #[inline]
    pub(crate) fn push_insert_non_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.push_index_undo(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertNonUnique(key, merge_old_deleted),
        });
    }

    /// Push a deferred index delete into statement rollback and GC state.
    #[inline]
    pub(crate) fn push_delete_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        unique: bool,
    ) {
        self.push_index_undo(IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::DeferDelete(key, unique),
        });
    }

    /// Push a unique-index update into statement rollback state.
    #[inline]
    pub(crate) fn push_update_unique_index_undo(
        &mut self,
        table_id: TableID,
        old_row_id: RowID,
        new_row_id: RowID,
        key: SelectKey,
        old_deleted: bool,
    ) {
        self.push_index_undo(IndexUndo {
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

    #[inline]
    fn push_index_undo(&mut self, index_undo: IndexUndo) {
        self.index_undo.push(index_undo);
    }

    /// Moves successful statement effects into the active transaction effects.
    #[inline]
    pub(crate) fn merge_into_trx_effects(&mut self, trx_effects: &mut TrxEffects) {
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
        self.redo.clear();
        trx_effects.row_undo_mut().merge(&mut self.row_undo);
        trx_effects.index_undo_mut().merge(&mut self.index_undo);
    }

    /// Rolls back statement-local row effects in reverse effect order.
    #[inline]
    pub(crate) async fn rollback_row(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
    ) -> RuntimeResult<()> {
        self.row_undo.rollback(table_cache, pool_guards).await
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
/// across callback await points. It lends direct disjoint borrows to
/// [`Statement`] and owns the final policy when that callback future is dropped.
pub(crate) struct StmtState {
    effects: StmtEffects,
    drop_action: StmtDropAction,
    checkout: Option<SessionOperationCheckout>,
}

impl StmtState {
    /// Arms public statement cancellation after a successful checkout.
    #[inline]
    pub(crate) fn public(mut checkout: SessionOperationCheckout) -> Self {
        let stmt_no = checkout.inner_mut().next_stmt_no();
        Self {
            effects: StmtEffects::new(stmt_no),
            drop_action: StmtDropAction::CancelPublicTransaction,
            checkout: Some(checkout),
        }
    }

    /// Lends one direct callback-facing statement facade.
    #[inline]
    pub(crate) fn statement(&mut self) -> Statement<'_> {
        let Self {
            effects, checkout, ..
        } = self;
        let checkout = checkout
            .as_mut()
            .expect("active statement state must own its transaction checkout");
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        Statement {
            inner,
            attachment,
            effects,
            disable_dml_validation: false,
        }
    }

    /// Ordinarily checks the core back in.
    #[inline]
    pub(crate) fn return_ordinary(mut self) {
        self.drop_action = StmtDropAction::Settled;
        self.checkout = None;
    }

    /// Publishes fatal rollback retention after statement effects were retained.
    #[inline]
    pub(crate) fn discard_after_fatal_rollback(mut self) {
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

/// Statement-scoped facade for one operation inside an active transaction.
///
/// `Transaction::exec` owns the statement lifecycle. It passes this facade to the
/// callback with transaction context and statement-local effects. Logical
/// locks acquired by statement operations belong directly to the transaction.
/// The enclosing statement state settles effects on every completion or
/// cancellation path.
pub struct Statement<'stmt> {
    inner: &'stmt mut TrxInner,
    attachment: &'stmt TrxAttachment,
    effects: &'stmt mut StmtEffects,
    disable_dml_validation: bool,
}

impl<'stmt> Statement<'stmt> {
    /// Create a callback-facing statement over borrowed transaction ownership.
    #[inline]
    pub(crate) fn new(
        inner: &'stmt mut TrxInner,
        attachment: &'stmt TrxAttachment,
        effects: &'stmt mut StmtEffects,
    ) -> Self {
        Self {
            inner,
            attachment,
            effects,
            disable_dml_validation: false,
        }
    }

    /// Disable default DML shape, type, nullability, sparse-update, key, and
    /// index-scan validation for this statement.
    ///
    /// Validation is enabled by default. Disable it only when the caller has
    /// already validated full-row payload shape, value types, nullability,
    /// sparse-update ordering/range/type compatibility, and DML lookup keys
    /// including primary keys against the target table metadata for this
    /// statement.
    #[inline]
    pub fn disable_dml_validation(&mut self) -> &mut Self {
        self.disable_dml_validation = true;
        self
    }

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
        request: TableAdmissionRequest,
        operation: &'static str,
    ) -> OperationOrFatalResult<(Arc<Table>, Arc<TableRuntimeLayout>)> {
        admit_user_table(self.inner, self.attachment, table_id, request, operation).await
    }

    /// Scans the catalog-owned user table's row store by table id.
    ///
    /// The table runtime is resolved and strongly pinned only for this statement
    /// method. The public caller supplies the stable [`TableID`], not a table
    /// runtime handle.
    #[inline]
    pub async fn table_scan_mvcc<F>(
        &mut self,
        table_id: TableID,
        read_set: &[usize],
        row_action: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        const OPERATION: &str = "table_scan_mvcc";
        let (table, layout) = self
            .admit_user_table(table_id, TableAdmissionRequest::TableRead, OPERATION)
            .await
            .disclose()?;
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .table_scan_mvcc(rt, read_set, row_action)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
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
    pub async fn table_mutate_mvcc<F>(
        &mut self,
        table_id: TableID,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        const OPERATION: &str = "table_mutate_mvcc";
        let (table, layout) = self
            .admit_user_table(table_id, TableAdmissionRequest::TableWrite, OPERATION)
            .await
            .disclose()?;
        self.acquire_table_exclusive_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let validate_updates = !self.disable_dml_validation;
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
    /// driven by a unique index must preserve that index's encoded logical key.
    #[inline]
    pub async fn table_index_mutate_mvcc<'r, R, F>(
        &mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        R: RangeBounds<&'r [Val]>,
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        const OPERATION: &str = "table_index_mutate_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexWrite { index_no },
                OPERATION,
            )
            .await
            .disclose()?;
        if !self.disable_dml_validation {
            DmlValidator::new(layout.metadata())
                .validate_index_range(index_no, &range)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()?;
        let validate_updates = !self.disable_dml_validation;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .table_index_mutate_mvcc(rt, effects, index_no, range, validate_updates, mutate_row)
            .await
    }

    /// Looks up one unique-key row in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_lookup_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        const OPERATION: &str = "table_lookup_unique_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexRead { index_no },
                OPERATION,
            )
            .await
            .disclose()?;
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_unique_mvcc(rt, index_no, key_vals, user_read_set)
            .await
            .attach_with(|| {
                format!("operation={OPERATION}, table_id={table_id}, index_no={index_no}")
            })
            .disclose()
    }

    /// Looks up one secondary-index key in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_index_lookup_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        const OPERATION: &str = "table_index_lookup_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexRead { index_no },
                OPERATION,
            )
            .await
            .disclose()?;
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_mvcc(rt, index_no, key_vals, user_read_set)
            .await
            .attach_with(|| {
                format!("operation={OPERATION}, table_id={table_id}, index_no={index_no}")
            })
            .disclose()
    }

    /// Scans one secondary-index range in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_index_scan_mvcc<'r, R>(
        &mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<ScanMvcc>
    where
        R: RangeBounds<&'r [Val]>,
    {
        const OPERATION: &str = "table_index_scan_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexRead { index_no },
                OPERATION,
            )
            .await
            .disclose()?;
        if !self.disable_dml_validation {
            DmlValidator::new(layout.metadata())
                .validate_index_scan(index_no, &range, read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
        }
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_scan_mvcc(rt, index_no, range, read_set)
            .await
            .attach_with(|| {
                format!("operation={OPERATION}, table_id={table_id}, index_no={index_no}")
            })
            .disclose()
    }

    /// Inserts one row into a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_insert_mvcc(&mut self, table_id: TableID, cols: Vec<Val>) -> Result<RowID> {
        const OPERATION: &str = "table_insert_mvcc";
        let (table, layout) = self
            .admit_user_table(table_id, TableAdmissionRequest::TableWrite, OPERATION)
            .await
            .disclose()?;
        if !self.disable_dml_validation {
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

    /// Inserts or replaces one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_upsert_unique_mvcc(
        &mut self,
        table_id: TableID,
        unique_index_no: usize,
        cols: Vec<Val>,
    ) -> Result<UpsertMvcc> {
        const OPERATION: &str = "table_upsert_unique_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexWrite {
                    index_no: unique_index_no,
                },
                OPERATION,
            )
            .await
            .disclose()?;
        if !self.disable_dml_validation {
            let validator = DmlValidator::new(layout.metadata());
            validator
                .validate_full_row(&cols)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
                .disclose()?;
            validator
                .validate_unique_index(unique_index_no)
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
            .upsert_unique_mvcc(rt, effects, unique_index_no, cols, false)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Updates one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_update_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        const OPERATION: &str = "table_update_unique_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexWrite { index_no },
                OPERATION,
            )
            .await
            .disclose()?;
        if !self.disable_dml_validation {
            let validator = DmlValidator::new(layout.metadata());
            validator
                .validate_unique_key(index_no, key_vals)
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
            .update_unique_mvcc(rt, effects, index_no, key_vals, update, false)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Deletes one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_delete_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
    ) -> Result<DeleteMvcc> {
        const OPERATION: &str = "table_delete_unique_mvcc";
        let (table, layout) = self
            .admit_user_table(
                table_id,
                TableAdmissionRequest::IndexWrite { index_no },
                OPERATION,
            )
            .await
            .disclose()?;
        if !self.disable_dml_validation {
            DmlValidator::new(layout.metadata())
                .validate_unique_key(index_no, key_vals)
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
            .delete_unique_mvcc(rt, effects, index_no, key_vals)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
            .disclose()
    }

    /// Inserts one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_insert_mvcc(
        &mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> RuntimeOrFatalResult<RowID> {
        let table_id = table.table_id();
        let result = self.catalog_insert_mvcc_inner(table, cols).await;
        assert_catalog_mutation_invariant(table_id, result)
    }

    /// Performs the catalog insert before the caller asserts catalog-operation
    /// invariants and narrows the result to the Runtime domain.
    #[inline]
    async fn catalog_insert_mvcc_inner(
        &mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> QuadResult<RowID> {
        const OPERATION: &str = "catalog_insert_mvcc";
        let table_id = table.table_id();
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        if !self.disable_dml_validation {
            DmlValidator::new(table.metadata())
                .validate_full_row(&cols)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .insert_mvcc(rt, effects, cols)
            .await
            .map_err(QuadError::from)
            .change_runtime_context(RuntimeError::CatalogAccess)
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))
    }

    /// Deletes one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_delete_primary_key_mvcc(
        &mut self,
        table: &CatalogTable,
        index_no: usize,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> RuntimeOrFatalResult<DeleteMvcc> {
        let table_id = table.table_id();
        let result = self
            .catalog_delete_primary_key_mvcc_inner(table, index_no, key_vals, log_by_key)
            .await;
        assert_catalog_mutation_invariant(table_id, result)
    }

    /// Performs the catalog delete before the caller asserts catalog-operation
    /// invariants and narrows the result to the Runtime domain.
    #[inline]
    async fn catalog_delete_primary_key_mvcc_inner(
        &mut self,
        table: &CatalogTable,
        index_no: usize,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> QuadResult<DeleteMvcc> {
        const OPERATION: &str = "catalog_delete_primary_key_mvcc";
        let table_id = table.table_id();
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        if !self.disable_dml_validation {
            DmlValidator::new(table.metadata())
                .validate_primary_key(index_no, key_vals)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .delete_unique_mvcc(rt, effects, index_no, key_vals, log_by_key)
            .await
            .change_runtime_context(RuntimeError::CatalogAccess)
            .attach_with(|| {
                format!("operation={OPERATION}, table_id={table_id}, index_no={index_no}")
            })
    }

    /// Moves successful statement effects into transaction effects.
    #[inline]
    pub(crate) fn merge_effects(&mut self) {
        self.effects
            .merge_into_trx_effects(self.inner.effects_mut());
    }

    /// Rolls back statement-local effects after an ordinary callback error.
    ///
    /// Index effects roll back before row effects so index entries stop
    /// pointing at uncommitted row state before row undo is unwound. Statement
    /// locks stay held until this method returns and the carrier finalizes.
    #[inline]
    pub(crate) async fn rollback_effects(&mut self) -> FatalResult<()> {
        let sts = self.inner.sts();
        let engine = self.attachment.engine();
        let pool_guards = self.attachment.pool_guards();
        let mut table_cache = TableCache::new(engine.catalog());
        if let Err(err) = self
            .effects
            .rollback_index(&mut table_cache, pool_guards, sts)
            .await
        {
            let retention = self.effects.take_for_fatal_retention();
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
        if let Err(err) = self
            .effects
            .rollback_row(&mut table_cache, pool_guards)
            .await
        {
            let retention = self.effects.take_for_fatal_retention();
            engine.trx_sys.retain_fatal_rollback(retention);
            let report = err
                .change_context(FatalError::RollbackAccess)
                .attach("statement row rollback failed");
            obs::error!(
                "event=engine_poison component=trx action=poison result=error error={:?}",
                report
            );
            return Err(engine.poisoner.poison(report).into_report());
        }
        self.effects.clear_redo();
        Ok(())
    }
}

/// Catalog mutations use internally derived keys and validated row shapes.
/// An Operation failure therefore means a catalog key, row shape, transaction,
/// or lock invariant was violated; only Runtime and Fatal failures may leave
/// this boundary.
#[inline]
fn assert_catalog_mutation_invariant<T>(
    table_id: TableID,
    result: QuadResult<T>,
) -> RuntimeOrFatalResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(QuadError::Operation(report)) => {
            panic!("catalog mutation invariant violated: table_id={table_id}, error={report:?}")
        }
        Err(QuadError::Runtime(report)) => Err(RuntimeOrFatalError::Runtime(report)),
        Err(QuadError::Lifecycle(report)) => {
            panic!(
                "catalog mutation lifecycle invariant violated: table_id={table_id}, error={report:?}"
            )
        }
        Err(QuadError::Fatal(report)) => Err(RuntimeOrFatalError::Fatal(report)),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{
        DiscloseError, DiscloseResultExt, ErrorKind, FatalError, InternalError, LifecycleError,
        OperationError, ResourceError,
    };
    use crate::id::TrxID;
    use crate::lock::LockOwner;
    use crate::lock::tests::debug_snapshot;
    use crate::log::redo::RowRedoKind;
    use crate::session::{SessionState, tests as session_tests};
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
    pub(crate) fn transaction_lock_owner(stmt: &Statement<'_>) -> LockOwner {
        stmt.inner.checked_lock_state().owner()
    }

    #[inline]
    pub(crate) async fn acquire_transaction_lock(
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
    pub(crate) fn runtime_and_effects_mut<'borrow>(
        stmt: &'borrow mut Statement<'_>,
    ) -> (TrxRuntime<'borrow>, &'borrow mut StmtEffects) {
        stmt.runtime_and_effects_mut()
    }

    #[inline]
    pub(crate) fn statement_effects_mut<'borrow>(
        stmt: &'borrow mut Statement<'_>,
    ) -> &'borrow mut StmtEffects {
        stmt.effects
    }

    #[inline]
    pub(crate) fn statement_redo<'borrow>(stmt: &'borrow Statement<'_>) -> &'borrow RedoLogs {
        &stmt.effects.redo
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
                        .role(PoolRole::Mem)
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
        assert!(effects.index_undo.is_empty());
        assert!(effects.redo.is_empty());
    }

    #[test]
    fn test_stmt_effects_empty() {
        let effects = StmtEffects::empty();
        assert_stmt_effects_empty(&effects);
    }

    #[test]
    fn test_public_statements_consume_monotonic_statement_numbers() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("stmt_number_sequence").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let mut observed = Vec::new();

            trx.exec(async |stmt| {
                observed.push(statement_effects_mut(stmt).stmt_no());
                Ok(())
            })
            .await
            .unwrap();
            let failed: Result<()> = trx
                .exec(async |stmt| {
                    observed.push(statement_effects_mut(stmt).stmt_no());
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            assert!(failed.is_err());
            trx.exec(async |stmt| {
                observed.push(statement_effects_mut(stmt).stmt_no());
                Ok(())
            })
            .await
            .unwrap();
            assert_eq!(observed, vec![1, 2, 3]);
            trx.commit().await.unwrap();

            let mut next = session.begin_trx().unwrap();
            next.exec(async |stmt| {
                assert_eq!(statement_effects_mut(stmt).stmt_no(), 1);
                Ok(())
            })
            .await
            .unwrap();
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
        trx_effects.index_undo_mut().push(IndexUndo {
            table_id: TableID::new(41),
            row_id: RowID::new(1),
            kind: IndexUndoKind::DeferDelete(SelectKey::new(0, vec![]), true),
        });

        let mut effects = StmtEffects::empty();
        effects.push_row_undo(OwnedRowUndo::new(
            NON_FOREGROUND_STMT_NO,
            TableID::new(42),
            None,
            RowID::new(2),
            RowUndoKind::Insert,
        ));
        effects.push_delete_index_undo(
            TableID::new(42),
            RowID::new(2),
            SelectKey::new(0, vec![]),
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
            let mut effects = StmtEffects::empty();
            effects.push_delete_index_undo(table_id, row_id, SelectKey::new(0, vec![]), true);
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
            let mut row_rollback = Box::pin(effects.rollback_row(&mut table_cache, &pool_guards));
            assert!(futures::poll!(row_rollback.as_mut()).is_pending());
            drop(row_rollback);
            assert_eq!(effects.row_undo.len(), 1);

            drop(effects.take_for_fatal_retention());
            drop(checkout);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_catalog_mutation_operation_errors_violate_invariant() {
        for error in [OperationError::DuplicateKey, OperationError::WriteConflict] {
            let panic = catch_unwind(|| {
                let result: QuadResult<()> = Err(Report::new(error).into());
                let _ = assert_catalog_mutation_invariant(TableID::new(42), result);
            });
            assert!(panic.is_err(), "operation error did not assert: {error:?}");
        }
    }

    #[test]
    fn test_catalog_mutation_runtime_error_preserves_stack() {
        let result: QuadResult<()> = Err(Report::new(ResourceError::BufferPoolFull)
            .attach("pool_role=Meta")
            .change_context(RuntimeError::CatalogAccess)
            .into());

        let err = assert_catalog_mutation_invariant(TableID::new(42), result).unwrap_err();
        let RuntimeOrFatalError::Runtime(err) = err else {
            panic!("runtime catalog failure changed domain")
        };

        assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
        assert_eq!(
            err.downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        assert!(format!("{err:?}").contains("pool_role=Meta"));
    }

    #[test]
    fn test_catalog_mutation_fatal_error_preserves_first_source() {
        let result: QuadResult<()> = Err(Report::new(FatalError::StorageIo)
            .attach("first catalog mutation poison source")
            .into());

        let err = assert_catalog_mutation_invariant(TableID::new(42), result).unwrap_err();
        let RuntimeOrFatalError::Fatal(err) = err else {
            panic!("fatal catalog failure changed domain")
        };

        assert_eq!(*err.current_context(), FatalError::StorageIo);
        assert!(format!("{err:?}").contains("first catalog mutation poison source"));
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
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let panic = AssertUnwindSafe(trx.exec(async |stmt| {
                let key = SelectKey::new(1, vec![Val::from(TableID::new(42))]);
                stmt.catalog_delete_primary_key_mvcc(
                    catalog_table.as_ref(),
                    key.index_no,
                    &key.vals,
                    true,
                )
                .await
                .disclose()?;
                Ok(())
            }))
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
            let err = trx.rollback().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<LifecycleError>().copied(),
                Some(LifecycleError::TransactionDiscarded)
            );
            session_tests::wait_for_session_idle(&engine.inner().session_registry, session.id())
                .await;
            engine.shutdown();
        });
    }

    #[test]
    fn test_table_scan_mvcc_missing_table_preserves_typed_context() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_table_scan_missing_context").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let table_id = TableID::new(91_225);

            let err = trx
                .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap_err();

            assert_eq!(err.kind(), ErrorKind::Operation);
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::TableNotFound)
            );
            let rendered = format!("{err:?}");
            assert_eq!(rendered.matches("operation=table_scan_mvcc").count(), 1);
            assert_eq!(rendered.matches(&format!("table_id={table_id}")).count(), 1);
            trx.rollback().await.unwrap();
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
            let res: Result<()> = trx
                .exec(async |stmt| {
                    assert_eq!(transaction_lock_owner(stmt), trx_owner);
                    acquire_transaction_lock(
                        stmt,
                        LockResource::TableMetadata(TableID::new(91_250)),
                        LockMode::Shared,
                    )
                    .await?;
                    // This row undo references a table that does not exist. If
                    // statement rollback ever runs row rollback before index
                    // rollback, this test fails before the injected index
                    // rollback error can discard the statement safely.
                    let effects = statement_effects_mut(stmt);
                    effects.push_row_undo(OwnedRowUndo::new(
                        effects.stmt_no(),
                        TableID::new(99_999_999),
                        None,
                        RowID::new(24),
                        RowUndoKind::Delete,
                    ));
                    effects.push_delete_index_undo(
                        TableID::new(12),
                        RowID::new(23),
                        SelectKey::new(0, vec![]),
                        true,
                    );
                    set_test_force_stmt_index_rollback_error(true);
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
