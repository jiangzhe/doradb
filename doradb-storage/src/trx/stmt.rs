use crate::buffer::PoolGuards;
use crate::id::{RowID, TableID, TrxID};

use crate::catalog::{CatalogTable, TableCache};
use crate::error::{
    DiscloseResultExt, FatalError, FatalResult, MultiDomainResultExt, OperationError,
    OperationOrFatalResult, OperationOrRuntimeError, OperationOrRuntimeResult, OperationResult,
    Result, RuntimeError, RuntimeResult,
};
use crate::lock::{LockMode, LockResource, LockScopeState};
use crate::log::redo::{DDLRedo, RedoLogs, RowRedo};
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
    FatalRollbackRetention, PreparedCatalogWriteAuthority, SessionOperationCheckout,
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
trait OperationOrRuntimeResultExt<T>: MultiDomainResultExt {
    fn change_runtime_context(self, context: RuntimeError) -> Self;
}

impl<T> OperationOrRuntimeResultExt<T> for OperationOrRuntimeResult<T> {
    #[inline]
    fn change_runtime_context(self, context: RuntimeError) -> Self {
        self.map_err(|error| match error {
            OperationOrRuntimeError::Operation(report) => {
                OperationOrRuntimeError::Operation(report)
            }
            OperationOrRuntimeError::Runtime(report) => {
                OperationOrRuntimeError::Runtime(report.change_context(context))
            }
        })
    }
}

/// Mutable effects accumulated by one statement before success or rollback.
///
/// These effects merge into transaction-level `TrxEffects` when the statement
/// succeeds. If the statement fails, index effects roll back before row effects
/// and redo is discarded.
pub(crate) struct StmtEffects {
    row_undo: RowUndoLogs,
    index_undo: IndexUndoLogs,
    redo: RedoLogs,
}

impl StmtEffects {
    /// Create an empty statement effect accumulator.
    #[inline]
    pub(crate) fn empty() -> Self {
        StmtEffects {
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    /// Returns whether this accumulator has no statement-local effects.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.row_undo.is_empty() && self.index_undo.is_empty() && self.redo.is_empty()
    }

    /// Push one row undo entry into this statement.
    #[inline]
    pub(crate) fn push_row_undo(&mut self, undo: OwnedRowUndo) {
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

    /// Borrow statement-local redo for producer assertions.
    #[cfg(test)]
    #[inline]
    pub(crate) fn redo_for_test(&self) -> &RedoLogs {
        &self.redo
    }

    /// Replace the statement's deferred DDL redo payload.
    #[inline]
    pub(crate) fn set_ddl_redo(&mut self, ddl: DDLRedo) -> Option<Box<DDLRedo>> {
        self.redo.ddl.replace(Box::new(ddl))
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

    /// Folds residual cancelled-statement undo into whole-transaction rollback.
    ///
    /// Redo from a statement that did not complete is never commit-visible.
    /// Undo remains ordered after prior successful statements so whole-
    /// transaction rollback unwinds this statement first.
    #[inline]
    fn fold_cancelled_into_trx_effects(&mut self, trx_effects: &mut TrxEffects) {
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
    PrivateMustComplete,
    Settled,
}

/// Lifetime-free owner of one checked-out statement operation.
///
/// The carrier keeps the transaction core, statement effects, and statement
/// locks together across callback await points. It lends direct disjoint
/// borrows to [`Statement`] and owns the final policy when that callback future
/// is dropped.
pub(crate) struct StmtState {
    effects: StmtEffects,
    curr_scope: Option<LockScopeState>,
    drop_action: StmtDropAction,
    checkout: Option<SessionOperationCheckout>,
}

impl StmtState {
    /// Arms public statement cancellation after a successful checkout.
    #[inline]
    pub(crate) fn public(mut checkout: SessionOperationCheckout) -> Self {
        let owner = checkout.inner_mut().next_statement_owner();
        Self {
            effects: StmtEffects::empty(),
            curr_scope: Some(LockScopeState::new(owner)),
            drop_action: StmtDropAction::CancelPublicTransaction,
            checkout: Some(checkout),
        }
    }

    /// Preserves the current must-complete invariant for private catalog work.
    #[inline]
    pub(crate) fn private(mut checkout: SessionOperationCheckout) -> Self {
        let owner = checkout.inner_mut().next_statement_owner();
        Self {
            effects: StmtEffects::empty(),
            curr_scope: Some(LockScopeState::new(owner)),
            drop_action: StmtDropAction::PrivateMustComplete,
            checkout: Some(checkout),
        }
    }

    /// Lends one direct callback-facing statement facade.
    #[inline]
    pub(crate) fn statement(&mut self) -> Statement<'_> {
        self.statement_with_authority(None)
    }

    /// Lends a callback facade backed by prepared catalog-write authority.
    #[inline]
    pub(crate) fn prepared_catalog_statement<'a>(
        &'a mut self,
        authority: PreparedCatalogWriteAuthority<'a>,
    ) -> Statement<'a> {
        self.statement_with_authority(Some(authority))
    }

    #[inline]
    fn statement_with_authority<'a>(
        &'a mut self,
        prepared_catalog_write: Option<PreparedCatalogWriteAuthority<'a>>,
    ) -> Statement<'a> {
        let Self {
            effects,
            curr_scope,
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
            curr_scope: curr_scope
                .as_mut()
                .expect("active statement state must retain curr_scope"),
            disable_dml_validation: false,
            prepared_catalog_write,
        }
    }

    /// Releases statement locks and ordinarily checks the core back in.
    #[inline]
    pub(crate) fn return_ordinary(mut self) {
        self.drop_action = StmtDropAction::Settled;
        self.release_statement_locks();
        self.checkout = None;
    }

    /// Publishes fatal rollback retention after statement effects were retained.
    #[inline]
    pub(crate) fn discard_after_fatal_rollback(mut self) {
        self.drop_action = StmtDropAction::Settled;
        self.release_statement_locks();
        if let Some(checkout) = self.checkout.as_mut() {
            checkout.discard_after_fatal_rollback();
        }
        self.checkout = None;
    }

    /// Preserve partial statement undo in the mandatory nested transaction.
    ///
    /// This path runs before resuming a prepared catalog callback panic. Redo
    /// from the incomplete statement is discarded, while row/index undo is
    /// checked back into the stable transaction core for outer fatal retention.
    #[inline]
    pub(crate) fn return_after_mandatory_panic(mut self) {
        self.drop_action = StmtDropAction::Settled;
        if let Some(checkout) = self.checkout.as_mut() {
            self.effects
                .fold_cancelled_into_trx_effects(checkout.inner_mut().effects_mut());
        }
        self.release_statement_locks();
        self.checkout = None;
    }

    #[inline]
    fn release_statement_locks(&mut self) {
        if let (Some(mut curr_scope), Some(checkout)) =
            (self.curr_scope.take(), self.checkout.as_mut())
        {
            let lock_manager = checkout.attachment().engine().lock_manager().clone();
            let family = checkout.inner_mut().checked_lock_state_mut().family_mut();
            family.close_scope(&mut curr_scope, &lock_manager);
        }
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
                if let Some(mut curr_scope) = self.curr_scope.take() {
                    let lock_manager = checkout.attachment().engine().lock_manager().clone();
                    let family = checkout.inner_mut().checked_lock_state_mut().family_mut();
                    family.close_scope(&mut curr_scope, &lock_manager);
                }
                checkout.return_cancelled();
            }
            StmtDropAction::PrivateMustComplete => {
                self.release_statement_locks();
                assert!(
                    self.effects.is_empty(),
                    "private statement must complete effect settlement before drop"
                );
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
/// callback with transaction context, statement-local effects, and
/// statement-owned logical locks. The enclosing statement state retains
/// ownership and settles those resources on every completion or cancellation
/// path.
pub struct Statement<'stmt> {
    inner: &'stmt mut TrxInner,
    attachment: &'stmt TrxAttachment,
    effects: &'stmt mut StmtEffects,
    curr_scope: &'stmt mut LockScopeState,
    disable_dml_validation: bool,
    prepared_catalog_write: Option<PreparedCatalogWriteAuthority<'stmt>>,
}

impl<'stmt> Statement<'stmt> {
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
        match self.prepared_catalog_write {
            Some(authority) => {
                TrxRuntime::new_prepared_catalog(self.inner.ctx(), self.attachment, authority)
            }
            None => TrxRuntime::new(self.inner.ctx(), self.attachment),
        }
    }

    /// Returns mutable access to this statement's effect accumulator.
    #[inline]
    pub(crate) fn effects_mut(&mut self) -> &mut StmtEffects {
        self.effects
    }

    #[inline]
    fn runtime_and_effects_mut(&mut self) -> (TrxRuntime<'_>, &mut StmtEffects) {
        let runtime = match self.prepared_catalog_write {
            Some(authority) => {
                TrxRuntime::new_prepared_catalog(self.inner.ctx(), self.attachment, authority)
            }
            None => TrxRuntime::new(self.inner.ctx(), self.attachment),
        };
        (runtime, self.effects)
    }

    /// Acquires transaction-lifetime metadata protection for a table write.
    #[inline]
    pub(crate) async fn acquire_table_write_metadata_lock(
        &mut self,
        table_id: TableID,
    ) -> OperationResult<()> {
        let lock_manager = self.attachment.engine().lock_manager();
        self.inner
            .checked_lock_state_mut()
            .acquire(
                lock_manager,
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
    ) -> OperationResult<()> {
        let lock_manager = self.attachment.engine().lock_manager();
        self.inner
            .checked_lock_state_mut()
            .acquire(
                lock_manager,
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
    ) -> OperationResult<()> {
        let lock_manager = self.attachment.engine().lock_manager();
        self.inner
            .checked_lock_state_mut()
            .acquire(
                lock_manager,
                LockResource::TableData(table_id),
                LockMode::Exclusive,
            )
            .await
            .map(|_| ())
    }

    #[inline]
    async fn admit_user_table(
        &mut self,
        table_id: TableID,
        request: TableAdmissionRequest,
        operation: &'static str,
    ) -> OperationOrFatalResult<(Arc<Table>, Arc<TableRuntimeLayout>)> {
        admit_user_table(
            self.inner,
            self.attachment,
            self.curr_scope,
            table_id,
            request,
            operation,
        )
        .await
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
    }

    /// Inserts one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_insert_mvcc(
        &mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> RuntimeResult<RowID> {
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
    ) -> OperationOrRuntimeResult<RowID> {
        const OPERATION: &str = "catalog_insert_mvcc";
        let table_id = table.table_id();
        if let Some(authority) = self.prepared_catalog_write {
            authority.assert_table_write(table_id);
        } else {
            self.acquire_table_write_metadata_lock(table_id)
                .await
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        if !self.disable_dml_validation {
            DmlValidator::new(table.metadata())
                .validate_full_row(&cols)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        if self.prepared_catalog_write.is_none() {
            self.acquire_table_write_data_lock(table_id)
                .await
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .insert_mvcc(rt, effects, cols)
            .await
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
    ) -> RuntimeResult<DeleteMvcc> {
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
    ) -> OperationOrRuntimeResult<DeleteMvcc> {
        const OPERATION: &str = "catalog_delete_primary_key_mvcc";
        let table_id = table.table_id();
        if let Some(authority) = self.prepared_catalog_write {
            authority.assert_table_write(table_id);
        } else {
            self.acquire_table_write_metadata_lock(table_id)
                .await
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        if !self.disable_dml_validation {
            DmlValidator::new(table.metadata())
                .validate_primary_key(index_no, key_vals)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
        if self.prepared_catalog_write.is_none() {
            self.acquire_table_write_data_lock(table_id)
                .await
                .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        }
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
/// or lock invariant was violated; only Runtime failures may leave this boundary.
#[inline]
fn assert_catalog_mutation_invariant<T>(
    table_id: TableID,
    result: OperationOrRuntimeResult<T>,
) -> RuntimeResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(OperationOrRuntimeError::Operation(report)) => {
            panic!("catalog mutation invariant violated: table_id={table_id}, error={report:?}")
        }
        Err(OperationOrRuntimeError::Runtime(report)) => Err(report),
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
    pub(crate) fn lock_owner(stmt: &Statement<'_>) -> LockOwner {
        stmt.curr_scope.owner()
    }

    #[inline]
    pub(crate) async fn acquire_statement_lock(
        stmt: &mut Statement<'_>,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        let lock_manager = stmt.attachment.engine().lock_manager();
        let family = stmt.inner.checked_lock_state_mut().family_mut();
        family
            .acquire(stmt.curr_scope, lock_manager, resource, mode)
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
        let lock_manager = attachment.engine().lock_manager();
        inner
            .checked_lock_state_mut()
            .acquire(lock_manager, resource, mode)
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
            .filter(|entry| entry.owner == owner)
            .count()
    }

    #[test]
    fn test_stmt_effects_empty() {
        let effects = StmtEffects::empty();
        assert!(effects.is_empty());
        assert!(effects.row_undo.is_empty());
        assert!(effects.index_undo.is_empty());
        assert!(effects.redo.is_empty());
    }

    #[test]
    fn test_cancelled_stmt_effects_fold_undo_and_discard_redo() {
        let mut trx_effects = TrxEffects::empty();
        trx_effects.row_undo_mut().push(OwnedRowUndo::new(
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
        effects.set_ddl_redo(DDLRedo::CreateTable(TableID::new(42)));
        let cancelled_row_undo = from_ref(&*effects.row_undo[0]);

        effects.fold_cancelled_into_trx_effects(&mut trx_effects);

        assert!(effects.is_empty());
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
                let result: OperationOrRuntimeResult<()> = Err(Report::new(error).into());
                let _ = assert_catalog_mutation_invariant(TableID::new(42), result);
            });
            assert!(panic.is_err(), "operation error did not assert: {error:?}");
        }
    }

    #[test]
    fn test_catalog_mutation_runtime_error_preserves_stack() {
        let result: OperationOrRuntimeResult<()> = Err(Report::new(ResourceError::BufferPoolFull)
            .attach("pool_role=Meta")
            .change_context(RuntimeError::CatalogAccess)
            .into());

        let err = assert_catalog_mutation_invariant(TableID::new(42), result).unwrap_err();

        assert_eq!(*err.current_context(), RuntimeError::CatalogAccess);
        assert_eq!(
            err.downcast_ref::<ResourceError>().copied(),
            Some(ResourceError::BufferPoolFull)
        );
        assert!(format!("{err:?}").contains("pool_role=Meta"));
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
            let stmt_owner = Cell::new(None);

            let res: Result<()> = trx
                .exec(async |stmt| {
                    stmt_owner.set(Some(lock_owner(stmt)));
                    acquire_statement_lock(
                        stmt,
                        LockResource::TableMetadata(TableID::new(91_250)),
                        LockMode::Shared,
                    )
                    .await?;
                    // This row undo references a table that does not exist. If
                    // statement rollback ever runs row rollback before index
                    // rollback, this test fails before the injected index
                    // rollback error can discard the statement safely.
                    stmt.effects_mut().push_row_undo(OwnedRowUndo::new(
                        TableID::new(99_999_999),
                        None,
                        RowID::new(24),
                        RowUndoKind::Delete,
                    ));
                    stmt.effects_mut().push_delete_index_undo(
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
            assert_eq!(lock_entry_count(&engine, stmt_owner.get().unwrap()), 0);
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
