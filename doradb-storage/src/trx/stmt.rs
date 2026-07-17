use crate::buffer::PoolGuards;
use crate::id::{RowID, TableID, TrxID};

use crate::catalog::{CatalogTable, TableCache, is_catalog_table};
use crate::error::{FatalError, LifecycleResult, OperationError, OperationResult, Result};
use crate::lock::{LockMode, LockOwner, LockResource, OwnerLockState};
use crate::log::redo::{DDLRedo, RedoLogs, RowRedo};
use crate::row::ops::{
    DeleteMvcc, ScanMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc, UpsertMvcc,
};
use crate::session::TrxAttachment;
use crate::table::{DmlValidationResultExt, DmlValidator, LazyRow, Table};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{FatalRollbackRetention, TrxEffects, TrxInner, TrxRuntime};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::mem;
use std::ops::RangeBounds;
use std::sync::Arc;

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

    /// Rolls back statement-local row effects in reverse effect order.
    #[inline]
    pub(crate) async fn rollback_row(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
    ) -> Result<()> {
        self.row_undo.rollback(table_cache, pool_guards).await
    }

    /// Rolls back statement-local secondary-index effects in reverse effect order.
    #[inline]
    pub(crate) async fn rollback_index(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
        sts: TrxID,
    ) -> Result<()> {
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

impl Drop for StmtEffects {
    #[inline]
    fn drop(&mut self) {
        assert!(
            self.is_empty(),
            "statement effects should be merged, rolled back, or discarded before drop"
        );
    }
}

/// Statement-scoped facade for one operation inside an active transaction.
///
/// `Transaction::exec` owns the statement lifecycle. It passes this facade to the
/// callback with transaction context, statement-local effects, and
/// statement-owned logical locks. Dropping this value releases every
/// statement-owned lock, so success and rollback paths cannot forget cleanup.
pub struct Statement<'stmt> {
    inner: &'stmt mut TrxInner,
    attachment: &'stmt TrxAttachment,
    effects: StmtEffects,
    stmt_locks: OwnerLockState,
    disable_dml_validation: bool,
}

impl<'stmt> Statement<'stmt> {
    /// Create a new statement.
    #[inline]
    pub(crate) fn new(
        inner: &'stmt mut TrxInner,
        attachment: &'stmt TrxAttachment,
        owner: LockOwner,
    ) -> LifecycleResult<Self> {
        let owner_group = inner.checked_lock_state()?.owner_group();
        Ok(Statement {
            inner,
            attachment,
            effects: StmtEffects::empty(),
            stmt_locks: match owner_group {
                Some(owner_group) => OwnerLockState::new_grouped(owner, owner_group),
                None => OwnerLockState::new(owner),
            },
            disable_dml_validation: false,
        })
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
        TrxRuntime::new(self.inner.ctx(), self.attachment)
    }

    /// Returns mutable access to this statement's effect accumulator.
    #[inline]
    pub(crate) fn effects_mut(&mut self) -> &mut StmtEffects {
        &mut self.effects
    }

    #[inline]
    fn runtime_and_effects_mut(&mut self) -> (TrxRuntime<'_>, &mut StmtEffects) {
        (
            TrxRuntime::new(self.inner.ctx(), self.attachment),
            &mut self.effects,
        )
    }

    /// Acquires a statement-owned logical lock.
    #[inline]
    async fn acquire_statement_lock(
        &mut self,
        resource: LockResource,
        mode: LockMode,
    ) -> OperationResult<()> {
        self.stmt_locks
            .acquire(self.attachment.engine().lock_manager(), resource, mode)
            .await
    }

    /// Acquires statement-lifetime metadata protection for a table read.
    #[inline]
    pub(crate) async fn acquire_table_read_lock(
        &mut self,
        table_id: TableID,
    ) -> OperationResult<()> {
        self.acquire_statement_lock(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await
    }

    /// Acquires transaction-lifetime metadata protection for a table write.
    ///
    /// An unavailable transaction lock state crosses from Lifecycle into the
    /// lock operation here. `change_context` retains the prerequisite frame.
    #[inline]
    pub(crate) async fn acquire_table_write_metadata_lock(
        &mut self,
        table_id: TableID,
    ) -> OperationResult<()> {
        let lock_manager = self.attachment.engine().lock_manager();
        self.inner
            .checked_lock_state_mut()
            .change_context(OperationError::LockUnavailable)
            .attach_with(|| "phase=resolve_transaction_lock_state")?
            .acquire(
                lock_manager,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            )
            .await
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
            .change_context(OperationError::LockUnavailable)
            .attach_with(|| "phase=resolve_transaction_lock_state")?
            .acquire(
                lock_manager,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
            )
            .await
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
            .change_context(OperationError::LockUnavailable)
            .attach_with(|| "phase=resolve_transaction_lock_state")?
            .acquire(
                lock_manager,
                LockResource::TableData(table_id),
                LockMode::Exclusive,
            )
            .await
    }

    #[inline]
    fn resolve_user_table(&mut self, table_id: TableID) -> OperationResult<Arc<Table>> {
        if !is_catalog_table(table_id) {
            if let Some(table) = self.inner.cached_user_table(table_id) {
                return Ok(table);
            }
            if let Some(table) = self.attachment.cached_user_table(table_id) {
                self.inner.cache_user_table(&table);
                return Ok(table);
            }
            let engine = self.attachment.engine();
            if let Some(table) = engine.catalog().get_table_now(table_id) {
                self.attachment.cache_user_table(&table);
                self.inner.cache_user_table(&table);
                return Ok(table);
            }
        }
        Err(Report::new(OperationError::TableNotFound).attach(format!("table_id={table_id}")))
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
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_read_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .table_scan_mvcc(rt, read_set, row_action)
            .await
            .map_err(|err| err.attach(format!("operation={OPERATION}, table_id={table_id}")))
    }

    /// Sequentially update callback-selected rows using latest modification reads.
    ///
    /// A cold persisted image is exposed only while it remains the current
    /// logical row. Hot-row values come from the latest physical page image
    /// rather than an older version reconstructed for the transaction snapshot.
    /// Another active row owner causes a write conflict; this transaction's own
    /// active state is followed to its latest hot image. Returning `None` skips
    /// the row, while `Some(update)` selects it and applies the sparse update. An
    /// empty update still counts as a selected row without creating physical
    /// row, index, undo, or redo work.
    #[inline]
    pub async fn table_update_mvcc<F>(&mut self, table_id: TableID, update_row: F) -> Result<usize>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<Option<Vec<UpdateCol>>>,
    {
        const OPERATION: &str = "table_update_mvcc";
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_exclusive_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        let validate_updates = !self.disable_dml_validation;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .table_update_mvcc(rt, effects, validate_updates, update_row)
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
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_read_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_unique_mvcc(rt, index_no, key_vals, user_read_set)
            .await
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
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_read_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_mvcc(rt, index_no, key_vals, user_read_set)
            .await
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
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_read_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        if !self.disable_dml_validation {
            DmlValidator::new(layout.metadata())
                .validate_index_scan(index_no, &range, read_set)
                .with_foreground_context(OPERATION, table_id)?;
        }
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_scan_mvcc(rt, index_no, range, read_set)
            .await
    }

    /// Inserts one row into a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_insert_mvcc(&mut self, table_id: TableID, cols: Vec<Val>) -> Result<RowID> {
        const OPERATION: &str = "table_insert_mvcc";
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        if !self.disable_dml_validation {
            DmlValidator::new(layout.metadata())
                .validate_full_row(&cols)
                .with_foreground_context(OPERATION, table_id)?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
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
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        if !self.disable_dml_validation {
            let validator = DmlValidator::new(layout.metadata());
            validator
                .validate_full_row(&cols)
                .with_foreground_context(OPERATION, table_id)?;
            validator
                .validate_unique_index(unique_index_no)
                .with_foreground_context(OPERATION, table_id)?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
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
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        if !self.disable_dml_validation {
            let validator = DmlValidator::new(layout.metadata());
            validator
                .validate_unique_key(index_no, key_vals)
                .with_foreground_context(OPERATION, table_id)?;
            validator
                .validate_sparse_update(&update)
                .with_foreground_context(OPERATION, table_id)?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
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
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        const OPERATION: &str = "table_delete_unique_mvcc";
        let table = self
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={OPERATION}"))?;
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={OPERATION}"))?;
        let layout = table.layout_snapshot();
        if !self.disable_dml_validation {
            DmlValidator::new(layout.metadata())
                .validate_unique_key(index_no, key_vals)
                .with_foreground_context(OPERATION, table_id)?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .delete_unique_mvcc(rt, effects, index_no, key_vals, log_by_key)
            .await
    }

    /// Inserts one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_insert_mvcc(
        &mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        const OPERATION: &str = "catalog_insert_mvcc";
        let table_id = table.table_id();
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        if !self.disable_dml_validation {
            DmlValidator::new(table.metadata())
                .validate_full_row(&cols)
                .with_foreground_context(OPERATION, table_id)?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table.insert_mvcc(rt, effects, cols).await
    }

    /// Deletes one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_delete_primary_key_mvcc(
        &mut self,
        table: &CatalogTable,
        index_no: usize,
        key_vals: &[Val],
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        const OPERATION: &str = "catalog_delete_primary_key_mvcc";
        let table_id = table.table_id();
        self.acquire_table_write_metadata_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        if !self.disable_dml_validation {
            DmlValidator::new(table.metadata())
                .validate_primary_key(index_no, key_vals)
                .with_foreground_context(OPERATION, table_id)?;
        }
        self.acquire_table_write_data_lock(table_id)
            .await
            .attach_with(|| format!("operation={OPERATION}, table_id={table_id}"))?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .delete_unique_mvcc(rt, effects, index_no, key_vals, log_by_key)
            .await
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
    /// locks stay held until this method returns and `Statement` drops.
    #[inline]
    pub(crate) async fn rollback_effects(&mut self) -> Result<()> {
        let sts = self.inner.sts();
        let engine = self.attachment.engine().clone();
        let pool_guards = self.attachment.pool_guards();
        let mut table_cache = TableCache::new(engine.catalog());
        if self
            .effects
            .rollback_index(&mut table_cache, pool_guards, sts)
            .await
            .is_err()
        {
            let retention = self.effects.take_for_fatal_retention();
            engine.trx_sys.retain_fatal_rollback(retention);
            return Err(engine
                .trx_sys
                .poison_engine(FatalError::RollbackAccess)
                .into());
        }
        if self
            .effects
            .rollback_row(&mut table_cache, pool_guards)
            .await
            .is_err()
        {
            let retention = self.effects.take_for_fatal_retention();
            engine.trx_sys.retain_fatal_rollback(retention);
            return Err(engine
                .trx_sys
                .poison_engine(FatalError::RollbackAccess)
                .into());
        }
        self.effects.clear_redo();
        Ok(())
    }
}

impl Drop for Statement<'_> {
    #[inline]
    fn drop(&mut self) {
        self.stmt_locks
            .release_all(self.attachment.engine().lock_manager());
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::catalog::storage::tables::TABLE_ID_TABLES;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{ErrorKind, FatalError, InternalError, LifecycleError, OperationError};
    use crate::id::TrxID;
    use crate::lock::LockManager;
    use crate::lock::tests::{debug_snapshot, try_acquire, try_acquire_grouped};
    use crate::session::{SessionState, tests as session_tests};
    use crate::trx::sys::tests as sys_tests;
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
    use crate::trx::{MIN_ACTIVE_TRX_ID, Transaction};
    use error_stack::Report;
    use std::cell::Cell;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use tempfile::TempDir;

    thread_local! {
        static TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn set_test_force_stmt_index_rollback_error(enabled: bool) {
        TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR.with(|flag| flag.set(enabled));
    }

    pub(super) fn maybe_force_stmt_index_rollback_error() -> Result<()> {
        if TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR.with(|flag| flag.get()) {
            // TODO(error-boundary): backlog 000160 should replace this generic
            // hook with a statement rollback source-domain failure.
            return Err(Report::new(InternalError::Generic)
                .attach("test statement index rollback failure")
                .into());
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn lock_owner(stmt: &Statement<'_>) -> LockOwner {
        stmt.stmt_locks.owner()
    }

    #[inline]
    pub(crate) fn try_acquire_statement_lock(
        stmt: &mut Statement<'_>,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        try_acquire_owner_lock_state(
            &mut stmt.stmt_locks,
            stmt.attachment.engine().lock_manager(),
            resource,
            mode,
        )
    }

    #[inline]
    pub(crate) async fn acquire_statement_lock(
        stmt: &mut Statement<'_>,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        Ok(stmt
            .stmt_locks
            .acquire(stmt.attachment.engine().lock_manager(), resource, mode)
            .await?)
    }

    #[inline]
    pub(crate) fn runtime_and_effects_mut<'borrow>(
        stmt: &'borrow mut Statement<'_>,
    ) -> (TrxRuntime<'borrow>, &'borrow mut StmtEffects) {
        stmt.runtime_and_effects_mut()
    }

    #[inline]
    fn trx_lock_owner(trx: &mut Transaction) -> Result<LockOwner> {
        let checkout = trx.checkout()?;
        Ok(checkout
            .inner()
            .checked_lock_state()
            .attach("operation=read_transaction_lock_owner")?
            .owner())
    }

    #[inline]
    fn try_acquire_transaction_lock(
        trx: &mut Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        let mut checkout = trx.checkout()?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let lock_manager = attachment.engine().lock_manager();
        try_acquire_owner_lock_state(
            inner
                .checked_lock_state_mut()
                .change_context(OperationError::LockUnavailable)
                .attach_with(|| {
                    "operation=try_acquire_transaction_lock, phase=resolve_transaction_lock_state"
                })?,
            lock_manager,
            resource,
            mode,
        )
    }

    #[inline]
    fn try_acquire_owner_lock_state(
        lock_state: &mut OwnerLockState,
        lock_manager: &LockManager,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        if lock_state.cached_covers(resource, mode)? {
            return Ok(true);
        }
        let owner = lock_state.owner();
        let acquired = match lock_state.owner_group() {
            Some(owner_group) => {
                try_acquire_grouped(lock_manager, resource, mode, owner, owner_group)?
            }
            None => try_acquire(lock_manager, resource, mode, owner)?,
        };
        if acquired {
            lock_state.cache_granted(resource, mode);
        }
        Ok(acquired)
    }

    async fn test_engine(log_file_stem: &str) -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let engine = EngineConfig::default()
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
            )
            .build()
            .await
            .unwrap();
        (temp_dir, engine)
    }

    fn test_trx(engine: &Engine, sts: TrxID) -> (Transaction, Arc<SessionState>) {
        let engine_ref = engine.new_ref().unwrap();
        let session_id = engine_ref.next_session_id();
        session_tests::create_test_transaction(
            &engine.inner().session_registry,
            engine_ref,
            session_id,
            MIN_ACTIVE_TRX_ID + sts.as_u64(),
            sts,
            0,
        )
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        debug_snapshot(engine.lock_manager())
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
    fn test_stmt_effects_drop_rejects_leaked_effects() {
        let res = catch_unwind(|| {
            let mut effects = StmtEffects::empty();
            effects.set_ddl_redo(DDLRedo::CreateTable(TableID::new(42)));
        });

        assert!(res.is_err());
    }

    #[test]
    fn test_catalog_delete_primary_key_mvcc_rejects_non_primary_key() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_catalog_delete_pk_mismatch").await;
            let catalog_table = engine
                .catalog()
                .storage
                .get_catalog_table(TABLE_ID_TABLES)
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let res: Result<()> = trx
                .exec(async |stmt| {
                    let key = SelectKey::new(1, vec![Val::from(TableID::new(42))]);
                    stmt.catalog_delete_primary_key_mvcc(
                        catalog_table.as_ref(),
                        key.index_no,
                        &key.vals,
                        true,
                    )
                    .await?;
                    Ok(())
                })
                .await;

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidDmlInput)
            );
            trx.rollback().await.unwrap();
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

    #[inline]
    fn assert_lock_state_lifecycle_error(err: Report<LifecycleError>, trx_id: TrxID, reason: &str) {
        assert_eq!(*err.current_context(), LifecycleError::TransactionDiscarded);
        assert!(err.downcast_ref::<InternalError>().is_none());
        let rendered = format!("{err:?}");
        assert!(rendered.contains(&format!("trx_id={trx_id}")));
        assert!(rendered.contains(&format!("reason={reason}")));
    }

    #[test]
    fn test_checked_lock_state_reports_lifecycle_reasons() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_checked_lock_state_lifecycle").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let trx_id = trx.trx_id();

            trx.exec(async |stmt| {
                stmt.inner.active = false;
                assert_lock_state_lifecycle_error(
                    stmt.inner.checked_lock_state().err().unwrap(),
                    trx_id,
                    "transaction_inactive",
                );
                assert_lock_state_lifecycle_error(
                    stmt.inner.checked_lock_state_mut().err().unwrap(),
                    trx_id,
                    "transaction_inactive",
                );
                stmt.inner.active = true;

                let lock_state = stmt.inner.lock_state.take();
                assert_lock_state_lifecycle_error(
                    stmt.inner.checked_lock_state().err().unwrap(),
                    trx_id,
                    "lock_state_missing",
                );
                assert_lock_state_lifecycle_error(
                    stmt.inner.checked_lock_state_mut().err().unwrap(),
                    trx_id,
                    "lock_state_missing",
                );
                stmt.inner.lock_state = lock_state;
                Ok(())
            })
            .await
            .unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_table_write_lock_stacks_operation_over_lifecycle_error() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_table_write_lock_error_stack").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            trx.exec(async |stmt| {
                stmt.inner.active = false;
                let err = stmt
                    .acquire_table_write_metadata_lock(TableID::new(91_226))
                    .await
                    .unwrap_err();
                stmt.inner.active = true;

                assert_eq!(*err.current_context(), OperationError::LockUnavailable);
                assert_eq!(
                    err.downcast_ref::<LifecycleError>().copied(),
                    Some(LifecycleError::TransactionDiscarded)
                );
                assert!(err.downcast_ref::<InternalError>().is_none());
                Ok(())
            })
            .await
            .unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_statement_index_rollback_failure_poisons_and_discards_transaction() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_index_rollback_fail").await;
            let (mut trx, _session_state) = test_trx(&engine, TrxID::new(52));
            let trx_owner = trx_lock_owner(&mut trx).unwrap();
            try_acquire_transaction_lock(
                &mut trx,
                LockResource::TableData(TableID::new(91_250)),
                LockMode::IntentExclusive,
            )
            .unwrap();
            let stmt_owner = Cell::new(None);

            let res: Result<()> = trx
                .exec(async |stmt| {
                    stmt_owner.set(Some(lock_owner(stmt)));
                    try_acquire_statement_lock(
                        stmt,
                        LockResource::TableMetadata(TableID::new(91_250)),
                        LockMode::Shared,
                    )?;
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
                    Err(Report::new(OperationError::NotSupported).into())
                })
                .await;
            set_test_force_stmt_index_rollback_error(false);

            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<FatalError>().copied(),
                Some(FatalError::RollbackAccess)
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
                    .trx_sys
                    .poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RollbackAccess)
            );

            let err = trx.commit().await.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );
        });
    }
}
