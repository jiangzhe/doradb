use crate::buffer::PoolGuards;
use crate::id::{RowID, TableID, TrxID};

use crate::catalog::{CatalogTable, TableCache, is_catalog_obj_id};
use crate::error::{FatalError, OperationError, Result};
use crate::lock::{LockMode, LockOwner, LockResource, OwnerLockState};
use crate::row::ops::{DeleteMvcc, ScanMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::session::TrxAttachment;
use crate::table::Table;
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{FatalRollbackRetention, TrxEffects, TrxInner, TrxRuntime};
use crate::value::Val;
use error_stack::Report;
use std::mem;
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
        sts: TrxID,
    ) -> Result<()> {
        self.row_undo.rollback(table_cache, pool_guards, sts).await
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
        if tests::test_force_stmt_index_rollback_error_enabled() {
            return Err(
                error_stack::Report::new(crate::error::InternalError::InjectedTestFailure).into(),
            );
        }
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
}

impl<'stmt> Statement<'stmt> {
    /// Create a new statement.
    #[inline]
    pub(crate) fn new(
        inner: &'stmt mut TrxInner,
        attachment: &'stmt TrxAttachment,
        owner: LockOwner,
    ) -> Result<Self> {
        let owner_group = inner.checked_lock_state("execute statement")?.owner_group();
        Ok(Statement {
            inner,
            attachment,
            effects: StmtEffects::empty(),
            stmt_locks: match owner_group {
                Some(owner_group) => OwnerLockState::new_grouped(owner, owner_group),
                None => OwnerLockState::new(owner),
            },
        })
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
    ) -> Result<()> {
        self.stmt_locks
            .acquire(self.attachment.engine().lock_manager(), resource, mode)
            .await
    }

    /// Acquires statement-lifetime metadata protection for a table read.
    #[inline]
    pub(crate) async fn acquire_table_read_lock(&mut self, table_id: TableID) -> Result<()> {
        self.acquire_statement_lock(LockResource::TableMetadata(table_id), LockMode::Shared)
            .await
    }

    /// Acquires transaction-lifetime metadata and table-data intent for a write.
    #[inline]
    pub(crate) async fn acquire_table_write_locks(&mut self, table_id: TableID) -> Result<()> {
        let lock_manager = self.attachment.engine().lock_manager();
        self.inner
            .checked_lock_state_mut("acquire table write locks")?
            .acquire(
                lock_manager,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            )
            .await?;
        self.inner
            .checked_lock_state_mut("acquire table write locks")?
            .acquire(
                lock_manager,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
            )
            .await
    }

    #[inline]
    fn table_not_found(table_id: TableID, operation: &'static str) -> crate::error::Error {
        Report::new(OperationError::TableNotFound)
            .attach(format!("operation={operation}, table_id={table_id}"))
            .into()
    }

    #[inline]
    fn resolve_user_table(
        &mut self,
        table_id: TableID,
        operation: &'static str,
    ) -> Result<Arc<Table>> {
        if is_catalog_obj_id(table_id) {
            return Err(Self::table_not_found(table_id, operation));
        }
        if let Some(table) = self.inner.cached_user_table(table_id) {
            return Ok(table);
        }
        if let Some(table) = self.attachment.cached_user_table(table_id) {
            self.inner.cache_user_table(&table);
            return Ok(table);
        }
        let engine = self.attachment.engine();
        let Some(table) = engine.catalog().get_table_now(table_id) else {
            return Err(Self::table_not_found(table_id, operation));
        };
        self.attachment.cache_user_table(&table);
        self.inner.cache_user_table(&table);
        Ok(table)
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
        let table = self.resolve_user_table(table_id, "table_scan_mvcc")?;
        self.acquire_table_read_lock(table_id).await?;
        table.check_foreground_live("table_scan_mvcc")?;
        let layout = table.layout_snapshot();
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .table_scan_mvcc(rt, read_set, row_action)
            .await
    }

    /// Looks up one unique-key row in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_lookup_unique_mvcc(
        &mut self,
        table_id: TableID,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        let table = self.resolve_user_table(table_id, "table_lookup_unique_mvcc")?;
        self.acquire_table_read_lock(table_id).await?;
        table.check_foreground_live("table_lookup_unique_mvcc")?;
        let layout = table.layout_snapshot();
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_lookup_unique_mvcc(rt, key, user_read_set)
            .await
    }

    /// Scans one secondary-index key in a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_index_scan_mvcc(
        &mut self,
        table_id: TableID,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        let table = self.resolve_user_table(table_id, "table_index_scan_mvcc")?;
        self.acquire_table_read_lock(table_id).await?;
        table.check_foreground_live("table_index_scan_mvcc")?;
        let layout = table.layout_snapshot();
        let rt = self.runtime();
        table
            .accessor_with_layout(&layout)
            .index_scan_mvcc(rt, key, user_read_set)
            .await
    }

    /// Inserts one row into a catalog-owned user table by table id.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_insert_mvcc(&mut self, table_id: TableID, cols: Vec<Val>) -> Result<RowID> {
        let table = self.resolve_user_table(table_id, "table_insert_mvcc")?;
        self.acquire_table_write_locks(table_id).await?;
        table.check_foreground_live("table_insert_mvcc")?;
        let layout = table.layout_snapshot();
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .insert_mvcc(rt, effects, cols)
            .await
    }

    /// Updates one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_update_unique_mvcc(
        &mut self,
        table_id: TableID,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        let table = self.resolve_user_table(table_id, "table_update_unique_mvcc")?;
        self.acquire_table_write_locks(table_id).await?;
        table.check_foreground_live("table_update_unique_mvcc")?;
        let layout = table.layout_snapshot();
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .update_unique_mvcc(rt, effects, key, update)
            .await
    }

    /// Deletes one catalog-owned user-table row by table id and unique key.
    ///
    /// Strong table-runtime access is internal and operation-local.
    #[inline]
    pub async fn table_delete_unique_mvcc(
        &mut self,
        table_id: TableID,
        key: &SelectKey,
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        let table = self.resolve_user_table(table_id, "table_delete_unique_mvcc")?;
        self.acquire_table_write_locks(table_id).await?;
        table.check_foreground_live("table_delete_unique_mvcc")?;
        let layout = table.layout_snapshot();
        let (rt, effects) = self.runtime_and_effects_mut();
        table
            .accessor_with_layout(&layout)
            .delete_unique_mvcc(rt, effects, key, log_by_key)
            .await
    }

    /// Inserts one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_insert_mvcc(
        &mut self,
        table: &CatalogTable,
        cols: Vec<Val>,
    ) -> Result<RowID> {
        self.acquire_table_write_locks(table.table_id()).await?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table.insert_mvcc(rt, effects, cols).await
    }

    /// Deletes one catalog-table row through the foreground lock-aware path.
    #[inline]
    pub(crate) async fn catalog_delete_unique_mvcc(
        &mut self,
        table: &CatalogTable,
        key: &SelectKey,
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        self.acquire_table_write_locks(table.table_id()).await?;
        let (rt, effects) = self.runtime_and_effects_mut();
        table.delete_unique_mvcc(rt, effects, key, log_by_key).await
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
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        if self
            .effects
            .rollback_row(&mut table_cache, pool_guards, sts)
            .await
            .is_err()
        {
            let retention = self.effects.take_for_fatal_retention();
            engine.trx_sys.retain_fatal_rollback(retention);
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
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
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{FatalError, InternalError, OperationError};
    use crate::id::TrxID;
    use crate::lock::LockManager;
    use crate::lock::tests::{debug_snapshot, try_acquire, try_acquire_grouped};
    use crate::session::{SessionState, tests as session_tests};
    use crate::trx::sys::tests as sys_tests;
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
    use crate::trx::{MIN_ACTIVE_TRX_ID, Transaction};
    use error_stack::Report;
    use std::cell::Cell;
    use std::sync::Arc;
    use tempfile::TempDir;

    thread_local! {
        static TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn set_test_force_stmt_index_rollback_error(enabled: bool) {
        TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR.with(|flag| flag.set(enabled));
    }

    pub(super) fn test_force_stmt_index_rollback_error_enabled() -> bool {
        TEST_FORCE_STMT_INDEX_ROLLBACK_ERROR.with(|flag| flag.get())
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
        stmt.stmt_locks
            .acquire(stmt.attachment.engine().lock_manager(), resource, mode)
            .await
    }

    #[inline]
    pub(crate) fn runtime_and_effects_mut<'borrow>(
        stmt: &'borrow mut Statement<'_>,
    ) -> (TrxRuntime<'borrow>, &'borrow mut StmtEffects) {
        stmt.runtime_and_effects_mut()
    }

    #[inline]
    fn trx_lock_owner(trx: &mut Transaction) -> Result<LockOwner> {
        let checkout = trx.checkout("read transaction lock owner")?;
        Ok(checkout
            .inner()
            .checked_lock_state("read transaction lock owner")?
            .owner())
    }

    #[inline]
    fn try_acquire_transaction_lock(
        trx: &mut Transaction,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        let mut checkout = trx.checkout("try acquire transaction lock")?;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        let lock_manager = attachment.engine().lock_manager();
        try_acquire_owner_lock_state(
            inner.checked_lock_state_mut("try acquire transaction lock")?,
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
        let res = std::panic::catch_unwind(|| {
            let mut effects = StmtEffects::empty();
            effects.set_ddl_redo(DDLRedo::CreateTable(TableID::new(42)));
        });

        assert!(res.is_err());
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
                    .storage_poison_error()
                    .as_ref()
                    .is_some_and(|err| *err.current_context() == FatalError::RollbackAccess)
            );

            let err = trx.commit().await.unwrap_err();
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::ActiveTransactionDiscarded)
            );
        });
    }
}
