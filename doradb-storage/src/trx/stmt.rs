use crate::buffer::PoolGuards;

use crate::catalog::{CatalogTable, TableCache, TableID, TableSpec};
use crate::error::{FatalError, InternalError, Result};
use crate::lock::{LockManager, LockMode, LockOwner, LockResource};
use crate::quiescent::QuiescentGuard;
use crate::row::RowID;
use crate::row::ops::{DeleteMvcc, ScanMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::table::{Table, TableAccess};
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{OwnerLockState, TrxContext, TrxEffects};
use crate::value::Val;
use error_stack::Report;
use std::mem;

/// Kind-specific payload for statements with deferred catalog-side effects.
pub enum StmtKind {
    /// Create-table statement payload used to record table metadata changes.
    CreateTable(TableSpec),
}

#[inline]
fn active_transaction_discarded_err(operation: &'static str) -> crate::error::Error {
    Report::new(InternalError::ActiveTransactionDiscarded)
        .attach(format!("operation={operation}"))
        .into()
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
        sts: Option<crate::trx::TrxID>,
    ) -> Result<()> {
        self.row_undo.rollback(table_cache, pool_guards, sts).await
    }

    /// Rolls back statement-local secondary-index effects in reverse effect order.
    #[inline]
    pub(crate) async fn rollback_index(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
        sts: crate::trx::TrxID,
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
    pub(crate) fn clear_for_discard(&mut self) {
        self.row_undo = RowUndoLogs::empty();
        self.index_undo = IndexUndoLogs::empty();
        self.redo.clear();
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
/// `ActiveTrx::exec` owns the statement lifecycle. It passes this facade to the
/// callback with transaction context, statement-local effects, and
/// statement-owned logical locks. Dropping this value releases every
/// statement-owned lock, so success and rollback paths cannot forget cleanup.
pub struct Statement<'stmt> {
    ctx: &'stmt TrxContext,
    effects: StmtEffects,
    lock_manager: QuiescentGuard<LockManager>,
    trx_locks: &'stmt mut OwnerLockState,
    stmt_locks: OwnerLockState,
}

impl<'stmt> Statement<'stmt> {
    /// Create a new statement.
    #[inline]
    pub(crate) fn new(
        ctx: &'stmt TrxContext,
        owner: LockOwner,
        lock_manager: QuiescentGuard<LockManager>,
        trx_locks: &'stmt mut OwnerLockState,
    ) -> Self {
        let owner_group = trx_locks.owner_group();
        Statement {
            ctx,
            effects: StmtEffects::empty(),
            lock_manager,
            trx_locks,
            stmt_locks: match owner_group {
                Some(owner_group) => OwnerLockState::new_grouped(owner, owner_group),
                None => OwnerLockState::new(owner),
            },
        }
    }

    /// Returns this statement's immutable transaction context.
    #[inline]
    pub fn ctx(&self) -> &TrxContext {
        self.ctx
    }

    /// Returns mutable access to this statement's effect accumulator.
    #[inline]
    pub(crate) fn effects_mut(&mut self) -> &mut StmtEffects {
        &mut self.effects
    }

    /// Returns the statement-owned logical lock owner.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn lock_owner(&self) -> LockOwner {
        self.stmt_locks.owner()
    }

    /// Attempts to acquire a statement-owned logical lock without waiting.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_statement_lock(
        &mut self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        self.stmt_locks
            .try_acquire(&self.lock_manager, resource, mode)
    }

    /// Acquires a statement-owned logical lock.
    #[inline]
    #[allow(dead_code)]
    pub(crate) async fn acquire_statement_lock(
        &mut self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        self.stmt_locks
            .acquire(&self.lock_manager, resource, mode)
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
        self.trx_locks
            .acquire(
                &self.lock_manager,
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            )
            .await?;
        self.trx_locks
            .acquire(
                &self.lock_manager,
                LockResource::TableData(table_id),
                LockMode::IntentExclusive,
            )
            .await
    }

    /// Scans the table's row store under statement-lifetime metadata protection.
    #[inline]
    pub async fn table_scan_mvcc<F>(
        &mut self,
        table: &Table,
        read_set: &[usize],
        row_action: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        self.acquire_table_read_lock(table.table_id()).await?;
        table.check_foreground_live("table_scan_mvcc")?;
        table
            .accessor()
            .table_scan_mvcc(self.ctx, read_set, row_action)
            .await;
        Ok(())
    }

    /// Looks up one unique-key row under statement-lifetime metadata protection.
    #[inline]
    pub async fn table_lookup_unique_mvcc(
        &mut self,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        self.acquire_table_read_lock(table.table_id()).await?;
        table.check_foreground_live("table_lookup_unique_mvcc")?;
        table
            .accessor()
            .index_lookup_unique_mvcc(self.ctx, key, user_read_set)
            .await
    }

    /// Scans one secondary-index key under statement-lifetime metadata protection.
    #[inline]
    pub async fn table_index_scan_mvcc(
        &mut self,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        self.acquire_table_read_lock(table.table_id()).await?;
        table.check_foreground_live("table_index_scan_mvcc")?;
        table
            .accessor()
            .index_scan_mvcc(self.ctx, key, user_read_set)
            .await
    }

    /// Inserts one user-table row after acquiring transaction-lifetime write locks.
    #[inline]
    pub async fn table_insert_mvcc(&mut self, table: &Table, cols: Vec<Val>) -> Result<RowID> {
        self.acquire_table_write_locks(table.table_id()).await?;
        table.check_foreground_live("table_insert_mvcc")?;
        table
            .accessor()
            .insert_mvcc(self.ctx, &mut self.effects, cols)
            .await
    }

    /// Updates one user-table row by unique key after acquiring write locks.
    #[inline]
    pub async fn table_update_unique_mvcc(
        &mut self,
        table: &Table,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        self.acquire_table_write_locks(table.table_id()).await?;
        table.check_foreground_live("table_update_unique_mvcc")?;
        table
            .accessor()
            .update_unique_mvcc(self.ctx, &mut self.effects, key, update)
            .await
    }

    /// Deletes one user-table row by unique key after acquiring write locks.
    #[inline]
    pub async fn table_delete_unique_mvcc(
        &mut self,
        table: &Table,
        key: &SelectKey,
        log_by_key: bool,
    ) -> Result<DeleteMvcc> {
        self.acquire_table_write_locks(table.table_id()).await?;
        table.check_foreground_live("table_delete_unique_mvcc")?;
        table
            .accessor()
            .delete_unique_mvcc(self.ctx, &mut self.effects, key, log_by_key)
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
        table
            .accessor()
            .insert_mvcc(self.ctx, &mut self.effects, cols)
            .await
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
        table
            .accessor()
            .delete_unique_mvcc(self.ctx, &mut self.effects, key, log_by_key)
            .await
    }

    /// Moves successful statement effects into transaction effects.
    #[inline]
    pub(crate) fn merge_effects_into(&mut self, trx_effects: &mut TrxEffects) {
        self.effects.merge_into_trx_effects(trx_effects);
    }

    /// Rolls back statement-local effects after an ordinary callback error.
    ///
    /// Index effects roll back before row effects so index entries stop
    /// pointing at uncommitted row state before row undo is unwound. Statement
    /// locks stay held until this method returns and `Statement` drops.
    #[inline]
    pub(crate) async fn rollback_effects(&mut self) -> Result<()> {
        let engine = self
            .ctx
            .engine()
            .cloned()
            .ok_or_else(|| active_transaction_discarded_err("rollback statement effects"))?;
        let pool_guards = self
            .ctx
            .pool_guards()
            .cloned()
            .ok_or_else(|| active_transaction_discarded_err("rollback statement effects"))?;
        let mut table_cache = TableCache::new(engine.catalog());
        let sts = self.ctx.sts();
        if self
            .effects
            .rollback_index(&mut table_cache, &pool_guards, sts)
            .await
            .is_err()
        {
            self.effects.clear_for_discard();
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        if self
            .effects
            .rollback_row(&mut table_cache, &pool_guards, Some(sts))
            .await
            .is_err()
        {
            self.effects.clear_for_discard();
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        self.effects.clear_redo();
        Ok(())
    }

    /// Returns disjoint transaction context and statement effects references.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn ctx_and_effects_mut(&mut self) -> (&TrxContext, &mut StmtEffects) {
        (self.ctx, &mut self.effects)
    }
}

impl Drop for Statement<'_> {
    #[inline]
    fn drop(&mut self) {
        self.stmt_locks.release_all(&self.lock_manager);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::PoolRole;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{FatalError, InternalError, OperationError};
    use crate::session::SessionState;
    use crate::trx::undo::{OwnedRowUndo, RowUndoKind};
    use crate::trx::{ActiveTrx, MIN_ACTIVE_TRX_ID, TrxID};
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

    fn test_trx(engine: &Engine, sts: TrxID) -> ActiveTrx {
        let engine_ref = engine.new_ref().unwrap();
        let session_id = engine_ref.next_session_id();
        let session_state = Arc::new(SessionState::new(engine_ref, session_id));
        ActiveTrx::new(session_state, MIN_ACTIVE_TRX_ID + sts, sts, 0, 0)
    }

    fn lock_entry_count(engine: &Engine, owner: LockOwner) -> usize {
        engine
            .lock_manager()
            .debug_snapshot()
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
            effects.set_ddl_redo(DDLRedo::CreateTable(42));
        });

        assert!(res.is_err());
    }

    #[test]
    fn test_statement_index_rollback_failure_poisons_and_discards_transaction() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("redo_stmt_index_rollback_fail").await;
            let mut trx = test_trx(&engine, 52);
            let trx_owner = trx.lock_owner().unwrap();
            trx.try_acquire_transaction_lock(
                LockResource::TableData(91_250),
                LockMode::IntentExclusive,
            )
            .unwrap();
            let stmt_owner = Cell::new(None);

            let res: Result<()> = trx
                .exec(async |stmt| {
                    stmt_owner.set(Some(stmt.lock_owner()));
                    stmt.try_acquire_statement_lock(
                        LockResource::TableMetadata(91_250),
                        LockMode::Shared,
                    )?;
                    // This row undo references a table that does not exist. If
                    // statement rollback ever runs row rollback before index
                    // rollback, this test fails before the injected index
                    // rollback error can discard the statement safely.
                    stmt.effects_mut().push_row_undo(OwnedRowUndo::new(
                        99_999_999,
                        None,
                        24,
                        RowUndoKind::Delete,
                    ));
                    stmt.effects_mut().push_delete_index_undo(
                        12,
                        23,
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
            assert_eq!(lock_entry_count(&engine, stmt_owner.get().unwrap()), 0);
            assert_eq!(lock_entry_count(&engine, trx_owner), 0);
            assert!(
                engine
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
