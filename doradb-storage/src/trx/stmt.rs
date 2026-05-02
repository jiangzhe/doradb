use crate::buffer::PoolGuards;

use crate::catalog::{TableCache, TableID, TableSpec};
use crate::error::{InternalError, Result};
use crate::lock::{LockMode, LockOwner, LockResource};
use crate::row::RowID;
use crate::row::ops::SelectKey;
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{ActiveTrx, TrxContext};
use error_stack::Report;
use std::mem;

/// Kind-specific payload for statements with deferred catalog-side effects.
pub enum StmtKind {
    /// Create-table statement payload used to record table metadata changes.
    CreateTable(TableSpec),
}

/// Mutable effects accumulated by one statement before success or rollback.
///
/// These effects merge into transaction-level `TrxEffects` when the statement
/// succeeds. If the statement fails, index effects roll back before row effects
/// and redo is discarded.
pub struct StmtEffects {
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

    /// Moves successful statement effects into the active transaction.
    #[inline]
    pub(crate) fn merge_into_trx(&mut self, trx: &mut ActiveTrx) {
        trx.merge_statement_effects(
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

/// Borrowed statement facade for one operation inside an active transaction.
///
/// `ActiveTrx::exec` owns the statement lifecycle. It passes this facade to the
/// callback with immutable transaction context and mutable statement-local
/// effects. Successful callbacks merge effects into the transaction; ordinary
/// callback errors roll back only the current statement.
pub struct Statement<'stmt> {
    ctx: &'stmt TrxContext,
    effects: &'stmt mut StmtEffects,
    #[allow(dead_code)]
    owner: LockOwner,
}

impl<'stmt> Statement<'stmt> {
    /// Create a new statement.
    #[inline]
    pub(crate) fn new(
        ctx: &'stmt TrxContext,
        effects: &'stmt mut StmtEffects,
        owner: LockOwner,
    ) -> Self {
        debug_assert!(effects.is_empty());
        Statement {
            ctx,
            effects,
            owner,
        }
    }

    /// Returns this statement's immutable transaction context.
    #[inline]
    pub fn ctx(&self) -> &TrxContext {
        self.ctx
    }

    /// Returns mutable access to this statement's effect accumulator.
    #[inline]
    pub fn effects_mut(&mut self) -> &mut StmtEffects {
        self.effects
    }

    /// Returns the statement-owned logical lock owner.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn lock_owner(&self) -> LockOwner {
        self.owner
    }

    /// Attempts to acquire a statement-owned logical lock without waiting.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn try_acquire_statement_lock(
        &self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<bool> {
        let engine = self.ctx.engine().ok_or_else(|| {
            Report::new(InternalError::ActiveTransactionDiscarded)
                .attach("operation=try acquire statement lock")
        })?;
        engine
            .lock_manager()
            .try_acquire(resource, mode, self.owner)
    }

    /// Acquires a statement-owned logical lock.
    #[inline]
    #[allow(dead_code)]
    pub(crate) async fn acquire_statement_lock(
        &self,
        resource: LockResource,
        mode: LockMode,
    ) -> Result<()> {
        let engine = self.ctx.engine().ok_or_else(|| {
            Report::new(InternalError::ActiveTransactionDiscarded)
                .attach("operation=acquire statement lock")
        })?;
        engine
            .lock_manager()
            .acquire(resource, mode, self.owner)
            .await
    }

    /// Returns disjoint transaction context and statement effects references.
    #[inline]
    pub fn ctx_and_effects_mut(&mut self) -> (&TrxContext, &mut StmtEffects) {
        (self.ctx, self.effects)
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
    use crate::trx::{MIN_ACTIVE_TRX_ID, TrxID};
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
