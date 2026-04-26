use crate::buffer::PoolGuards;

use crate::catalog::{TableCache, TableID, TableSpec};
use crate::error::{FatalError, Result};
use crate::row::RowID;
use crate::row::ops::SelectKey;
use crate::trx::redo::{DDLRedo, RedoLogs, RowRedo};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{ActiveTrx, TrxContext};
use std::mem;

/// Kind-specific payload for statements with deferred catalog-side effects.
pub enum StmtKind {
    /// Create-table statement payload used to record table metadata changes.
    CreateTable(TableSpec),
}

/// Mutable effects accumulated by one statement before success or rollback.
///
/// These effects merge into transaction-level `TrxEffects` when the statement
/// succeeds. If the statement fails, row and index effects roll back in reverse
/// order and redo is discarded.
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

    #[inline]
    fn merge_into_trx(&mut self, trx: &mut ActiveTrx) {
        trx.merge_statement_effects(
            &mut self.row_undo,
            &mut self.index_undo,
            mem::take(&mut self.redo),
        );
    }

    #[inline]
    async fn rollback_row(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
        sts: Option<crate::trx::TrxID>,
    ) -> Result<()> {
        self.row_undo.rollback(table_cache, pool_guards, sts).await
    }

    #[inline]
    async fn rollback_index(
        &mut self,
        table_cache: &mut TableCache<'_>,
        pool_guards: &PoolGuards,
        sts: crate::trx::TrxID,
    ) -> Result<()> {
        self.index_undo
            .rollback(table_cache, pool_guards, sts)
            .await
    }

    #[inline]
    fn clear_redo(&mut self) {
        self.redo.clear();
    }
}

/// Linear statement handle for one operation inside an active transaction.
///
/// A statement owns the active transaction while it runs and accumulates
/// statement-local row undo, index undo, and redo effects. Successful
/// statements merge those effects back into the transaction; failed statements
/// roll them back or discard them before returning the transaction.
pub struct Statement {
    /// Transaction this statement belongs to.
    trx: ActiveTrx,
    // Statement-level row undo, index undo, and redo effects.
    effects: StmtEffects,
}

impl Statement {
    /// Create a new statement.
    #[inline]
    pub fn new(trx: ActiveTrx) -> Self {
        let effects = StmtEffects::empty();
        debug_assert!(effects.is_empty());
        Statement { trx, effects }
    }

    /// Succeed current statement and return transaction it belongs to.
    /// All undo and redo logs it holds will be merged into transaction buffer.
    #[inline]
    pub fn succeed(mut self) -> ActiveTrx {
        self.effects.merge_into_trx(&mut self.trx);
        self.trx
    }

    /// Fail current statement and return transaction it belongs to.
    /// This will trigger statement-level rollback based on its undo.
    /// Redo logs will be discarded.
    #[inline]
    pub async fn fail(mut self) -> Result<ActiveTrx> {
        // rollback row data.
        // todo: group by page level may be better.
        let engine = self.trx.engine().cloned().unwrap();
        let pool_guards = self
            .trx
            .pool_guards()
            .expect("statement rollback requires session pool guards")
            .clone();
        let mut table_cache = TableCache::new(engine.catalog());
        let sts = self.trx.sts();
        if self
            .effects
            .rollback_row(&mut table_cache, &pool_guards, Some(sts))
            .await
            .is_err()
        {
            self.trx.discard_after_fatal_rollback();
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        // rollback index data.
        if self
            .effects
            .rollback_index(&mut table_cache, &pool_guards, sts)
            .await
            .is_err()
        {
            self.trx.discard_after_fatal_rollback();
            return Err(engine
                .trx_sys
                .poison_storage(FatalError::RollbackAccess)
                .into());
        }
        // clear redo logs.
        self.effects.clear_redo();
        Ok(self.trx)
    }

    /// Returns this statement's immutable transaction context.
    #[inline]
    pub fn ctx(&self) -> &TrxContext {
        self.trx.ctx()
    }

    /// Returns mutable access to this statement's effect accumulator.
    #[inline]
    pub fn effects_mut(&mut self) -> &mut StmtEffects {
        &mut self.effects
    }

    /// Returns disjoint transaction context and statement effects references.
    #[inline]
    pub fn ctx_and_effects_mut(&mut self) -> (&TrxContext, &mut StmtEffects) {
        (self.trx.ctx(), &mut self.effects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stmt_effects_empty() {
        let effects = StmtEffects::empty();
        assert!(effects.is_empty());
        assert!(effects.row_undo.is_empty());
        assert!(effects.index_undo.is_empty());
        assert!(effects.redo.is_empty());
    }
}
