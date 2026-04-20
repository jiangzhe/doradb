use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;

use crate::catalog::{TableCache, TableID, TableSpec};
use crate::error::{Result, StoragePoisonSource};
use crate::row::RowID;
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::table::{Table, TableAccess};
use crate::trx::redo::{RedoLogs, RowRedo};
use crate::trx::undo::{
    IndexUndo, IndexUndoKind, IndexUndoLogs, OwnedRowUndo, RowUndoKind, RowUndoLogs,
};
use crate::trx::{ActiveTrx, TrxContext};
use crate::value::Val;
use std::mem;

pub enum StmtKind {
    CreateTable(TableSpec),
}

/// Mutable effects accumulated by one statement before success or rollback.
///
/// These effects merge into transaction-level `TrxEffects` when the statement
/// succeeds. If the statement fails, row and index effects roll back in reverse
/// order and redo is discarded.
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

    /// Returns mutable redo access for compatibility during the migration.
    #[inline]
    pub(crate) fn redo_mut(&mut self) -> &mut RedoLogs {
        &mut self.redo
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
    pub trx: ActiveTrx,
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

    /// Rewrite the latest provisional row undo lock into its final operation.
    #[inline]
    pub fn update_last_row_undo(&mut self, kind: RowUndoKind) {
        self.effects_mut().update_last_row_undo(kind);
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
                .poison_storage(StoragePoisonSource::RollbackAccess));
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
                .poison_storage(StoragePoisonSource::RollbackAccess));
        }
        // clear redo logs.
        self.effects.clear_redo();
        Ok(self.trx)
    }

    /// Loads the cached active insert page for the given table.
    #[inline]
    pub fn load_active_insert_page(&self, table_id: TableID) -> Option<(VersionedPageID, RowID)> {
        self.trx.load_active_insert_page(table_id)
    }

    /// Saves the cached active insert page for the given table.
    #[inline]
    pub fn save_active_insert_page(
        &self,
        table_id: TableID,
        page_id: VersionedPageID,
        row_id: RowID,
    ) {
        self.trx.save_active_insert_page(table_id, page_id, row_id);
    }

    /// Returns the buffer pool guards attached to this statement's session.
    #[inline]
    pub fn pool_guards(&self) -> &PoolGuards {
        self.ctx()
            .pool_guards()
            .expect("statement requires an attached session for pool guards")
    }

    /// Returns this statement's immutable transaction context.
    #[inline]
    pub(crate) fn ctx(&self) -> &TrxContext {
        self.trx.ctx()
    }

    /// Returns mutable access to this statement's effect accumulator.
    #[inline]
    pub(crate) fn effects_mut(&mut self) -> &mut StmtEffects {
        &mut self.effects
    }

    /// Insert a row into a table.
    #[inline]
    pub async fn insert_row(&mut self, table: &Table, cols: Vec<Val>) -> Result<InsertMvcc> {
        table.accessor().insert_mvcc(self, cols).await
    }

    /// Delete a row selected by a unique key.
    #[inline]
    pub async fn delete_row(&mut self, table: &Table, key: &SelectKey) -> Result<DeleteMvcc> {
        table.accessor().delete_unique_mvcc(self, key, false).await
    }

    /// Select a row using MVCC visibility and a unique key lookup.
    #[inline]
    pub async fn select_row_mvcc(
        &self,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        table
            .accessor()
            .index_lookup_unique_mvcc(self, key, user_read_set)
            .await
    }

    /// Update a row selected by a unique key.
    #[inline]
    pub async fn update_row(
        &mut self,
        table: &Table,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        table.accessor().update_unique_mvcc(self, key, update).await
    }

    #[inline]
    pub(crate) fn push_insert_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.effects
            .push_insert_unique_index_undo(table_id, row_id, key, merge_old_deleted);
    }

    #[inline]
    pub(crate) fn push_insert_non_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.effects
            .push_insert_non_unique_index_undo(table_id, row_id, key, merge_old_deleted);
    }

    #[inline]
    pub(crate) fn push_delete_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        unique: bool,
    ) {
        self.effects
            .push_delete_index_undo(table_id, row_id, key, unique);
    }

    #[inline]
    pub(crate) fn push_update_unique_index_undo(
        &mut self,
        table_id: TableID,
        old_row_id: RowID,
        new_row_id: RowID,
        key: SelectKey,
        old_deleted: bool,
    ) {
        self.effects.push_update_unique_index_undo(
            table_id,
            old_row_id,
            new_row_id,
            key,
            old_deleted,
        );
    }

    /// Push one row undo entry into this statement's effects.
    #[inline]
    pub(crate) fn push_row_undo(&mut self, undo: OwnedRowUndo) {
        self.effects_mut().push_row_undo(undo);
    }

    /// Insert one row redo entry into this statement's effects.
    #[inline]
    pub(crate) fn insert_row_redo(&mut self, table_id: TableID, entry: RowRedo) {
        self.effects_mut().insert_row_redo(table_id, entry);
    }

    /// Returns mutable access to statement redo for compatibility callers.
    #[inline]
    pub(crate) fn redo_mut(&mut self) -> &mut RedoLogs {
        self.effects_mut().redo_mut()
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
