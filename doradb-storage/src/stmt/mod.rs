use crate::buffer::page::PageID;

use crate::catalog::{SchemaID, TableID, TableSpec};
use crate::row::RowID;
use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
use crate::table::{Table, TableAccess};
use crate::trx::ActiveTrx;
use crate::trx::redo::RedoLogs;
use crate::trx::undo::{IndexUndo, IndexUndoKind, IndexUndoLogs, RowUndoKind, RowUndoLogs};
use crate::value::Val;
use semistr::SemiStr;
use std::mem;

pub enum StmtKind {
    CreateSchema(SemiStr),
    CreateTable(SchemaID, TableSpec),
}

pub struct Statement {
    // Transaction it belongs to.
    pub trx: ActiveTrx,
    // statement-level undo logs of row data.
    pub row_undo: RowUndoLogs,
    // statement-level index undo operations.
    pub index_undo: IndexUndoLogs,
    // statement-level redo logs.
    pub redo: RedoLogs,
}

impl Statement {
    /// Create a new statement.
    #[inline]
    pub fn new(trx: ActiveTrx) -> Self {
        Statement {
            trx,
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: RedoLogs::default(),
        }
    }

    #[inline]
    pub fn update_last_row_undo(&mut self, kind: RowUndoKind) {
        let last_undo = self.row_undo.last_mut().unwrap();
        // Currently the update can only be applied on LOCK entry.
        debug_assert!(matches!(last_undo.kind, RowUndoKind::Lock));
        last_undo.kind = kind;
    }

    /// Succeed current statement and return transaction it belongs to.
    /// All undo and redo logs it holds will be merged into transaction buffer.
    #[inline]
    pub fn succeed(mut self) -> ActiveTrx {
        self.trx.row_undo.merge(&mut self.row_undo);
        self.trx.index_undo.merge(&mut self.index_undo);
        self.trx.redo.merge(mem::take(&mut self.redo));
        self.trx
    }

    /// Fail current statement and return transaction it belongs to.
    /// This will trigger statement-level rollback based on its undo.
    /// Redo logs will be discarded.
    #[inline]
    pub async fn fail(mut self) -> ActiveTrx {
        // rollback row data.
        // todo: group by page level may be better.
        let engine = self.trx.engine().unwrap();
        self.row_undo
            .rollback(engine.data_pool, engine.catalog(), Some(self.trx.sts))
            .await;
        // rollback index data.
        self.index_undo
            .rollback(engine.data_pool, engine.catalog(), self.trx.sts)
            .await;
        // clear redo logs.
        self.redo.clear();
        self.trx
    }

    #[inline]
    pub fn load_active_insert_page(&mut self, table_id: TableID) -> Option<(PageID, RowID)> {
        self.trx
            .session
            .as_mut()
            .and_then(|session| session.load_active_insert_page(table_id))
    }

    #[inline]
    pub fn save_active_insert_page(&mut self, table_id: TableID, page_id: PageID, row_id: RowID) {
        if let Some(session) = self.trx.session.as_mut() {
            session.save_active_insert_page(table_id, page_id, row_id);
        }
    }

    /// Insert a row into a table.
    #[inline]
    pub async fn insert_row(&mut self, table: &Table, cols: Vec<Val>) -> InsertMvcc {
        table.insert_mvcc(self, cols).await
    }

    #[inline]
    pub async fn delete_row(&mut self, table: &Table, key: &SelectKey) -> DeleteMvcc {
        table.delete_unique_mvcc(self, key, false).await
    }

    #[inline]
    pub async fn select_row_mvcc(
        &self,
        table: &Table,
        key: &SelectKey,
        user_read_set: &[usize],
    ) -> SelectMvcc {
        table
            .index_lookup_unique_mvcc(self, key, user_read_set)
            .await
    }

    #[inline]
    pub async fn update_row(
        &mut self,
        table: &Table,
        key: &SelectKey,
        update: Vec<UpdateCol>,
    ) -> UpdateMvcc {
        table.update_unique_mvcc(self, key, update).await
    }

    #[inline]
    pub(crate) fn push_insert_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        let index_undo = IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertUnique(key, merge_old_deleted),
        };
        self.index_undo.push(index_undo);
    }

    #[inline]
    pub(crate) fn push_insert_non_unique_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        let index_undo = IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::InsertNonUnique(key, merge_old_deleted),
        };
        self.index_undo.push(index_undo);
    }

    #[inline]
    pub(crate) fn push_delete_index_undo(
        &mut self,
        table_id: TableID,
        row_id: RowID,
        key: SelectKey,
        unique: bool,
    ) {
        let index_undo = IndexUndo {
            table_id,
            row_id,
            kind: IndexUndoKind::DeferDelete(key, unique),
        };
        self.index_undo.push(index_undo);
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
        let index_undo = IndexUndo {
            table_id,
            row_id: new_row_id,
            kind: IndexUndoKind::UpdateUnique(key, old_row_id, old_deleted),
        };
        self.index_undo.push(index_undo);
    }
}
