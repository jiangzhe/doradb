use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::row::RowID;
use crate::table::TableID;
use crate::trx::redo::RedoEntry;
use crate::trx::undo::{IndexUndoLogs, RowUndoLogs};
use crate::trx::ActiveTrx;

pub struct Statement {
    pub trx: ActiveTrx,
    // statement-level undo logs of row data.
    pub row_undo: RowUndoLogs,
    // statement-level index undo operations.
    pub index_undo: IndexUndoLogs,
    // statement-level redo logs.
    pub redo: Vec<RedoEntry>,
}

impl Statement {
    #[inline]
    pub fn new(trx: ActiveTrx) -> Self {
        Statement {
            trx,
            row_undo: RowUndoLogs::empty(),
            index_undo: IndexUndoLogs::empty(),
            redo: vec![],
        }
    }
}

impl Statement {
    /// Succeed current statement and return transaction it belongs to.
    /// All undo and redo logs it holds will be merged into transaction buffer.
    #[inline]
    pub fn succeed(mut self) -> ActiveTrx {
        self.trx.row_undo.merge(&mut self.row_undo);
        self.trx.index_undo.merge(&mut self.index_undo);
        self.trx.redo.extend(self.redo.drain(..));
        self.trx
    }

    /// Fail current statement and return transaction it belongs to.
    /// This will trigger statement-level rollback based on its undo.
    /// Redo logs will be discarded.
    #[inline]
    pub async fn fail<P: BufferPool>(mut self, buf_pool: P, catalog: &Catalog<P>) -> ActiveTrx {
        // rollback row data.
        // todo: group by page level may be better.
        self.row_undo.rollback(buf_pool).await;
        // rollback index data.
        self.index_undo.rollback(catalog);
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
}
