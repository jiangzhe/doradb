use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::row::RowID;
use crate::table::TableID;
use crate::trx::redo::RedoEntry;
use crate::trx::undo::OwnedUndoEntry;
use crate::trx::ActiveTrx;

pub struct Statement {
    pub trx: ActiveTrx,
    // statement-level undo logs.
    pub undo: Vec<OwnedUndoEntry>,
    // statement-level redo logs.
    pub redo: Vec<RedoEntry>,
}

impl Statement {
    #[inline]
    pub fn new(trx: ActiveTrx) -> Self {
        Statement {
            trx,
            undo: vec![],
            redo: vec![],
        }
    }

    #[inline]
    pub fn commit(mut self) -> ActiveTrx {
        self.trx.undo.extend(self.undo.drain(..));
        self.trx.redo.extend(self.redo.drain(..));
        self.trx
    }

    #[inline]
    pub fn rollback<P: BufferPool>(self, buf_pool: &P) -> ActiveTrx {
        todo!();
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
