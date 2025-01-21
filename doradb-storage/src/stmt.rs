use crate::buffer::guard::PageGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::latch::LatchFallbackMode;
use crate::row::{RowID, RowPage};
use crate::table::TableID;
use crate::trx::redo::RedoEntry;
use crate::trx::undo::{IndexUndo, IndexUndoKind, OwnedRowUndo};
use crate::trx::ActiveTrx;

pub struct Statement {
    pub trx: ActiveTrx,
    // statement-level undo logs of row data.
    pub row_undo: Vec<OwnedRowUndo>,
    // statement-level index undo operations.
    pub index_undo: Vec<IndexUndo>,
    // statement-level redo logs.
    pub redo: Vec<RedoEntry>,
}

impl Statement {
    #[inline]
    pub fn new(trx: ActiveTrx) -> Self {
        Statement {
            trx,
            row_undo: vec![],
            index_undo: vec![],
            redo: vec![],
        }
    }
}

impl Statement {
    #[inline]
    pub fn commit(mut self) -> ActiveTrx {
        self.trx.row_undo.extend(self.row_undo.drain(..));
        self.trx.index_undo.extend(self.index_undo.drain(..));
        self.trx.redo.extend(self.redo.drain(..));
        self.trx
    }

    /// Statement level rollback.
    #[inline]
    pub fn rollback<P: BufferPool>(mut self, buf_pool: &P, catalog: &Catalog<P>) -> ActiveTrx {
        // rollback row data.
        // todo: group by page level may be better.
        while let Some(entry) = self.row_undo.pop() {
            let page_guard: PageGuard<'_, RowPage> =
                buf_pool.get_page(entry.page_id, LatchFallbackMode::Shared);
            let page_guard = page_guard.block_until_shared();
            let row_idx = page_guard.page().row_idx(entry.row_id);
            let mut access = page_guard.write_row(row_idx);
            access.rollback_first_undo(entry);
        }
        // rollback index data.
        while let Some(entry) = self.index_undo.pop() {
            let table = catalog.get_table(entry.table_id).unwrap();
            match entry.kind {
                IndexUndoKind::Insert(key, row_id) => {
                    let res = table.sec_idx.delete(&key);
                    assert!(res.unwrap() == row_id);
                }
                IndexUndoKind::Update(key, old_id, new_id) => {
                    assert!(table.sec_idx.compare_exchange(&key, new_id, old_id));
                }
            }
        }
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
