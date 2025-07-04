use crate::buffer::page::PageID;
use crate::row::RowID;
use crate::trx::sys::TransactionSystem;
use doradb_catalog::TableID;

pub(super) struct ParentPosition<G> {
    pub(super) g: G,
    // -1 means lower fence in btree node.
    pub(super) idx: isize,
}

pub(super) struct RedoLogPageCommitter {
    trx_sys: &'static TransactionSystem,
    table_id: TableID,
}

impl Clone for RedoLogPageCommitter {
    #[inline]
    fn clone(&self) -> Self {
        RedoLogPageCommitter {
            trx_sys: self.trx_sys,
            table_id: self.table_id,
        }
    }
}

impl RedoLogPageCommitter {
    #[inline]
    pub fn new(trx_sys: &'static TransactionSystem, table_id: TableID) -> Self {
        RedoLogPageCommitter { trx_sys, table_id }
    }

    pub async fn commit_row_page(&self, page_id: PageID, start_row_id: RowID, end_row_id: RowID) {
        let mut trx = self.trx_sys.begin_sys_trx();
        let table_id = self.table_id;
        // Once a row page is added to block index, we start
        // a new internal transaction and log its information.
        trx.create_row_page(table_id, page_id, start_row_id, end_row_id);
        let res = self.trx_sys.commit_sys(trx).await;
        assert!(res.is_ok());
    }
}
