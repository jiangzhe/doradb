use crate::buffer::page::PageID;
use crate::row::RowID;
use crate::table::TableID;
use crate::trx::sys::TransactionSystem;
use crate::trx::ActiveTrx;
use flume::{Receiver, Sender};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Session {
    active_insert_pages: HashMap<TableID, (PageID, RowID)>,
    abort_signal: Arc<Mutex<Option<Sender<()>>>>,
    abort_notifier: Receiver<()>,
}

impl Session {
    #[inline]
    pub fn new() -> Self {
        let (tx, rx) = flume::unbounded();
        Session {
            active_insert_pages: HashMap::new(),
            abort_signal: Arc::new(Mutex::new(Some(tx))),
            abort_notifier: rx,
        }
    }

    #[inline]
    pub fn begin_trx(&mut self, trx_sys: &TransactionSystem) -> ActiveTrx {
        trx_sys.new_trx(self)
    }

    #[inline]
    pub fn load_active_insert_page(&mut self, table_id: TableID) -> Option<(PageID, RowID)> {
        self.active_insert_pages.remove(&table_id)
    }

    #[inline]
    pub fn save_active_insert_page(&mut self, table_id: TableID, page_id: PageID, row_id: RowID) {
        let res = self.active_insert_pages.insert(table_id, (page_id, row_id));
        debug_assert!(res.is_none());
    }
}
