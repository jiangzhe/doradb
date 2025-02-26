use crate::buffer::page::PageID;
use crate::row::RowID;
use crate::table::TableID;
use crate::trx::sys::TransactionSystem;
use crate::trx::ActiveTrx;
use std::collections::HashMap;

pub struct Session {
    inner: Option<Box<InternalSession>>,
}

impl Session {
    #[inline]
    pub fn new() -> Session {
        Session {
            inner: Some(Box::new(InternalSession::new())),
        }
    }

    #[inline]
    pub fn with_internal_session(inner: Box<InternalSession>) -> Self {
        Session { inner: Some(inner) }
    }

    #[inline]
    pub fn begin_trx(mut self, trx_sys: &TransactionSystem) -> ActiveTrx {
        trx_sys.new_trx(self.inner.take())
    }
}

pub trait IntoSession: Sized {
    fn into_session(self) -> Session;

    fn split_session(&mut self) -> Session;
}

pub struct InternalSession {
    active_insert_pages: HashMap<TableID, (PageID, RowID)>,
}

impl InternalSession {
    #[inline]
    pub fn new() -> Self {
        InternalSession {
            active_insert_pages: HashMap::new(),
        }
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
