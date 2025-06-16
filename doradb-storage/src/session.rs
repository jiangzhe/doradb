use crate::buffer::page::PageID;
use crate::engine::Engine;
use crate::row::RowID;
use crate::stmt::Statement;
use crate::table::TableID;
use crate::trx::ActiveTrx;
use std::collections::HashMap;

pub struct Session {
    pub(crate) engine: Engine,
    inner: Option<Box<InternalSession>>,
}

impl Session {
    #[inline]
    pub(crate) fn new(engine: Engine) -> Self {
        Session {
            engine,
            inner: Some(Box::new(InternalSession::new())),
        }
    }

    #[inline]
    pub fn with_internal_session(engine: Engine, inner: Box<InternalSession>) -> Self {
        Session {
            engine,
            inner: Some(inner),
        }
    }

    #[inline]
    pub fn load_active_insert_page(&mut self, table_id: TableID) -> Option<(PageID, RowID)> {
        self.inner
            .as_mut()
            .and_then(|inner| inner.load_active_insert_page(table_id))
    }

    #[inline]
    pub fn save_active_insert_page(&mut self, table_id: TableID, page_id: PageID, row_id: RowID) {
        self.inner
            .get_or_insert_default()
            .save_active_insert_page(table_id, page_id, row_id);
    }

    #[inline]
    pub fn begin_trx(self) -> ActiveTrx {
        self.engine.trx_sys.begin_trx(self)
    }
}

pub trait IntoSession: Sized {
    fn into_session(self) -> Option<Session>;

    fn split_session(&mut self) -> Option<Session>;
}

#[derive(Default)]
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

pub enum SessionState {
    // Idle state. The user session does not have any active transaction
    // and wait for user command.
    Idle(Session),
    // Active transaction. There is one active transaction in progress.
    ActiveTrx(ActiveTrx),
    // One statement is in progress.
    Statement(Statement),
}
