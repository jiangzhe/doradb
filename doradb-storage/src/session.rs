use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::engine::Engine;
use crate::error::Result;
use crate::row::RowID;
use crate::stmt::Statement;
use crate::table::TableID;
use crate::trx::ActiveTrx;
use std::collections::HashMap;

pub struct Session<P: BufferPool> {
    pub(crate) engine: &'static Engine<P>,
    inner: Option<Box<InternalSession>>,
}

impl<P: BufferPool> Session<P> {
    #[inline]
    pub(crate) fn new(engine: &'static Engine<P>) -> Self {
        Session {
            engine,
            inner: Some(Box::new(InternalSession::new())),
        }
    }

    #[inline]
    pub fn with_internal_session(engine: &'static Engine<P>, inner: Box<InternalSession>) -> Self {
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
    pub fn begin_trx(self) -> ActiveTrx<P> {
        self.engine.trx_sys.begin_trx(self)
    }
}

pub trait IntoSession<P: BufferPool>: Sized {
    fn into_session(self) -> Option<Session<P>>;

    fn split_session(&mut self) -> Option<Session<P>>;
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

pub enum SessionState<P: BufferPool> {
    // Idle state. The user session does not have any active transaction
    // and wait for user command.
    Idle(Session<P>),
    // Active transaction. There is one active transaction in progress.
    ActiveTrx(ActiveTrx<P>),
    // One statement is in progress.
    Statement(Statement<P>),
}

pub struct SessionWorker<P: BufferPool> {
    pub(crate) engine: &'static Engine<P>,
    state: SessionState<P>,
}

impl<P: BufferPool> SessionWorker<P> {
    #[inline]
    pub fn new(engine: &'static Engine<P>) -> Self {
        SessionWorker {
            engine,
            state: SessionState::Idle(Session::new(engine)),
        }
    }

    #[inline]
    pub async fn step(&mut self) -> Result<()> {
        match &mut self.state {
            SessionState::Idle(session) => {
                todo!("wait for user command")
            }
            SessionState::ActiveTrx(trx) => {
                todo!("wait for trx to finish")
            }
            SessionState::Statement(stmt) => {
                todo!("wait for stmt to finish")
            }
        }
    }

    #[inline]
    pub async fn run(&mut self) {
        loop {
            match self.step().await {
                Ok(_) => {}
                Err(e) => {
                    todo!("handle error: {:?}", e);
                }
            }
        }
    }
}
