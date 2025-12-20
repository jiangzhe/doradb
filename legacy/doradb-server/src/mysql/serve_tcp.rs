use crate::mysql::session::Session;
use crate::mysql::MySQLServer;
use async_executor::Executor;
use async_net::TcpListener;
use doradb_catalog::Catalog;
use doradb_protocol::mysql::conn::{Buf, MyConn};
use doradb_protocol::mysql::error::Result;
use doradb_protocol::mysql::serde::{SerdeCtx, SerdeMode};
use std::sync::atomic::Ordering;
use std::sync::Arc;

const DEFAULT_READ_BUF_SIZE: usize = 16 * 1024;
const DEFAULT_WRITE_BUF_SIZE: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub struct ServeTCP {
    pub(crate) addr: String,
    read_buf_size: usize,
    write_buf_size: usize,
}

impl ServeTCP {
    #[inline]
    pub fn new<T: Into<String>>(addr: T) -> Self {
        ServeTCP {
            addr: addr.into(),
            read_buf_size: DEFAULT_READ_BUF_SIZE,
            write_buf_size: DEFAULT_WRITE_BUF_SIZE,
        }
    }

    #[inline]
    pub fn read_buf_size(mut self, read_buf_size: usize) -> Self {
        self.read_buf_size = read_buf_size;
        self
    }

    #[inline]
    pub fn write_buf_size(mut self, write_buf_size: usize) -> Self {
        self.write_buf_size = write_buf_size;
        self
    }
}

impl<C: Catalog> MySQLServer<C> {
    #[inline]
    pub async fn serve_tcp<'a>(&'a self, opts: ServeTCP, ex: Arc<Executor<'a>>) -> Result<()> {
        log::debug!("start listen to {}", opts.addr);
        let listener = TcpListener::bind(&opts.addr).await?;
        loop {
            let (conn, peer_addr) = listener.accept().await?;
            let conn_id = self.conn_id_gen.fetch_add(1, Ordering::SeqCst);
            log::debug!(
                "Accept connection from {}, assign connection id {}",
                peer_addr,
                conn_id
            );
            let buf = Buf::new(opts.write_buf_size);
            let conn = MyConn::new(
                conn_id,
                conn,
                SerdeCtx::default().with_mode(SerdeMode::Server),
                buf,
            );
            let session = Session::new(self, conn);
            ex.spawn(session.start()).detach();
        }
    }
}
