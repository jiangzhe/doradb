pub mod serve_tcp;
pub mod session;
pub mod signal;

use crate::mysql::serve_tcp::ServeTCP;
use async_executor::Executor;
use async_io::block_on;
use doradb_catalog::Catalog;
use doradb_protocol::mysql::error::{Error, Result};
use doradb_protocol::mysql::ServerSpec;
use easy_parallel::Parallel;
use flume::{Receiver, Sender};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

const DEFAULT_SERVER_THREADS: usize = 1;

pub struct MySQLServer<C: Catalog> {
    spec: ServerSpec,
    catalog: C,
    worker_threads: usize,
    // auto-incremental connection id, starts from 1.
    conn_id_gen: AtomicU32,
    stop_signal: Option<Receiver<()>>,
}

impl<C: Catalog> MySQLServer<C> {
    #[inline]
    pub fn new(catalog: C) -> Self {
        MySQLServer {
            spec: ServerSpec::default(),
            catalog,
            worker_threads: DEFAULT_SERVER_THREADS,
            conn_id_gen: AtomicU32::new(1),
            stop_signal: None,
        }
    }

    /// Signal to notify if server is stopped.
    #[inline]
    pub fn stop_signal(&self) -> Result<Receiver<()>> {
        self.stop_signal
            .as_ref()
            .cloned()
            .ok_or(Error::ServerNotStarted())
    }

    pub fn start<F>(self, opts: ServeTCP, process_stop_signal: F) -> Result<()>
    where
        F: FnOnce(Sender<()>) -> Result<()>,
    {
        let (stop_signal, stop_notify) = flume::unbounded();
        // dedicate one thread to handle signal and stop server gracefully.
        process_stop_signal(stop_signal)?;
        // setup worker threads
        let ex = Arc::new(Executor::new());
        let worker_threads = self.worker_threads;
        let (_, res) = Parallel::new()
            .each(0..worker_threads, |i| {
                log::debug!("start worker thread {}", i);
                block_on(ex.run(stop_notify.recv_async()))
            })
            .finish(|| {
                log::debug!("start tcp serving at {}", opts.addr);
                let serve_tcp = self.serve_tcp(opts, Arc::clone(&ex));
                ex.spawn(serve_tcp).detach();

                assert!(block_on(ex.run(stop_notify.recv_async())).is_err());
                Ok(())
            });
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::serve_tcp::ServeTCP;
    use async_io::{block_on, Timer};
    use doradb_catalog::mem_impl::MemCatalog;
    use doradb_protocol::buf::ByteBuffer;
    use doradb_protocol::mysql::conn::TcpClientOpts;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_server_start() -> Result<()> {
        let catalog = MemCatalog::default();
        let (tx, rx) = flume::unbounded::<()>();
        let handle = thread::spawn(move || {
            let server = MySQLServer::new(catalog);
            let _ = server.start(ServeTCP::new("127.0.0.1:23306"), |signal| {
                thread::spawn(move || {
                    let _ = rx.recv();
                    drop(signal);
                });
                Ok(())
            });
        });
        thread::sleep(Duration::from_millis(100));

        block_on(async {
            let read_buf = ByteBuffer::with_capacity(4096);
            let mut tries = 0;
            loop {
                let _ = Timer::after(Duration::from_millis(100)).await;
                if let Ok(_) = TcpClientOpts::new("127.0.0.1:23306")
                    .username("root")
                    .password("password")
                    .connect(&read_buf, 4096, None)
                    .await
                {
                    break;
                }
                tries += 1;
                if tries >= 5 {
                    return Err(Error::IOError(std::io::ErrorKind::ConnectionRefused));
                }
            }
            Ok(())
        })?;
        drop(tx);
        handle.join().unwrap();
        Ok(())
    }
}
