use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuards, PoolRole, ReadonlyBufferPool,
};
use crate::catalog::Catalog;
use crate::component::Supplier;
use crate::error::{ConfigResult, Error, Result};
use crate::file::fs::FileSystem;
use crate::io::{StorageBackend, align_to_sector_size};
use crate::quiescent::QuiescentGuard;
use crate::trx::log::{LOG_HEADER_PAGES, LogSync, RedoLogInitializer, RedoLogMode};
use crate::trx::purge::Purge;
use crate::trx::recover::{RecoveryDeps, log_recover};
use crate::trx::sys::{
    TransactionSystem, TransactionSystemQueues, TransactionSystemWorkers,
    TransactionSystemWorkersOwned, TrxCleanupMessage,
};
use byte_unit::Byte;
use error_stack::ResultExt;
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use super::consts::{
    DEFAULT_LOG_DIR, DEFAULT_LOG_FILE_MAX_SIZE, DEFAULT_LOG_FILE_STEM, DEFAULT_LOG_IO_DEPTH,
    DEFAULT_LOG_IO_MAX_SIZE, DEFAULT_LOG_SYNC, DEFAULT_PURGE_THREADS,
};
use super::path::{path_to_utf8, validate_log_file_stem};

use crate::trx::log::discover_redo_log_files;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrxSysConfig {
    // Controls infight IO request number of each log file.
    pub io_depth_per_log: usize,
    // Controls maximum IO size of each IO request.
    // This only limit the combination of multiple transactions.
    // If single transaction has very large redo log. It is kept
    // what it is and sent to the redo IO worker as one request.
    pub max_io_size: Byte,
    // Directory where redo log files live.
    pub log_dir: PathBuf,
    // Base file name of one redo log family.
    // the complete file name pattern is:
    // <log-dir>/<log-file-stem>.<file-sequence>
    pub log_file_stem: String,
    // Controls the maximum size of each log file.
    // Log file will be rotated once the size limit is reached.
    // a u32 suffix is appended at end of the file name in
    // hexdigit format.
    pub log_file_max_size: Byte,
    // Controls which method to sync data on disk.
    // By default, use fsync(),
    // Can be switched to fdatasync() or not sync.
    pub log_sync: LogSync,
    // Threads for purging undo logs.
    pub purge_threads: usize,
}

pub(crate) struct PendingTransactionSystemStartup {
    purge_tx: Sender<Purge>,
    purge_rx: Receiver<Purge>,
    cleanup_tx: Sender<TrxCleanupMessage>,
    cleanup_rx: Receiver<TrxCleanupMessage>,
    mem_pool: QuiescentGuard<EvictableBufferPool>,
    pool_guards: PoolGuards,
}

impl PendingTransactionSystemStartup {
    #[inline]
    pub(crate) fn start(
        self,
        trx_sys: QuiescentGuard<TransactionSystem>,
    ) -> TransactionSystemWorkersOwned {
        let io_thread = TransactionSystem::start_io_thread(trx_sys.clone());
        let purge_threads = TransactionSystem::start_purge_threads(
            trx_sys.clone(),
            self.mem_pool,
            self.pool_guards,
            self.purge_rx,
        );
        let cleanup_thread = TransactionSystem::start_cleanup_thread(self.cleanup_rx);
        TransactionSystemWorkersOwned::new(
            trx_sys.into_sync(),
            self.purge_tx,
            self.cleanup_tx,
            io_thread,
            purge_threads,
            cleanup_thread,
        )
    }
}

impl Supplier<TransactionSystemWorkers> for TransactionSystem {
    type Provision = PendingTransactionSystemStartup;
}

impl TrxSysConfig {
    #[inline]
    pub fn io_depth_per_log(mut self, io_depth_per_log: usize) -> Self {
        self.io_depth_per_log = io_depth_per_log;
        self
    }

    /// How large single IO operation can be.
    #[inline]
    pub fn max_io_size<T>(mut self, max_io_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_io_size = Byte::from(max_io_size);
        self
    }

    /// How many threads to execute for purge undo logs and remove
    /// unused index and row pages.
    #[inline]
    pub fn purge_threads(mut self, purge_threads: usize) -> Self {
        self.purge_threads = purge_threads;
        self
    }

    /// Redo log directory.
    #[inline]
    pub fn log_dir(mut self, log_dir: impl Into<PathBuf>) -> Self {
        self.log_dir = log_dir.into();
        self
    }

    /// Redo log base file name.
    #[inline]
    pub fn log_file_stem(mut self, log_file_stem: impl Into<String>) -> Self {
        self.log_file_stem = log_file_stem.into();
        self
    }

    /// Maximum size of single log file.
    #[inline]
    pub fn log_file_max_size<T>(mut self, log_file_max_size: T) -> Self
    where
        Byte: From<T>,
    {
        let size = Byte::from(log_file_max_size);
        let aligned_size = align_to_sector_size(size.as_u64() as usize);
        self.log_file_max_size = <Byte as From<usize>>::from(aligned_size);
        self
    }

    /// Sync method of log files.
    #[inline]
    pub fn log_sync(mut self, log_sync: LogSync) -> Self {
        self.log_sync = log_sync;
        self
    }

    #[inline]
    pub(crate) fn log_dir_ref(&self) -> &Path {
        &self.log_dir
    }

    #[inline]
    pub(crate) fn log_file_stem_ref(&self) -> &str {
        &self.log_file_stem
    }

    #[inline]
    pub(crate) fn file_prefix(&self) -> ConfigResult<String> {
        let file_prefix = self.log_dir.join(&self.log_file_stem);
        Ok(path_to_utf8(&file_prefix, "redo log path")
            .attach_with(|| format!("invalid redo log path: {}", file_prefix.display()))?
            .to_owned())
    }

    #[inline]
    pub(crate) fn redo_log_initializer(&self) -> Result<RedoLogInitializer> {
        debug_assert!(validate_log_file_stem(&self.log_file_stem));
        let ctx = StorageBackend::new(self.io_depth_per_log)?;
        let file_prefix = self.file_prefix().map_err(Error::from)?;

        let logs = discover_redo_log_files(&file_prefix, false)?;
        let mode = if logs.is_empty() {
            RedoLogMode::Done
        } else {
            RedoLogMode::Recovery(VecDeque::from(logs))
        };
        Ok(RedoLogInitializer {
            ctx,
            mode,
            file_prefix,
            file_max_size: self.log_file_max_size.as_u64() as usize,
            page_size: self.max_io_size.as_u64() as usize,
            file_header_size: self.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
            io_depth_per_log: self.io_depth_per_log,
            file_seq: None,
        })
    }

    pub(crate) async fn prepare(
        self,
        meta_pool: QuiescentGuard<FixedBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        table_fs: QuiescentGuard<FileSystem>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
        catalog: QuiescentGuard<Catalog>,
    ) -> Result<(TransactionSystem, PendingTransactionSystemStartup)> {
        let redo_log_initializer = self.redo_log_initializer()?;

        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_pool.pool_guard())
            .push(PoolRole::Index, index_pool.pool_guard())
            .push(PoolRole::Mem, mem_pool.pool_guard())
            .push(PoolRole::Disk, disk_pool.pool_guard())
            .build();

        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let (redo_log, initial_trx_ts, initial_file_deletes) = log_recover(
            &meta_pool,
            RecoveryDeps {
                index_pool,
                mem_pool: mem_pool.clone(),
                table_fs: table_fs.clone(),
                disk_pool,
            },
            &catalog,
            redo_log_initializer,
            purge_tx.clone(),
        )
        .await?;

        let trx_sys = TransactionSystem::new(
            self,
            catalog,
            table_fs,
            redo_log,
            initial_trx_ts,
            TransactionSystemQueues {
                purge_tx: purge_tx.clone(),
                cleanup_tx: cleanup_tx.clone(),
            },
            initial_file_deletes,
        );
        Ok((
            trx_sys,
            PendingTransactionSystemStartup {
                purge_tx,
                purge_rx,
                cleanup_tx,
                cleanup_rx,
                mem_pool,
                pool_guards,
            },
        ))
    }
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            io_depth_per_log: DEFAULT_LOG_IO_DEPTH,
            max_io_size: DEFAULT_LOG_IO_MAX_SIZE,
            log_dir: PathBuf::from(DEFAULT_LOG_DIR),
            log_file_stem: String::from(DEFAULT_LOG_FILE_STEM),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_sync: DEFAULT_LOG_SYNC,
            purge_threads: DEFAULT_PURGE_THREADS,
        }
    }
}
