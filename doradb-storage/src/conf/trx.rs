use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, GlobalReadonlyBufferPool, PoolGuards,
    PoolRole,
};
use crate::catalog::Catalog;
use crate::component::Supplier;
use crate::error::Result;
use crate::file::table_fs::TableFileSystem;
use crate::io::{StorageBackend, align_to_sector_size};
use crate::quiescent::QuiescentGuard;
use crate::trx::log::{LOG_HEADER_PAGES, LogPartitionInitializer, LogPartitionMode, LogSync};
use crate::trx::purge::{GC, Purge};
use crate::trx::recover::log_recover;
use crate::trx::sys::{TransactionSystem, TransactionSystemWorkers, TransactionSystemWorkersOwned};
use byte_unit::Byte;
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use super::consts::{
    DEFAULT_LOG_DIR, DEFAULT_LOG_FILE_MAX_SIZE, DEFAULT_LOG_FILE_STEM, DEFAULT_LOG_IO_DEPTH,
    DEFAULT_LOG_IO_MAX_SIZE, DEFAULT_LOG_PARTITIONS, DEFAULT_LOG_SYNC, DEFAULT_PURGE_THREADS,
    DEFAULT_SKIP_RECOVERY, MAX_LOG_PARTITIONS,
};
use super::path::{path_to_utf8, validate_log_file_stem};

use crate::trx::log::list_log_files;

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
    // <log-dir>/<log-file-stem>.<partition_idx>.<file-sequence>
    pub log_file_stem: String,
    // Log partition number.
    pub log_partitions: usize,
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
    // Controls whether to skip recovery when rebooting transaction system.
    pub skip_recovery: bool,
}

pub(crate) struct PendingTransactionSystem {
    pub(crate) trx_sys: TransactionSystem,
    startup: PendingTransactionSystemStartup,
}

pub(crate) struct PendingTransactionSystemStartup {
    gc_rxs: Vec<Receiver<GC>>,
    purge_tx: Sender<Purge>,
    purge_rx: Receiver<Purge>,
    mem_pool: QuiescentGuard<EvictableBufferPool>,
    pool_guards: PoolGuards,
}

impl PendingTransactionSystem {
    #[inline]
    pub(crate) fn into_parts(self) -> (TransactionSystem, PendingTransactionSystemStartup) {
        (self.trx_sys, self.startup)
    }
}

impl PendingTransactionSystemStartup {
    #[inline]
    pub(crate) fn start(
        self,
        trx_sys: QuiescentGuard<TransactionSystem>,
    ) -> TransactionSystemWorkersOwned {
        let io_threads = TransactionSystem::start_io_threads(trx_sys.clone());
        let gc_threads = TransactionSystem::start_gc_threads(
            trx_sys.clone(),
            self.gc_rxs,
            self.purge_tx.clone(),
        );
        let purge_threads = TransactionSystem::start_purge_threads(
            trx_sys.clone(),
            self.mem_pool,
            self.pool_guards,
            self.purge_rx,
        );
        TransactionSystemWorkersOwned {
            trx_sys: trx_sys.into_sync(),
            purge_tx: self.purge_tx,
            io_threads: parking_lot::Mutex::new(io_threads),
            gc_threads: parking_lot::Mutex::new(gc_threads),
            purge_threads: parking_lot::Mutex::new(purge_threads),
            shutdown_started: std::sync::atomic::AtomicBool::new(false),
        }
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

    /// Controls how many files can be used for concurrent logging.
    #[inline]
    pub fn log_partitions(mut self, log_partitions: usize) -> Self {
        assert!(log_partitions > 0 && log_partitions <= MAX_LOG_PARTITIONS);
        self.log_partitions = log_partitions;
        self
    }

    /// Sync method of log files.
    #[inline]
    pub fn log_sync(mut self, log_sync: LogSync) -> Self {
        self.log_sync = log_sync;
        self
    }

    #[inline]
    pub fn skip_recovery(mut self, skip_recovery: bool) -> Self {
        self.skip_recovery = skip_recovery;
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
    pub(crate) fn file_prefix(&self) -> Result<String> {
        let file_prefix = self.log_dir.join(&self.log_file_stem);
        Ok(path_to_utf8(&file_prefix, "redo log path")?.to_owned())
    }

    #[inline]
    pub fn log_partition_initializer(&self, log_no: usize) -> Result<LogPartitionInitializer> {
        debug_assert!(validate_log_file_stem(&self.log_file_stem));
        let ctx = StorageBackend::new(self.io_depth_per_log)?;
        let file_prefix = self.file_prefix()?;

        let mode = if self.skip_recovery {
            LogPartitionMode::Done
        } else {
            let logs = list_log_files(&file_prefix, log_no, false)?;
            if logs.is_empty() {
                LogPartitionMode::Done
            } else {
                LogPartitionMode::Recovery(VecDeque::from(logs))
            }
        };
        Ok(LogPartitionInitializer {
            ctx,
            mode,
            file_prefix,
            file_max_size: self.log_file_max_size.as_u64() as usize,
            page_size: self.max_io_size.as_u64() as usize,
            file_header_size: self.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
            io_depth_per_log: self.io_depth_per_log,
            log_no,
            file_seq: None,
        })
    }

    pub(crate) async fn prepare(
        self,
        meta_pool: QuiescentGuard<FixedBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        table_fs: QuiescentGuard<TableFileSystem>,
        global_disk_pool: QuiescentGuard<GlobalReadonlyBufferPool>,
        catalog: QuiescentGuard<Catalog>,
    ) -> Result<PendingTransactionSystem> {
        let mut log_partition_initializers = Vec::with_capacity(self.log_partitions);
        for idx in 0..self.log_partitions {
            let initializer = self.log_partition_initializer(idx)?;
            log_partition_initializers.push(initializer);
        }

        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_pool.pool_guard())
            .push(PoolRole::Index, index_pool.pool_guard())
            .push(PoolRole::Mem, mem_pool.pool_guard())
            .push(PoolRole::Disk, global_disk_pool.pool_guard())
            .build();

        let (log_partitions, gc_rxs) = log_recover(
            &meta_pool,
            crate::trx::recover::RecoveryDeps {
                index_pool,
                mem_pool: mem_pool.clone(),
                table_fs,
                global_disk_pool,
            },
            &catalog,
            log_partition_initializers,
            self.skip_recovery,
        )
        .await?;

        let (purge_tx, purge_rx) = flume::unbounded();
        let trx_sys = TransactionSystem::new(self, catalog, log_partitions);
        Ok(PendingTransactionSystem {
            trx_sys,
            startup: PendingTransactionSystemStartup {
                gc_rxs,
                purge_tx,
                purge_rx,
                mem_pool,
                pool_guards,
            },
        })
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
            log_partitions: DEFAULT_LOG_PARTITIONS,
            log_sync: DEFAULT_LOG_SYNC,
            purge_threads: DEFAULT_PURGE_THREADS,
            skip_recovery: DEFAULT_SKIP_RECOVERY,
        }
    }
}
