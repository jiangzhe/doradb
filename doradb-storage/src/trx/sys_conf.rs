use crate::buffer::{BufferPool, FixedBufferPool};
use crate::catalog::Catalog;
use crate::catalog::storage::CatalogStorage;
use crate::error::Result;
use crate::file::table_fs::TableFileSystem;
use crate::io::{AIOContext, align_to_sector_size};
use crate::lifetime::StaticLifetime;
use crate::trx::log::{LOG_HEADER_PAGES, LogPartitionInitializer, LogPartitionMode, LogSync};
use crate::trx::recover::log_recover;
use crate::trx::sys::TransactionSystem;
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::Path;

use super::log::list_log_files;

pub const DEFAULT_LOG_IO_DEPTH: usize = 32;
pub const DEFAULT_LOG_IO_MAX_SIZE: Byte = Byte::from_u64(8192);
pub const DEFAULT_LOG_FILE_PREFIX: &str = "redo.log";
pub const DEFAULT_LOG_PARTITIONS: usize = 1;
pub const MAX_LOG_PARTITIONS: usize = 99; // big enough for log partitions, so fix two digits in file name.
pub const DEFAULT_LOG_FILE_MAX_SIZE: Byte = Byte::from_u64(1024 * 1024 * 1024); // 1GB, sparse file will not occupy space until actual write.
pub const DEFAULT_LOG_SYNC: LogSync = LogSync::Fsync;
pub const DEFAULT_PURGE_THREADS: usize = 2;
pub const DEFAULT_SKIP_RECOVERY: bool = false;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrxSysConfig {
    // Controls infight IO request number of each log file.
    pub io_depth_per_log: usize,
    // Controls maximum IO size of each IO request.
    // This only limit the combination of multiple transactions.
    // If single transaction has very large redo log. It is kept
    // what it is and send to AIO manager as one IO request.
    pub max_io_size: Byte,
    // Prefix of log file.
    // the complete file name pattern is:
    // <file-prefix>.<partition_idx>.<file-sequence>
    // e.g. redo.log.0.00000001
    pub log_file_prefix: String,
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

impl TrxSysConfig {
    #[inline]
    pub fn with_main_dir(mut self, main_dir: impl AsRef<Path>) -> Self {
        let path = main_dir.as_ref().join(&self.log_file_prefix);
        self.log_file_prefix = path.to_string_lossy().to_string();
        self
    }

    /// How many commits can be issued concurrently.
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

    /// Log file name.
    #[inline]
    pub fn log_file_prefix(mut self, log_file_prefix: impl Into<String>) -> Self {
        self.log_file_prefix = log_file_prefix.into();
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
    pub fn log_partition_initializer(&self, log_no: usize) -> Result<LogPartitionInitializer> {
        let ctx = AIOContext::new(self.io_depth_per_log)?;

        // determine whether we should recovery from previous logs.
        let mode = if self.skip_recovery {
            LogPartitionMode::Done
        } else {
            let logs = list_log_files(&self.log_file_prefix, log_no, false)?;
            if logs.is_empty() {
                LogPartitionMode::Done
            } else {
                LogPartitionMode::Recovery(VecDeque::from(logs))
            }
        };
        Ok(LogPartitionInitializer {
            ctx,
            mode,
            file_prefix: self.log_file_prefix.clone(),
            file_max_size: self.log_file_max_size.as_u64() as usize,
            page_size: self.max_io_size.as_u64() as usize,
            file_header_size: self.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
            io_depth_per_log: self.io_depth_per_log,
            log_no,
            file_seq: None,
        })
    }

    pub async fn build_static<P: BufferPool>(
        self,
        meta_pool: &'static FixedBufferPool,
        index_pool: &'static FixedBufferPool,
        data_pool: &'static P,
        table_fs: &'static TableFileSystem,
    ) -> Result<&'static TransactionSystem> {
        let mut log_partition_initializers = Vec::with_capacity(self.log_partitions);
        for idx in 0..self.log_partitions {
            let initializer = self.log_partition_initializer(idx)?;
            log_partition_initializers.push(initializer);
        }

        let catalog_storage = CatalogStorage::new(meta_pool, index_pool, table_fs).await?;
        let mut catalog = Catalog::new(catalog_storage);

        // Now we have an empty catalog, all log partitions and buffer pool.
        // Recover all committed data if required.
        let (log_partitions, gc_rxs) = log_recover(
            index_pool,
            data_pool,
            table_fs,
            &mut catalog,
            log_partition_initializers,
            self.skip_recovery,
        )
        .await?;

        let (purge_chan, purge_rx) = flume::unbounded();
        let trx_sys = TransactionSystem::new(self, catalog, log_partitions, purge_chan);
        let trx_sys = StaticLifetime::new_static(trx_sys);

        trx_sys
            .catalog
            .enable_page_committer_for_tables(trx_sys)
            .await;
        trx_sys.start_io_threads();
        trx_sys.start_gc_threads(gc_rxs);
        trx_sys.start_purge_threads(data_pool, purge_rx);

        Ok(trx_sys)
    }
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            io_depth_per_log: DEFAULT_LOG_IO_DEPTH,
            max_io_size: DEFAULT_LOG_IO_MAX_SIZE,
            log_file_prefix: String::from(DEFAULT_LOG_FILE_PREFIX),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_partitions: DEFAULT_LOG_PARTITIONS,
            log_sync: DEFAULT_LOG_SYNC,
            purge_threads: DEFAULT_PURGE_THREADS,
            skip_recovery: DEFAULT_SKIP_RECOVERY,
        }
    }
}
