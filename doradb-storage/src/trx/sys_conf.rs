use crate::buffer::BufferPool;
use crate::catalog::storage::CatalogStorage;
use crate::catalog::Catalog;
use crate::error::Result;
use crate::io::{align_to_sector_size, AIOManagerConfig};
use crate::lifetime::StaticLifetime;
use crate::trx::log::{LogPartitionInitializer, LogPartitionMode, LogSync, LOG_HEADER_PAGES};
use crate::trx::recover::log_recover;
use crate::trx::sys::TransactionSystem;
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::ops::Deref;

use super::log::list_log_files;

pub const DEFAULT_LOG_IO_DEPTH: usize = 32;
pub const DEFAULT_LOG_IO_MAX_SIZE: Byte = Byte::from_u64(8192);
pub const DEFAULT_LOG_FILE_PREFIX: &'static str = "redo.log";
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
        let aio_mgr = AIOManagerConfig::default()
            .max_events(self.io_depth_per_log)
            .build()?;

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
            aio_mgr,
            mode,
            file_prefix: self.log_file_prefix.clone(),
            file_max_size: self.log_file_max_size.as_u64() as usize,
            page_size: self.max_io_size.as_u64() as usize,
            file_header_size: self.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
            io_depth_per_log: self.io_depth_per_log,
            log_no,
            file_seq: None,
        })

        // // let mut file_seq = 0;
        // let log_file = create_log_file(
        //     &aio_mgr,
        //     &self.log_file_prefix,
        //     log_no,
        //     file_seq,
        //     self.log_file_max_size.as_u64() as usize,
        //     self.max_io_size.as_u64() as usize * LOG_HEADER_PAGES,
        // )?;
        // file_seq += 1;

        // let group_commit = GroupCommit {
        //     queue: VecDeque::new(),
        //     log_file: Some(log_file),
        // };
        // let max_io_size = self.max_io_size.as_u64() as usize;
        // let gc_info: Vec<_> = (0..GC_BUCKETS).map(|_| GCBucket::new()).collect();
        // let (gc_chan, gc_rx) = flume::unbounded();
        // (
        //     LogPartition {
        //         group_commit: CachePadded::new((Mutex::new(group_commit), Signal::default())),
        //         persisted_cts: CachePadded::new(AtomicU64::new(MIN_SNAPSHOT_TS)),
        //         stats: CachePadded::new(LogPartitionStats::default()),
        //         gc_chan,
        //         gc_buckets: gc_info.into_boxed_slice(),
        //         aio_mgr,
        //         log_no,
        //         max_io_size,
        //         file_prefix: self.log_file_prefix.clone(),
        //         file_seq: AtomicU32::new(file_seq),
        //         file_max_size: self.log_file_max_size.as_u64() as usize,
        //         buf_free_list: FreeListWithFactory::prefill(self.io_depth_per_log, move || {
        //             let mut buf = DirectBuf::page_zeroed(max_io_size);
        //             buf.set_data_len(0);
        //             buf
        //         }),
        //         io_thread: Mutex::new(None),
        //         gc_thread: Mutex::new(None),
        //     },
        //     gc_rx,
        // )
    }

    #[inline]
    pub fn build(self) -> TrxSysInitializer {
        TrxSysInitializer { config: self }
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

/// TrxSysInitializer initializes transaction system
/// with catalog initialization, log initialization
/// and log recovery.
/// todo: chceckpoint recovery.
pub struct TrxSysInitializer {
    config: TrxSysConfig,
}

impl Deref for TrxSysInitializer {
    type Target = TrxSysConfig;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl TrxSysInitializer {
    pub async fn init<P: BufferPool>(
        self,
        buf_pool: &'static P,
    ) -> Result<&'static TransactionSystem<P>> {
        let mut log_partition_initializers = Vec::with_capacity(self.log_partitions);
        for idx in 0..self.log_partitions {
            let initializer = self.log_partition_initializer(idx)?;
            log_partition_initializers.push(initializer);
        }

        let catalog_storage = CatalogStorage::new(buf_pool).await;
        let mut catalog = Catalog::new(catalog_storage);

        // Now we have an empty catalog, all log partitions and buffer pool.
        // Recover all committed data if required.
        let (log_partitions, gc_rxs) = log_recover(
            buf_pool,
            &mut catalog,
            log_partition_initializers,
            self.skip_recovery,
        )
        .await?;

        let (purge_chan, purge_rx) = flume::unbounded();
        let trx_sys = TransactionSystem::new(self.config, catalog, log_partitions, purge_chan);
        let trx_sys = StaticLifetime::new_static(trx_sys);

        trx_sys
            .catalog
            .enable_page_committer_for_all_tables(trx_sys);
        trx_sys.start_io_threads();
        trx_sys.start_gc_threads(gc_rxs);
        trx_sys.start_purge_threads(buf_pool, purge_rx);

        Ok(trx_sys)
    }
}
