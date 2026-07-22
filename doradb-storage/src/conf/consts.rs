use crate::log::LogSync;
use byte_unit::Byte;

/// Default bytes reserved for the fully resident metadata pool.
pub(crate) const DEFAULT_ENGINE_META_BUFFER: usize = 32 * 1024 * 1024;
/// Default in-memory budget for the user secondary-index buffer pool.
pub(crate) const DEFAULT_ENGINE_INDEX_BUFFER: usize = 1024 * 1024 * 1024;
/// Default ephemeral swap file for the user secondary-index buffer pool.
pub(crate) const DEFAULT_ENGINE_INDEX_SWAP_FILE: &str = "index.swp";
/// Default on-disk size cap for the user secondary-index swap file.
pub(crate) const DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE: usize = 2 * 1024 * 1024 * 1024;

/// Default async IO depth for the table-file subsystem worker.
pub(crate) const DEFAULT_TABLE_FILE_IO_DEPTH: usize = 64;
/// Default relative data directory for durable table and catalog files.
pub(crate) const DEFAULT_TABLE_FILE_DATA_DIR: &str = ".";
/// Default size of the shared readonly cache used for persisted table pages.
pub(crate) const DEFAULT_TABLE_FILE_READONLY_BUFFER_SIZE: usize = 256 * 1024 * 1024;
/// Default unified catalog multi-table file name under `data_dir`.
pub(crate) const DEFAULT_CATALOG_FILE_NAME: &str = "catalog.mtb";

/// Required suffix for ephemeral swap files managed by evictable buffer pools.
pub(crate) const SWAP_FILE_SUFFIX: &str = ".swp";
/// Default ephemeral swap file name for a generic evictable buffer pool.
pub(crate) const DEFAULT_EVICTABLE_BUFFER_POOL_DATA_SWAP_FILE: &str = "data.swp";
/// Default sparse-file size cap for a generic evictable buffer pool.
pub(crate) const DEFAULT_EVICTABLE_BUFFER_POOL_MAX_FILE_SIZE: Byte =
    Byte::from_u64(2 * 1024 * 1024 * 1024);
/// Default in-memory budget for a generic evictable buffer pool.
pub(crate) const DEFAULT_EVICTABLE_BUFFER_POOL_MAX_MEM_SIZE: Byte =
    Byte::from_u64(1024 * 1024 * 1024);

/// Default async IO depth allocated to live redo-log writes.
pub const DEFAULT_LOG_WRITE_IO_DEPTH: usize = 32;
/// Default direct-IO read-ahead depth allocated to startup recovery.
pub const DEFAULT_RECOVERY_IO_DEPTH: usize = 32;
/// Default direct-IO read-ahead depth allocated to catalog checkpoint scans.
pub const DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH: usize = 32;
/// Default upper bound for one redo-log IO request payload.
pub const DEFAULT_LOG_BLOCK_SIZE: Byte = Byte::from_u64(4096);
/// Default relative directory where redo log files live.
pub const DEFAULT_LOG_DIR: &str = ".";
/// Default base file name for the redo-log family.
pub const DEFAULT_LOG_FILE_STEM: &str = "redo.log";
/// Default sparse-file size cap for each redo-log file.
pub const DEFAULT_LOG_FILE_MAX_SIZE: Byte = Byte::from_u64(1024 * 1024 * 1024);
/// Default durability mode used when flushing redo-log writes.
pub const DEFAULT_LOG_SYNC: LogSync = LogSync::Fsync;
/// Default number of purge-bucket worker threads, including the dispatcher-worker.
pub const DEFAULT_PURGE_THREADS: usize = 2;
/// Default number of transaction GC buckets.
pub const DEFAULT_GC_BUCKETS: usize = 32;
