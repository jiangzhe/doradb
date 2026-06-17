use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuards, PoolRole, ReadonlyBufferPool,
};
use crate::catalog::Catalog;
use crate::component::Supplier;
use crate::error::{ConfigError, ConfigResult, Error, Result};
use crate::file::fs::FileSystem;
use crate::io::{Completion, StorageBackend, align_to_sector_size};
use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
use crate::log::recover::{RecoveryDeps, log_recover};
use crate::log::{LogSync, RedoLogInitializer, RedoLogMode};
use crate::quiescent::QuiescentGuard;
use crate::trx::purge::Purge;
use crate::trx::sys::{
    TransactionSystem, TransactionSystemQueues, TransactionSystemWorkers,
    TransactionSystemWorkersOwned, TrxCleanupMessage,
};
use byte_unit::Byte;
use error_stack::{Report, ResultExt};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::consts::{
    DEFAULT_LOG_BLOCK_SIZE, DEFAULT_LOG_DIR, DEFAULT_LOG_FILE_MAX_SIZE, DEFAULT_LOG_FILE_STEM,
    DEFAULT_LOG_IO_DEPTH, DEFAULT_LOG_SYNC, DEFAULT_PURGE_THREADS,
};
use super::path::{path_to_utf8, validate_log_file_stem};

use crate::log::discover_redo_log_files;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrxSysConfig {
    // Controls inflight IO request depth of the redo log stream.
    pub io_depth: usize,
    // Controls the sector-aligned log block size for small-group IO.
    // This only limits grouped transactions. If a single transaction has a very
    // large redo log, it is kept as-is and submitted by the log thread as one
    // request.
    pub log_block_size: Byte,
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
    initial_redo_header: Arc<Completion<()>>,
    purge_tx: Sender<Purge>,
    purge_rx: Receiver<Purge>,
    cleanup_tx: Sender<TrxCleanupMessage>,
    cleanup_rx: Receiver<TrxCleanupMessage>,
    mem_pool: QuiescentGuard<EvictableBufferPool>,
    pool_guards: PoolGuards,
}

impl PendingTransactionSystemStartup {
    #[inline]
    pub(crate) async fn start(
        self,
        trx_sys: QuiescentGuard<TransactionSystem>,
    ) -> Result<TransactionSystemWorkersOwned> {
        // Start the log thread first because it owns the redo write driver and
        // must process the initial redo header write submitted during recovery.
        let log_thread = TransactionSystem::start_log_thread(trx_sys.clone());
        // Wait until the initial header is durable before exposing transaction
        // workers; later redo records must not be accepted until the active log
        // file has a valid super-block.
        if let Err(report) = self.initial_redo_header.wait_result().await {
            log_thread.join().unwrap();
            return Err(Error::from_completion_report(
                report,
                "wait for initial redo super-block write",
            ));
        }
        let purge_threads = TransactionSystem::start_purge_threads(
            trx_sys.clone(),
            self.mem_pool,
            self.pool_guards,
            self.purge_rx,
        );
        let cleanup_thread = TransactionSystem::start_cleanup_thread(self.cleanup_rx);
        Ok(TransactionSystemWorkersOwned::new(
            trx_sys.into_sync(),
            self.purge_tx,
            self.cleanup_tx,
            log_thread,
            purge_threads,
            cleanup_thread,
        ))
    }
}

impl Supplier<TransactionSystemWorkers> for TransactionSystem {
    type Provision = PendingTransactionSystemStartup;
}

impl TrxSysConfig {
    #[inline]
    pub fn io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    /// Sector-aligned log block size for small redo groups.
    #[inline]
    pub fn log_block_size<T>(mut self, log_block_size: T) -> Self
    where
        Byte: From<T>,
    {
        let size = Byte::from(log_block_size);
        let aligned_size = align_to_sector_size(size.as_u64() as usize);
        self.log_block_size = <Byte as From<usize>>::from(aligned_size);
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
        let ctx = StorageBackend::new(self.io_depth)?;
        let file_prefix = self.file_prefix().map_err(Error::from)?;
        let log_block_size = align_to_sector_size(self.log_block_size.as_u64() as usize);
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;

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
            file_max_size,
            log_block_size,
            io_depth: self.io_depth,
            file_seq: None,
        })
    }

    #[inline]
    fn normalize_redo_file_layout(mut self) -> Result<Self> {
        let log_block_size = align_to_sector_size(self.log_block_size.as_u64() as usize);
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;
        self.log_block_size = <Byte as From<usize>>::from(log_block_size);
        self.log_file_max_size = <Byte as From<usize>>::from(file_max_size);
        Ok(self)
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
        let config = self.normalize_redo_file_layout()?;
        let redo_log_initializer = config.redo_log_initializer()?;

        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_pool.pool_guard())
            .push(PoolRole::Index, index_pool.pool_guard())
            .push(PoolRole::Mem, mem_pool.pool_guard())
            .push(PoolRole::Disk, disk_pool.pool_guard())
            .build();

        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let (redo_log, initial_trx_ts, initial_file_deletes, initial_redo_header) = log_recover(
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
            config,
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
                initial_redo_header,
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

#[inline]
fn normalize_redo_file_max_size(
    requested_file_max_size: usize,
    log_block_size: usize,
) -> Result<usize> {
    let min_file_max_size = REDO_DEFAULT_DATA_START_OFFSET
        .checked_add(log_block_size)
        .ok_or_else(invalid_log_file_max_size)?;
    let requested_file_max_size = requested_file_max_size.max(min_file_max_size);
    let data_region_len = requested_file_max_size - REDO_DEFAULT_DATA_START_OFFSET;
    let block_count = data_region_len.div_ceil(log_block_size);
    let normalized_data_region_len = block_count
        .checked_mul(log_block_size)
        .ok_or_else(invalid_log_file_max_size)?;
    REDO_DEFAULT_DATA_START_OFFSET
        .checked_add(normalized_data_region_len)
        .ok_or_else(invalid_log_file_max_size)
}

#[inline]
fn invalid_log_file_max_size() -> Error {
    Report::new(ConfigError::InvalidLogFileMaxSize)
        .attach("redo file max size cannot be represented after log-block alignment")
        .into()
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            io_depth: DEFAULT_LOG_IO_DEPTH,
            log_block_size: DEFAULT_LOG_BLOCK_SIZE,
            log_dir: PathBuf::from(DEFAULT_LOG_DIR),
            log_file_stem: String::from(DEFAULT_LOG_FILE_STEM),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_sync: DEFAULT_LOG_SYNC,
            purge_threads: DEFAULT_PURGE_THREADS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::STORAGE_SECTOR_SIZE;

    #[test]
    fn log_block_size_rounds_below_sector_to_sector_size() {
        let config = TrxSysConfig::default().log_block_size(1usize);

        assert_eq!(config.log_block_size.as_u64(), STORAGE_SECTOR_SIZE as u64);
    }

    #[test]
    fn log_block_size_preserves_sector_aligned_size() {
        let config = TrxSysConfig::default().log_block_size(STORAGE_SECTOR_SIZE);

        assert_eq!(config.log_block_size.as_u64(), STORAGE_SECTOR_SIZE as u64);
    }

    #[test]
    fn log_block_size_rounds_above_sector_to_next_sector() {
        let config = TrxSysConfig::default().log_block_size(STORAGE_SECTOR_SIZE + 1);

        assert_eq!(
            config.log_block_size.as_u64(),
            (STORAGE_SECTOR_SIZE * 2) as u64
        );
    }

    #[test]
    fn log_file_max_size_normalizes_below_one_data_block() {
        let log_block_size = STORAGE_SECTOR_SIZE * 2;
        let normalized = normalize_redo_file_max_size(1, log_block_size).unwrap();

        assert_eq!(normalized, REDO_DEFAULT_DATA_START_OFFSET + log_block_size);
    }

    #[test]
    fn log_file_max_size_normalizes_data_region_to_log_block() {
        let log_block_size = STORAGE_SECTOR_SIZE * 2;
        let requested = REDO_DEFAULT_DATA_START_OFFSET + log_block_size + STORAGE_SECTOR_SIZE;
        let normalized = normalize_redo_file_max_size(requested, log_block_size).unwrap();

        assert_eq!(
            normalized,
            REDO_DEFAULT_DATA_START_OFFSET + log_block_size * 2
        );
    }

    #[test]
    fn log_file_max_size_preserves_aligned_data_region() {
        let log_block_size = STORAGE_SECTOR_SIZE * 2;
        let requested = REDO_DEFAULT_DATA_START_OFFSET + log_block_size * 2;
        let normalized = normalize_redo_file_max_size(requested, log_block_size).unwrap();

        assert_eq!(normalized, requested);
    }

    #[test]
    fn normalize_redo_file_layout_updates_config_values() {
        let log_block_size = STORAGE_SECTOR_SIZE * 2;
        let config = TrxSysConfig::default()
            .log_block_size(log_block_size)
            .log_file_max_size(REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE)
            .normalize_redo_file_layout()
            .unwrap();

        assert_eq!(config.log_block_size.as_u64(), log_block_size as u64);
        assert_eq!(
            config.log_file_max_size.as_u64(),
            (REDO_DEFAULT_DATA_START_OFFSET + log_block_size) as u64
        );
    }
}
