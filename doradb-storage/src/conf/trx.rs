use crate::buffer::{EvictableBufferPool, FixedBufferPool, PoolGuards, ReadonlyBufferPool};
use crate::catalog::Catalog;
use crate::component::Supplier;
use crate::error::{ConfigError, ConfigResult, DataIntegrityError, Error, Result};
use crate::file::fs::FileSystem;
use crate::id::TrxID;
use crate::io::{Completion, align_to_sector_size};
use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
use crate::log::{LogSync, RedoLogInitializer};
use crate::quiescent::QuiescentGuard;
use crate::recovery::stream::RedoLogStream;
use crate::recovery::{RecoveryBuffers, RecoveryCoordinator, RecoveryResources};
use crate::trx::MAX_SNAPSHOT_TS;
use crate::trx::purge::Purge;
use crate::trx::sys::{
    TransactionSystem, TransactionSystemQueues, TransactionSystemWorkers,
    TransactionSystemWorkersOwned, TrxCleanupMessage,
};
use byte_unit::Byte;
use crossbeam_utils::CachePadded;
use error_stack::{Report, ResultExt};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::consts::{
    DEFAULT_LOG_BLOCK_SIZE, DEFAULT_LOG_DIR, DEFAULT_LOG_FILE_MAX_SIZE, DEFAULT_LOG_FILE_STEM,
    DEFAULT_LOG_IO_DEPTH, DEFAULT_LOG_SYNC, DEFAULT_PURGE_THREADS,
};
use super::path::{path_to_utf8, validate_log_file_stem};

use crate::log::discover_redo_log_files;

/// Configuration for redo logging, recovery, and transaction-system workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrxSysConfig {
    /// Inflight IO request depth of the redo log stream.
    pub io_depth: usize,
    /// Sector-aligned physical write size for fixed-block redo data.
    ///
    /// Every redo payload write uses exactly this size. Ordinary grouped
    /// transactions must fit in one block; one oversized transaction may span
    /// multiple fixed-block writes as a single logical redo group.
    pub log_block_size: Byte,
    /// Directory where redo log files live.
    pub log_dir: PathBuf,
    /// Base file name of one redo log family.
    ///
    /// The complete file name pattern is
    /// `<log-dir>/<log-file-stem>.<file-sequence>`.
    pub log_file_stem: String,
    /// Maximum size of each log file.
    ///
    /// Log files are rotated once the size limit is reached. A `u32` suffix is
    /// appended to the file name in hexadecimal format.
    pub log_file_max_size: Byte,
    /// Method used to sync log data to disk.
    pub log_sync: LogSync,
    /// Number of threads used to purge undo logs.
    pub purge_threads: usize,
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

impl TrxSysConfig {
    /// Set the redo-log IO queue depth.
    #[inline]
    pub fn io_depth(mut self, io_depth: usize) -> Self {
        self.io_depth = io_depth;
        self
    }

    /// Sector-aligned physical write size for fixed-block redo data.
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

    /// Borrow the configured redo-log directory.
    #[inline]
    pub(crate) fn log_dir_ref(&self) -> &Path {
        &self.log_dir
    }

    /// Borrow the configured redo-log file stem.
    #[inline]
    pub(crate) fn log_file_stem_ref(&self) -> &str {
        &self.log_file_stem
    }

    /// Build the redo-log file prefix from the configured directory and stem.
    #[inline]
    pub(crate) fn file_prefix(&self) -> ConfigResult<String> {
        let file_prefix = self.log_dir.join(&self.log_file_stem);
        Ok(path_to_utf8(&file_prefix, "redo log path")
            .attach_with(|| format!("invalid redo log path: {}", file_prefix.display()))?
            .to_owned())
    }

    /// Prepare redo-log recovery resources from the configured log layout.
    #[inline]
    pub(crate) fn prepare_recovery<'a>(
        &self,
        resources: RecoveryResources<'a>,
    ) -> Result<(RecoveryCoordinator<'a>, RedoLogInitializer)> {
        debug_assert!(validate_log_file_stem(&self.log_file_stem));
        let file_prefix = self.file_prefix().map_err(Error::from)?;
        let log_block_size = align_to_sector_size(self.log_block_size.as_u64() as usize);
        validate_redo_log_block_size(log_block_size)?;
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;

        let logs = discover_redo_log_files(&file_prefix, false)?;
        let stream = RedoLogStream::new(logs);
        let next_file_seq = stream.next_file_seq()?.unwrap_or(0);
        let initializer = RedoLogInitializer::new(
            file_prefix,
            self.io_depth,
            file_max_size,
            log_block_size,
            next_file_seq,
        )?;
        Ok((RecoveryCoordinator::new(resources, stream), initializer))
    }

    #[inline]
    fn normalize_redo_file_layout(mut self) -> Result<Self> {
        let log_block_size = align_to_sector_size(self.log_block_size.as_u64() as usize);
        validate_redo_log_block_size(log_block_size)?;
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;
        self.log_block_size = <Byte as From<usize>>::from(log_block_size);
        self.log_file_max_size = <Byte as From<usize>>::from(file_max_size);
        Ok(self)
    }

    /// Recover durable state and prepare transaction-system startup handles.
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
        let mem_pool_for_purge = mem_pool.clone();
        let recovery_buffers = RecoveryBuffers::new(meta_pool, index_pool, mem_pool, disk_pool);
        let pool_guards = recovery_buffers.pool_guards().clone();

        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let recovery_resources =
            RecoveryResources::new(recovery_buffers, table_fs.clone(), &catalog);
        let (coordinator, initializer) = config.prepare_recovery(recovery_resources)?;
        let (max_recovered_cts, initial_file_deletes) = coordinator.recover_all().await?;
        let initial_trx_ts = recovery_initial_trx_ts(max_recovered_cts)?;
        let (redo_log, initial_redo_header) = initializer.finish(purge_tx.clone())?;
        let redo_log = CachePadded::new(redo_log);

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
                mem_pool: mem_pool_for_purge,
                pool_guards,
            },
        ))
    }
}

/// Deferred transaction-system startup resources produced after recovery.
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
    /// Start transaction-system background workers after recovery is durable.
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
fn validate_redo_log_block_size(log_block_size: usize) -> Result<()> {
    const MAX_REDO_LOG_BLOCK_SIZE: usize = u16::MAX as usize + 1;
    if log_block_size <= MAX_REDO_LOG_BLOCK_SIZE {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidLogBlockSize)
        .attach(format!(
            "log_block_size={log_block_size}, max_supported={MAX_REDO_LOG_BLOCK_SIZE}"
        ))
        .into())
}

#[inline]
fn invalid_log_file_max_size() -> Error {
    Report::new(ConfigError::InvalidLogFileMaxSize)
        .attach("redo file max size cannot be represented after log-block alignment")
        .into()
}

#[inline]
fn recovery_initial_trx_ts(max_recovered_cts: TrxID) -> Result<TrxID> {
    max_recovered_cts
        .checked_add(1)
        .filter(|ts| *ts < MAX_SNAPSHOT_TS)
        .ok_or_else(|| {
            Error::from(
                Report::new(DataIntegrityError::LogFileCorrupted).attach(format!(
                    "recovered commit timestamp out of range: max_recovered_cts={max_recovered_cts}"
                )),
            )
        })
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
