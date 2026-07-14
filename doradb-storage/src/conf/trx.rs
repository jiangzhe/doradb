use crate::buffer::PoolGuards;
use crate::catalog::Catalog;
use crate::component::{EnginePools, Supplier};
use crate::engine_poison::EnginePoisoner;
use crate::error::{ConfigError, ConfigResult, DataIntegrityError, Error, Result};
use crate::file::fs::FileSystem;
use crate::id::TrxID;
use crate::io::{Completion, STORAGE_SECTOR_SIZE, align_to_sector_size};
use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
use crate::log::{LogSync, RedoLogFinalizer};
use crate::obs;
use crate::quiescent::QuiescentGuard;
use crate::recovery::stream::RedoReplayPlanner;
use crate::recovery::{RecoveryCoordinator, RecoveryResources};
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
use std::panic::resume_unwind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::consts::{
    DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH, DEFAULT_GC_BUCKETS, DEFAULT_LOG_BLOCK_SIZE,
    DEFAULT_LOG_DIR, DEFAULT_LOG_FILE_MAX_SIZE, DEFAULT_LOG_FILE_STEM, DEFAULT_LOG_SYNC,
    DEFAULT_LOG_WRITE_IO_DEPTH, DEFAULT_PURGE_THREADS, DEFAULT_RECOVERY_IO_DEPTH,
};
use super::path::{path_to_utf8, validate_log_file_stem};

use crate::log::discover_redo_log_files;

const MAX_REDO_LOG_BLOCK_SIZE: usize = u16::MAX as usize + 1;

/// Configuration for redo logging, recovery, and transaction-system workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrxSysConfig {
    /// In-flight IO request depth of the live redo writer.
    pub log_write_io_depth: usize,
    /// Direct-IO read-ahead depth used during startup recovery.
    pub recovery_io_depth: usize,
    /// Direct-IO read-ahead depth used by catalog checkpoint redo scans.
    pub catalog_checkpoint_scan_io_depth: usize,
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
    /// Total number of threads that execute purge-bucket work.
    ///
    /// In multi-thread mode this includes the dispatcher-worker.
    pub purge_threads: usize,
    /// Number of buckets used to shard transaction GC tracking.
    ///
    /// Supported values are powers of two from 1 through 256. The bucket
    /// count is fixed for the lifetime of one engine instance and does not
    /// affect persistent storage formats.
    #[serde(default = "default_gc_buckets")]
    pub gc_buckets: usize,
    /// Disable DML payload validation during recovery/no-transaction replay.
    pub recovery_disable_dml_validation: bool,
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            log_write_io_depth: DEFAULT_LOG_WRITE_IO_DEPTH,
            recovery_io_depth: DEFAULT_RECOVERY_IO_DEPTH,
            catalog_checkpoint_scan_io_depth: DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH,
            log_block_size: DEFAULT_LOG_BLOCK_SIZE,
            log_dir: PathBuf::from(DEFAULT_LOG_DIR),
            log_file_stem: String::from(DEFAULT_LOG_FILE_STEM),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_sync: DEFAULT_LOG_SYNC,
            purge_threads: DEFAULT_PURGE_THREADS,
            gc_buckets: DEFAULT_GC_BUCKETS,
            recovery_disable_dml_validation: false,
        }
    }
}

impl TrxSysConfig {
    /// Set the live redo-writer IO queue depth.
    #[inline]
    pub fn log_write_io_depth(mut self, io_depth: usize) -> Self {
        self.log_write_io_depth = io_depth;
        self
    }

    /// Set the startup recovery direct-IO read-ahead depth.
    #[inline]
    pub fn recovery_io_depth(mut self, io_depth: usize) -> Self {
        self.recovery_io_depth = io_depth;
        self
    }

    /// Disable recovery/no-transaction DML shape, type, and nullability validation.
    #[inline]
    pub fn recovery_disable_dml_validation(mut self, disable: bool) -> Self {
        self.recovery_disable_dml_validation = disable;
        self
    }

    /// Set the catalog checkpoint redo-scan direct-IO read-ahead depth.
    #[inline]
    pub fn catalog_checkpoint_scan_io_depth(mut self, io_depth: usize) -> Self {
        self.catalog_checkpoint_scan_io_depth = io_depth;
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

    /// Set the total number of threads that execute purge-bucket work.
    ///
    /// In multi-thread mode this includes the dispatcher-worker.
    #[inline]
    pub fn purge_threads(mut self, purge_threads: usize) -> Self {
        self.purge_threads = purge_threads;
        self
    }

    /// Set the number of buckets used to shard transaction GC tracking.
    ///
    /// Supported values are powers of two from 1 through 256.
    #[inline]
    pub fn gc_buckets(mut self, gc_buckets: usize) -> Self {
        self.gc_buckets = gc_buckets;
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
    ) -> Result<RecoveryCoordinator<'a>> {
        debug_assert!(validate_log_file_stem(&self.log_file_stem));
        let file_prefix = self.file_prefix().map_err(Error::from)?;
        let configured_log_block_size = self.log_block_size.as_u64() as usize;
        validate_redo_log_block_size(configured_log_block_size)?;
        let log_block_size = align_to_sector_size(configured_log_block_size);
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;

        let first_retained_file_seq = resources
            .catalog
            .storage
            .checkpoint_snapshot()?
            .meta
            .first_redo_log_seq;
        let logs = discover_redo_log_files(&file_prefix, first_retained_file_seq, false)?;
        let planner = RedoReplayPlanner::new(logs);
        let finalizer = RedoLogFinalizer::new(
            file_prefix,
            self.log_write_io_depth,
            file_max_size,
            log_block_size,
            0,
        );
        Ok(RecoveryCoordinator::new(
            resources,
            planner,
            self.recovery_io_depth,
            self.recovery_disable_dml_validation,
            finalizer,
        ))
    }

    #[inline]
    fn normalize_redo_file_layout(mut self) -> Result<Self> {
        validate_purge_threads(self.purge_threads)?;
        validate_gc_buckets(self.gc_buckets)?;
        validate_redo_io_depth("log_write_io_depth", self.log_write_io_depth)?;
        validate_redo_io_depth("recovery_io_depth", self.recovery_io_depth)?;
        validate_redo_io_depth(
            "catalog_checkpoint_scan_io_depth",
            self.catalog_checkpoint_scan_io_depth,
        )?;
        let configured_log_block_size = self.log_block_size.as_u64() as usize;
        validate_redo_log_block_size(configured_log_block_size)?;
        let log_block_size = align_to_sector_size(configured_log_block_size);
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;
        self.log_block_size = <Byte as From<usize>>::from(log_block_size);
        self.log_file_max_size = <Byte as From<usize>>::from(file_max_size);
        Ok(self)
    }

    /// Recover durable state and prepare transaction-system startup handles.
    pub(crate) async fn prepare(
        self,
        engine_poisoner: QuiescentGuard<EnginePoisoner>,
        pools: EnginePools,
        table_fs: QuiescentGuard<FileSystem>,
        catalog: QuiescentGuard<Catalog>,
    ) -> Result<(TransactionSystem, PendingTransactionSystemStartup)> {
        let config = self.normalize_redo_file_layout()?;
        let pool_guards = pools.pool_guards();

        let (purge_tx, purge_rx) = flume::unbounded();
        let (cleanup_tx, cleanup_rx) = flume::unbounded();
        let recovery_resources = RecoveryResources::new(pools, table_fs.clone(), &catalog);
        let coordinator = config.prepare_recovery(recovery_resources)?;
        let (max_recovered_cts, finalizer) = coordinator.recover_all().await?;
        let initial_trx_ts = recovery_initial_trx_ts(max_recovered_cts)?;
        let (redo_log, initial_redo_header) = finalizer.finalize(purge_tx.clone())?;
        let redo_log = CachePadded::new(redo_log);

        let trx_sys = TransactionSystem::new(
            config,
            engine_poisoner,
            catalog,
            table_fs,
            redo_log,
            initial_trx_ts,
            TransactionSystemQueues {
                purge_tx: purge_tx.clone(),
                cleanup_tx: cleanup_tx.clone(),
            },
        );
        Ok((
            trx_sys,
            PendingTransactionSystemStartup {
                initial_redo_header,
                purge_tx,
                purge_rx,
                cleanup_tx,
                cleanup_rx,
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
            if let Err(payload) = log_thread.join().inspect_err(|_| {
                obs::error!(
                    "event=worker_shutdown component=trx worker=Log-Thread action=join result=error reason=panic"
                );
            }) {
                // Known startup redo failures are returned through the header
                // completion above. A log-thread panic means an invariant broke,
                // so report its position and keep the original panic payload.
                resume_unwind(payload);
            }
            return Err(Error::from_completion_report(
                report,
                "wait for initial redo super-block write",
            ));
        }
        let purge_threads = TransactionSystem::start_purge_threads(
            trx_sys.clone(),
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
fn validate_redo_io_depth(field: &'static str, io_depth: usize) -> Result<()> {
    if io_depth != 0 {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidIoDepth)
        .attach(format!("{field}=0"))
        .into())
}

#[inline]
fn validate_purge_threads(purge_threads: usize) -> Result<()> {
    const MIN_PURGE_THREADS: usize = 1;
    if purge_threads >= MIN_PURGE_THREADS {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidPurgeThreads)
        .attach(format!(
            "purge_threads={purge_threads}, min_supported={MIN_PURGE_THREADS}"
        ))
        .into())
}

#[inline]
const fn default_gc_buckets() -> usize {
    DEFAULT_GC_BUCKETS
}

#[inline]
fn validate_gc_buckets(gc_buckets: usize) -> Result<()> {
    const MIN_GC_BUCKETS: usize = 1;
    const MAX_GC_BUCKETS: usize = 256;
    if (MIN_GC_BUCKETS..=MAX_GC_BUCKETS).contains(&gc_buckets) && gc_buckets.is_power_of_two() {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidGcBuckets)
        .attach(format!(
            "gc_buckets={gc_buckets}, min_supported={MIN_GC_BUCKETS}, max_supported={MAX_GC_BUCKETS}, requirement=power_of_two"
        ))
        .into())
}

#[inline]
fn validate_redo_log_block_size(log_block_size: usize) -> Result<()> {
    if (STORAGE_SECTOR_SIZE..=MAX_REDO_LOG_BLOCK_SIZE).contains(&log_block_size) {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidLogBlockSize)
        .attach(format!(
            "log_block_size={log_block_size}, min_supported={STORAGE_SECTOR_SIZE}, max_supported={MAX_REDO_LOG_BLOCK_SIZE}"
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

    fn assert_invalid_log_block_size(err: Error) {
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::InvalidLogBlockSize)
        );
    }

    fn assert_invalid_io_depth(err: Error) {
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::InvalidIoDepth)
        );
    }

    fn assert_invalid_purge_threads(err: Error) {
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::InvalidPurgeThreads)
        );
        let report = err.to_string();
        assert!(report.contains("purge_threads=0"), "report={report}");
        assert!(report.contains("min_supported=1"), "report={report}");
    }

    fn assert_invalid_gc_buckets(err: Error, gc_buckets: usize) {
        assert!(err.is_kind(crate::error::ErrorKind::Config));
        assert_eq!(
            err.report().downcast_ref::<ConfigError>().copied(),
            Some(ConfigError::InvalidGcBuckets)
        );
        let report = err.to_string();
        assert!(
            report.contains(&format!("gc_buckets={gc_buckets}")),
            "report={report}"
        );
        assert!(report.contains("min_supported=1"), "report={report}");
        assert!(report.contains("max_supported=256"), "report={report}");
        assert!(report.contains("power_of_two"), "report={report}");
    }

    fn assert_normalize_rejects_invalid_io_depth(config: TrxSysConfig) {
        let err = match config.normalize_redo_file_layout() {
            Ok(_) => panic!("zero redo IO depth must be rejected"),
            Err(err) => err,
        };
        assert_invalid_io_depth(err);
    }

    #[test]
    fn redo_io_depth_defaults_are_split_but_preserved() {
        let config = TrxSysConfig::default();

        assert_eq!(config.log_write_io_depth, DEFAULT_LOG_WRITE_IO_DEPTH);
        assert_eq!(config.recovery_io_depth, DEFAULT_RECOVERY_IO_DEPTH);
        assert_eq!(
            config.catalog_checkpoint_scan_io_depth,
            DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH
        );
        assert_eq!(config.log_write_io_depth, 32);
        assert_eq!(config.recovery_io_depth, 32);
        assert_eq!(config.catalog_checkpoint_scan_io_depth, 32);
        assert_eq!(config.gc_buckets, DEFAULT_GC_BUCKETS);
        assert_eq!(config.gc_buckets, 32);
        assert!(!config.recovery_disable_dml_validation);
    }

    #[test]
    fn redo_io_depth_builders_are_independent() {
        let config = TrxSysConfig::default()
            .log_write_io_depth(2)
            .recovery_io_depth(3)
            .catalog_checkpoint_scan_io_depth(4);

        assert_eq!(config.log_write_io_depth, 2);
        assert_eq!(config.recovery_io_depth, 3);
        assert_eq!(config.catalog_checkpoint_scan_io_depth, 4);
    }

    #[test]
    fn recovery_dml_validation_builder_sets_flag() {
        let config = TrxSysConfig::default().recovery_disable_dml_validation(true);
        assert!(config.recovery_disable_dml_validation);
    }

    #[test]
    fn normalize_rejects_zero_purge_threads() {
        let err = TrxSysConfig::default()
            .purge_threads(0)
            .normalize_redo_file_layout()
            .expect_err("zero purge threads must be rejected");
        assert_invalid_purge_threads(err);
    }

    #[test]
    fn normalize_accepts_positive_purge_thread_counts() {
        for purge_threads in [1, DEFAULT_PURGE_THREADS, 65] {
            let config = TrxSysConfig::default()
                .purge_threads(purge_threads)
                .normalize_redo_file_layout()
                .unwrap();
            assert_eq!(config.purge_threads, purge_threads);
        }
    }

    #[test]
    fn normalize_accepts_supported_gc_bucket_counts() {
        for gc_buckets in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
            let config = TrxSysConfig::default()
                .gc_buckets(gc_buckets)
                .normalize_redo_file_layout()
                .unwrap();
            assert_eq!(config.gc_buckets, gc_buckets);
        }
    }

    #[test]
    fn normalize_rejects_unsupported_gc_bucket_counts() {
        for gc_buckets in [0, 3, 255, 257, usize::MAX] {
            let err = TrxSysConfig::default()
                .gc_buckets(gc_buckets)
                .normalize_redo_file_layout()
                .expect_err("unsupported GC bucket count must be rejected");
            assert_invalid_gc_buckets(err, gc_buckets);
        }
    }

    #[test]
    fn legacy_serialized_config_defaults_gc_buckets() {
        let serialized = toml::to_string(&TrxSysConfig::default()).unwrap();
        assert!(serialized.contains("gc_buckets = 32"));
        let legacy = serialized
            .lines()
            .filter(|line| !line.starts_with("gc_buckets ="))
            .collect::<Vec<_>>()
            .join("\n");

        let config: TrxSysConfig = toml::from_str(&legacy).unwrap();
        assert_eq!(config.gc_buckets, DEFAULT_GC_BUCKETS);
    }

    #[test]
    fn normalize_redo_file_layout_rejects_zero_redo_io_depths() {
        assert_normalize_rejects_invalid_io_depth(TrxSysConfig::default().log_write_io_depth(0));
        assert_normalize_rejects_invalid_io_depth(TrxSysConfig::default().recovery_io_depth(0));
        assert_normalize_rejects_invalid_io_depth(
            TrxSysConfig::default().catalog_checkpoint_scan_io_depth(0),
        );
    }

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
    fn normalize_redo_file_layout_rejects_direct_zero_log_block_size() {
        let config = TrxSysConfig {
            log_block_size: Byte::from(0usize),
            ..TrxSysConfig::default()
        };

        let err = match config.normalize_redo_file_layout() {
            Ok(_) => panic!("zero redo log block size must be rejected"),
            Err(err) => err,
        };

        assert_invalid_log_block_size(err);
    }

    #[test]
    fn normalize_redo_file_layout_rejects_direct_too_large_log_block_size() {
        let config = TrxSysConfig {
            log_block_size: Byte::from(MAX_REDO_LOG_BLOCK_SIZE + STORAGE_SECTOR_SIZE),
            ..TrxSysConfig::default()
        };

        let err = match config.normalize_redo_file_layout() {
            Ok(_) => panic!("oversized redo log block size must be rejected"),
            Err(err) => err,
        };

        assert_invalid_log_block_size(err);
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
