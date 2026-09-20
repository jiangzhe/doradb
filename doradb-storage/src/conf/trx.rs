use crate::error::{ConfigError, ConfigResult};
use crate::io::{STORAGE_SECTOR_SIZE, align_to_sector_size};
use crate::log::LogSync;
use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
use byte_unit::Byte;
use error_stack::{Report, ResultExt};
use std::path::{Path, PathBuf};

use super::consts::{
    DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH, DEFAULT_GC_BUCKETS, DEFAULT_LOG_BLOCK_SIZE,
    DEFAULT_LOG_DIR, DEFAULT_LOG_FILE_MAX_SIZE, DEFAULT_LOG_FILE_STEM, DEFAULT_LOG_SYNC,
    DEFAULT_LOG_WRITE_IO_DEPTH, DEFAULT_PURGE_THREADS,
};
use super::path::{path_to_utf8, validate_log_file_stem};

const MAX_REDO_LOG_BLOCK_SIZE: usize = u16::MAX as usize + 1;

/// Configuration for redo logging and transaction-system workers.
#[derive(Debug, Clone)]
pub struct TrxSysConfig {
    /// In-flight IO request depth of the live redo writer.
    pub log_write_io_depth: usize,
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
    pub gc_buckets: usize,
}

impl Default for TrxSysConfig {
    #[inline]
    fn default() -> Self {
        TrxSysConfig {
            log_write_io_depth: DEFAULT_LOG_WRITE_IO_DEPTH,
            catalog_checkpoint_scan_io_depth: DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH,
            log_block_size: DEFAULT_LOG_BLOCK_SIZE,
            log_dir: PathBuf::from(DEFAULT_LOG_DIR),
            log_file_stem: String::from(DEFAULT_LOG_FILE_STEM),
            log_file_max_size: DEFAULT_LOG_FILE_MAX_SIZE,
            log_sync: DEFAULT_LOG_SYNC,
            purge_threads: DEFAULT_PURGE_THREADS,
            gc_buckets: DEFAULT_GC_BUCKETS,
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
        Ok(path_to_utf8(&file_prefix)
            .attach_with(|| format!("invalid redo log path: {}", file_prefix.display()))?
            .to_owned())
    }

    /// Validate and normalize transaction-system configuration in place.
    #[inline]
    pub(crate) fn validate(&mut self) -> ConfigResult<()> {
        validate_purge_threads(self.purge_threads)?;
        validate_gc_buckets(self.gc_buckets)?;
        validate_redo_io_depth(self.log_write_io_depth).attach("invalid log_write_io_depth")?;
        validate_redo_io_depth(self.catalog_checkpoint_scan_io_depth)
            .attach("invalid catalog_checkpoint_scan_io_depth")?;
        if !validate_log_file_stem(&self.log_file_stem) {
            return Err(Report::new(ConfigError::InvalidLogFileStem).attach(format!(
                "log file stem must be a plain file name without glob characters: {}",
                self.log_file_stem
            )));
        }
        self.file_prefix()?;

        let configured_log_block_size = self.log_block_size.as_u64() as usize;
        validate_redo_log_block_size(configured_log_block_size)?;
        let log_block_size = align_to_sector_size(configured_log_block_size);
        let file_max_size =
            normalize_redo_file_max_size(self.log_file_max_size.as_u64() as usize, log_block_size)?;
        self.log_block_size = <Byte as From<usize>>::from(log_block_size);
        self.log_file_max_size = <Byte as From<usize>>::from(file_max_size);
        Ok(())
    }
}

/// Validated transaction configuration and its resolved redo-file prefix.
///
/// Public engine bootstrap constructs this wrapper while Config disclosure is
/// still owned at the public boundary. Transaction-system components consume
/// it without reopening configuration-domain failure paths.
pub(crate) struct ValidatedTrxSysConfig {
    config: TrxSysConfig,
    file_prefix: String,
}

impl ValidatedTrxSysConfig {
    /// Validate, normalize, and resolve one transaction-system configuration.
    #[inline]
    pub(crate) fn try_new(mut config: TrxSysConfig) -> ConfigResult<Self> {
        config.validate()?;
        let file_prefix = config.file_prefix()?;
        Ok(Self {
            config,
            file_prefix,
        })
    }

    /// Consume the wrapper into normalized configuration and resolved prefix.
    #[inline]
    pub(crate) fn into_parts(self) -> (TrxSysConfig, String) {
        (self.config, self.file_prefix)
    }
}

#[inline]
fn normalize_redo_file_max_size(
    requested_file_max_size: usize,
    log_block_size: usize,
) -> ConfigResult<usize> {
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
fn validate_redo_io_depth(io_depth: usize) -> ConfigResult<()> {
    if io_depth != 0 {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidIoDepth).attach("io_depth=0"))
}

#[inline]
fn validate_purge_threads(purge_threads: usize) -> ConfigResult<()> {
    const MIN_PURGE_THREADS: usize = 1;
    if purge_threads >= MIN_PURGE_THREADS {
        return Ok(());
    }
    Err(
        Report::new(ConfigError::InvalidPurgeThreads).attach(format!(
            "purge_threads={purge_threads}, min_supported={MIN_PURGE_THREADS}"
        )),
    )
}

#[inline]
fn validate_gc_buckets(gc_buckets: usize) -> ConfigResult<()> {
    const MIN_GC_BUCKETS: usize = 1;
    const MAX_GC_BUCKETS: usize = 256;
    if (MIN_GC_BUCKETS..=MAX_GC_BUCKETS).contains(&gc_buckets) && gc_buckets.is_power_of_two() {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidGcBuckets)
        .attach(format!(
            "gc_buckets={gc_buckets}, min_supported={MIN_GC_BUCKETS}, max_supported={MAX_GC_BUCKETS}, requirement=power_of_two"
        )))
}

#[inline]
fn validate_redo_log_block_size(log_block_size: usize) -> ConfigResult<()> {
    if (STORAGE_SECTOR_SIZE..=MAX_REDO_LOG_BLOCK_SIZE).contains(&log_block_size) {
        return Ok(());
    }
    Err(Report::new(ConfigError::InvalidLogBlockSize)
        .attach(format!(
            "log_block_size={log_block_size}, min_supported={STORAGE_SECTOR_SIZE}, max_supported={MAX_REDO_LOG_BLOCK_SIZE}"
        )))
}

#[inline]
fn invalid_log_file_max_size() -> Report<ConfigError> {
    Report::new(ConfigError::InvalidLogFileMaxSize)
        .attach("redo file max size cannot be represented after log-block alignment")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid_purge_threads(err: Report<ConfigError>) {
        assert_eq!(err.current_context(), &ConfigError::InvalidPurgeThreads);
        let report = format!("{err:?}");
        assert!(report.contains("purge_threads=0"), "report={report}");
        assert!(report.contains("min_supported=1"), "report={report}");
    }

    fn assert_invalid_gc_buckets(err: Report<ConfigError>, gc_buckets: usize) {
        assert_eq!(err.current_context(), &ConfigError::InvalidGcBuckets);
        let report = format!("{err:?}");
        assert!(
            report.contains(&format!("gc_buckets={gc_buckets}")),
            "report={report}"
        );
        assert!(report.contains("min_supported=1"), "report={report}");
        assert!(report.contains("max_supported=256"), "report={report}");
        assert!(report.contains("power_of_two"), "report={report}");
    }

    fn assert_validate_rejects_invalid_io_depth(mut config: TrxSysConfig, field: &str) {
        let err = config
            .validate()
            .expect_err("zero redo IO depth must be rejected");
        assert_eq!(
            err.current_context(),
            &ConfigError::InvalidIoDepth,
            "{field}"
        );
        let diagnostic = format!("{err:?}");
        assert!(
            diagnostic.contains(&format!("invalid {field}")),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("io_depth=0"), "{diagnostic}");
    }

    /// Purpose: Preserve default sizing for separate redo IO queues and transaction GC buckets.
    /// Expected: Default fields retain the established queue depths and bucket count.
    #[test]
    fn redo_io_depth_defaults_are_split_but_preserved() {
        let config = TrxSysConfig::default();

        assert_eq!(config.log_write_io_depth, DEFAULT_LOG_WRITE_IO_DEPTH);
        assert_eq!(
            config.catalog_checkpoint_scan_io_depth,
            DEFAULT_CATALOG_CHECKPOINT_SCAN_IO_DEPTH
        );
        assert_eq!(config.log_write_io_depth, 32);
        assert_eq!(config.catalog_checkpoint_scan_io_depth, 32);
        assert_eq!(config.gc_buckets, DEFAULT_GC_BUCKETS);
        assert_eq!(config.gc_buckets, 32);
    }

    /// Purpose: Keep redo-writer and catalog-checkpoint queue-depth builders independent.
    /// Expected: Updating either queue leaves the other unchanged and preserves explicit overrides.
    #[test]
    fn redo_io_depth_builders_are_independent() {
        for (case, config, writer_depth, scan_depth) in [
            (
                "writer only",
                TrxSysConfig::default().log_write_io_depth(2),
                2,
                32,
            ),
            (
                "scan only",
                TrxSysConfig::default().catalog_checkpoint_scan_io_depth(4),
                32,
                4,
            ),
            (
                "writer then scan",
                TrxSysConfig::default()
                    .log_write_io_depth(2)
                    .catalog_checkpoint_scan_io_depth(4),
                2,
                4,
            ),
            (
                "scan then writer",
                TrxSysConfig::default()
                    .catalog_checkpoint_scan_io_depth(4)
                    .log_write_io_depth(2),
                2,
                4,
            ),
        ] {
            assert_eq!(config.log_write_io_depth, writer_depth, "{case}");
            assert_eq!(
                config.catalog_checkpoint_scan_io_depth, scan_depth,
                "{case}"
            );
        }
    }

    /// Purpose: Reject transaction configurations without a purge worker.
    /// Expected: Validation reports the purge-thread error with the rejected value and lower bound.
    #[test]
    fn validate_rejects_zero_purge_threads() {
        let mut config = TrxSysConfig::default().purge_threads(0);
        let err = config
            .validate()
            .expect_err("zero purge threads must be rejected");
        assert_invalid_purge_threads(err);
    }

    /// Purpose: Accept positive purge-worker counts beyond the default configuration.
    /// Expected: Validation preserves each supported worker count.
    #[test]
    fn validate_accepts_positive_purge_thread_counts() {
        for purge_threads in [1, DEFAULT_PURGE_THREADS, 65] {
            let mut config = TrxSysConfig::default().purge_threads(purge_threads);
            config.validate().unwrap();
            assert_eq!(config.purge_threads, purge_threads);
        }
    }

    /// Purpose: Accept power-of-two GC bucket counts throughout the supported range.
    /// Expected: Validation preserves each supported bucket count, including both boundaries.
    #[test]
    fn validate_accepts_supported_gc_bucket_counts() {
        for gc_buckets in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
            let mut config = TrxSysConfig::default().gc_buckets(gc_buckets);
            config.validate().unwrap();
            assert_eq!(config.gc_buckets, gc_buckets);
        }
    }

    /// Purpose: Reject GC bucket counts outside the supported power-of-two range.
    /// Expected: Errors identify the rejected count, supported bounds, and power-of-two requirement.
    #[test]
    fn validate_rejects_unsupported_gc_bucket_counts() {
        for gc_buckets in [0, 3, 255, 257, usize::MAX] {
            let mut config = TrxSysConfig::default().gc_buckets(gc_buckets);
            let err = config
                .validate()
                .expect_err("unsupported GC bucket count must be rejected");
            assert_invalid_gc_buckets(err, gc_buckets);
        }
    }

    /// Purpose: Reject unusable queue depths for either redo IO role.
    /// Expected: Each zero-depth configuration reports the IO-depth error and its owning field.
    #[test]
    fn validate_rejects_zero_redo_io_depths() {
        assert_validate_rejects_invalid_io_depth(
            TrxSysConfig::default().log_write_io_depth(0),
            "log_write_io_depth",
        );
        assert_validate_rejects_invalid_io_depth(
            TrxSysConfig::default().catalog_checkpoint_scan_io_depth(0),
            "catalog_checkpoint_scan_io_depth",
        );
    }

    /// Purpose: Reject glob metacharacters in redo log file stems.
    /// Expected: Validation returns the log-file-stem configuration error.
    #[test]
    fn validate_rejects_invalid_log_file_stem() {
        let mut config = TrxSysConfig::default().log_file_stem("redo*.log");
        let err = config
            .validate()
            .expect_err("glob characters in redo log stem must be rejected");

        assert_eq!(err.current_context(), &ConfigError::InvalidLogFileStem);
    }

    /// Purpose: Normalize redo block sizes around a storage-sector boundary.
    /// Expected: Unaligned requests round up while aligned requests remain unchanged.
    #[test]
    fn log_block_size_normalizes_sector_boundaries() {
        for (case, requested, expected) in [
            ("below_sector", 1, STORAGE_SECTOR_SIZE),
            ("aligned", STORAGE_SECTOR_SIZE, STORAGE_SECTOR_SIZE),
            (
                "above_sector",
                STORAGE_SECTOR_SIZE + 1,
                STORAGE_SECTOR_SIZE * 2,
            ),
        ] {
            let config = TrxSysConfig::default().log_block_size(requested);
            assert_eq!(
                config.log_block_size.as_u64(),
                expected as u64,
                "{case}: requested={requested}"
            );
        }
    }

    /// Purpose: Reject unsupported redo block sizes assigned without the builder.
    /// Expected: Validation reports the block-size configuration error and rejected value.
    #[test]
    fn validate_rejects_direct_invalid_log_block_sizes() {
        for (case, requested) in [
            ("zero", 0),
            ("below_sector", STORAGE_SECTOR_SIZE - 1),
            ("oversized", MAX_REDO_LOG_BLOCK_SIZE + STORAGE_SECTOR_SIZE),
        ] {
            let mut config = TrxSysConfig {
                log_block_size: Byte::from(requested),
                ..TrxSysConfig::default()
            };
            let err = config.validate().expect_err(case);
            assert_eq!(
                err.current_context(),
                &ConfigError::InvalidLogBlockSize,
                "{case}: requested={requested}"
            );
            let diagnostic = format!("{err:?}");
            assert!(
                diagnostic.contains(&format!("log_block_size={requested}")),
                "{case}: {diagnostic}"
            );
        }
    }

    /// Purpose: Normalize redo file limits around data-block boundaries after metadata.
    /// Expected: Limits reserve metadata and complete data blocks, preserving already aligned sizes.
    #[test]
    fn log_file_max_size_normalizes_data_region_boundaries() {
        let log_block_size = STORAGE_SECTOR_SIZE * 2;
        for (case, requested, expected) in [
            (
                "below_one_data_block",
                1,
                REDO_DEFAULT_DATA_START_OFFSET + log_block_size,
            ),
            (
                "partial_data_block",
                REDO_DEFAULT_DATA_START_OFFSET + log_block_size + STORAGE_SECTOR_SIZE,
                REDO_DEFAULT_DATA_START_OFFSET + log_block_size * 2,
            ),
            (
                "aligned_data_region",
                REDO_DEFAULT_DATA_START_OFFSET + log_block_size * 2,
                REDO_DEFAULT_DATA_START_OFFSET + log_block_size * 2,
            ),
        ] {
            let normalized = normalize_redo_file_max_size(requested, log_block_size).expect(case);
            assert_eq!(normalized, expected, "{case}: requested={requested}");
        }
    }

    /// Purpose: Reject redo file limits whose aligned size cannot be represented.
    /// Expected: Unrepresentable results return the file-size configuration error instead of wrapping.
    #[test]
    fn log_file_max_size_rejects_unrepresentable_alignment() {
        for (case, log_block_size) in [
            ("ordinary_blocks", STORAGE_SECTOR_SIZE * 2),
            ("maximum_blocks", MAX_REDO_LOG_BLOCK_SIZE),
        ] {
            let err = normalize_redo_file_max_size(usize::MAX, log_block_size).expect_err(case);
            assert_eq!(
                err.current_context(),
                &ConfigError::InvalidLogFileMaxSize,
                "{case}"
            );
        }
    }

    /// Purpose: Apply redo file-layout normalization through transaction configuration validation.
    /// Expected: Validation preserves the block size and expands the file for metadata and data.
    #[test]
    fn validate_normalizes_redo_file_layout() {
        let log_block_size = STORAGE_SECTOR_SIZE * 2;
        let mut config = TrxSysConfig::default()
            .log_block_size(log_block_size)
            .log_file_max_size(REDO_DEFAULT_DATA_START_OFFSET + STORAGE_SECTOR_SIZE);
        config.validate().unwrap();

        assert_eq!(config.log_block_size.as_u64(), log_block_size as u64);
        assert_eq!(
            config.log_file_max_size.as_u64(),
            (REDO_DEFAULT_DATA_START_OFFSET + log_block_size) as u64
        );
    }
}
