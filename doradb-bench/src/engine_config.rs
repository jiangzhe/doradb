use crate::error::{BenchError, Result};
use byte_unit::Byte;
use doradb_storage::{EngineConfig, EvictableBufferPoolConfig, LogSync};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Strict, field-wise engine configuration overlay used by benchmark plans.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfigOverlay {
    /// Finite sync/async thread-pool sizing overrides.
    pub thread_pool: ThreadPoolOverlay,
    /// Mandatory runtime sizing overrides.
    pub mandatory_runtime: MandatoryRuntimeOverlay,
    /// Deterministic table-scan planning overrides.
    pub table_scan: TableScanConfigOverlay,
    /// Transaction-system overrides.
    pub transaction: TransactionConfigOverlay,
    /// Startup recovery overrides.
    pub recovery: RecoveryConfigOverlay,
    /// Metadata buffer-pool size.
    pub meta_buffer_size: Option<Byte>,
    /// User-index buffer-pool overrides.
    pub index_buffer: EvictableBufferPoolConfigOverlay,
    /// Data buffer-pool overrides.
    pub data_buffer: EvictableBufferPoolConfigOverlay,
    /// Table/catalog filesystem overrides.
    pub file: FileSystemConfigOverlay,
}

impl EngineConfigOverlay {
    /// Apply every set leaf from `other`, retaining unaffected sibling leaves.
    #[inline]
    pub fn merge(&mut self, other: Self) {
        self.thread_pool.merge(other.thread_pool);
        self.mandatory_runtime.merge(other.mandatory_runtime);
        self.table_scan.merge(other.table_scan);
        self.transaction.merge(other.transaction);
        self.recovery.merge(other.recovery);
        replace(&mut self.meta_buffer_size, other.meta_buffer_size);
        self.index_buffer.merge(other.index_buffer);
        self.data_buffer.merge(other.data_buffer);
        self.file.merge(other.file);
    }
}

/// Strict deterministic table-scan planning overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TableScanConfigOverlay {
    /// Persisted LWC blocks per homogeneous partition.
    pub lwc_blocks_per_partition: Option<usize>,
    /// Hot row pages per homogeneous partition.
    pub row_pages_per_partition: Option<usize>,
}

impl TableScanConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(
            &mut self.lwc_blocks_per_partition,
            other.lwc_blocks_per_partition,
        );
        replace(
            &mut self.row_pages_per_partition,
            other.row_pages_per_partition,
        );
    }
}

/// Strict finite sync/async thread-pool overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ThreadPoolOverlay {
    /// Fixed worker-thread count shared by sync and async jobs.
    pub worker_threads: Option<usize>,
}

impl ThreadPoolOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.worker_threads, other.worker_threads);
    }
}

/// Strict mandatory-runtime overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MandatoryRuntimeOverlay {
    /// Accepted caller-operation concurrency limit.
    pub concurrency_limit: Option<usize>,
}

impl MandatoryRuntimeOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.concurrency_limit, other.concurrency_limit);
    }
}

/// Strict transaction-system overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransactionConfigOverlay {
    /// Live redo-writer I/O depth.
    pub log_write_io_depth: Option<usize>,
    /// Catalog-checkpoint redo-scan I/O depth.
    pub catalog_checkpoint_scan_io_depth: Option<usize>,
    /// Redo block size.
    pub log_block_size: Option<Byte>,
    /// Redo directory relative to the storage root.
    pub log_dir: Option<PathBuf>,
    /// Redo log-family file stem.
    pub log_file_stem: Option<String>,
    /// Maximum redo file size.
    pub log_file_max_size: Option<Byte>,
    /// Redo durability mode.
    pub log_sync: Option<LogSyncValue>,
    /// Purge worker-thread count.
    pub purge_threads: Option<usize>,
    /// Transaction GC bucket count.
    pub gc_buckets: Option<usize>,
}

impl TransactionConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.log_write_io_depth, other.log_write_io_depth);
        replace(
            &mut self.catalog_checkpoint_scan_io_depth,
            other.catalog_checkpoint_scan_io_depth,
        );
        replace(&mut self.log_block_size, other.log_block_size);
        replace(&mut self.log_dir, other.log_dir);
        replace(&mut self.log_file_stem, other.log_file_stem);
        replace(&mut self.log_file_max_size, other.log_file_max_size);
        replace(&mut self.log_sync, other.log_sync);
        replace(&mut self.purge_threads, other.purge_threads);
        replace(&mut self.gc_buckets, other.gc_buckets);
    }
}

/// Strict startup recovery overlay; omitted limits use storage defaults.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecoveryConfigOverlay {
    /// Startup redo read-ahead depth.
    pub io_depth: Option<usize>,
    /// Disable catalog and row replay DML validation.
    pub disable_dml_validation: Option<bool>,
    /// Positive outstanding-batch override; omission preserves automatic sizing.
    pub max_in_flight_batches: Option<usize>,
    /// Positive active-page override; omission preserves automatic sizing.
    pub max_active_pages: Option<usize>,
    /// Maximum operations in one replay batch.
    pub max_batch_ops: Option<usize>,
    /// Used batch storage flush target, expressed as a byte-size string.
    pub target_batch_bytes: Option<Byte>,
    /// Idle batch capacity cap; zero disables recycling.
    pub max_recycled_bytes: Option<Byte>,
}

impl RecoveryConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.io_depth, other.io_depth);
        replace(
            &mut self.disable_dml_validation,
            other.disable_dml_validation,
        );
        replace(&mut self.max_in_flight_batches, other.max_in_flight_batches);
        replace(&mut self.max_active_pages, other.max_active_pages);
        replace(&mut self.max_batch_ops, other.max_batch_ops);
        replace(&mut self.target_batch_bytes, other.target_batch_bytes);
        replace(&mut self.max_recycled_bytes, other.max_recycled_bytes);
    }
}

/// Strict evictable buffer-pool overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvictableBufferPoolConfigOverlay {
    /// Swap-file path relative to the storage root.
    pub swap_file: Option<PathBuf>,
    /// Maximum swap-file size.
    pub max_file_size: Option<Byte>,
    /// Maximum resident-memory size.
    pub max_mem_size: Option<Byte>,
}

impl EvictableBufferPoolConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.swap_file, other.swap_file);
        replace(&mut self.max_file_size, other.max_file_size);
        replace(&mut self.max_mem_size, other.max_mem_size);
    }
}

/// Strict table/catalog filesystem overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileSystemConfigOverlay {
    /// Table/catalog storage I/O depth.
    pub io_depth: Option<usize>,
    /// Data directory relative to the storage root.
    pub data_dir: Option<PathBuf>,
    /// Readonly buffer-pool size.
    pub readonly_buffer_size: Option<Byte>,
    /// Catalog multi-table file name.
    pub catalog_file_name: Option<String>,
    /// Maximum logical size of each durable CoW table or catalog file.
    pub cow_file_max_size: Option<Byte>,
}

impl FileSystemConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.io_depth, other.io_depth);
        replace(&mut self.data_dir, other.data_dir);
        replace(&mut self.readonly_buffer_size, other.readonly_buffer_size);
        replace(&mut self.catalog_file_name, other.catalog_file_name);
        replace(&mut self.cow_file_max_size, other.cow_file_max_size);
    }
}

/// Serde-owned redo durability mode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogSyncValue {
    /// Flush with `fsync`.
    Fsync,
    /// Flush with `fdatasync`.
    Fdatasync,
    /// Do not issue a durability sync.
    None,
}

impl LogSyncValue {
    #[inline]
    fn storage(self) -> LogSync {
        match self {
            Self::Fsync => LogSync::Fsync,
            Self::Fdatasync => LogSync::Fdatasync,
            Self::None => LogSync::None,
        }
    }

    #[inline]
    fn from_storage(value: LogSync) -> Self {
        match value {
            LogSync::Fsync => Self::Fsync,
            LogSync::Fdatasync => Self::Fdatasync,
            LogSync::None => Self::None,
        }
    }
}

/// Serializable normalized engine configuration recorded with plan results.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedEngineConfig {
    /// Finite sync/async thread-pool sizing.
    pub thread_pool: ResolvedThreadPoolConfig,
    /// Transaction-system settings.
    pub transaction: ResolvedTransactionConfig,
    /// Startup recovery settings with automatic limits resolved.
    pub recovery: ResolvedRecoveryConfig,
    /// Mandatory runtime sizing.
    pub mandatory_runtime: ResolvedMandatoryRuntimeConfig,
    /// Deterministic table-scan planning settings.
    pub table_scan: ResolvedTableScanConfig,
    /// Metadata buffer-pool bytes.
    pub meta_buffer_bytes: u64,
    /// User-index buffer-pool settings.
    pub index_buffer: ResolvedEvictableBufferPoolConfig,
    /// Data buffer-pool settings.
    pub data_buffer: ResolvedEvictableBufferPoolConfig,
    /// Table/catalog filesystem settings.
    pub file: ResolvedFileSystemConfig,
}

impl ResolvedEngineConfig {
    #[inline]
    fn from_config(config: &EngineConfig) -> Self {
        Self {
            thread_pool: ResolvedThreadPoolConfig {
                worker_threads: config.thread_pool.worker_threads,
            },
            transaction: ResolvedTransactionConfig {
                log_write_io_depth: config.trx.log_write_io_depth,
                catalog_checkpoint_scan_io_depth: config.trx.catalog_checkpoint_scan_io_depth,
                log_block_size_bytes: config.trx.log_block_size.as_u64(),
                log_dir: config.trx.log_dir.clone(),
                log_file_stem: config.trx.log_file_stem.clone(),
                log_file_max_size_bytes: config.trx.log_file_max_size.as_u64(),
                log_sync: LogSyncValue::from_storage(config.trx.log_sync),
                purge_threads: config.trx.purge_threads,
                gc_buckets: config.trx.gc_buckets,
            },
            // Storage validation resolves both automatic limits before this snapshot.
            recovery: ResolvedRecoveryConfig {
                io_depth: config.recovery.io_depth,
                disable_dml_validation: config.recovery.disable_dml_validation,
                max_in_flight_batches: config
                    .recovery
                    .max_in_flight_batches
                    .expect("validated recovery batch limit"),
                max_active_pages: config
                    .recovery
                    .max_active_pages
                    .expect("validated recovery active-page limit"),
                max_batch_ops: config.recovery.max_batch_ops,
                target_batch_bytes: config.recovery.target_batch_bytes,
                max_recycled_bytes: config.recovery.max_recycled_bytes,
            },
            mandatory_runtime: ResolvedMandatoryRuntimeConfig {
                concurrency_limit: config.mandatory_runtime.concurrency_limit,
            },
            table_scan: ResolvedTableScanConfig {
                lwc_blocks_per_partition: config.table_scan.lwc_blocks_per_partition,
                row_pages_per_partition: config.table_scan.row_pages_per_partition,
            },
            meta_buffer_bytes: config.meta_buffer.as_u64(),
            index_buffer: ResolvedEvictableBufferPoolConfig::from_config(&config.index_buffer),
            data_buffer: ResolvedEvictableBufferPoolConfig::from_config(&config.data_buffer),
            file: ResolvedFileSystemConfig {
                io_depth: config.file.io_depth,
                data_dir: config.file.data_dir.clone(),
                readonly_buffer_size_bytes: config.file.readonly_buffer_size,
                catalog_file_name: config.file.catalog_file_name.clone(),
                cow_file_max_size_bytes: config.file.cow_file_max_size,
            },
        }
    }
}

/// Serializable normalized deterministic table-scan planning configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedTableScanConfig {
    /// Persisted LWC blocks per homogeneous partition.
    pub lwc_blocks_per_partition: usize,
    /// Hot row pages per homogeneous partition.
    pub row_pages_per_partition: usize,
}

/// Serializable normalized finite sync/async thread-pool configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedThreadPoolConfig {
    /// Fixed worker-thread count shared by sync and async jobs.
    pub worker_threads: usize,
}

/// Serializable normalized transaction configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedTransactionConfig {
    /// Live redo-writer I/O depth.
    pub log_write_io_depth: usize,
    /// Catalog-checkpoint redo-scan I/O depth.
    pub catalog_checkpoint_scan_io_depth: usize,
    /// Sector-aligned physical redo block bytes.
    pub log_block_size_bytes: u64,
    /// Redo directory relative to the storage root.
    pub log_dir: PathBuf,
    /// Redo log-family file stem.
    pub log_file_stem: String,
    /// Normalized maximum redo file bytes.
    pub log_file_max_size_bytes: u64,
    /// Redo durability mode.
    pub log_sync: LogSyncValue,
    /// Purge worker-thread count.
    pub purge_threads: usize,
    /// Transaction GC bucket count.
    pub gc_buckets: usize,
}

/// Serializable startup recovery settings with concrete positive replay limits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedRecoveryConfig {
    /// Startup redo read-ahead depth.
    pub io_depth: usize,
    /// Whether catalog and row replay DML validation is disabled.
    pub disable_dml_validation: bool,
    /// Maximum outstanding replay batches.
    pub max_in_flight_batches: usize,
    /// Maximum active replay pages.
    pub max_active_pages: usize,
    /// Maximum operations in one replay batch.
    pub max_batch_ops: usize,
    /// Used batch storage flush target in bytes.
    pub target_batch_bytes: usize,
    /// Maximum retained idle vector capacity in bytes.
    pub max_recycled_bytes: usize,
}

/// Serializable normalized mandatory-runtime configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedMandatoryRuntimeConfig {
    /// Accepted caller-operation concurrency limit.
    pub concurrency_limit: usize,
}

/// Serializable normalized evictable buffer-pool configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedEvictableBufferPoolConfig {
    /// Swap-file path relative to the storage root.
    pub swap_file: PathBuf,
    /// Maximum swap-file bytes.
    pub max_file_size_bytes: u64,
    /// Maximum resident-memory bytes.
    pub max_mem_size_bytes: u64,
}

impl ResolvedEvictableBufferPoolConfig {
    #[inline]
    fn from_config(config: &EvictableBufferPoolConfig) -> Self {
        Self {
            swap_file: config.swap_file.clone(),
            max_file_size_bytes: config.max_file_size.as_u64(),
            max_mem_size_bytes: config.max_mem_size.as_u64(),
        }
    }
}

/// Serializable normalized table/catalog filesystem configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedFileSystemConfig {
    /// Table/catalog storage I/O depth.
    pub io_depth: usize,
    /// Table/catalog data directory relative to the storage root.
    pub data_dir: PathBuf,
    /// Readonly buffer-pool bytes.
    pub readonly_buffer_size_bytes: usize,
    /// Catalog multi-table file name.
    pub catalog_file_name: String,
    /// Maximum logical bytes for each durable CoW table or catalog file.
    pub cow_file_max_size_bytes: usize,
}

/// Apply a merged overlay to authoritative storage defaults and validate it.
pub fn resolve_engine_config(
    storage_root: &Path,
    overlay: &EngineConfigOverlay,
) -> Result<(EngineConfig, ResolvedEngineConfig)> {
    let default = EngineConfig::default();
    let mut thread_pool = default.thread_pool;
    if let Some(value) = overlay.thread_pool.worker_threads {
        thread_pool = thread_pool.worker_threads(value);
    }
    let mut mandatory = default.mandatory_runtime;
    if let Some(value) = overlay.mandatory_runtime.concurrency_limit {
        mandatory = mandatory.concurrency_limit(value);
    }
    let mut table_scan = default.table_scan;
    if let Some(value) = overlay.table_scan.lwc_blocks_per_partition {
        table_scan = table_scan.lwc_blocks_per_partition(value);
    }
    if let Some(value) = overlay.table_scan.row_pages_per_partition {
        table_scan = table_scan.row_pages_per_partition(value);
    }

    let mut transaction = default.trx.clone();
    if let Some(value) = overlay.transaction.log_write_io_depth {
        transaction = transaction.log_write_io_depth(value);
    }
    if let Some(value) = overlay.transaction.catalog_checkpoint_scan_io_depth {
        transaction = transaction.catalog_checkpoint_scan_io_depth(value);
    }
    if let Some(value) = overlay.transaction.log_block_size {
        transaction = transaction.log_block_size(byte_u64(value, "transaction.log_block_size")?);
    }
    if let Some(value) = &overlay.transaction.log_dir {
        transaction = transaction.log_dir(value);
    }
    if let Some(value) = &overlay.transaction.log_file_stem {
        transaction = transaction.log_file_stem(value);
    }
    if let Some(value) = overlay.transaction.log_file_max_size {
        transaction =
            transaction.log_file_max_size(byte_u64(value, "transaction.log_file_max_size")?);
    }
    if let Some(value) = overlay.transaction.log_sync {
        transaction = transaction.log_sync(value.storage());
    }
    if let Some(value) = overlay.transaction.purge_threads {
        transaction = transaction.purge_threads(value);
    }
    if let Some(value) = overlay.transaction.gc_buckets {
        transaction = transaction.gc_buckets(value);
    }
    let mut recovery = default.recovery;
    if let Some(value) = overlay.recovery.io_depth {
        recovery = recovery.io_depth(value);
    }
    if let Some(value) = overlay.recovery.disable_dml_validation {
        recovery = recovery.disable_dml_validation(value);
    }
    if let Some(value) = overlay.recovery.max_in_flight_batches {
        recovery = recovery.max_in_flight_batches(Some(value));
    }
    if let Some(value) = overlay.recovery.max_active_pages {
        recovery = recovery.max_active_pages(Some(value));
    }
    if let Some(value) = overlay.recovery.max_batch_ops {
        recovery = recovery.max_batch_ops(value);
    }

    if let Some(value) = overlay.recovery.target_batch_bytes {
        recovery = recovery.target_batch_bytes(byte_usize(value, "recovery.target_batch_bytes")?);
    }
    if let Some(value) = overlay.recovery.max_recycled_bytes {
        recovery = recovery.max_recycled_bytes(byte_usize(value, "recovery.max_recycled_bytes")?);
    }

    let index_buffer = apply_evictable_buffer_overlay(
        default.index_buffer.clone(),
        &overlay.index_buffer,
        "index_buffer",
    )?;
    let data_buffer = apply_evictable_buffer_overlay(
        default.data_buffer.clone(),
        &overlay.data_buffer,
        "data_buffer",
    )?;

    let mut file = default.file.clone();
    if let Some(value) = overlay.file.io_depth {
        file = file.io_depth(value);
    }
    if let Some(value) = &overlay.file.data_dir {
        file = file.data_dir(value);
    }
    if let Some(value) = overlay.file.readonly_buffer_size {
        file = file.readonly_buffer_size(byte_usize(value, "file.readonly_buffer_size")?);
    }
    if let Some(value) = &overlay.file.catalog_file_name {
        file = file.catalog_file_name(value);
    }
    if let Some(value) = overlay.file.cow_file_max_size {
        file = file.cow_file_max_size(byte_usize(value, "file.cow_file_max_size")?);
    }

    let config = EngineConfig::default()
        .storage_root(storage_root)
        .thread_pool(thread_pool)
        .mandatory_runtime(mandatory)
        .table_scan(table_scan)
        .trx(transaction)
        .recovery(recovery)
        .meta_buffer(
            overlay
                .meta_buffer_size
                .map_or(Ok(default.meta_buffer.as_u64()), |value| {
                    byte_u64(value, "meta_buffer_size")
                })?,
        )
        .index_buffer(index_buffer)
        .data_buffer(data_buffer)
        .file(file)
        .validate()?;
    let resolved = ResolvedEngineConfig::from_config(&config);
    Ok((config, resolved))
}

fn apply_evictable_buffer_overlay(
    mut config: EvictableBufferPoolConfig,
    overlay: &EvictableBufferPoolConfigOverlay,
    field: &str,
) -> Result<EvictableBufferPoolConfig> {
    if let Some(value) = &overlay.swap_file {
        config = config.swap_file(value);
    }
    if let Some(value) = overlay.max_file_size {
        config = config.max_file_size(byte_u64(value, &format!("{field}.max_file_size"))?);
    }
    if let Some(value) = overlay.max_mem_size {
        config = config.max_mem_size(byte_u64(value, &format!("{field}.max_mem_size"))?);
    }
    Ok(config)
}

fn replace<T>(target: &mut Option<T>, value: Option<T>) {
    if value.is_some() {
        *target = value;
    }
}

fn byte_u64(value: Byte, field: &str) -> Result<u64> {
    value
        .as_u64_checked()
        .ok_or_else(|| BenchError::message(format!("{field} exceeds u64 bytes")))
}

fn byte_usize(value: Byte, field: &str) -> Result<usize> {
    usize::try_from(value)
        .map_err(|_| BenchError::message(format!("{field} exceeds addressable memory")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[track_caller]
    fn assert_resolved_round_trip(resolved: &ResolvedEngineConfig) -> String {
        let encoded = toml::to_string(resolved).unwrap();
        let decoded: ResolvedEngineConfig = toml::from_str(&encoded).unwrap();
        assert_eq!(&decoded, resolved);
        encoded
    }

    /// Purpose: Merge partial transaction and index-buffer overlays into existing settings.
    /// Expected: Overridden fields take local values while untouched sibling fields retain their
    /// prior values.
    #[test]
    fn nested_overlay_merge_preserves_siblings() {
        let mut base: EngineConfigOverlay =
            toml::from_str("[transaction]\npurge_threads = 3\ngc_buckets = 8\n").unwrap();
        let local = toml::from_str("[transaction]\npurge_threads = 5\n").unwrap();
        base.merge(local);
        assert_eq!(base.transaction.purge_threads, Some(5));
        assert_eq!(base.transaction.gc_buckets, Some(8));

        let mut base: EngineConfigOverlay = toml::from_str(
            "[index_buffer]\nmax_file_size = \"128 MiB\"\nmax_mem_size = \"64 MiB\"\n",
        )
        .unwrap();
        let local = toml::from_str("[index_buffer]\nmax_mem_size = \"96 MiB\"\n").unwrap();
        base.merge(local);
        assert_eq!(
            base.index_buffer.max_file_size,
            Some(Byte::from_u64(128 * 1024 * 1024))
        );
        assert_eq!(
            base.index_buffer.max_mem_size,
            Some(Byte::from_u64(96 * 1024 * 1024))
        );
    }

    /// Purpose: Resolve non-sector-aligned redo byte settings against storage normalization.
    /// Expected: A 5000-byte block resolves to 8192 bytes, the file limit accommodates a block, and
    /// the CoW file limit retains its default.
    #[test]
    fn resolved_config_uses_normalized_storage_values() {
        let temp = TempDir::new().unwrap();
        let overlay: EngineConfigOverlay = toml::from_str(
            "[transaction]\nlog_block_size = \"5000 B\"\nlog_file_max_size = \"9000 B\"\n",
        )
        .unwrap();
        let (_, resolved) = resolve_engine_config(temp.path(), &overlay).unwrap();
        assert_eq!(resolved.transaction.log_block_size_bytes, 8192);
        assert!(resolved.transaction.log_file_max_size_bytes >= 8192);
        assert_eq!(
            resolved.file.cow_file_max_size_bytes,
            doradb_storage::DEFAULT_COW_FILE_MAX_SIZE
        );
    }

    /// Purpose: Resolve independent swap-file and memory/file byte limits for index and data
    /// buffers.
    /// Expected: Both engine and reported settings preserve the configured paths and exact binary
    /// byte sizes.
    #[test]
    fn index_and_data_buffers_share_the_same_overlay_shape() {
        let temp = TempDir::new().unwrap();
        let overlay: EngineConfigOverlay = toml::from_str(
            "[index_buffer]\nswap_file = \"custom-index.swp\"\nmax_file_size = \"128 MiB\"\nmax_mem_size = \"64 MiB\"\n\n[data_buffer]\nswap_file = \"custom-data.swp\"\nmax_file_size = \"256 MiB\"\nmax_mem_size = \"128 MiB\"\n",
        )
        .unwrap();
        let (config, resolved) = resolve_engine_config(temp.path(), &overlay).unwrap();

        assert_eq!(
            config.index_buffer.swap_file,
            PathBuf::from("custom-index.swp")
        );
        assert_eq!(
            config.data_buffer.swap_file,
            PathBuf::from("custom-data.swp")
        );
        assert_eq!(
            resolved.index_buffer,
            ResolvedEvictableBufferPoolConfig {
                swap_file: PathBuf::from("custom-index.swp"),
                max_file_size_bytes: 134_217_728,
                max_mem_size_bytes: 67_108_864,
            }
        );
        assert_eq!(
            resolved.data_buffer.swap_file,
            PathBuf::from("custom-data.swp")
        );
        assert_eq!(resolved.data_buffer.max_file_size_bytes, 268_435_456);
        assert_eq!(resolved.data_buffer.max_mem_size_bytes, 134_217_728);
    }

    /// Purpose: Decode unknown and obsolete engine-overlay fields across nested configuration
    /// sections.
    /// Expected: Each unsupported field or legacy section shape is rejected during TOML decoding.
    #[test]
    fn strict_nested_overlay_rejects_unknown_field() {
        assert!(toml::from_str::<EngineConfigOverlay>("[file]\nunknown = 1").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[data_buffer]\ntarget_free = 4").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[index_buffer]\ntarget_free = 4").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("meta_buffer_bytes = 4096").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("meta_buffer_size = 4096").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[thread_pool]\nunknown = 1").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[table_scan]\nunknown = 1").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[recovery]\nunknown = 1").is_err());
        for field in ["max_batch_bytes", "max_buffered_bytes"] {
            assert!(
                toml::from_str::<EngineConfigOverlay>(&format!(
                    "[recovery]\n{field} = \"64 KiB\"\n"
                ))
                .is_err(),
                "{field}"
            );
        }
        assert!(
            toml::from_str::<EngineConfigOverlay>("[transaction]\nrecovery_io_depth = 1").is_err()
        );
        assert!(
            toml::from_str::<EngineConfigOverlay>(
                "[transaction]\nrecovery_disable_dml_validation = true"
            )
            .is_err()
        );
        assert!(
            toml::from_str::<EngineConfigOverlay>("[mandatory_runtime]\nworker_threads = 2")
                .is_err()
        );
    }

    /// Purpose: Merge explicit recovery limits and worker settings, then serialize the resolved
    /// configuration.
    /// Expected: Specified overrides and retained limits match the expected recovery record; round-
    /// trip is exact, and obsolete or missing recovery fields are rejected.
    #[test]
    fn recovery_overlay_merges_resolves_and_round_trips() {
        let temp = TempDir::new().unwrap();
        let mut base: EngineConfigOverlay = toml::from_str(
            r"
            [thread_pool]
            worker_threads = 3
            [recovery]
            io_depth = 2
            disable_dml_validation = true
            max_in_flight_batches = 5
            max_active_pages = 7
            max_batch_ops = 11
        ",
        )
        .unwrap();
        base.merge(
            toml::from_str(
                r"
            [recovery]
            io_depth = 4
            disable_dml_validation = false
            max_in_flight_batches = 6
        ",
            )
            .unwrap(),
        );
        let (config, resolved) = resolve_engine_config(temp.path(), &base).unwrap();
        assert_eq!(config.recovery.max_in_flight_batches, Some(6));
        assert_eq!(config.recovery.max_active_pages, Some(7));
        assert_eq!(
            resolved.recovery,
            ResolvedRecoveryConfig {
                io_depth: 4,
                disable_dml_validation: false,
                max_in_flight_batches: 6,
                max_active_pages: 7,
                max_batch_ops: 11,
                target_batch_bytes: 256 * 1024,
                max_recycled_bytes: 16 * 1024 * 1024,
            }
        );
        let encoded = assert_resolved_round_trip(&resolved);
        for field in ["max_batch_bytes", "max_buffered_bytes"] {
            let mut obsolete: toml::Value = toml::from_str(&encoded).unwrap();
            obsolete["recovery"]
                .as_table_mut()
                .unwrap()
                .insert(field.to_owned(), toml::Value::Integer(65536));
            assert!(
                obsolete.try_into::<ResolvedEngineConfig>().is_err(),
                "{field}"
            );
        }
        let mut missing_recovery: toml::Value = toml::from_str(&encoded).unwrap();
        missing_recovery.as_table_mut().unwrap().remove("recovery");
        assert!(missing_recovery.try_into::<ResolvedEngineConfig>().is_err());
    }

    /// Purpose: Derive automatic recovery limits after merging worker-count and batch-count
    /// overrides.
    /// Expected: Three workers yield six in-flight batches and 24 active pages; explicitly
    /// selecting one batch reduces the automatic page limit to four.
    #[test]
    fn recovery_overlay_automatic_limits_follow_final_worker_and_batch_overrides() {
        let temp = TempDir::new().unwrap();
        let mut overlay: EngineConfigOverlay =
            toml::from_str("[thread_pool]\nworker_threads = 3\n").unwrap();
        let (_, resolved) = resolve_engine_config(temp.path(), &overlay).unwrap();
        assert_eq!(resolved.recovery.max_in_flight_batches, 6);
        assert_eq!(resolved.recovery.max_active_pages, 24);
        overlay.merge(toml::from_str("[recovery]\nmax_in_flight_batches = 1\n").unwrap());
        let (_, resolved) = resolve_engine_config(temp.path(), &overlay).unwrap();
        assert_eq!(resolved.recovery.max_in_flight_batches, 1);
        assert_eq!(resolved.recovery.max_active_pages, 4);
    }

    /// Purpose: Merge a zero-recycling override with a configured recovery batch byte target.
    /// Expected: The engine and report retain a 32768-byte target and zero recycled bytes, and
    /// resolved TOML round-trips exactly.
    #[test]
    fn recovery_byte_overlays_merge_normalize_and_allow_zero_recycling() {
        let temp = TempDir::new().unwrap();
        let mut overlay: EngineConfigOverlay = toml::from_str(
            r#"[recovery]
            target_batch_bytes = "32 KiB"
            max_recycled_bytes = "1 MiB"
        "#,
        )
        .unwrap();
        overlay.merge(
            toml::from_str(
                r#"[recovery]
            max_recycled_bytes = "0 B"
        "#,
            )
            .unwrap(),
        );
        let (config, resolved) = resolve_engine_config(temp.path(), &overlay).unwrap();
        assert_eq!(config.recovery.target_batch_bytes, 32768);
        assert_eq!(resolved.recovery.target_batch_bytes, 32768);
        assert_eq!(config.recovery.max_recycled_bytes, 0);
        assert_eq!(resolved.recovery.max_recycled_bytes, 0);
        assert_resolved_round_trip(&resolved);
    }

    /// Purpose: Resolve each mandatory recovery limit with a zero value.
    /// Expected: Zero I/O depth, in-flight batches, active pages, batch operations, or target batch
    /// bytes each fail validation.
    #[test]
    fn recovery_overlay_rejects_invalid_limits() {
        let temp = TempDir::new().unwrap();
        for field in [
            "io_depth = 0",
            "max_in_flight_batches = 0",
            "max_active_pages = 0",
            "max_batch_ops = 0",
            "target_batch_bytes = \"0 B\"",
        ] {
            let overlay = toml::from_str(&format!("[recovery]\n{field}\n")).unwrap();
            assert!(
                resolve_engine_config(temp.path(), &overlay).is_err(),
                "{field}"
            );
        }
    }

    /// Purpose: Override thread-pool workers while preserving the mandatory-runtime concurrency
    /// setting.
    /// Expected: Engine and report use four workers and concurrency three; the resolved
    /// configuration round-trips through TOML unchanged.
    #[test]
    fn thread_pool_and_mandatory_runtime_merge_and_round_trip() {
        let temp = TempDir::new().unwrap();
        let mut base: EngineConfigOverlay = toml::from_str(
            "[thread_pool]\nworker_threads = 1\n[mandatory_runtime]\nconcurrency_limit = 3\n",
        )
        .unwrap();
        let local: EngineConfigOverlay =
            toml::from_str("[thread_pool]\nworker_threads = 4\n").unwrap();
        base.merge(local);

        let (config, resolved) = resolve_engine_config(temp.path(), &base).unwrap();
        assert_eq!(config.thread_pool.worker_threads, 4);
        assert_eq!(config.mandatory_runtime.concurrency_limit, 3);
        assert_eq!(resolved.thread_pool.worker_threads, 4);
        assert_eq!(resolved.mandatory_runtime.concurrency_limit, 3);
        assert_resolved_round_trip(&resolved);
    }

    /// Purpose: Merge one table-scan partition limit and validate the resolved section during TOML
    /// round-trip.
    /// Expected: The limits resolve to seven LWC blocks and 21 row pages; round-trip preserves them
    /// and omitting table_scan fails decoding.
    #[test]
    fn table_scan_overlay_merges_resolves_round_trips_and_is_required() {
        let temp = TempDir::new().unwrap();
        let mut base: EngineConfigOverlay = toml::from_str(
            "[table_scan]\nlwc_blocks_per_partition = 7\nrow_pages_per_partition = 15\n",
        )
        .unwrap();
        let local: EngineConfigOverlay =
            toml::from_str("[table_scan]\nrow_pages_per_partition = 21\n").unwrap();
        base.merge(local);
        let (config, resolved) = resolve_engine_config(temp.path(), &base).unwrap();
        assert_eq!(config.table_scan.lwc_blocks_per_partition, 7);
        assert_eq!(config.table_scan.row_pages_per_partition, 21);
        assert_eq!(
            resolved.table_scan,
            ResolvedTableScanConfig {
                lwc_blocks_per_partition: 7,
                row_pages_per_partition: 21,
            }
        );
        let encoded = assert_resolved_round_trip(&resolved);
        assert!(encoded.contains("[table_scan]"));

        let mut skipping_scan = false;
        let without_scan = encoded
            .lines()
            .filter(|line| {
                if *line == "[table_scan]" {
                    skipping_scan = true;
                    return false;
                }
                if skipping_scan && line.starts_with('[') {
                    skipping_scan = false;
                }
                !skipping_scan
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(toml::from_str::<ResolvedEngineConfig>(&without_scan).is_err());
    }

    /// Purpose: Resolve a metadata-buffer byte count larger than u64 can represent.
    /// Expected: The 18446744073709551616-byte overlay is rejected instead of narrowing or
    /// wrapping.
    #[test]
    fn byte_values_use_checked_storage_boundaries() {
        let temp = TempDir::new().unwrap();
        let overlay: EngineConfigOverlay =
            toml::from_str("meta_buffer_size = \"18446744073709551616 B\"\n").unwrap();
        assert!(resolve_engine_config(temp.path(), &overlay).is_err());
    }

    /// Purpose: Override the CoW file limit while preserving an existing catalog filename.
    /// Expected: Engine and report use 48 MiB and custom.mtb, and resolved TOML round-trips
    /// exactly.
    #[test]
    fn cow_file_max_size_overlay_merges_and_round_trips() {
        let temp = TempDir::new().unwrap();
        let mut base: EngineConfigOverlay = toml::from_str(
            "[file]\ncow_file_max_size = \"32 MiB\"\ncatalog_file_name = \"custom.mtb\"\n",
        )
        .unwrap();
        let local: EngineConfigOverlay =
            toml::from_str("[file]\ncow_file_max_size = \"48 MiB\"\n").unwrap();
        base.merge(local);

        let (config, resolved) = resolve_engine_config(temp.path(), &base).unwrap();
        assert_eq!(config.file.cow_file_max_size, 48 * 1024 * 1024);
        assert_eq!(resolved.file.cow_file_max_size_bytes, 48 * 1024 * 1024);
        assert_eq!(resolved.file.catalog_file_name, "custom.mtb");
        assert_resolved_round_trip(&resolved);
    }
}
