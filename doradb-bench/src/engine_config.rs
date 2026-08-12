use crate::error::{BenchError, Result};
use byte_unit::Byte;
use doradb_storage::{EngineConfig, EvictableBufferPoolConfig, LogSync};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Strict, field-wise engine configuration overlay used by benchmark plans.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfigOverlay {
    /// Mandatory runtime sizing overrides.
    pub mandatory_runtime: MandatoryRuntimeOverlay,
    /// Transaction-system overrides.
    pub transaction: TransactionConfigOverlay,
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
        self.mandatory_runtime.merge(other.mandatory_runtime);
        self.transaction.merge(other.transaction);
        replace(&mut self.meta_buffer_size, other.meta_buffer_size);
        self.index_buffer.merge(other.index_buffer);
        self.data_buffer.merge(other.data_buffer);
        self.file.merge(other.file);
    }
}

/// Strict mandatory-runtime overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MandatoryRuntimeOverlay {
    /// Fixed runner thread count.
    pub worker_threads: Option<usize>,
    /// Accepted caller-operation concurrency limit.
    pub concurrency_limit: Option<usize>,
}

impl MandatoryRuntimeOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.worker_threads, other.worker_threads);
        replace(&mut self.concurrency_limit, other.concurrency_limit);
    }
}

/// Strict transaction-system overlay.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransactionConfigOverlay {
    /// Live redo-writer I/O depth.
    pub log_write_io_depth: Option<usize>,
    /// Startup recovery I/O depth.
    pub recovery_io_depth: Option<usize>,
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
    /// Disable DML validation during recovery.
    pub recovery_disable_dml_validation: Option<bool>,
}

impl TransactionConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.log_write_io_depth, other.log_write_io_depth);
        replace(&mut self.recovery_io_depth, other.recovery_io_depth);
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
        replace(
            &mut self.recovery_disable_dml_validation,
            other.recovery_disable_dml_validation,
        );
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
}

impl FileSystemConfigOverlay {
    #[inline]
    fn merge(&mut self, other: Self) {
        replace(&mut self.io_depth, other.io_depth);
        replace(&mut self.data_dir, other.data_dir);
        replace(&mut self.readonly_buffer_size, other.readonly_buffer_size);
        replace(&mut self.catalog_file_name, other.catalog_file_name);
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
    /// Transaction-system settings.
    pub transaction: ResolvedTransactionConfig,
    /// Mandatory runtime sizing.
    pub mandatory_runtime: ResolvedMandatoryRuntimeConfig,
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
            transaction: ResolvedTransactionConfig {
                log_write_io_depth: config.trx.log_write_io_depth,
                recovery_io_depth: config.trx.recovery_io_depth,
                catalog_checkpoint_scan_io_depth: config.trx.catalog_checkpoint_scan_io_depth,
                log_block_size_bytes: config.trx.log_block_size.as_u64(),
                log_dir: config.trx.log_dir.clone(),
                log_file_stem: config.trx.log_file_stem.clone(),
                log_file_max_size_bytes: config.trx.log_file_max_size.as_u64(),
                log_sync: LogSyncValue::from_storage(config.trx.log_sync),
                purge_threads: config.trx.purge_threads,
                gc_buckets: config.trx.gc_buckets,
                recovery_disable_dml_validation: config.trx.recovery_disable_dml_validation,
            },
            mandatory_runtime: ResolvedMandatoryRuntimeConfig {
                worker_threads: config.mandatory_runtime.worker_threads,
                concurrency_limit: config.mandatory_runtime.concurrency_limit,
            },
            meta_buffer_bytes: config.meta_buffer.as_u64(),
            index_buffer: ResolvedEvictableBufferPoolConfig::from_config(&config.index_buffer),
            data_buffer: ResolvedEvictableBufferPoolConfig::from_config(&config.data_buffer),
            file: ResolvedFileSystemConfig {
                io_depth: config.file.io_depth,
                data_dir: config.file.data_dir.clone(),
                readonly_buffer_size_bytes: config.file.readonly_buffer_size,
                catalog_file_name: config.file.catalog_file_name.clone(),
            },
        }
    }
}

/// Serializable normalized transaction configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedTransactionConfig {
    /// Live redo-writer I/O depth.
    pub log_write_io_depth: usize,
    /// Startup recovery I/O depth.
    pub recovery_io_depth: usize,
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
    /// Whether recovery DML validation is disabled.
    pub recovery_disable_dml_validation: bool,
}

/// Serializable normalized mandatory-runtime configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedMandatoryRuntimeConfig {
    /// Fixed runtime runner-thread count.
    pub worker_threads: usize,
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
}

/// Apply a merged overlay to authoritative storage defaults and validate it.
pub fn resolve_engine_config(
    storage_root: &Path,
    overlay: &EngineConfigOverlay,
) -> Result<(EngineConfig, ResolvedEngineConfig)> {
    let default = EngineConfig::default();
    let mut mandatory = default.mandatory_runtime;
    if let Some(value) = overlay.mandatory_runtime.worker_threads {
        mandatory = mandatory.worker_threads(value);
    }
    if let Some(value) = overlay.mandatory_runtime.concurrency_limit {
        mandatory = mandatory.concurrency_limit(value);
    }

    let mut transaction = default.trx.clone();
    if let Some(value) = overlay.transaction.log_write_io_depth {
        transaction = transaction.log_write_io_depth(value);
    }
    if let Some(value) = overlay.transaction.recovery_io_depth {
        transaction = transaction.recovery_io_depth(value);
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
    if let Some(value) = overlay.transaction.recovery_disable_dml_validation {
        transaction = transaction.recovery_disable_dml_validation(value);
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

    let config = EngineConfig::default()
        .storage_root(storage_root)
        .mandatory_runtime(mandatory)
        .trx(transaction)
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
    }

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

    #[test]
    fn strict_nested_overlay_rejects_unknown_field() {
        assert!(toml::from_str::<EngineConfigOverlay>("[file]\nunknown = 1").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[data_buffer]\ntarget_free = 4").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("[index_buffer]\ntarget_free = 4").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("meta_buffer_bytes = 4096").is_err());
        assert!(toml::from_str::<EngineConfigOverlay>("meta_buffer_size = 4096").is_err());
    }

    #[test]
    fn byte_values_use_checked_storage_boundaries() {
        let temp = TempDir::new().unwrap();
        let overlay: EngineConfigOverlay =
            toml::from_str("meta_buffer_size = \"18446744073709551616 B\"\n").unwrap();
        assert!(resolve_engine_config(temp.path(), &overlay).is_err());
    }
}
