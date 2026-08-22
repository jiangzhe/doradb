use crate::buffer::minimum_fixed_pool_bytes;
use crate::error::{ConfigError, ConfigResult, DiscloseResultExt, Result};
use crate::root::{ResolvedStoragePaths, StoragePathResolveInput};
use byte_unit::Byte;
use error_stack::{Report, ResultExt};
use std::path::PathBuf;

use super::consts::{
    DEFAULT_ENGINE_INDEX_BUFFER, DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE,
    DEFAULT_ENGINE_INDEX_SWAP_FILE, DEFAULT_ENGINE_META_BUFFER,
};
use super::{EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};

/// Immutable sizing for the engine-owned CPU thread pool.
///
/// The default is two fixed operating-system workers. Pool tasks are finite,
/// synchronous CPU computations submitted only by storage-internal owners.
/// Sizing is validated once during engine startup; a running engine cannot be
/// resized.
#[derive(Clone, Debug)]
pub struct ThreadPoolConfig {
    /// Number of fixed operating-system threads executing CPU tasks.
    pub worker_threads: usize,
}

impl Default for ThreadPoolConfig {
    #[inline]
    fn default() -> Self {
        Self { worker_threads: 2 }
    }
}

impl ThreadPoolConfig {
    /// Set the fixed number of CPU worker threads.
    #[inline]
    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = worker_threads;
        self
    }

    /// Validate immutable pool sizing.
    #[inline]
    pub(crate) fn validate(&self) -> ConfigResult<()> {
        if self.worker_threads == 0 {
            return Err(Report::new(ConfigError::InvalidThreadPoolWorkerThreads)
                .attach("thread_pool.worker_threads=0"));
        }
        Ok(())
    }
}

/// Immutable sizing for the engine-owned mandatory runtime.
///
/// The runtime has one fixed operating-system runner and four accepted
/// caller-operation permits by default. Several accepted tasks can make
/// cooperative progress when they reach await or yield points.
///
/// `concurrency_limit` does not count caller-side preparation futures or
/// engine-internal transaction cleanup. Internal cleanup bypasses caller quota
/// so correctness obligations are not lost, while shutdown still waits for
/// both classes. Raising the caller limit may retain more locks, memory, and
/// publication work without raising executor throughput. Sizing is validated
/// once during engine startup; a running engine cannot be resized.
#[derive(Clone, Debug)]
pub struct MandatoryRuntimeConfig {
    /// Maximum accepted caller obligations, excluding internal cleanup.
    pub concurrency_limit: usize,
}

impl Default for MandatoryRuntimeConfig {
    #[inline]
    fn default() -> Self {
        Self {
            concurrency_limit: 4,
        }
    }
}

impl MandatoryRuntimeConfig {
    /// Set accepted caller capacity without limiting internal cleanup.
    #[inline]
    pub fn concurrency_limit(mut self, concurrency_limit: usize) -> Self {
        self.concurrency_limit = concurrency_limit;
        self
    }

    /// Validate immutable runtime sizing.
    #[inline]
    pub(crate) fn validate(&self) -> ConfigResult<()> {
        if self.concurrency_limit == 0 {
            return Err(Report::new(ConfigError::InvalidMandatoryConcurrencyLimit)
                .attach("mandatory_runtime.concurrency_limit=0"));
        }
        Ok(())
    }
}

/// Storage-engine configuration.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Root directory for all storage-engine files.
    pub storage_root: PathBuf,
    /// Transaction-system configuration.
    pub trx: TrxSysConfig,
    /// Engine-owned CPU thread-pool configuration.
    pub thread_pool: ThreadPoolConfig,
    /// Engine-owned mandatory runtime configuration.
    pub mandatory_runtime: MandatoryRuntimeConfig,
    /// Metadata buffer-pool size.
    pub meta_buffer: Byte,
    /// User-index buffer-pool configuration.
    pub index_buffer: EvictableBufferPoolConfig,
    /// Data buffer-pool configuration.
    pub data_buffer: EvictableBufferPoolConfig,
    /// Table and catalog file-system configuration.
    pub file: FileSystemConfig,
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        EngineConfig {
            storage_root: PathBuf::from("."),
            trx: TrxSysConfig::default(),
            thread_pool: ThreadPoolConfig::default(),
            mandatory_runtime: MandatoryRuntimeConfig::default(),
            meta_buffer: Byte::from_u64(DEFAULT_ENGINE_META_BUFFER as u64),
            index_buffer: EvictableBufferPoolConfig::default()
                .swap_file(DEFAULT_ENGINE_INDEX_SWAP_FILE)
                .max_file_size(DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE)
                .max_mem_size(DEFAULT_ENGINE_INDEX_BUFFER),
            data_buffer: EvictableBufferPoolConfig::default(),
            file: FileSystemConfig::default(),
        }
    }
}

impl EngineConfig {
    /// Validate and normalize all engine construction inputs without mutating
    /// the filesystem.
    #[inline]
    pub fn validate(self) -> Result<Self> {
        self.validate_inner().disclose()
    }

    /// Validate and normalize engine construction inputs while preserving the
    /// configuration-domain report for storage-internal callers.
    #[inline]
    pub(crate) fn validate_inner(mut self) -> ConfigResult<Self> {
        self.thread_pool.validate()?;
        self.mandatory_runtime.validate()?;
        self.trx.validate()?;
        self.index_buffer = self
            .index_buffer
            .validate()
            .attach("config_field=index_buffer")?;
        self.data_buffer = self
            .data_buffer
            .validate()
            .attach("config_field=data_buffer")?;
        let min_fixed_buffer_bytes = minimum_fixed_pool_bytes() as u64;
        if self.meta_buffer.as_u64() < min_fixed_buffer_bytes {
            return Err(
                Report::new(ConfigError::InvalidFixedBufferPoolSize).attach(format!(
                    "meta_buffer={}, min_supported={min_fixed_buffer_bytes}",
                    self.meta_buffer.as_u64()
                )),
            );
        }
        let resolved = self.resolve_storage_paths()?;
        self.file
            .clone()
            .data_dir(resolved.data_dir_path())
            .validate()?;
        Ok(self)
    }

    /// Set the root directory for storage-engine files.
    #[inline]
    pub fn storage_root(mut self, storage_root: impl Into<PathBuf>) -> Self {
        self.storage_root = storage_root.into();
        self
    }

    /// Set the transaction-system configuration.
    #[inline]
    pub fn trx(mut self, trx: TrxSysConfig) -> Self {
        self.trx = trx;
        self
    }

    /// Set the engine-owned CPU thread-pool configuration.
    #[inline]
    pub fn thread_pool(mut self, thread_pool: ThreadPoolConfig) -> Self {
        self.thread_pool = thread_pool;
        self
    }

    /// Set the engine-owned mandatory runtime configuration.
    #[inline]
    pub fn mandatory_runtime(mut self, mandatory_runtime: MandatoryRuntimeConfig) -> Self {
        self.mandatory_runtime = mandatory_runtime;
        self
    }

    /// Set the metadata buffer-pool size.
    #[inline]
    pub fn meta_buffer(mut self, meta_buffer: impl Into<Byte>) -> Self {
        self.meta_buffer = meta_buffer.into();
        self
    }

    /// Set the user-index buffer-pool configuration.
    #[inline]
    pub fn index_buffer(mut self, index_buffer: EvictableBufferPoolConfig) -> Self {
        self.index_buffer = index_buffer;
        self
    }

    /// Set the data buffer-pool configuration.
    #[inline]
    pub fn data_buffer(mut self, data_buffer: EvictableBufferPoolConfig) -> Self {
        self.data_buffer = data_buffer;
        self
    }

    /// Set the table and catalog file-system configuration.
    #[inline]
    pub fn file(mut self, file: FileSystemConfig) -> Self {
        self.file = file;
        self
    }

    /// Resolve and validate storage paths for engine startup.
    #[inline]
    pub(crate) fn resolve_storage_paths(&self) -> ConfigResult<ResolvedStoragePaths> {
        ResolvedStoragePaths::resolve(StoragePathResolveInput::new(
            &self.storage_root,
            &self.file.data_dir,
            &self.file.catalog_file_name,
            self.trx.log_dir_ref(),
            self.trx.log_file_stem_ref(),
            self.data_buffer.swap_file_ref(),
            self.index_buffer.swap_file_ref(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn runtime_configs_reject_zero_sizes() {
        let error = ThreadPoolConfig::default()
            .worker_threads(0)
            .validate()
            .unwrap_err();
        assert_eq!(
            error.current_context(),
            &ConfigError::InvalidThreadPoolWorkerThreads
        );
        let error = MandatoryRuntimeConfig::default()
            .concurrency_limit(0)
            .validate()
            .unwrap_err();
        assert_eq!(
            error.current_context(),
            &ConfigError::InvalidMandatoryConcurrencyLimit
        );
    }

    #[test]
    fn engine_validation_is_pure_and_config_reflects_normalization() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("engine");
        let config = EngineConfig::default()
            .storage_root(&root)
            .trx(
                TrxSysConfig::default()
                    .log_block_size(5000u64)
                    .log_file_max_size(9000u64),
            )
            .validate()
            .unwrap();
        assert!(!root.exists());
        assert_eq!(config.storage_root, root);
        assert_eq!(config.trx.log_block_size.as_u64(), 8192);
        assert!(config.trx.log_file_max_size.as_u64() >= 8192);
        assert_eq!(config.file.catalog_file_name, "catalog.mtb");
    }

    #[test]
    fn invalid_engine_validation_preserves_typed_and_public_errors_without_creating_root() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("invalid");
        let config = EngineConfig::default()
            .storage_root(&root)
            .thread_pool(ThreadPoolConfig::default().worker_threads(0));

        let typed_error = config.clone().validate_inner().unwrap_err();
        assert_eq!(
            typed_error.current_context(),
            &ConfigError::InvalidThreadPoolWorkerThreads
        );

        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), crate::error::ErrorKind::Config);
        assert!(!root.exists());
    }

    #[test]
    fn evictable_pool_validation_reports_the_engine_field() {
        let invalid = EvictableBufferPoolConfig::default()
            .max_mem_size(1024usize * 1024)
            .max_file_size(2usize * 1024 * 1024);
        for (config, field) in [
            (
                EngineConfig::default().index_buffer(invalid.clone().swap_file("index.swp")),
                "index_buffer",
            ),
            (
                EngineConfig::default().data_buffer(invalid.clone()),
                "data_buffer",
            ),
        ] {
            let error = config.validate_inner().unwrap_err();
            assert_eq!(
                error.current_context(),
                &ConfigError::InvalidBufferPoolConfig
            );
            assert!(format!("{error:?}").contains(field));
        }
    }
}
