use crate::error::{ConfigError, ConfigResult};
use crate::root::{ResolvedStoragePaths, StoragePathResolveInput};
use byte_unit::Byte;
use error_stack::Report;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::consts::{
    DEFAULT_ENGINE_INDEX_BUFFER, DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE,
    DEFAULT_ENGINE_INDEX_SWAP_FILE, DEFAULT_ENGINE_META_BUFFER,
};
use super::{EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};

/// Fixed engine-owned mandatory runtime configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MandatoryRuntimeConfig {
    /// Number of operating-system threads driving the mandatory executor.
    pub worker_threads: usize,
    /// Maximum number of accepted caller operations.
    pub concurrency_limit: usize,
}

impl Default for MandatoryRuntimeConfig {
    #[inline]
    fn default() -> Self {
        Self {
            worker_threads: 2,
            concurrency_limit: 4,
        }
    }
}

impl MandatoryRuntimeConfig {
    /// Set the number of mandatory-runtime runner threads.
    #[inline]
    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = worker_threads;
        self
    }

    /// Set the maximum number of accepted caller operations.
    #[inline]
    pub fn concurrency_limit(mut self, concurrency_limit: usize) -> Self {
        self.concurrency_limit = concurrency_limit;
        self
    }

    /// Validate immutable runtime sizing.
    #[inline]
    pub(crate) fn validate(&self) -> ConfigResult<()> {
        if self.worker_threads == 0 {
            return Err(Report::new(ConfigError::InvalidMandatoryWorkerThreads)
                .attach("mandatory_runtime.worker_threads=0"));
        }
        if self.concurrency_limit == 0 {
            return Err(Report::new(ConfigError::InvalidMandatoryConcurrencyLimit)
                .attach("mandatory_runtime.concurrency_limit=0"));
        }
        Ok(())
    }
}

/// Storage-engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Root directory for all storage-engine files.
    pub(crate) storage_root: PathBuf,
    /// Transaction-system configuration.
    pub(crate) trx: TrxSysConfig,
    /// Engine-owned mandatory runtime configuration.
    #[serde(default)]
    pub(crate) mandatory_runtime: MandatoryRuntimeConfig,
    /// Metadata buffer-pool size.
    pub(crate) meta_buffer: Byte,
    /// Index buffer-pool memory size.
    pub(crate) index_buffer: Byte,
    /// Index-buffer swap-file path relative to `storage_root`.
    pub(crate) index_swap_file: PathBuf,
    /// Maximum size allowed for the index-buffer swap file.
    pub(crate) index_max_file_size: Byte,
    /// Data buffer-pool configuration.
    pub(crate) data_buffer: EvictableBufferPoolConfig,
    /// Table and catalog file-system configuration.
    pub(crate) file: FileSystemConfig,
}

impl Default for EngineConfig {
    #[inline]
    fn default() -> Self {
        EngineConfig {
            storage_root: PathBuf::from("."),
            trx: TrxSysConfig::default(),
            mandatory_runtime: MandatoryRuntimeConfig::default(),
            meta_buffer: Byte::from_u64(DEFAULT_ENGINE_META_BUFFER as u64),
            index_buffer: Byte::from_u64(DEFAULT_ENGINE_INDEX_BUFFER as u64),
            index_swap_file: PathBuf::from(DEFAULT_ENGINE_INDEX_SWAP_FILE),
            index_max_file_size: Byte::from_u64(DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE as u64),
            data_buffer: EvictableBufferPoolConfig::default(),
            file: FileSystemConfig::default(),
        }
    }
}

impl EngineConfig {
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

    /// Set the index buffer-pool memory size.
    #[inline]
    pub fn index_buffer(mut self, index_buffer: impl Into<Byte>) -> Self {
        self.index_buffer = index_buffer.into();
        self
    }

    /// Set the index-buffer swap-file path relative to `storage_root`.
    #[inline]
    pub fn index_swap_file(mut self, index_swap_file: impl Into<PathBuf>) -> Self {
        self.index_swap_file = index_swap_file.into();
        self
    }

    /// Set the maximum size allowed for the index-buffer swap file.
    #[inline]
    pub fn index_max_file_size(mut self, index_max_file_size: impl Into<Byte>) -> Self {
        self.index_max_file_size = index_max_file_size.into();
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
            self.data_buffer.data_swap_file_ref(),
            &self.index_swap_file,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_engine_config_defaults_mandatory_runtime() {
        let mut value =
            toml::Value::try_from(EngineConfig::default()).expect("serialize engine config");
        value
            .as_table_mut()
            .expect("engine config serializes as table")
            .remove("mandatory_runtime");
        let decoded: EngineConfig = value.try_into().expect("deserialize legacy engine config");
        assert_eq!(decoded.mandatory_runtime.worker_threads, 2);
        assert_eq!(decoded.mandatory_runtime.concurrency_limit, 4);
    }

    #[test]
    fn mandatory_runtime_rejects_zero_sizes() {
        let error = MandatoryRuntimeConfig::default()
            .worker_threads(0)
            .validate()
            .unwrap_err();
        assert_eq!(
            error.current_context(),
            &ConfigError::InvalidMandatoryWorkerThreads
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
}
