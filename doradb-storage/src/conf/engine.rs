use crate::error::ConfigResult;
use crate::root::{ResolvedStoragePaths, StoragePathResolveInput};
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::consts::{
    DEFAULT_ENGINE_INDEX_BUFFER, DEFAULT_ENGINE_INDEX_MAX_FILE_SIZE,
    DEFAULT_ENGINE_INDEX_SWAP_FILE, DEFAULT_ENGINE_META_BUFFER,
};
use super::{EvictableBufferPoolConfig, FileSystemConfig, TrxSysConfig};

/// Storage-engine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Root directory for all storage-engine files.
    pub(crate) storage_root: PathBuf,
    /// Transaction-system configuration.
    pub(crate) trx: TrxSysConfig,
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
