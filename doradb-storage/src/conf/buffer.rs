use crate::buffer::{
    EvictableBufferPool, EvictionArbiter, EvictionArbiterBuilder, PoolRole,
    build_pool_with_swap_file_field,
};
use crate::error::Result;
use crate::file::SparseFile;
use crate::file::fs::FileSystem;
use crate::quiescent::QuiescentGuard;
use byte_unit::Byte;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use super::consts::{
    DEFAULT_EVICTABLE_BUFFER_POOL_DATA_SWAP_FILE, DEFAULT_EVICTABLE_BUFFER_POOL_MAX_FILE_SIZE,
    DEFAULT_EVICTABLE_BUFFER_POOL_MAX_IO_DEPTH, DEFAULT_EVICTABLE_BUFFER_POOL_MAX_MEM_SIZE,
};

/// Builder-style configuration for [`crate::buffer::EvictableBufferPool`].
///
/// Besides file and memory sizing, this type carries eviction-arbiter tuning
/// used to build the background evictor policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictableBufferPoolConfig {
    #[serde(default)]
    pub(crate) role: PoolRole,
    pub(crate) data_swap_file: PathBuf,
    pub(crate) max_file_size: Byte,
    pub(crate) max_mem_size: Byte,
    pub(crate) max_io_depth: usize,
    pub(crate) eviction_arbiter_builder: EvictionArbiterBuilder,
}

impl Default for EvictableBufferPoolConfig {
    #[inline]
    fn default() -> Self {
        EvictableBufferPoolConfig {
            role: PoolRole::Invalid,
            data_swap_file: PathBuf::from(DEFAULT_EVICTABLE_BUFFER_POOL_DATA_SWAP_FILE),
            max_file_size: DEFAULT_EVICTABLE_BUFFER_POOL_MAX_FILE_SIZE,
            max_mem_size: DEFAULT_EVICTABLE_BUFFER_POOL_MAX_MEM_SIZE,
            max_io_depth: DEFAULT_EVICTABLE_BUFFER_POOL_MAX_IO_DEPTH,
            eviction_arbiter_builder: EvictionArbiter::builder(),
        }
    }
}

impl EvictableBufferPoolConfig {
    #[inline]
    pub fn role(mut self, role: PoolRole) -> Self {
        self.role = role;
        self
    }

    #[inline]
    pub fn data_swap_file(mut self, data_swap_file: impl Into<PathBuf>) -> Self {
        self.data_swap_file = data_swap_file.into();
        self
    }

    #[inline]
    pub(crate) fn data_swap_file_ref(&self) -> &Path {
        &self.data_swap_file
    }

    #[inline]
    pub fn max_file_size<T>(mut self, max_file_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_file_size = Byte::from(max_file_size);
        self
    }

    #[inline]
    pub fn max_mem_size<T>(mut self, max_mem_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_mem_size = Byte::from(max_mem_size);
        self
    }

    #[inline]
    pub fn max_io_depth(mut self, max_io_depth: usize) -> Self {
        self.max_io_depth = max_io_depth;
        self
    }

    #[inline]
    pub fn eviction_arbiter_builder(
        mut self,
        eviction_arbiter_builder: EvictionArbiterBuilder,
    ) -> Self {
        self.eviction_arbiter_builder = eviction_arbiter_builder;
        self
    }

    #[inline]
    pub fn target_free(mut self, target_free: usize) -> Self {
        self.eviction_arbiter_builder = self.eviction_arbiter_builder.target_free(target_free);
        self
    }

    #[inline]
    pub fn hysteresis(mut self, hysteresis: usize) -> Self {
        self.eviction_arbiter_builder = self.eviction_arbiter_builder.hysteresis(hysteresis);
        self
    }

    #[inline]
    pub fn failure_rate_threshold(mut self, threshold: f64) -> Self {
        self.eviction_arbiter_builder = self
            .eviction_arbiter_builder
            .failure_rate_threshold(threshold);
        self
    }

    #[inline]
    pub fn failure_window(mut self, window: usize) -> Self {
        self.eviction_arbiter_builder = self.eviction_arbiter_builder.failure_window(window);
        self
    }

    #[inline]
    pub fn dynamic_batch_bounds(mut self, min_batch: usize, max_batch: usize) -> Self {
        self.eviction_arbiter_builder = self
            .eviction_arbiter_builder
            .dynamic_batch_bounds(min_batch, max_batch);
        self
    }

    #[inline]
    pub(crate) fn build_for_engine(
        self,
        fs: QuiescentGuard<FileSystem>,
    ) -> Result<(EvictableBufferPool, SparseFile)> {
        build_pool_with_swap_file_field(self, "data_swap_file", fs)
    }

    #[inline]
    pub(crate) fn build_index_for_engine(
        self,
        fs: QuiescentGuard<FileSystem>,
    ) -> Result<(EvictableBufferPool, SparseFile)> {
        build_pool_with_swap_file_field(self, "index_swap_file", fs)
    }
}
