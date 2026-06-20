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
    DEFAULT_EVICTABLE_BUFFER_POOL_MAX_MEM_SIZE,
};

/// Builder-style configuration for an evictable buffer pool.
///
/// Besides file and memory sizing, this type carries eviction-arbiter tuning
/// used to build the background evictor policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictableBufferPoolConfig {
    /// Logical role assigned to the buffer pool at engine construction.
    #[serde(default)]
    pub(crate) role: PoolRole,
    /// Swap-file path used when evicting pages from memory.
    pub(crate) data_swap_file: PathBuf,
    /// Maximum size allowed for the swap file.
    pub(crate) max_file_size: Byte,
    /// Maximum memory budget for resident pages.
    pub(crate) max_mem_size: Byte,
    /// Builder used to tune the eviction arbiter.
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
            eviction_arbiter_builder: EvictionArbiter::builder(),
        }
    }
}

impl EvictableBufferPoolConfig {
    /// Set the logical role assigned to the buffer pool.
    #[inline]
    pub fn role(mut self, role: PoolRole) -> Self {
        self.role = role;
        self
    }

    /// Set the swap-file path used by the buffer pool.
    #[inline]
    pub fn data_swap_file(mut self, data_swap_file: impl Into<PathBuf>) -> Self {
        self.data_swap_file = data_swap_file.into();
        self
    }

    /// Borrow the configured swap-file path.
    #[inline]
    pub(crate) fn data_swap_file_ref(&self) -> &Path {
        &self.data_swap_file
    }

    /// Set the maximum size allowed for the swap file.
    #[inline]
    pub fn max_file_size<T>(mut self, max_file_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_file_size = Byte::from(max_file_size);
        self
    }

    /// Set the maximum memory budget for resident pages.
    #[inline]
    pub fn max_mem_size<T>(mut self, max_mem_size: T) -> Self
    where
        Byte: From<T>,
    {
        self.max_mem_size = Byte::from(max_mem_size);
        self
    }

    /// Replace the eviction-arbiter builder.
    #[inline]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reserved eviction arbiter builder")
    )]
    pub(crate) fn eviction_arbiter_builder(
        mut self,
        eviction_arbiter_builder: EvictionArbiterBuilder,
    ) -> Self {
        self.eviction_arbiter_builder = eviction_arbiter_builder;
        self
    }

    /// Set the target number of free frames the evictor maintains.
    #[inline]
    pub fn target_free(mut self, target_free: usize) -> Self {
        self.eviction_arbiter_builder = self.eviction_arbiter_builder.target_free(target_free);
        self
    }

    /// Set the free-frame hysteresis used by the evictor.
    #[inline]
    pub fn hysteresis(mut self, hysteresis: usize) -> Self {
        self.eviction_arbiter_builder = self.eviction_arbiter_builder.hysteresis(hysteresis);
        self
    }

    /// Set the failure-rate threshold that triggers dynamic eviction tuning.
    #[inline]
    pub fn failure_rate_threshold(mut self, threshold: f64) -> Self {
        self.eviction_arbiter_builder = self
            .eviction_arbiter_builder
            .failure_rate_threshold(threshold);
        self
    }

    /// Set the observation window used for eviction failure-rate tracking.
    #[inline]
    pub fn failure_window(mut self, window: usize) -> Self {
        self.eviction_arbiter_builder = self.eviction_arbiter_builder.failure_window(window);
        self
    }

    /// Set dynamic eviction batch-size bounds.
    #[inline]
    pub fn dynamic_batch_bounds(mut self, min_batch: usize, max_batch: usize) -> Self {
        self.eviction_arbiter_builder = self
            .eviction_arbiter_builder
            .dynamic_batch_bounds(min_batch, max_batch);
        self
    }

    /// Build a data buffer pool and its swap file for engine startup.
    #[inline]
    pub(crate) fn build_for_engine(
        self,
        fs: QuiescentGuard<FileSystem>,
    ) -> Result<(EvictableBufferPool, SparseFile)> {
        build_pool_with_swap_file_field(self, "data_swap_file", fs)
    }

    /// Build an index buffer pool and its swap file for engine startup.
    #[inline]
    pub(crate) fn build_index_for_engine(
        self,
        fs: QuiescentGuard<FileSystem>,
    ) -> Result<(EvictableBufferPool, SparseFile)> {
        build_pool_with_swap_file_field(self, "index_swap_file", fs)
    }
}
