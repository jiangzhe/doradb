use crate::buffer::{EvictionArbiter, EvictionArbiterBuilder, evictable_resident_pages};
use crate::conf::path::validate_swap_file_path_candidate;
use crate::error::{ConfigError, ConfigResult};
use byte_unit::Byte;
use error_stack::Report;
use std::path::{Path, PathBuf};

use super::consts::{
    DEFAULT_EVICTABLE_BUFFER_POOL_MAX_FILE_SIZE, DEFAULT_EVICTABLE_BUFFER_POOL_MAX_MEM_SIZE,
    DEFAULT_EVICTABLE_BUFFER_POOL_SWAP_FILE,
};

/// Builder-style configuration for an evictable buffer pool.
///
/// Besides file and memory sizing, this type carries eviction-arbiter tuning
/// used to build the background evictor policy.
#[derive(Debug, Clone)]
pub struct EvictableBufferPoolConfig {
    /// Swap-file path used when evicting pages from memory.
    pub swap_file: PathBuf,
    /// Maximum size allowed for the swap file.
    pub max_file_size: Byte,
    /// Maximum memory budget for resident pages.
    pub max_mem_size: Byte,
    /// Builder used to tune the eviction arbiter.
    pub(crate) eviction_arbiter_builder: EvictionArbiterBuilder,
}

impl Default for EvictableBufferPoolConfig {
    #[inline]
    fn default() -> Self {
        EvictableBufferPoolConfig {
            swap_file: PathBuf::from(DEFAULT_EVICTABLE_BUFFER_POOL_SWAP_FILE),
            max_file_size: DEFAULT_EVICTABLE_BUFFER_POOL_MAX_FILE_SIZE,
            max_mem_size: DEFAULT_EVICTABLE_BUFFER_POOL_MAX_MEM_SIZE,
            eviction_arbiter_builder: EvictionArbiter::builder(),
        }
    }
}

impl EvictableBufferPoolConfig {
    /// Validate and normalize buffer-pool construction inputs without touching
    /// the filesystem.
    #[inline]
    pub(crate) fn validate(mut self) -> ConfigResult<Self> {
        validate_swap_file_path_candidate(&self.swap_file)?;
        let resident_pages =
            validate_evictable_sizing(self.max_file_size.as_u64(), self.max_mem_size.as_u64())?;
        self.eviction_arbiter_builder = self
            .eviction_arbiter_builder
            .normalize_for_capacity(resident_pages);
        Ok(self)
    }

    /// Set the swap-file path used by the buffer pool.
    #[inline]
    pub fn swap_file(mut self, swap_file: impl Into<PathBuf>) -> Self {
        self.swap_file = swap_file.into();
        self
    }

    /// Borrow the configured swap-file path.
    #[inline]
    pub(crate) fn swap_file_ref(&self) -> &Path {
        &self.swap_file
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
}

/// Validate the sizing shared by the index and data evictable pools.
#[inline]
pub(crate) fn validate_evictable_sizing(
    max_file_size: u64,
    max_mem_size: u64,
) -> ConfigResult<usize> {
    let max_file_size_usize = usize::try_from(max_file_size).map_err(|_| {
        Report::new(ConfigError::InvalidBufferPoolConfig).attach("max_file_size exceeds usize")
    })?;
    let max_mem_size_usize = usize::try_from(max_mem_size).map_err(|_| {
        Report::new(ConfigError::InvalidBufferPoolConfig).attach("max_mem_size exceeds usize")
    })?;
    let Some(resident_pages) = evictable_resident_pages(max_file_size_usize, max_mem_size_usize)
    else {
        return Err(
            Report::new(ConfigError::InvalidBufferPoolConfig).attach(format!(
                "max_file_size={max_file_size}, max_mem_size={max_mem_size}"
            )),
        );
    };
    Ok(resident_pages)
}
