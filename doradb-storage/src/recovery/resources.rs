use crate::buffer::PoolGuards;
use crate::catalog::Catalog;
use crate::component::EnginePools;
use crate::conf::HotIndexBuildConfig;
use crate::conf::path::validate_log_file_stem;
use crate::conf::{RecoveryConfig, TrxSysConfig};
use crate::error::RuntimeResult;
use crate::file::fs::FileSystem;
use crate::index::build::HotBuildPolicy;
use crate::io::STORAGE_SECTOR_SIZE;
use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
use crate::log::{RedoLogFinalizer, discover_redo_log_files};
use crate::quiescent::QuiescentGuard;
use crate::recovery::stream::RedoReplayPlanner;
use crate::runtime::thread_pool::ThreadPool;

use super::RecoveryCoordinator;
#[cfg(feature = "profiling")]
use crate::profiling::HotIndexBuildProfiler;
#[cfg(feature = "profiling")]
use std::sync::Arc;

/// Catalog, table files, pools, and guards consumed by startup recovery.
pub(crate) struct RecoveryResources<'a> {
    /// Buffer pools used by recovery.
    pub(crate) pools: EnginePools,
    /// Stable pool guards shared by recovery operations.
    pub(crate) pool_guards: PoolGuards,
    /// Table file system used to reload and clean recovered user-table files.
    pub(crate) table_fs: QuiescentGuard<FileSystem>,
    /// Existing finite-job pool, already running before recovery starts.
    pub(crate) thread_pool: QuiescentGuard<ThreadPool>,
    /// Catalog runtime being rebuilt from checkpointed metadata and redo logs.
    pub(crate) catalog: &'a Catalog,
    /// Validated extraction limits retained for bootstrap builds.
    pub(crate) hot_build_policy: HotBuildPolicy,
    /// Recorder shared by bootstrap extraction and later runtime builds.
    #[cfg(feature = "profiling")]
    pub(crate) hot_build_profiler: Arc<HotIndexBuildProfiler>,
}

impl<'a> RecoveryResources<'a> {
    /// Create recovery resources from explicit domain groupings.
    #[inline]
    pub(crate) fn new(
        pools: EnginePools,
        table_fs: QuiescentGuard<FileSystem>,
        thread_pool: QuiescentGuard<ThreadPool>,
        catalog: &'a Catalog,
    ) -> Self {
        let pool_guards = pools.pool_guards().clone();
        let hot_build_policy =
            HotBuildPolicy::new(HotIndexBuildConfig::default(), thread_pool.worker_threads())
                .unwrap_or_else(|_| unreachable!("running pool has validated positive sizing"));
        Self {
            pools,
            pool_guards,
            table_fs,
            thread_pool,
            catalog,
            hot_build_policy,
            #[cfg(feature = "profiling")]
            hot_build_profiler: Arc::new(HotIndexBuildProfiler::default()),
        }
    }

    /// Carry the engine-normalized policy into bootstrap before recovery runs.
    pub(crate) fn with_hot_build_policy(mut self, policy: HotBuildPolicy) -> Self {
        self.hot_build_policy = policy;
        self
    }

    /// Prepare startup recovery from validated transaction configuration.
    #[inline]
    pub(crate) fn prepare(
        self,
        config: &TrxSysConfig,
        recovery: &RecoveryConfig,
        file_prefix: String,
    ) -> RuntimeResult<RecoveryCoordinator<'a>> {
        let log_block_size = config.log_block_size.as_u64() as usize;
        let file_max_size = config.log_file_max_size.as_u64() as usize;
        debug_assert!(config.log_write_io_depth != 0);
        debug_assert!(validate_log_file_stem(&config.log_file_stem));
        debug_assert!((STORAGE_SECTOR_SIZE..=u16::MAX as usize + 1).contains(&log_block_size));
        debug_assert_eq!(log_block_size % STORAGE_SECTOR_SIZE, 0);
        debug_assert!(file_max_size >= REDO_DEFAULT_DATA_START_OFFSET + log_block_size);
        debug_assert_eq!(
            (file_max_size - REDO_DEFAULT_DATA_START_OFFSET) % log_block_size,
            0
        );

        let first_retained_file_seq = self
            .catalog
            .storage
            .checkpoint_snapshot()
            .meta
            .first_redo_log_seq;
        let logs = discover_redo_log_files(&file_prefix, first_retained_file_seq, false)?;
        let planner = RedoReplayPlanner::new(logs);
        let finalizer = RedoLogFinalizer::new(
            file_prefix,
            config.log_write_io_depth,
            file_max_size,
            log_block_size,
            0,
        );
        Ok(RecoveryCoordinator::new(self, planner, recovery, finalizer))
    }
}
