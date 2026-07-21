use crate::buffer::PoolGuards;
use crate::catalog::Catalog;
use crate::component::EnginePools;
use crate::conf::TrxSysConfig;
use crate::conf::path::validate_log_file_stem;
use crate::error::Result;
use crate::file::fs::FileSystem;
use crate::io::STORAGE_SECTOR_SIZE;
use crate::log::format::REDO_DEFAULT_DATA_START_OFFSET;
use crate::log::{RedoLogFinalizer, discover_redo_log_files};
use crate::quiescent::QuiescentGuard;
use crate::recovery::stream::RedoReplayPlanner;

use super::RecoveryCoordinator;

/// Catalog, table files, pools, and guards consumed by startup recovery.
pub(crate) struct RecoveryResources<'a> {
    /// Buffer pools used by recovery.
    pub(crate) pools: EnginePools,
    /// Stable pool guards shared by recovery operations.
    pub(crate) pool_guards: PoolGuards,
    /// Table file system used to reload and clean recovered user-table files.
    pub(crate) table_fs: QuiescentGuard<FileSystem>,
    /// Catalog runtime being rebuilt from checkpointed metadata and redo logs.
    pub(crate) catalog: &'a Catalog,
}

impl<'a> RecoveryResources<'a> {
    /// Create recovery resources from explicit domain groupings.
    #[inline]
    pub(crate) fn new(
        pools: EnginePools,
        table_fs: QuiescentGuard<FileSystem>,
        catalog: &'a Catalog,
    ) -> Self {
        let pool_guards = pools.pool_guards();
        Self {
            pools,
            pool_guards,
            table_fs,
            catalog,
        }
    }

    /// Prepare startup recovery from validated transaction configuration.
    #[inline]
    pub(crate) fn prepare(self, config: &TrxSysConfig) -> Result<RecoveryCoordinator<'a>> {
        let log_block_size = config.log_block_size.as_u64() as usize;
        let file_max_size = config.log_file_max_size.as_u64() as usize;
        debug_assert!(config.log_write_io_depth != 0);
        debug_assert!(config.recovery_io_depth != 0);
        debug_assert!(validate_log_file_stem(&config.log_file_stem));
        debug_assert!((STORAGE_SECTOR_SIZE..=u16::MAX as usize + 1).contains(&log_block_size));
        debug_assert_eq!(log_block_size % STORAGE_SECTOR_SIZE, 0);
        debug_assert!(file_max_size >= REDO_DEFAULT_DATA_START_OFFSET + log_block_size);
        debug_assert_eq!(
            (file_max_size - REDO_DEFAULT_DATA_START_OFFSET) % log_block_size,
            0
        );

        let file_prefix = config.file_prefix()?;
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
        Ok(RecoveryCoordinator::new(
            self,
            planner,
            config.recovery_io_depth,
            config.recovery_disable_dml_validation,
            finalizer,
        ))
    }
}
