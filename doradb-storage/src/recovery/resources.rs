use crate::buffer::PoolGuards;
use crate::catalog::Catalog;
use crate::component::EnginePools;
use crate::file::fs::FileSystem;
use crate::quiescent::QuiescentGuard;

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
}
