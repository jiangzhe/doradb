use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuards, PoolRole, ReadonlyBufferPool,
};
use crate::catalog::Catalog;
use crate::file::fs::FileSystem;
use crate::quiescent::QuiescentGuard;

/// Buffer pools and stable pool guards used throughout recovery.
pub(crate) struct RecoveryBuffers {
    /// Metadata buffer pool used for checkpointed catalog storage.
    pub(crate) meta_pool: QuiescentGuard<FixedBufferPool>,
    /// Mutable secondary-index buffer pool used while rebuilding hot indexes.
    pub(crate) index_pool: QuiescentGuard<EvictableBufferPool>,
    /// Mutable row-page buffer pool used for recovered heap pages.
    pub(crate) mem_pool: QuiescentGuard<EvictableBufferPool>,
    /// Readonly disk buffer pool used by checkpointed column and DiskTree state.
    pub(crate) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    /// Stable pool guards shared by recovery operations.
    pub(crate) pool_guards: PoolGuards,
}

impl RecoveryBuffers {
    /// Build the recovery buffer grouping and its reusable guard bundle.
    #[inline]
    pub(crate) fn new(
        meta_pool: QuiescentGuard<FixedBufferPool>,
        index_pool: QuiescentGuard<EvictableBufferPool>,
        mem_pool: QuiescentGuard<EvictableBufferPool>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Self {
        let pool_guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta_pool.pool_guard())
            .push(PoolRole::Index, index_pool.pool_guard())
            .push(PoolRole::Mem, mem_pool.pool_guard())
            .push(PoolRole::Disk, disk_pool.pool_guard())
            .build();
        Self {
            meta_pool,
            index_pool,
            mem_pool,
            disk_pool,
            pool_guards,
        }
    }

    /// Return the shared guard bundle.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        let _keepalive = &self.meta_pool;
        &self.pool_guards
    }
}

/// Catalog, table files, and buffers consumed by startup recovery.
pub(crate) struct RecoveryResources<'a> {
    /// Recovery buffer pools and guard bundle.
    pub(crate) buffers: RecoveryBuffers,
    /// Table file system used to reload and clean recovered user-table files.
    pub(crate) table_fs: QuiescentGuard<FileSystem>,
    /// Catalog runtime being rebuilt from checkpointed metadata and redo logs.
    pub(crate) catalog: &'a Catalog,
}

impl<'a> RecoveryResources<'a> {
    /// Create recovery resources from explicit domain groupings.
    #[inline]
    pub(crate) fn new(
        buffers: RecoveryBuffers,
        table_fs: QuiescentGuard<FileSystem>,
        catalog: &'a Catalog,
    ) -> Self {
        Self {
            buffers,
            table_fs,
            catalog,
        }
    }
}
