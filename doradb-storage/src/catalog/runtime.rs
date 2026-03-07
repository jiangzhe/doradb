use crate::buffer::FixedBufferPool;
use crate::catalog::{TableID, TableMetadata};
use crate::index::{BlockIndex, SecondaryIndex};
use crate::table::{MemTableAccessor, build_secondary_indexes};
use crate::trx::MIN_SNAPSHOT_TS;
use std::sync::Arc;

/// Dedicated runtime wrapper for catalog logical tables.
#[derive(Clone)]
pub struct CatalogTable {
    pub(crate) mem_pool: &'static FixedBufferPool,
    pub(crate) metadata: Arc<TableMetadata>,
    pub(crate) blk_idx: Arc<BlockIndex>,
    pub(crate) sec_idx: Arc<[SecondaryIndex]>,
}

impl CatalogTable {
    /// Build a catalog table runtime from catalog-specific construction inputs.
    #[inline]
    pub async fn new(
        mem_pool: &'static FixedBufferPool,
        index_pool: &'static FixedBufferPool,
        blk_idx: BlockIndex,
        metadata: Arc<TableMetadata>,
    ) -> Self {
        let sec_idx = build_secondary_indexes(index_pool, &metadata, MIN_SNAPSHOT_TS).await;
        CatalogTable {
            mem_pool,
            metadata,
            blk_idx: Arc::new(blk_idx),
            sec_idx,
        }
    }

    /// Return table id of this catalog table.
    #[inline]
    pub fn table_id(&self) -> TableID {
        self.blk_idx.table_id
    }

    /// Build a lightweight operation accessor over this catalog table runtime.
    #[inline]
    pub fn accessor(&self) -> MemTableAccessor<'_> {
        MemTableAccessor::from(self)
    }

    /// Return immutable metadata of this catalog table.
    #[inline]
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}
