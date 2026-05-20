use crate::buffer::{FixedBufferPool, PoolGuard, PoolRole};
use crate::catalog::{TableID, TableMetadata};
use crate::error::Result;
use crate::index::BlockIndex;
use crate::quiescent::QuiescentGuard;
use crate::table::GenericMemTable;
use crate::trx::MIN_SNAPSHOT_TS;
use std::ops::Deref;
use std::sync::Arc;

/// Dedicated runtime wrapper for catalog logical tables.
pub struct CatalogTable {
    pub(crate) mem: GenericMemTable<FixedBufferPool, FixedBufferPool>,
}

impl CatalogTable {
    /// Build a catalog table runtime from catalog-specific construction inputs.
    #[inline]
    pub async fn new(
        mem_pool: QuiescentGuard<FixedBufferPool>,
        meta_pool_guard: &PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        metadata: Arc<TableMetadata>,
    ) -> Result<Self> {
        let mem = GenericMemTable::new(
            mem_pool.clone(),
            mem_pool.row_pool_role(),
            mem_pool,
            PoolRole::Meta,
            meta_pool_guard,
            table_id,
            metadata,
            blk_idx,
            MIN_SNAPSHOT_TS,
        )
        .await?;
        Ok(CatalogTable { mem })
    }
}

impl Deref for CatalogTable {
    type Target = GenericMemTable<FixedBufferPool, FixedBufferPool>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}
