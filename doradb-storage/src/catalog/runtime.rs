use crate::buffer::{FixedBufferPool, PoolGuard};
use crate::catalog::{TableID, TableMetadata};
use crate::index::{BlockIndex, RowLocation};
use crate::table::{GenericMemTable, MemTableAccessor};
use crate::trx::MIN_SNAPSHOT_TS;
use std::ops::Deref;
use std::sync::Arc;

/// Dedicated runtime wrapper for catalog logical tables.
pub struct CatalogTable {
    pub(crate) mem: GenericMemTable<FixedBufferPool>,
}

impl CatalogTable {
    /// Build a catalog table runtime from catalog-specific construction inputs.
    #[inline]
    pub async fn new(
        mem_pool: &'static FixedBufferPool,
        index_pool: &'static FixedBufferPool,
        index_pool_guard: &PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        metadata: Arc<TableMetadata>,
    ) -> Self {
        let mem = GenericMemTable::new(
            mem_pool,
            mem_pool.row_pool_identity(),
            index_pool,
            index_pool_guard,
            table_id,
            metadata,
            blk_idx,
            MIN_SNAPSHOT_TS,
        )
        .await;
        CatalogTable { mem }
    }

    /// Build a lightweight operation accessor over this catalog table runtime.
    #[inline]
    pub fn accessor(&self) -> MemTableAccessor<'_> {
        MemTableAccessor::from(self)
    }

    #[inline]
    pub(crate) async fn find_row(
        &self,
        guards: &crate::buffer::PoolGuards,
        row_id: crate::row::RowID,
    ) -> RowLocation {
        GenericMemTable::find_row(self, guards, row_id, None).await
    }
}

impl Deref for CatalogTable {
    type Target = GenericMemTable<FixedBufferPool>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}
