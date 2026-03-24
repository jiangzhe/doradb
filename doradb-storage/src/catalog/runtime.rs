use crate::buffer::{FixedBufferPool, PoolGuard, PoolGuards};
use crate::catalog::{TableID, TableMetadata};
use crate::error::Result;
use crate::index::{BlockIndex, RowLocation};
use crate::quiescent::QuiescentGuard;
use crate::row::ops::SelectKey;
use crate::table::{GenericMemTable, MemTableAccessor};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::value::Val;
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
        mem_pool: QuiescentGuard<FixedBufferPool>,
        index_pool: QuiescentGuard<FixedBufferPool>,
        index_pool_guard: &PoolGuard,
        table_id: TableID,
        blk_idx: BlockIndex,
        metadata: Arc<TableMetadata>,
    ) -> Self {
        let mem = GenericMemTable::new(
            mem_pool.clone(),
            mem_pool.row_pool_role(),
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
    pub(crate) async fn insert_no_trx(&self, guards: &PoolGuards, cols: &[Val]) -> Result<()> {
        self.accessor().insert_catalog_no_trx(guards, cols).await
    }

    #[inline]
    pub(crate) async fn delete_unique_no_trx(
        &self,
        guards: &PoolGuards,
        key: &SelectKey,
    ) -> Result<()> {
        self.accessor()
            .delete_catalog_unique_no_trx(guards, key)
            .await
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
