mod columns;
mod indexes;
mod object;
mod tables;

use crate::buffer::{EvictableBufferPool, FixedBufferPool, GlobalReadonlyBufferPool};
use crate::catalog::runtime::CatalogTable;
use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
pub use crate::catalog::storage::object::*;
use crate::catalog::storage::tables::*;
use crate::catalog::table::TableMetadata;
use crate::catalog::{ObjID, TableID};
use crate::error::Result;
use crate::file::multi_table_file::{
    CATALOG_TABLE_ROOT_DESC_COUNT, CatalogTableRootDesc, MultiTableFile,
};
use crate::file::table_fs::TableFileSystem;
use crate::index::BlockIndex;
use crate::trx::TrxID;
use std::sync::Arc;

/// Runtime storage container for all catalog logical tables.
pub struct CatalogStorage {
    pub(super) meta_pool: &'static FixedBufferPool,
    pub(super) mem_pool: &'static EvictableBufferPool,
    tables: Box<[CatalogTable]>,
    next_user_obj_id: ObjID,
    mtb: Arc<MultiTableFile>,
}

impl CatalogStorage {
    /// Open or initialize catalog storage and bootstrap catalog table runtimes.
    #[inline]
    pub async fn new(
        meta_pool: &'static FixedBufferPool,
        index_pool: &'static FixedBufferPool,
        mem_pool: &'static EvictableBufferPool,
        table_fs: &'static TableFileSystem,
        _global_disk_pool: &'static GlobalReadonlyBufferPool,
    ) -> Result<Self> {
        let mtb = table_fs.open_or_create_multi_table_file().await?;
        let mtb_snapshot = mtb.load_snapshot()?;

        let mut cat: Vec<CatalogTable> = vec![];
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_index_columns(),
        ] {
            // make sure table id matches.
            assert_eq!(cat.len(), *table_id as usize);
            let metadata = Arc::new(metadata.clone());
            let blk_idx =
                BlockIndex::new_catalog(meta_pool, *table_id, Arc::clone(&metadata)).await;
            let table = CatalogTable::new(meta_pool, index_pool, blk_idx, metadata).await;
            cat.push(table);
        }
        Ok(CatalogStorage {
            meta_pool,
            mem_pool,
            tables: cat.into_boxed_slice(),
            next_user_obj_id: mtb_snapshot.meta.next_user_obj_id,
            mtb,
        })
    }

    /// Accessor of `catalog.tables`.
    #[inline]
    pub fn tables(&self) -> Tables<'_> {
        Tables {
            table: &self.tables[TABLE_ID_TABLES as usize],
        }
    }

    /// Accessor of `catalog.columns`.
    #[inline]
    pub fn columns(&self) -> Columns<'_> {
        Columns {
            table: &self.tables[TABLE_ID_COLUMNS as usize],
        }
    }

    /// Accessor of `catalog.indexes`.
    #[inline]
    pub fn indexes(&self) -> Indexes<'_> {
        Indexes {
            table: &self.tables[TABLE_ID_INDEXES as usize],
        }
    }

    /// Accessor of `catalog.index_columns`.
    #[inline]
    pub fn index_columns(&self) -> IndexColumns<'_> {
        IndexColumns {
            table: &self.tables[TABLE_ID_INDEX_COLUMNS as usize],
        }
    }

    /// Return one catalog table runtime by table id.
    #[inline]
    pub fn get_catalog_table(&self, table_id: TableID) -> Option<CatalogTable> {
        self.tables.get(table_id as usize).cloned()
    }

    /// Return number of catalog logical tables.
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Return current next user object id persisted in catalog snapshot.
    #[inline]
    pub fn next_user_obj_id(&self) -> ObjID {
        self.next_user_obj_id
    }

    /// Publish one catalog metadata snapshot into `catalog.mtb`.
    ///
    /// This method is temporary in RFC 0006 and is expected to change in phase 3,
    /// where a dedicated catalog checkpointer will replay catalog redo logs and
    /// merge checkpoint deltas into `catalog.mtb`.
    #[inline]
    pub async fn publish_checkpoint(
        &self,
        checkpoint_cts: TrxID,
        next_user_obj_id: ObjID,
    ) -> Result<()> {
        self.mtb
            .publish_checkpoint(checkpoint_cts, next_user_obj_id, self.catalog_table_roots())
            .await
    }

    #[inline]
    fn catalog_table_roots(&self) -> [CatalogTableRootDesc; CATALOG_TABLE_ROOT_DESC_COUNT] {
        let mut roots = [CatalogTableRootDesc::default(); CATALOG_TABLE_ROOT_DESC_COUNT];
        for table in &self.tables {
            let table_id = table.table_id() as usize;
            if table_id >= roots.len() {
                continue;
            }
            let (pivot_row_id, root_page_id) = table.blk_idx.root_snapshot();
            roots[table_id] = CatalogTableRootDesc {
                table_id: table_id as u64,
                root_page_id,
                pivot_row_id,
            };
        }
        roots
    }
}

/// Static definition used to bootstrap one catalog logical table.
pub struct CatalogDefinition {
    pub table_id: TableID,
    pub metadata: TableMetadata,
}
