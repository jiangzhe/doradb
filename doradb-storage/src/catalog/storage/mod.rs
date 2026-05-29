mod checkpoint;
mod columns;
mod indexes;
mod object;
pub(crate) mod tables;

use crate::buffer::{BufferPool, FixedBufferPool, ReadonlyBufferPool};
use crate::catalog::CatalogTable;
use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
pub(crate) use crate::catalog::storage::object::*;
use crate::catalog::storage::tables::*;
use crate::catalog::table::TableMetadata;
use crate::error::Result;
use crate::file::fs::FileSystem;
use crate::file::multi_table_file::{MultiTableFile, MultiTableFileSnapshot};
use crate::id::TableID;
use crate::index::BlockIndex;
use crate::quiescent::QuiescentGuard;
use std::sync::Arc;

/// Runtime storage container for all catalog logical tables.
pub(crate) struct CatalogStorage {
    pub(super) meta_pool: QuiescentGuard<FixedBufferPool>,
    pub(super) table_fs: QuiescentGuard<FileSystem>,
    tables: Box<[Arc<CatalogTable>]>,
    next_table_id: TableID,
    pub(super) mtb: Arc<MultiTableFile>,
    pub(super) disk_pool: QuiescentGuard<ReadonlyBufferPool>,
}

impl CatalogStorage {
    /// Open or initialize catalog storage and bootstrap catalog table runtimes.
    #[inline]
    pub(crate) async fn new(
        meta_pool: QuiescentGuard<FixedBufferPool>,
        table_fs: QuiescentGuard<FileSystem>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        let meta_pool_guard = meta_pool.pool_guard();
        let mtb = table_fs
            .open_or_create_multi_table_file(disk_pool.clone())
            .await?;
        let mtb_snapshot = mtb.load_snapshot()?;

        let mut cat: Vec<Arc<CatalogTable>> = vec![];
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_index_columns(),
        ] {
            // make sure table id matches.
            assert_eq!(cat.len(), table_id.as_usize());
            let metadata = Arc::new(metadata.clone());
            let blk_idx = BlockIndex::new_catalog(meta_pool.clone(), &meta_pool_guard).await?;
            let table = Arc::new(
                CatalogTable::new(
                    meta_pool.clone(),
                    &meta_pool_guard,
                    *table_id,
                    blk_idx,
                    metadata,
                )
                .await?,
            );
            cat.push(table);
        }
        let storage = CatalogStorage {
            meta_pool,
            table_fs,
            tables: cat.into_boxed_slice(),
            next_table_id: mtb_snapshot.meta.next_table_id,
            mtb,
            disk_pool,
        };
        Ok(storage)
    }

    /// Accessor of `catalog.tables`.
    #[inline]
    pub(crate) fn tables(&self) -> Tables<'_> {
        Tables {
            table: &self.tables[TABLE_ID_TABLES.as_usize()],
        }
    }

    /// Accessor of `catalog.columns`.
    #[inline]
    pub(crate) fn columns(&self) -> Columns<'_> {
        Columns {
            table: &self.tables[TABLE_ID_COLUMNS.as_usize()],
        }
    }

    /// Accessor of `catalog.indexes`.
    #[inline]
    pub(crate) fn indexes(&self) -> Indexes<'_> {
        Indexes {
            table: &self.tables[TABLE_ID_INDEXES.as_usize()],
        }
    }

    /// Accessor of `catalog.index_columns`.
    #[inline]
    pub(crate) fn index_columns(&self) -> IndexColumns<'_> {
        IndexColumns {
            table: &self.tables[TABLE_ID_INDEX_COLUMNS.as_usize()],
        }
    }

    /// Return one catalog table runtime by table id.
    #[inline]
    pub(crate) fn get_catalog_table(&self, table_id: TableID) -> Option<Arc<CatalogTable>> {
        self.tables.get(table_id.as_usize()).map(Arc::clone)
    }

    /// Return current next table id persisted in catalog snapshot.
    #[inline]
    pub(crate) fn next_table_id(&self) -> TableID {
        self.next_table_id
    }

    /// Returns current persisted catalog checkpoint snapshot from `catalog.mtb`.
    #[inline]
    pub(crate) fn checkpoint_snapshot(&self) -> Result<MultiTableFileSnapshot> {
        self.mtb.load_snapshot()
    }
}

/// Static definition used to bootstrap one catalog logical table.
pub(crate) struct CatalogDefinition {
    pub(crate) table_id: TableID,
    pub(crate) metadata: TableMetadata,
}
