mod columns;
mod indexes;
mod object;
mod schemas;
mod tables;

use crate::buffer::FixedBufferPool;
use crate::catalog::table::TableMetadata;
use crate::index::BlockIndex;
use crate::table::Table;
use doradb_catalog::TableID;

use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
pub use crate::catalog::storage::object::*;
use crate::catalog::storage::schemas::*;
use crate::catalog::storage::tables::*;

pub struct CatalogStorage {
    pub(super) meta_pool: &'static FixedBufferPool,
    tables: Box<[Table]>,
}

impl CatalogStorage {
    #[inline]
    pub async fn new(meta_pool: &'static FixedBufferPool) -> Self {
        let mut cat: Vec<Table> = vec![];
        for CatalogDefinition { table_id, metadata } in [
            catalog_definition_of_schemas(),
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_index_columns(),
        ] {
            // make sure table id matches.
            assert_eq!(cat.len(), *table_id as usize);
            // catalog table with manually allocated page id.
            let blk_idx = BlockIndex::new(meta_pool, *table_id).await;
            let table = Table::new(blk_idx, metadata.clone());
            cat.push(table);
        }
        CatalogStorage {
            meta_pool,
            tables: cat.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn schemas(&self) -> Schemas<FixedBufferPool> {
        Schemas {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_SCHEMAS as usize],
        }
    }

    #[inline]
    pub fn tables(&self) -> Tables<FixedBufferPool> {
        Tables {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_TABLES as usize],
        }
    }

    #[inline]
    pub fn columns(&self) -> Columns<FixedBufferPool> {
        Columns {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_COLUMNS as usize],
        }
    }

    #[inline]
    pub fn indexes(&self) -> Indexes<FixedBufferPool> {
        Indexes {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_INDEXES as usize],
        }
    }

    #[inline]
    pub fn index_columns(&self) -> IndexColumns<FixedBufferPool> {
        IndexColumns {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_INDEX_COLUMNS as usize],
        }
    }

    #[inline]
    pub fn all(&self) -> Vec<(TableID, Table)> {
        self.tables
            .iter()
            .enumerate()
            .map(|(idx, table)| (idx as TableID, table.clone()))
            .collect()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.tables.len()
    }
}

pub struct CatalogDefinition {
    pub table_id: TableID,
    pub metadata: TableMetadata,
}
