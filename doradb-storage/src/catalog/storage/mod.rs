mod columns;
mod indexes;
mod object;
mod schemas;
mod tables;

use crate::buffer::FixedBufferPool;
use crate::catalog::TableID;
use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
pub use crate::catalog::storage::object::*;
use crate::catalog::storage::schemas::*;
use crate::catalog::storage::tables::*;
use crate::catalog::table::TableMetadata;
use crate::error::Result;
use crate::file::table_fs::TableFileSystem;
use crate::index::BlockIndex;
use crate::table::Table;
use crate::trx::MIN_SNAPSHOT_TS;
use std::sync::Arc;

pub struct CatalogStorage {
    pub(super) meta_pool: &'static FixedBufferPool,
    tables: Box<[Table]>,
}

impl CatalogStorage {
    #[inline]
    pub async fn new(
        meta_pool: &'static FixedBufferPool,
        index_pool: &'static FixedBufferPool,
        table_fs: &'static TableFileSystem,
    ) -> Result<Self> {
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
            let table_file =
                table_fs.create_table_file(*table_id, Arc::new(metadata.clone()), true)?;
            let (table_file, old_root) = table_file.commit(MIN_SNAPSHOT_TS, false).await?;
            debug_assert!(old_root.is_none());
            debug_assert!(metadata == &*table_file.active_root().metadata);
            let blk_idx = BlockIndex::new(
                meta_pool,
                *table_id,
                table_file.active_root().row_id_bound,
                table_file.active_root_ptr(),
            )
            .await;
            let table = Table::new(index_pool, blk_idx, table_file).await;
            cat.push(table);
        }
        Ok(CatalogStorage {
            meta_pool,
            tables: cat.into_boxed_slice(),
        })
    }

    #[inline]
    pub fn schemas(&self) -> Schemas<'_, FixedBufferPool> {
        Schemas {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_SCHEMAS as usize],
        }
    }

    #[inline]
    pub fn tables(&self) -> Tables<'_, FixedBufferPool> {
        Tables {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_TABLES as usize],
        }
    }

    #[inline]
    pub fn columns(&self) -> Columns<'_, FixedBufferPool> {
        Columns {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_COLUMNS as usize],
        }
    }

    #[inline]
    pub fn indexes(&self) -> Indexes<'_, FixedBufferPool> {
        Indexes {
            buf_pool: self.meta_pool,
            table: &self.tables[TABLE_ID_INDEXES as usize],
        }
    }

    #[inline]
    pub fn index_columns(&self) -> IndexColumns<'_, FixedBufferPool> {
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

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.tables.len()
    }
}

pub struct CatalogDefinition {
    pub table_id: TableID,
    pub metadata: TableMetadata,
}
