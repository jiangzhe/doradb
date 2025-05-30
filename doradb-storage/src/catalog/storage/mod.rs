mod columns;
mod indexes;
mod schemas;
mod tables;

use crate::buffer::guard::PageExclusiveGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::table::TableMetadata;
use crate::index::{BlockIndex, BlockNode};
use crate::table::Table;
use doradb_catalog::TableID;

use crate::catalog::storage::columns::*;
use crate::catalog::storage::indexes::*;
use crate::catalog::storage::schemas::*;
use crate::catalog::storage::tables::*;

pub struct CatalogStorage<P: BufferPool>(Box<[Table<P>]>);

impl<P: BufferPool> CatalogStorage<P> {
    #[inline]
    pub async fn new(buf_pool: &'static P) -> Self {
        let mut cat = vec![];
        for CatalogDefinition {
            table_id,
            root_page_id,
            metadata,
        } in [
            catalog_definition_of_schemas(),
            catalog_definition_of_tables(),
            catalog_definition_of_columns(),
            catalog_definition_of_indexes(),
            catalog_definition_of_index_columns(),
        ] {
            // make sure table id matches.
            assert_eq!(cat.len(), *table_id as usize);
            // catalog table with manually allocated page id.
            let g: PageExclusiveGuard<BlockNode> = buf_pool.allocate_page().await;
            let page_id = g.page_id();
            assert_eq!(*root_page_id, page_id);
            let blk_idx = BlockIndex::new_with_page(g, *table_id).await;
            let table = Table::new(blk_idx, metadata.clone());
            cat.push(table);
        }
        CatalogStorage(cat.into_boxed_slice())
    }

    #[inline]
    pub fn schemas(&self) -> Schemas<P> {
        Schemas(&self.0[TABLE_ID_SCHEMAS as usize])
    }

    #[inline]
    pub fn tables(&self) -> Tables<P> {
        Tables(&self.0[TABLE_ID_TABLES as usize])
    }

    #[inline]
    pub fn columns(&self) -> Columns<P> {
        Columns(&self.0[TABLE_ID_COLUMNS as usize])
    }

    #[inline]
    pub fn indexes(&self) -> Indexes<P> {
        Indexes(&self.0[TABLE_ID_INDEXES as usize])
    }

    #[inline]
    pub fn index_columns(&self) -> IndexColumns<P> {
        IndexColumns(&self.0[TABLE_ID_INDEX_COLUMNS as usize])
    }
}

pub struct CatalogDefinition {
    pub table_id: TableID,
    pub root_page_id: PageID,
    pub metadata: TableMetadata,
}
