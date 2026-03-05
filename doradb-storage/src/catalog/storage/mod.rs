mod columns;
mod indexes;
mod object;
mod tables;

use crate::buffer::{EvictableBufferPool, FixedBufferPool, GlobalReadonlyBufferPool};
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
use crate::table::Table;
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::TrxID;
use std::sync::Arc;

pub struct CatalogStorage {
    pub(super) meta_pool: &'static FixedBufferPool,
    pub(super) mem_pool: &'static EvictableBufferPool,
    tables: Box<[Table]>,
    next_user_obj_id: ObjID,
    mtb: Arc<MultiTableFile>,
}

impl CatalogStorage {
    #[inline]
    pub async fn new(
        meta_pool: &'static FixedBufferPool,
        index_pool: &'static FixedBufferPool,
        mem_pool: &'static EvictableBufferPool,
        table_fs: &'static TableFileSystem,
        global_disk_pool: &'static GlobalReadonlyBufferPool,
    ) -> Result<Self> {
        let mtb = table_fs.open_or_create_multi_table_file().await?;
        let mtb_snapshot = mtb.load_snapshot()?;

        let mut cat: Vec<Table> = vec![];
        for CatalogDefinition { table_id, metadata } in [
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
                table_file.active_root().pivot_row_id,
                table_file.active_root().column_block_index_root,
                Arc::clone(&table_file),
                global_disk_pool,
            )
            .await;
            let table =
                Table::new(mem_pool, index_pool, global_disk_pool, blk_idx, table_file).await;

            // Catalog table files are only runtime scratch structures today.
            // Persistent catalog state is managed by catalog.mtb.
            //
            // TODO(RFC-0006 phase 3+): remove this unlink path once catalog
            // tables are fully in-memory. That change likely requires
            // reshaping `Table` so catalog tables no longer depend on legacy
            // `TableFile`, and no old-style `*.tbl` files are created.
            let _ = std::fs::remove_file(table_fs.table_file_path(*table_id));
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

    #[inline]
    pub fn tables(&self) -> Tables<'_> {
        Tables {
            table: &self.tables[TABLE_ID_TABLES as usize],
        }
    }

    #[inline]
    pub fn columns(&self) -> Columns<'_> {
        Columns {
            table: &self.tables[TABLE_ID_COLUMNS as usize],
        }
    }

    #[inline]
    pub fn indexes(&self) -> Indexes<'_> {
        Indexes {
            table: &self.tables[TABLE_ID_INDEXES as usize],
        }
    }

    #[inline]
    pub fn index_columns(&self) -> IndexColumns<'_> {
        IndexColumns {
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
            let active_root = table.file.active_root();
            roots[table_id] = CatalogTableRootDesc {
                table_id: table_id as u64,
                root_page_id: active_root.column_block_index_root,
                pivot_row_id: active_root.pivot_row_id,
            };
        }
        roots
    }
}

pub struct CatalogDefinition {
    pub table_id: TableID,
    pub metadata: TableMetadata,
}
