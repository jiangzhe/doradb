// mod index;
pub mod storage;
pub mod table;

pub use storage::*;
pub use table::*;

use crate::buffer::FixedBufferPool;
use crate::error::{Error, Result};
use crate::file::table_fs::TableFileSystem;
use crate::index::BlockIndex;
use crate::latch::RwLock;
use crate::lifetime::StaticLifetime;
use crate::table::Table;
use crate::trx::sys::TransactionSystem;
use doradb_catalog::{ColumnSpec, IndexKey, IndexSpec, SchemaID, TableID};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub const ROW_ID_COL_NAME: &str = "__row_id";

/// Catalog contains metadata of user tables.
pub struct Catalog {
    obj_id: AtomicU64,
    pub cache: CatalogCache,
    pub storage: CatalogStorage,
}

impl Catalog {
    #[inline]
    pub fn new(storage: CatalogStorage) -> Self {
        let mut cache = CatalogCache::new();
        let obj_id = storage.len() as u64;
        for (table_id, table) in storage.all() {
            cache.tables.get_mut().insert(table_id, table);
        }
        Catalog {
            obj_id: AtomicU64::new(obj_id),
            cache,
            storage,
        }
    }

    #[inline]
    pub fn next_obj_id(&self) -> u64 {
        self.obj_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn try_update_obj_id(&self, next_obj_id: u64) {
        let obj_id = self.obj_id.load(Ordering::Relaxed);
        if next_obj_id > obj_id {
            self.obj_id.fetch_max(obj_id, Ordering::SeqCst);
        }
    }

    /// Returns whether a table is user table.
    #[inline]
    pub fn is_user_table(&self, table_id: TableID) -> bool {
        table_id as usize >= self.storage.len()
    }

    /// Enable page committer for tables, excluding catalog tables.
    /// The catalog tables uses different buffer pool than data(user) tables.
    /// So no page creation should be persisted in redo log.
    #[inline]
    pub async fn enable_page_committer_for_tables(&self, trx_sys: &'static TransactionSystem) {
        let tables_g = self.cache.tables.read().await;
        for table in tables_g.values() {
            if self.is_user_table(table.table_id()) {
                table.blk_idx.enable_page_committer(trx_sys);
            }
        }
    }

    /// Reload schema cache for given schema id.
    /// If exists flag is set to true, schema should exist.
    /// Otherwise, schema should not exist.
    pub async fn reload_schema(&self, schema_id: SchemaID, exists: bool) -> Result<()> {
        let res = self
            .storage
            .schemas()
            .find_uncommitted_by_id(schema_id)
            .await;
        match (exists, res) {
            (true, Some(schema)) => {
                let schema_id = schema.schema_id;
                self.try_update_obj_id(schema_id);
                let mut g = self.cache.schemas.write().await;
                let _ = g.insert(schema_id, schema);
                Ok(())
            }
            (false, None) => {
                let mut g = self.cache.schemas.write().await;
                let _ = g.remove(&schema_id);
                Ok(())
            }
            (true, None) => Err(Error::SchemaNotFound),
            (false, Some(_)) => Err(Error::SchemaNotDeleted),
        }
    }

    pub async fn reload_create_table(
        &self,
        index_pool: &'static FixedBufferPool,
        table_fs: &'static TableFileSystem,
        table_id: TableID,
    ) -> Result<()> {
        // todo
        let res = self.storage.tables().find_uncommitted_by_id(table_id).await;
        match res {
            Some(table) => {
                // A table creation involves table, column and index.
                // Try to find the maximum object id and update the global one.
                let mut next_obj_id = table.table_id;

                // todo: use secondary index to improve performance
                let columns = self
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                debug_assert!(!columns.is_empty());
                if let Some(column_id) = columns.iter().map(|c| c.column_id).max() {
                    next_obj_id = next_obj_id.max(column_id);
                }

                let column_specs = columns
                    .into_iter()
                    .map(|c| ColumnSpec::new(&c.column_name, c.column_type, c.column_attributes))
                    .collect::<Vec<_>>();

                let indexes = self
                    .storage
                    .indexes()
                    .list_uncommitted_by_table_id(table_id)
                    .await;
                if let Some(index_id) = indexes.iter().map(|i| i.index_id).max() {
                    next_obj_id = next_obj_id.max(index_id);
                }
                self.try_update_obj_id(next_obj_id);

                let mut index_specs = vec![];
                for index in indexes {
                    let mut index_columns = self
                        .storage
                        .index_columns()
                        .list_uncommitted_by_index_id(index.index_id)
                        .await;
                    index_columns.sort_by_key(|ic| ic.index_column_no);
                    let mut index_cols = vec![];
                    for index_column in index_columns {
                        let ik = IndexKey {
                            col_no: index_column.column_no,
                            order: index_column.index_order,
                        };
                        index_cols.push(ik);
                    }
                    index_specs.push(IndexSpec::new(
                        &index.index_name,
                        index_cols,
                        index.index_attributes,
                    ));
                }
                let table_file = table_fs.open_table_file(table.table_id).await?;
                // todo: refine recovery of corrupted file.
                let metadata_in_catalog = TableMetadata::new(column_specs, index_specs);
                let metadata_in_file = &table_file.active_root().metadata;
                debug_assert_eq!(&metadata_in_catalog, metadata_in_file);

                let blk_idx = BlockIndex::new(self.storage.meta_pool, table.table_id).await;
                let table = Table::new(index_pool, blk_idx, table_file).await;
                // Update table into cache
                let mut table_cache_g = self.cache.tables.write().await;
                let res = table_cache_g.insert(table_id, table);
                assert!(res.is_none());
                Ok(())
            }
            None => Err(Error::TableNotFound),
        }
    }

    #[inline]
    pub async fn get_table(&self, table_id: TableID) -> Option<Table> {
        let g = self.cache.tables.read().await;
        g.get(&table_id).cloned()
    }

    #[inline]
    pub fn meta_pool(&self) -> &'static FixedBufferPool {
        self.storage.meta_pool
    }
}

unsafe impl Send for Catalog {}
unsafe impl Sync for Catalog {}
unsafe impl StaticLifetime for Catalog {}

pub struct CatalogCache {
    pub schemas: RwLock<HashMap<SchemaID, SchemaObject>>,
    pub tables: RwLock<HashMap<TableID, Table>>,
}

impl CatalogCache {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        CatalogCache {
            schemas: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
        }
    }
}

pub struct TableCache<'a> {
    catalog: &'a Catalog,
    map: HashMap<TableID, Option<Table>>,
}

impl<'a> TableCache<'a> {
    #[inline]
    pub fn new(catalog: &'a Catalog) -> Self {
        TableCache {
            catalog,
            map: HashMap::new(),
        }
    }

    #[inline]
    pub async fn get_table(&mut self, table_id: TableID) -> &Option<Table> {
        if self.map.contains_key(&table_id) {
            return &self.map[&table_id];
        }
        let table = self.catalog.get_table(table_id).await;
        self.map.entry(table_id).or_insert(table)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::engine::Engine;
    use doradb_catalog::{ColumnAttributes, IndexAttributes, IndexKey, IndexSpec, TableSpec};
    use doradb_datatype::{Collation, PreciseType};
    use semistr::SemiStr;

    #[inline]
    pub(crate) async fn db1(engine: &Engine) -> SchemaID {
        let mut session = engine.new_session();
        let schema_id = session.create_schema("db1", true).await.unwrap();
        drop(session);
        schema_id
    }

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1(engine: &Engine) -> TableID {
        let schema_id = db1(engine).await;

        let mut session = engine.new_session();
        let table_id = session
            .create_table(
                schema_id,
                TableSpec {
                    table_name: SemiStr::new("table1"),
                    columns: vec![ColumnSpec {
                        column_name: SemiStr::new("id"),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                },
                vec![IndexSpec::new(
                    "idx_table1_id",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table2 has i32(unique key) and string column.
    #[inline]
    pub(crate) async fn table2(engine: &Engine) -> TableID {
        let schema_id = db1(engine).await;

        let mut session = engine.new_session();
        let table_id = session
            .create_table(
                schema_id,
                TableSpec {
                    table_name: SemiStr::new("table2"),
                    columns: vec![
                        ColumnSpec {
                            column_name: SemiStr::new("id"),
                            column_type: PreciseType::Int(4, false),
                            column_attributes: ColumnAttributes::empty(),
                        },
                        ColumnSpec {
                            column_name: SemiStr::new("name"),
                            column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                            column_attributes: ColumnAttributes::empty(),
                        },
                    ],
                },
                vec![IndexSpec::new(
                    "idx_table2_id",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table3 has single string key column.
    #[inline]
    pub(crate) async fn table3(engine: &Engine) -> TableID {
        let schema_id = db1(engine).await;

        let mut session = engine.new_session();

        let table_id = session
            .create_table(
                schema_id,
                TableSpec {
                    table_name: SemiStr::new("table3"),
                    columns: vec![ColumnSpec {
                        column_name: SemiStr::new("name"),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::empty(),
                    }],
                },
                vec![IndexSpec::new(
                    "idx_table3_name",
                    vec![IndexKey::new(0)],
                    IndexAttributes::PK,
                )],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }

    /// Table4 has two i32 columns.
    /// First is unique index.
    /// Second is non-unique index.
    #[inline]
    pub(crate) async fn table4(engine: &Engine) -> TableID {
        let schema_id = db1(engine).await;

        let mut session = engine.new_session();

        let table_id = session
            .create_table(
                schema_id,
                TableSpec {
                    table_name: SemiStr::new("table4"),
                    columns: vec![
                        ColumnSpec {
                            column_name: SemiStr::new("id"),
                            column_type: PreciseType::Int(4, false),
                            column_attributes: ColumnAttributes::empty(),
                        },
                        ColumnSpec {
                            column_name: SemiStr::new("val"),
                            column_type: PreciseType::Int(4, false),
                            column_attributes: ColumnAttributes::empty(),
                        },
                    ],
                },
                vec![
                    IndexSpec::new(
                        "idx_table4_id",
                        vec![IndexKey::new(0)],
                        // unique index.
                        IndexAttributes::PK,
                    ),
                    IndexSpec::new(
                        "idx_table4_val",
                        vec![IndexKey::new(1)],
                        // non-unique index.
                        IndexAttributes::empty(),
                    ),
                ],
            )
            .await
            .unwrap();

        drop(session);
        table_id
    }
}
