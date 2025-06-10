// mod index;
pub mod storage;
pub mod table;

pub use storage::*;
pub use table::*;

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::index::BlockIndex;
use crate::lifetime::StaticLifetime;
use crate::table::Table;
use crate::trx::sys::TransactionSystem;
use doradb_catalog::{ColumnAttributes, ColumnSpec, IndexKey, IndexSpec, SchemaID, TableID};
use doradb_datatype::PreciseType;
use parking_lot::RwLock;
use semistr::SemiStr;
use std::collections::HashMap;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};

pub const ROW_ID_COL_NAME: &'static str = "__row_id";

pub fn row_id_spec() -> ColumnSpec {
    ColumnSpec {
        column_name: SemiStr::new(ROW_ID_COL_NAME),
        column_type: PreciseType::Int(8, false),
        column_attributes: ColumnAttributes::empty(),
    }
}

/// Catalog contains metadata of user tables.
pub struct Catalog<P: BufferPool> {
    obj_id: AtomicU64,
    pub cache: CatalogCache<P>,
    pub storage: CatalogStorage<P>,
}

impl<P: BufferPool> Catalog<P> {
    #[inline]
    pub fn new(storage: CatalogStorage<P>) -> Self {
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

    #[inline]
    pub fn enable_page_committer_for_all_tables(&self, trx_sys: &'static TransactionSystem<P>) {
        let tables_g = self.cache.tables.read();
        for table in tables_g.values() {
            table.blk_idx.enable_page_committer(trx_sys);
        }
    }

    /// Reload schema cache for given schema id.
    /// If exists flag is set to true, schema should exist.
    /// Otherwise, schema should not exist.
    pub async fn reload_schema(
        &self,
        buf_pool: &'static P,
        schema_id: SchemaID,
        exists: bool,
    ) -> Result<()> {
        let res = self
            .storage
            .schemas()
            .find_uncommitted_by_id(buf_pool, schema_id)
            .await;
        match (exists, res) {
            (true, Some(schema)) => {
                let schema_id = schema.schema_id;
                self.try_update_obj_id(schema_id);
                let mut g = self.cache.schemas.write();
                let _ = g.insert(schema_id, schema);
                Ok(())
            }
            (false, None) => {
                let mut g = self.cache.schemas.write();
                let _ = g.remove(&schema_id);
                Ok(())
            }
            (true, None) => Err(Error::SchemaNotFound),
            (false, Some(_)) => Err(Error::SchemaNotDeleted),
        }
    }

    pub async fn reload_create_table(&self, buf_pool: &'static P, table_id: TableID) -> Result<()> {
        // todo
        let res = self
            .storage
            .tables()
            .find_uncommitted_by_id(buf_pool, table_id)
            .await;
        match res {
            Some(table) => {
                // A table creation involves table, column and index.
                // Try to find the maximum object id and update the global one.
                let mut next_obj_id = table.table_id;

                // todo: use secondary index to improve performance
                let columns = self
                    .storage
                    .columns()
                    .list_uncommitted_by_table_id(buf_pool, table_id)
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
                    .list_uncommitted_by_table_id(buf_pool, table_id)
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
                        .list_uncommitted_by_index_id(buf_pool, index.index_id)
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

                let table_metadata = TableMetadata::new(column_specs, index_specs);
                let root_page = buf_pool
                    .allocate_page_at(table.block_index_root_page)
                    .await?;
                let blk_idx = BlockIndex::new_with_page(root_page, table.table_id).await;
                let table = Table::new(blk_idx, table_metadata);
                // Update table into cache
                let mut table_cache_g = self.cache.tables.write();
                let res = table_cache_g.insert(table_id, table);
                assert!(res.is_none());
                Ok(())
            }
            None => Err(Error::TableNotFound),
        }
    }

    #[inline]
    pub fn get_table(&self, table_id: TableID) -> Option<Table<P>> {
        let g = self.cache.tables.read();
        g.get(&table_id).cloned()
    }
}

unsafe impl<P: BufferPool> Send for Catalog<P> {}
unsafe impl<P: BufferPool> Sync for Catalog<P> {}
unsafe impl<P: BufferPool> StaticLifetime for Catalog<P> {}
impl<P: BufferPool> UnwindSafe for Catalog<P> {}
impl<P: BufferPool> RefUnwindSafe for Catalog<P> {}

pub struct CatalogCache<P: BufferPool> {
    pub schemas: RwLock<HashMap<SchemaID, SchemaObject>>,
    pub tables: RwLock<HashMap<TableID, Table<P>>>,
}

impl<P: BufferPool> CatalogCache<P> {
    #[inline]
    pub fn new() -> Self {
        CatalogCache {
            schemas: RwLock::new(HashMap::new()),
            tables: RwLock::new(HashMap::new()),
        }
    }
}

pub struct TableCache<'a, P: BufferPool> {
    catalog: &'a Catalog<P>,
    map: HashMap<TableID, Option<Table<P>>>,
}

impl<'a, P: BufferPool> TableCache<'a, P> {
    #[inline]
    pub fn new(catalog: &'a Catalog<P>) -> Self {
        TableCache {
            catalog,
            map: HashMap::new(),
        }
    }

    #[inline]
    pub fn get_table(&mut self, table_id: TableID) -> &Option<Table<P>> {
        if self.map.contains_key(&table_id) {
            return &self.map[&table_id];
        }
        let table = self.catalog.get_table(table_id);
        self.map.entry(table_id).or_insert(table)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::engine::Engine;
    use doradb_catalog::{IndexAttributes, IndexKey, IndexSpec, TableSpec};
    use doradb_datatype::Collation;

    #[inline]
    pub(crate) async fn db1<P: BufferPool>(engine: &Engine<P>) -> SchemaID {
        let session = engine.new_session();
        let trx = session.begin_trx();
        let mut stmt = trx.start_stmt();

        let schema_id = stmt.create_schema("db1").await.unwrap();

        let trx = stmt.succeed();
        let session = trx.commit().await.unwrap();
        drop(session);
        schema_id
    }

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1<P: BufferPool>(engine: &Engine<P>) -> TableID {
        let schema_id = db1(engine).await;

        let session = engine.new_session();
        let trx = session.begin_trx();
        let mut stmt = trx.start_stmt();

        let table_id = stmt
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

        let trx = stmt.succeed();
        let session = trx.commit().await.unwrap();
        drop(session);
        table_id
    }

    /// Table2 has i32(unique key) and string column.
    #[inline]
    pub(crate) async fn table2<P: BufferPool>(engine: &Engine<P>) -> TableID {
        let schema_id = db1(engine).await;

        let session = engine.new_session();
        let trx = session.begin_trx();
        let mut stmt = trx.start_stmt();

        let table_id = stmt
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

        let trx = stmt.succeed();
        let session = trx.commit().await.unwrap();
        drop(session);
        table_id
    }
}
