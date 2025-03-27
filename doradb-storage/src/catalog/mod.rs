// mod index;
pub mod storage;
pub mod table;

// pub use index::*;
pub use storage::*;
pub use table::*;

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::lifetime::StaticLifetime;
use crate::stmt::Statement;
use crate::table::Table;
use crate::trx::redo::DDLRedo;
use doradb_catalog::{
    ColumnAttributes, ColumnID, ColumnObject, ColumnSpec, IndexAttributes, IndexColumnObject,
    IndexID, IndexObject, IndexOrder, IndexSpec, SchemaID, SchemaObject, TableID, TableObject,
    TableSpec,
};
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
    pub fn new(cache: CatalogCache<P>, storage: CatalogStorage<P>) -> Self {
        Catalog {
            obj_id: AtomicU64::new(0),
            cache,
            storage,
        }
    }

    #[inline]
    pub fn next_obj_id(&self) -> u64 {
        self.obj_id.fetch_add(1, Ordering::SeqCst)
    }
}

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

impl<P: BufferPool> Catalog<P> {
    #[inline]
    pub async fn empty(buf_pool: &'static P) -> Self {
        Catalog {
            obj_id: AtomicU64::new(0),
            cache: CatalogCache::new(),
            storage: CatalogStorage::new(buf_pool).await,
        }
    }

    #[inline]
    pub async fn empty_static(buf_pool: &'static P) -> &'static Self {
        let cat = Self::empty(buf_pool).await;
        StaticLifetime::new_static(cat)
    }

    #[inline]
    fn update_obj_id(&self, obj_id: u64) {
        self.obj_id.fetch_max(obj_id, Ordering::SeqCst);
    }

    pub async fn create_schema_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        schema_name: SemiStr,
    ) -> Option<SchemaObject> {
        if self
            .storage
            .schemas
            .find_uncommitted_by_name(buf_pool, &schema_name)
            .await
            .is_some()
        {
            return None;
        }

        let schema_id = self.obj_id.fetch_add(1, Ordering::SeqCst);
        let schema_object = SchemaObject {
            schema_id,
            schema_name,
        };
        self.storage
            .schemas
            .insert(buf_pool, stmt, &schema_object)
            .await;
        Some(schema_object)
    }

    pub async fn create_table_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        table_name: SemiStr,
        schema_id: SchemaID,
    ) -> Option<TableObject> {
        if self
            .storage
            .tables
            .find_uncommitted_by_name(buf_pool, schema_id, &table_name)
            .await
            .is_some()
        {
            return None;
        }

        let table_id = self.obj_id.fetch_add(1, Ordering::SeqCst);
        let table_object = TableObject {
            table_id,
            table_name,
            schema_id,
        };
        self.storage
            .tables
            .insert(buf_pool, stmt, &table_object)
            .await;
        Some(table_object)
    }

    pub async fn create_column_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        column_name: SemiStr,
        table_id: TableID,
        column_no: u16,
        column_type: PreciseType,
        column_attributes: ColumnAttributes,
    ) -> Option<ColumnObject> {
        let column_id = self.obj_id.fetch_add(1, Ordering::SeqCst);
        let column_object = ColumnObject {
            column_id,
            column_name,
            table_id,
            column_no,
            column_type,
            column_attributes,
        };
        self.storage
            .columns
            .insert(buf_pool, stmt, &column_object)
            .await;
        Some(column_object)
    }

    pub async fn create_index_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        index_name: SemiStr,
        table_id: TableID,
        index_attributes: IndexAttributes,
    ) -> Option<IndexObject> {
        // todo: index check
        let index_id = self.obj_id.fetch_add(1, Ordering::SeqCst);
        let index_object = IndexObject {
            index_id,
            table_id,
            index_name,
            index_attributes,
        };
        self.storage
            .indexes
            .insert(buf_pool, stmt, &index_object)
            .await;
        Some(index_object)
    }

    pub async fn create_index_column_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        column_id: ColumnID,
        index_id: IndexID,
        index_order: IndexOrder,
    ) -> Option<IndexColumnObject> {
        // todo: index column check
        let index_column_object = IndexColumnObject {
            column_id,
            index_id,
            index_order,
        };
        self.storage
            .index_columns
            .insert(buf_pool, stmt, &index_column_object)
            .await;
        Some(index_column_object)
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
    use doradb_catalog::{IndexKey, IndexSpec};
    use doradb_datatype::Collation;

    #[inline]
    pub(crate) async fn db1<P: BufferPool>(engine: &'static Engine<P>) -> SchemaID {
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
    pub(crate) async fn table1<P: BufferPool>(engine: &'static Engine<P>) -> TableID {
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
    pub(crate) async fn table2<P: BufferPool>(engine: &'static Engine<P>) -> TableID {
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
