// mod index;
mod storage;
mod table;

// pub use index::*;
pub use storage::*;
pub use table::*;

use crate::buffer::BufferPool;
use crate::error::{Error, Result};
use crate::lifetime::StaticLifetime;
use crate::stmt::Statement;
use crate::table::Table;
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

/// Catalog contains metadata of user tables.
pub struct Catalog<P: BufferPool> {
    obj_id: AtomicU64,

    schemas: Schemas<P>,

    schema_cache: RwLock<HashMap<SchemaID, SchemaObject>>,

    tables: Tables<P>,

    table_cache: RwLock<HashMap<TableID, Table<P>>>,

    columns: Columns<P>,

    indexes: Indexes<P>,

    index_columns: IndexColumns<P>,
}

impl<P: BufferPool> Catalog<P> {
    #[inline]
    pub async fn empty(buf_pool: &'static P) -> Self {
        Catalog {
            obj_id: AtomicU64::new(0),
            schemas: Schemas::new(buf_pool).await,
            schema_cache: RwLock::new(HashMap::new()),
            tables: Tables::new(buf_pool).await,
            table_cache: RwLock::new(HashMap::new()),
            columns: Columns::new(buf_pool).await,
            indexes: Indexes::new(buf_pool).await,
            index_columns: IndexColumns::new(buf_pool).await,
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

    #[inline]
    pub async fn recover_schema_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        schema_object: &SchemaObject,
    ) -> bool {
        self.update_obj_id(schema_object.schema_id);
        self.schemas.insert(buf_pool, stmt, schema_object).await
    }

    #[inline]
    pub async fn recover_table_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        table_object: &TableObject,
    ) -> bool {
        self.update_obj_id(table_object.table_id);
        self.tables.insert(buf_pool, stmt, table_object).await
    }

    #[inline]
    pub async fn recover_column_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        column_object: &ColumnObject,
    ) -> bool {
        self.columns.insert(buf_pool, stmt, column_object).await
    }

    #[inline]
    pub async fn recover_index_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        index_object: &IndexObject,
    ) -> bool {
        self.indexes.insert(buf_pool, stmt, index_object).await
    }

    #[inline]
    pub async fn recover_index_column_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        index_column_object: &IndexColumnObject,
    ) -> bool {
        self.index_columns
            .insert(buf_pool, stmt, index_column_object)
            .await
    }

    pub async fn create_schema_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        schema_name: SemiStr,
    ) -> Option<SchemaObject> {
        if self
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
        self.schemas.insert(buf_pool, stmt, &schema_object).await;
        Some(schema_object)
    }

    pub async fn create_table_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        table_name: SemiStr,
        schema_id: SchemaID,
    ) -> Option<TableObject> {
        if self
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
        self.tables.insert(buf_pool, stmt, &table_object).await;
        Some(table_object)
    }

    pub async fn create_column_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
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
        self.columns.insert(buf_pool, stmt, &column_object).await;
        Some(column_object)
    }

    pub async fn create_index_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
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
        self.indexes.insert(buf_pool, stmt, &index_object).await;
        Some(index_object)
    }

    pub async fn create_index_column_object(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
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
        self.index_columns
            .insert(buf_pool, stmt, &index_column_object)
            .await;
        Some(index_column_object)
    }

    #[inline]
    pub async fn create_schema(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        schema_name: &str,
    ) -> Result<SchemaID> {
        // Check if schema exists
        if self
            .schemas
            .find_uncommitted_by_name(buf_pool, schema_name)
            .await
            .is_some()
        {
            return Err(Error::SchemaAlreadyExists);
        }

        // Prepare schema object
        let schema_id = self.obj_id.fetch_add(1, Ordering::SeqCst);
        let schema_object = SchemaObject {
            schema_id,
            schema_name: SemiStr::new(schema_name),
        };

        let mut schema_cache_g = self.schema_cache.write();

        let inserted = self.schemas.insert(buf_pool, stmt, &schema_object).await;
        if !inserted {
            return Err(Error::SchemaAlreadyExists);
        }

        schema_cache_g.insert(schema_id, schema_object);
        Ok(schema_id)
    }

    #[inline]
    pub async fn create_table(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        schema_id: SchemaID,
        table_spec: TableSpec,
        index_specs: Vec<IndexSpec>,
    ) -> Result<TableID> {
        // Check if schema exists
        if self
            .schemas
            .find_uncommitted_by_id(buf_pool, schema_id)
            .await
            .is_none()
        {
            return Err(Error::SchemaNotFound);
        }

        // Check if table exists
        if self
            .tables
            .find_uncommitted_by_name(buf_pool, schema_id, &table_spec.table_name)
            .await
            .is_some()
        {
            return Err(Error::TableAlreadyExists);
        }

        // Prepare table object
        let table_id = self.obj_id.fetch_add(1, Ordering::SeqCst);
        let table_object = TableObject {
            table_id,
            table_name: table_spec.table_name.clone(),
            schema_id,
        };
        let row_id_spec = ColumnSpec {
            column_name: SemiStr::new(ROW_ID_COL_NAME),
            column_type: PreciseType::Int(8, false),
            column_attributes: ColumnAttributes::empty(),
        };

        // Prepare column objects
        let column_objects: Vec<_> = std::iter::once(&row_id_spec)
            .chain(table_spec.columns.iter())
            .enumerate()
            .map(|(col_no, col_spec)| ColumnObject {
                column_id: self.obj_id.fetch_add(1, Ordering::SeqCst),
                column_name: col_spec.column_name.clone(),
                table_id,
                column_no: col_no as u16,
                column_type: col_spec.column_type.clone(),
                column_attributes: col_spec.column_attributes,
            })
            .collect();

        // Prepare index objects
        let index_objects: Vec<_> = index_specs
            .iter()
            .map(|index_spec| IndexObject {
                index_id: self.obj_id.fetch_add(1, Ordering::SeqCst),
                table_id,
                index_name: index_spec.index_name.clone(),
                index_attributes: index_spec.index_attributes,
            })
            .collect();

        let mut table_cache_g = self.table_cache.write();

        // Insert table object, column objects, index objects
        let inserted = self.tables.insert(buf_pool, stmt, &table_object).await;
        if !inserted {
            return Err(Error::TableAlreadyExists);
        }

        for column_object in column_objects {
            let inserted = self.columns.insert(buf_pool, stmt, &column_object).await;
            debug_assert!(inserted);
        }
        for index_object in index_objects {
            let inserted = self.indexes.insert(buf_pool, stmt, &index_object).await;
            debug_assert!(inserted);
        }

        // Prepare in-memory representation of new table
        let table_metadata = TableMetadata::new(table_spec.columns, index_specs);
        let table = Table::new(buf_pool, table_id, table_metadata).await;
        let res = table_cache_g.insert(table_id, table);
        debug_assert!(res.is_none());
        Ok(table_id)
    }

    #[inline]
    pub fn get_table(&self, table_id: TableID) -> Option<Table<P>> {
        let g = self.table_cache.read();
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
    use crate::session::Session;
    use crate::trx::sys::TransactionSystem;
    use doradb_catalog::{IndexKey, IndexSpec};
    use doradb_datatype::Collation;

    #[inline]
    pub(crate) async fn db1<P: BufferPool>(
        buf_pool: &'static P,
        trx_sys: &'static TransactionSystem,
        catalog: &Catalog<P>,
    ) -> SchemaID {
        let session = Session::new();
        let trx = session.begin_trx(trx_sys);
        let mut stmt = Statement::new(trx);

        let schema_id = catalog
            .create_schema(buf_pool, &mut stmt, "db1")
            .await
            .unwrap();

        let trx = stmt.succeed();
        let session = trx_sys.commit(trx, buf_pool, catalog).await.unwrap();
        drop(session);
        schema_id
    }

    /// Table1 has single i32 column, with unique index of this column.
    #[inline]
    pub(crate) async fn table1<P: BufferPool>(
        buf_pool: &'static P,
        trx_sys: &'static TransactionSystem,
        catalog: &Catalog<P>,
    ) -> TableID {
        let schema_id = db1(buf_pool, trx_sys, catalog).await;

        let session = Session::new();
        let trx = session.begin_trx(trx_sys);
        let mut stmt = Statement::new(trx);

        let table_id = catalog
            .create_table(
                buf_pool,
                &mut stmt,
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
        let session = trx_sys.commit(trx, buf_pool, catalog).await.unwrap();
        drop(session);
        table_id
    }

    /// Table2 has i32(unique key) and string column.
    #[inline]
    pub(crate) async fn table2<P: BufferPool>(
        buf_pool: &'static P,
        trx_sys: &'static TransactionSystem,
        catalog: &Catalog<P>,
    ) -> TableID {
        let schema_id = db1(buf_pool, trx_sys, catalog).await;

        let session = Session::new();
        let trx = session.begin_trx(trx_sys);
        let mut stmt = Statement::new(trx);

        let table_id = catalog
            .create_table(
                buf_pool,
                &mut stmt,
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
        let session = trx_sys.commit(trx, buf_pool, catalog).await.unwrap();
        drop(session);
        table_id
    }
}
