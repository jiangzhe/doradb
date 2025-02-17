use crate::buffer::BufferPool;
use crate::catalog::index::{IndexKey, IndexSchema};
use crate::catalog::table::TableSchema;
use crate::row::ops::{SelectKey, SelectUncommitted};
use crate::row::RowRead;
use crate::table::Table;
use crate::value::{Val, ValKind};
use doradb_catalog::{
    ColumnObject, IndexColumnObject, IndexObject, SchemaID, SchemaObject, TableID, TableObject,
};
use semistr::SemiStr;

pub const TABLE_ID_SCHEMAS: TableID = 0;
pub const TABLE_ID_TABLES: TableID = 1;
pub const TABLE_ID_COLUMNS: TableID = 2;
pub const TABLEID_INDEXES: TableID = 3;

/// Metadata storage interface
pub(crate) trait MetadataStorage<P: BufferPool>: Sized {
    type Object;
    type ObjID;

    /// Create a new metadata persitence instance.
    async fn new(buf_pool: P) -> Self;

    /// Find object by name.
    async fn find(&self, buf_pool: P, name: &str) -> Option<Self::Object>;

    /// Find object by id.
    async fn find_by_id(&self, buf_pool: P, id: Self::ObjID) -> Option<Self::Object>;

    /// Insert object.
    async fn insert(&self, buf_pool: P, obj: Self::Object) -> bool;

    /// Delete object by id.
    async fn delete_by_id(&self, buf_pool: P, id: Self::ObjID) -> bool;
}

#[inline]
fn schema_of_schemas() -> TableSchema {
    TableSchema::new(
        vec![
            // schema_id bigint primary key not null
            ValKind::I64.nullable(false),
            // schema_name string unique not null
            ValKind::VarByte.nullable(false),
        ],
        vec![
            // unique key idx_schemas_schema_id (schema_id)
            IndexSchema::new(vec![IndexKey::new(0)], true),
            // unique key idx_schemas_schema_name (schema_name)
            IndexSchema::new(vec![IndexKey::new(1)], true),
        ],
    )
}

const COL_NO_SCHEMAS_SCHEMA_ID: usize = 0;
const COL_NO_SCHEMAS_SCHEMA_NAME: usize = 1;
const INDEX_NO_SCHEMAS_SCHEMA_ID: usize = 0;
const INDEX_NO_SCHEMAS_SCHEMA_NAME: usize = 1;

pub struct Schemas<P: BufferPool> {
    table: Table<P>,
}

impl<P: BufferPool> MetadataStorage<P> for Schemas<P> {
    type Object = SchemaObject;
    type ObjID = SchemaID;

    #[inline]
    async fn new(buf_pool: P) -> Self {
        let table = Table::new(buf_pool, TABLE_ID_SCHEMAS, schema_of_schemas()).await;
        Schemas { table }
    }

    #[inline]
    async fn find(&self, buf_pool: P, name: &str) -> Option<SchemaObject> {
        let name = Val::from(name);
        let key = SelectKey::new(INDEX_NO_SCHEMAS_SCHEMA_NAME, vec![name]);
        self.table
            .select_row_uncommitted(buf_pool, &key, |row| {
                let schema_id = row.user_val::<u64>(COL_NO_SCHEMAS_SCHEMA_ID);
                let schema_name = row.user_str(COL_NO_SCHEMAS_SCHEMA_NAME);
                SchemaObject {
                    schema_id: *schema_id,
                    schema_name: SemiStr::new(schema_name),
                }
            })
            .await
    }

    #[inline]
    async fn find_by_id(&self, buf_pool: P, id: SchemaID) -> Option<Self::Object> {
        let id = Val::from(id);
        let key = SelectKey::new(INDEX_NO_SCHEMAS_SCHEMA_ID, vec![id]);
        self.table
            .select_row_uncommitted(buf_pool, &key, |row| {
                let schema_id = row.user_val::<u64>(COL_NO_SCHEMAS_SCHEMA_ID);
                let schema_name = row.user_str(COL_NO_SCHEMAS_SCHEMA_NAME);
                SchemaObject {
                    schema_id: *schema_id,
                    schema_name: SemiStr::new(schema_name),
                }
            })
            .await
    }

    #[inline]
    async fn insert(&self, buf_pool: P, obj: Self::Object) -> bool {
        todo!()
    }

    #[inline]
    async fn delete_by_id(&self, buf_pool: P, id: Self::ObjID) -> bool {
        todo!()
    }
}
