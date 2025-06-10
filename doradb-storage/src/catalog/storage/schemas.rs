use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::storage::object::SchemaObject;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::table::TableMetadata;
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::Table;
use crate::value::Val;
use doradb_catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, SchemaID, TableID,
};
use doradb_datatype::{Collation, PreciseType};
use semistr::SemiStr;
use std::sync::OnceLock;

pub const TABLE_ID_SCHEMAS: TableID = 0;
const ROOT_PAGE_ID_SCHEMAS: PageID = 0;
const COL_NO_SCHEMAS_SCHEMA_ID: usize = 0;
const COL_NAME_SCHEMAS_SCHEMA_ID: &'static str = "schema_id";
const COL_NO_SCHEMAS_SCHEMA_NAME: usize = 1;
const COL_NAME_SCHEMAS_SCHEMA_NAME: &'static str = "schema_name";
const INDEX_NO_SCHEMAS_SCHEMA_ID: usize = 0;
const INDEX_NAME_SCHEMAS_SCHEMA_ID: &'static str = "idx_schemas_schema_id";
const INDEX_NO_SCHEMAS_SCHEMA_NAME: usize = 1;
const INDEX_NAME_SCHEMAS_SCHEMA_NAME: &'static str = "idx_schemas_schema_name";

pub fn catalog_definition_of_schemas() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_SCHEMAS,
            root_page_id: ROOT_PAGE_ID_SCHEMAS,
            metadata: TableMetadata::new(
                vec![
                    // schema_id bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_SCHEMAS_SCHEMA_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // schema_name string unique not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_SCHEMAS_SCHEMA_NAME),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key idx_schemas_schema_id (schema_id)
                    IndexSpec::new(
                        INDEX_NAME_SCHEMAS_SCHEMA_ID,
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    ),
                    // unique key idx_schemas_schema_name (schema_name)
                    IndexSpec::new(
                        INDEX_NAME_SCHEMAS_SCHEMA_NAME,
                        vec![IndexKey::new(1)],
                        IndexAttributes::UK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_schema_object(row: Row<'_>) -> SchemaObject {
    let schema_id = row.user_val::<u64>(COL_NO_SCHEMAS_SCHEMA_ID);
    let schema_name = row.user_str(COL_NO_SCHEMAS_SCHEMA_NAME);
    SchemaObject {
        schema_id: *schema_id,
        schema_name: SemiStr::new(schema_name),
    }
}

pub struct Schemas<'a, P: BufferPool>(pub(super) &'a Table<P>);

impl<P: BufferPool> Schemas<'_, P> {
    /// Find a schema by name.
    #[inline]
    pub async fn find_uncommitted_by_name(
        &self,
        buf_pool: &'static P,
        name: &str,
    ) -> Option<SchemaObject> {
        let name = Val::from(name);
        let key = SelectKey::new(INDEX_NO_SCHEMAS_SCHEMA_NAME, vec![name]);
        self.0
            .select_row_uncommitted(buf_pool, &key, row_to_schema_object)
            .await
    }

    #[inline]
    pub async fn find_uncommitted_by_id(
        &self,
        buf_pool: &'static P,
        id: SchemaID,
    ) -> Option<SchemaObject> {
        let key = SelectKey::new(INDEX_NO_SCHEMAS_SCHEMA_ID, vec![Val::from(id)]);
        self.0
            .select_row_uncommitted(buf_pool, &key, row_to_schema_object)
            .await
    }

    /// Insert a schema.
    #[inline]
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        obj: &SchemaObject,
    ) -> bool {
        let cols = vec![
            Val::from(obj.schema_id),
            Val::from(obj.schema_name.as_str()),
        ];
        self.0.insert_row(buf_pool, stmt, cols).await.is_ok()
    }

    /// Delete a schema by id.
    #[inline]
    pub async fn delete_by_id(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        id: SchemaID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_SCHEMAS_SCHEMA_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }
}
