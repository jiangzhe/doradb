use crate::buffer::BufferPool;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::TableObject;
use crate::catalog::table::TableMetadata;
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::{Table, TableAccess};
use crate::value::Val;
use doradb_catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, SchemaID, TableID,
};
use doradb_datatype::{Collation, PreciseType};
use semistr::SemiStr;
use std::sync::OnceLock;

pub const TABLE_ID_TABLES: TableID = 1;
const COL_NO_TABLES_TABLE_ID: usize = 0;
const COL_NAME_TABLES_TABLE_ID: &str = "table_id";
const COL_NO_TABLES_SCHEMA_ID: usize = 1;
const COL_NAME_TABLES_SCHEMA_ID: &str = "schema_id";
const COL_NO_TABLES_TABLE_NAME: usize = 2;
const COL_NAME_TABLES_TABLE_NAME: &str = "table_name";
const INDEX_NO_TABLES_TABLE_ID: usize = 0;
const INDEX_NAME_TABLES_TABLE_ID: &str = "idx_tables_table_id";
const INDEX_NO_TABLES_TABLE_NAME: usize = 1;
const INDEX_NAME_TABLES_TABLE_NAME: &str = "idx_tables_table_name";

pub fn catalog_definition_of_tables() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_TABLES,
            metadata: TableMetadata::new(
                vec![
                    // table_id bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_TABLE_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // schema_id bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_SCHEMA_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // table_name string not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_TABLE_NAME),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                ],
                vec![
                    // primary key idx_tables_table_id (table_id)
                    IndexSpec::new(
                        INDEX_NAME_TABLES_TABLE_ID,
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    ),
                    // unique key idx_tables_table_name (schema_id, table_name)
                    IndexSpec::new(
                        INDEX_NAME_TABLES_TABLE_NAME,
                        vec![IndexKey::new(1), IndexKey::new(2)],
                        IndexAttributes::UK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_table_object(row: Row<'_>) -> TableObject {
    let table_id = row.val::<u64>(COL_NO_TABLES_TABLE_ID);
    let schema_id = row.val::<u64>(COL_NO_TABLES_SCHEMA_ID);
    let table_name = row.str(COL_NO_TABLES_TABLE_NAME);
    TableObject {
        table_id: *table_id,
        schema_id: *schema_id,
        table_name: SemiStr::new(table_name),
    }
}

pub struct Tables<'a, P: BufferPool> {
    pub(super) buf_pool: &'static P,
    pub(super) table: &'a Table,
}

impl<P: BufferPool> Tables<'_, P> {
    /// Find a table by id.
    #[inline]
    pub async fn find_uncommitted_by_id(&self, table_id: TableID) -> Option<TableObject> {
        let key = SelectKey::new(INDEX_NO_TABLES_TABLE_ID, vec![Val::from(table_id)]);
        self.table
            .index_lookup_unique_uncommitted(self.buf_pool, &key, row_to_table_object)
            .await
    }

    /// Find a table by name.
    #[inline]
    pub async fn find_uncommitted_by_name(
        &self,
        schema_id: SchemaID,
        name: &str,
    ) -> Option<TableObject> {
        let key = SelectKey::new(
            INDEX_NO_TABLES_TABLE_NAME,
            vec![Val::from(schema_id), Val::from(name)],
        );
        self.table
            .index_lookup_unique_uncommitted(self.buf_pool, &key, row_to_table_object)
            .await
    }

    /// Insert a table.
    pub async fn insert(&self, stmt: &mut Statement, obj: &TableObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.schema_id),
            Val::from(obj.table_name.as_str()),
        ];
        self.table
            .insert_mvcc(self.buf_pool, stmt, cols)
            .await
            .is_ok()
    }

    /// Delete a table by id.
    pub async fn delete_by_id(&self, stmt: &mut Statement, id: TableID) -> bool {
        let key = SelectKey::new(INDEX_NO_TABLES_TABLE_ID, vec![Val::from(id)]);
        self.table
            .delete_unique_mvcc(self.buf_pool, stmt, &key, true)
            .await
            .is_ok()
    }
}
