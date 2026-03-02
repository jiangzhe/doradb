use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::TableObject;
use crate::catalog::table::TableMetadata;
use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableID};
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::{Table, TableAccess};
use crate::value::Val;
use crate::value::ValKind;
use semistr::SemiStr;
use std::sync::OnceLock;

pub const TABLE_ID_TABLES: TableID = 0;
const COL_NO_TABLES_TABLE_ID: usize = 0;
const COL_NAME_TABLES_TABLE_ID: &str = "table_id";
const INDEX_NO_TABLES_TABLE_ID: usize = 0;
const INDEX_NAME_TABLES_TABLE_ID: &str = "idx_tables_table_id";

pub fn catalog_definition_of_tables() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_TABLES,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsigned bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_TABLES_TABLE_ID),
                        column_type: ValKind::U64,
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
                ],
            ),
        }
    })
}

#[inline]
fn row_to_table_object(metadata: &TableMetadata, row: Row<'_>) -> TableObject {
    let table_id = row.val(metadata, COL_NO_TABLES_TABLE_ID).as_u64().unwrap();
    TableObject { table_id }
}

pub struct Tables<'a> {
    pub(super) table: &'a Table,
}

impl Tables<'_> {
    /// Find a table by id.
    #[inline]
    pub async fn find_uncommitted_by_id(&self, table_id: TableID) -> Option<TableObject> {
        let key = SelectKey::new(INDEX_NO_TABLES_TABLE_ID, vec![Val::from(table_id)]);
        self.table
            .index_lookup_unique_uncommitted(&key, row_to_table_object)
            .await
    }

    /// Insert a table.
    pub async fn insert(&self, stmt: &mut Statement, obj: &TableObject) -> bool {
        let cols = vec![Val::from(obj.table_id)];
        self.table.insert_mvcc(stmt, cols).await.is_ok()
    }

    /// Delete a table by id.
    pub async fn delete_by_id(&self, stmt: &mut Statement, id: TableID) -> bool {
        let key = SelectKey::new(INDEX_NO_TABLES_TABLE_ID, vec![Val::from(id)]);
        self.table
            .delete_unique_mvcc(stmt, &key, true)
            .await
            .is_ok()
    }
}
