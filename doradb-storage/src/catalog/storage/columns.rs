use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::ColumnObject;
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

pub const TABLE_ID_COLUMNS: TableID = 1;
const COL_NO_COLUMNS_TABLE_ID: usize = 0;
const COL_NAME_COLUMNS_TABLE_ID: &str = "table_id";
const COL_NO_COLUMNS_COLUMN_NO: usize = 1;
const COL_NAME_COLUMNS_COLUMN_NO: &str = "column_no";
const COL_NO_COLUMNS_COLUMN_NAME: usize = 2;
const COL_NAME_COLUMNS_COLUMN_NAME: &str = "column_name";
const COL_NO_COLUMNS_COLUMN_TYPE: usize = 3;
const COL_NAME_COLUMNS_COLUMN_TYPE: &str = "column_type";
const COL_NO_COLUMNS_COLUMN_ATTRIBUTES: usize = 4;
const COL_NAME_COLUMNS_COLUMN_ATTRIBUTES: &str = "column_attributes";
const PK_NO_COLUMNS: usize = 0;
const PK_NAME_COLUMNS: &str = "pk_columns";

pub fn catalog_definition_of_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_COLUMNS,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsigned bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_name string not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_NAME),
                        column_type: ValKind::VarByte,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_type unsgined int not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_TYPE),
                        column_type: ValKind::U32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_attributes unsgined int not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_ATTRIBUTES),
                        column_type: ValKind::U32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key pk_columns (table_id, column_no)
                    IndexSpec::new(
                        PK_NAME_COLUMNS,
                        vec![IndexKey::new(0), IndexKey::new(1)],
                        IndexAttributes::PK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_column_object(metadata: &TableMetadata, row: Row<'_>) -> ColumnObject {
    let table_id = row.val(metadata, COL_NO_COLUMNS_TABLE_ID).as_u64().unwrap();
    let column_no = row
        .val(metadata, COL_NO_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let column_name = row.str(COL_NO_COLUMNS_COLUMN_NAME).unwrap();
    let column_type = row
        .val(metadata, COL_NO_COLUMNS_COLUMN_TYPE)
        .as_u32()
        .unwrap();
    let column_attributes = row
        .val(metadata, COL_NO_COLUMNS_COLUMN_ATTRIBUTES)
        .as_u32()
        .unwrap();
    ColumnObject {
        table_id,
        column_no,
        column_name: SemiStr::new(column_name),
        column_type: ValKind::try_from(column_type as u8).unwrap(),
        column_attributes: ColumnAttributes::from_bits_truncate(column_attributes),
    }
}

pub struct Columns<'a> {
    pub(super) table: &'a Table,
}

impl Columns<'_> {
    /// Insert a column.
    pub async fn insert(&self, stmt: &mut Statement, obj: &ColumnObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.column_no),
            Val::from(obj.column_name.as_str()),
            Val::from(obj.column_type as u32),
            Val::from(obj.column_attributes.bits()),
        ];
        self.table.insert_mvcc(stmt, cols).await.is_ok()
    }

    pub async fn list_uncommitted_by_table_id(&self, table_id: TableID) -> Vec<ColumnObject> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(0, |metadata, row| {
                // filter by table id before deserializing the whole object.
                let table_id_in_row = row.val(metadata, COL_NO_COLUMNS_TABLE_ID).as_u64().unwrap();
                if table_id_in_row == table_id {
                    let obj = row_to_column_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }

    /// Delete a column by (table_id, column_no).
    pub async fn delete_by_id(
        &self,
        stmt: &mut Statement,
        table_id: TableID,
        column_no: u16,
    ) -> bool {
        let key = SelectKey::new(
            PK_NO_COLUMNS,
            vec![Val::from(table_id), Val::from(column_no)],
        );
        self.table
            .delete_unique_mvcc(stmt, &key, true)
            .await
            .is_ok()
    }
}
