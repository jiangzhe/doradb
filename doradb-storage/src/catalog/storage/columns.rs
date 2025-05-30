use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::catalog::storage::CatalogDefinition;
use crate::catalog::table::TableMetadata;
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::Table;
use crate::value::Val;
use doradb_catalog::{
    ColumnAttributes, ColumnID, ColumnObject, ColumnSpec, IndexAttributes, IndexKey, IndexSpec,
    TableID,
};
use doradb_datatype::{Collation, PreciseType};
use semistr::SemiStr;
use std::sync::OnceLock;

pub const TABLE_ID_COLUMNS: TableID = 2;
const ROOT_PAGE_ID_COLUMNS: PageID = 2;
const COL_NO_COLUMNS_COLUMN_ID: usize = 0;
const COL_NAME_COLUMNS_COLUMN_ID: &'static str = "column_id";
const COL_NO_COLUMNS_TABLE_ID: usize = 1;
const COL_NAME_COLUMNS_TABLE_ID: &'static str = "table_id";
const COL_NO_COLUMNS_COLUMN_NAME: usize = 2;
const COL_NAME_COLUMNS_COLUMN_NAME: &'static str = "column_name";
const COL_NO_COLUMNS_COLUMN_NO: usize = 3;
const COL_NAME_COLUMNS_COLUMN_NO: &'static str = "column_no";
const COL_NO_COLUMNS_COLUMN_TYPE: usize = 4;
const COL_NAME_COLUMNS_COLUMN_TYPE: &'static str = "column_type";
const COL_NO_COLUMNS_COLUMN_ATTRIBUTES: usize = 5;
const COL_NAME_COLUMNS_COLUMN_ATTRIBUTES: &'static str = "column_attributes";
const INDEX_NO_COLUMNS_COLUMN_ID: usize = 0;
const INDEX_NAME_COLUMNS_COLUMN_ID: &'static str = "idx_columns_column_id";
const INDEX_NO_COLUMNS_TABLE_ID: usize = 1;
const INDEX_NAME_COLUMNS_TABLE_ID: &'static str = "idx_columns_table_id";

pub fn catalog_definition_of_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_COLUMNS,
            root_page_id: ROOT_PAGE_ID_COLUMNS,
            metadata: TableMetadata::new(
                vec![
                    // column_id bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // table_id bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_TABLE_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_name string not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_NAME),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_no integer not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_NO),
                        column_type: PreciseType::Int(2, false),
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_type integer not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_TYPE),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // column_attributes integer not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_COLUMNS_COLUMN_ATTRIBUTES),
                        column_type: PreciseType::Int(4, false),
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key idx_columns_column_id (column_id)
                    IndexSpec::new(
                        INDEX_NAME_COLUMNS_COLUMN_ID,
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    ),
                    // todo: non-unique key idx_columns_table_id (table_id)
                    // IndexSpec::new(
                    //     INDEX_NAME_COLUMNS_TABLE_ID,
                    //     vec![IndexKey::new(1)],
                    //     IndexAttributes::empty(),
                    // ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_column_object(row: Row<'_>) -> ColumnObject {
    let column_id = row.user_val::<u64>(COL_NO_COLUMNS_COLUMN_ID);
    let table_id = row.user_val::<u64>(COL_NO_COLUMNS_TABLE_ID);
    let column_name = row.user_str(COL_NO_COLUMNS_COLUMN_NAME);
    let column_no = row.user_val::<u16>(COL_NO_COLUMNS_COLUMN_NO);
    let column_type = row.user_val::<u32>(COL_NO_COLUMNS_COLUMN_TYPE);
    let column_attributes = row.user_val::<u32>(COL_NO_COLUMNS_COLUMN_ATTRIBUTES);
    ColumnObject {
        column_id: *column_id,
        table_id: *table_id,
        column_name: SemiStr::new(column_name),
        column_no: *column_no,
        column_type: PreciseType::from(*column_type),
        column_attributes: ColumnAttributes::from_bits_truncate(*column_attributes),
    }
}

pub struct Columns<'a, P: BufferPool>(pub(super) &'a Table<P>);

impl<P: BufferPool> Columns<'_, P> {
    /// Insert a column.
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        obj: &ColumnObject,
    ) -> bool {
        let cols = vec![
            Val::from(obj.column_id),
            Val::from(obj.table_id),
            Val::from(obj.column_name.as_str()),
            Val::from(obj.column_no),
            Val::from(u32::from(obj.column_type)),
            Val::from(obj.column_attributes.bits()),
        ];
        self.0.insert_row(buf_pool, stmt, cols).await.is_ok()
    }

    pub async fn list_uncommitted_by_table_id(
        &self,
        buf_pool: &'static P,
        table_id: TableID,
    ) -> Vec<ColumnObject> {
        let mut res = vec![];
        self.0
            .scan_rows_uncommitted(buf_pool, |row| {
                // filter by table id before deserializing the whole object.
                let table_id_in_row = *row.user_val::<TableID>(COL_NO_COLUMNS_TABLE_ID);
                if table_id_in_row == table_id {
                    let obj = row_to_column_object(row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }

    // pub async fn

    /// Delete a column by id.
    pub async fn delete_by_id(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        id: ColumnID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_COLUMNS_COLUMN_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }
}
