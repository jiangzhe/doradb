use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::{IndexColumnObject, IndexObject};
use crate::catalog::table::TableMetadata;
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexID, IndexKey, IndexOrder, IndexSpec,
    TableID,
};
use crate::row::ops::SelectKey;
use crate::row::{Row, RowRead};
use crate::stmt::Statement;
use crate::table::{Table, TableAccess};
use crate::value::Val;
use crate::value::ValKind;
use semistr::SemiStr;
use std::sync::OnceLock;

/* Indexes table */

pub const TABLE_ID_INDEXES: TableID = 3;
const COL_NO_INDEXES_INDEX_ID: usize = 0;
const COL_NAME_INDEXES_INDEX_ID: &str = "index_id";
const COL_NO_INDEXES_TABLE_ID: usize = 1;
const COL_NAME_INDEXES_TABLE_ID: &str = "table_id";
const COL_NO_INDEXES_INDEX_NAME: usize = 2;
const COL_NAME_INDEXES_INDEX_NAME: &str = "index_name";
const COL_NO_INDEXES_INDEX_ATTRIBUTES: usize = 3;
const COL_NAME_INDEXES_INDEX_ATTRIBUTES: &str = "index_attributes";
const INDEX_NO_INDEXES_INDEX_ID: usize = 0;
const INDEX_NAME_INDEXES_INDEX_ID: &str = "idx_indexes_index_id";
#[expect(
    dead_code,
    reason = "reserved for planned non-unique index on indexes.table_id"
)]
const INDEX_NO_INDEXES_TABLE_ID: usize = 1;
#[expect(
    dead_code,
    reason = "reserved for planned non-unique index on indexes.table_id"
)]
const INDEX_NAME_INDEXES_TABLE_ID: &str = "idx_indexes_table_id";

pub fn catalog_definition_of_indexes() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEXES,
            metadata: TableMetadata::new(
                vec![
                    // index_id unsgined bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // table_id unsgined bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_name string unique not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_NAME),
                        column_type: ValKind::VarByte,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // index_attributes unsigned int not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_ATTRIBUTES),
                        column_type: ValKind::U32,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // primary key idx_indexes_index_id (index_id)
                    IndexSpec::new(
                        INDEX_NAME_INDEXES_INDEX_ID,
                        vec![IndexKey::new(0)],
                        IndexAttributes::PK,
                    ),
                    // todo: non-unique key idx_indexes_index_name (index_name)
                    // IndexSpec::new(
                    //     INDEX_NAME_INDEXES_TABLE_ID,
                    //     vec![IndexKey::new(1)],
                    //     IndexAttributes::empty(),
                    // ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_index_object(metadata: &TableMetadata, row: Row<'_>) -> IndexObject {
    let index_id = row.val(metadata, COL_NO_INDEXES_INDEX_ID).as_u64().unwrap();
    let table_id = row.val(metadata, COL_NO_INDEXES_TABLE_ID).as_u64().unwrap();
    let index_name = row.str(COL_NO_INDEXES_INDEX_NAME).unwrap();
    let index_attributes = row
        .val(metadata, COL_NO_INDEXES_INDEX_ATTRIBUTES)
        .as_u32()
        .unwrap();
    IndexObject {
        index_id,
        table_id,
        index_name: SemiStr::new(index_name),
        index_attributes: IndexAttributes::from_bits_truncate(index_attributes),
    }
}

pub struct Indexes<'a> {
    pub(super) table: &'a Table,
}

impl Indexes<'_> {
    /// Insert an index.
    pub async fn insert(&self, stmt: &mut Statement, obj: &IndexObject) -> bool {
        let cols = vec![
            Val::from(obj.index_id),
            Val::from(obj.table_id),
            Val::from(obj.index_name.as_str()),
            Val::from(obj.index_attributes.bits()),
        ];
        self.table.insert_mvcc(stmt, cols).await.is_ok()
    }

    /// Delete an index by id.
    pub async fn delete_by_id(&self, stmt: &mut Statement, id: IndexID) -> bool {
        let key = SelectKey::new(INDEX_NO_INDEXES_INDEX_ID, vec![Val::from(id)]);
        self.table
            .delete_unique_mvcc(stmt, &key, true)
            .await
            .is_ok()
    }

    /// List all indexes by given table id.
    pub async fn list_uncommitted_by_table_id(&self, table_id: TableID) -> Vec<IndexObject> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(0, |metadata, row| {
                // filter by table id before deserializing the whole object.
                let table_id_in_row = row.val(metadata, COL_NO_INDEXES_TABLE_ID).as_u64().unwrap();
                if table_id_in_row == table_id {
                    let obj = row_to_index_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }
}

/* Index columns table */

pub const TABLE_ID_INDEX_COLUMNS: TableID = 4;
const COL_NO_INDEX_COLUMNS_COLUMN_ID: usize = 0;
const COL_NAME_INDEX_COLUMNS_COLUMN_ID: &str = "column_id";
const COL_NO_INDEX_COLUMNS_INDEX_ID: usize = 1;
const COL_NAME_INDEX_COLUMNS_INDEX_ID: &str = "index_id";
const COL_NO_INDEX_COLUMNS_COLUMN_NO: usize = 2;
const COL_NAME_INDEX_COLUMNS_COLUMN_NO: &str = "column_no";
const COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO: usize = 3;
const COL_NAME_INDEX_COLUMNS_INDEX_COLUMN_NO: &str = "index_column_no";

const COL_NO_INDEX_COLUMNS_INDEX_ORDER: usize = 4;
const COL_NAME_INDEX_COLUMNS_INDEX_ORDER: &str = "index_order";
#[expect(
    dead_code,
    reason = "reserved for future index-columns lookups by index_id"
)]
const INDEX_NO_INDEX_COLUMNS_INDEX_ID: usize = 0;
const INDEX_NAME_INDEX_COLUMNS_INDEX_ID: &str = "idx_index_columns_index_id";

pub fn catalog_definition_of_index_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEX_COLUMNS,
            metadata: TableMetadata::new(
                vec![
                    // column_id unsigned bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_COLUMN_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_id unsigned bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // index_column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // descending boolean not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_ORDER),
                        column_type: ValKind::U8,
                        column_attributes: ColumnAttributes::empty(),
                    },
                ],
                vec![
                    // unique key idx_index_columns_index_id (index_id, column_id)
                    IndexSpec::new(
                        INDEX_NAME_INDEX_COLUMNS_INDEX_ID,
                        vec![IndexKey::new(1), IndexKey::new(0)],
                        IndexAttributes::UK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_index_column_object(metadata: &TableMetadata, row: Row<'_>) -> IndexColumnObject {
    let column_id = row
        .val(metadata, COL_NO_INDEX_COLUMNS_COLUMN_ID)
        .as_u64()
        .unwrap();
    let index_id = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_ID)
        .as_u64()
        .unwrap();
    let column_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let index_column_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO)
        .as_u16()
        .unwrap();
    let index_order = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_ORDER)
        .as_u8()
        .unwrap();
    IndexColumnObject {
        column_id,
        index_id,
        column_no,
        index_column_no,
        index_order: IndexOrder::from(index_order),
    }
}

pub struct IndexColumns<'a> {
    pub(super) table: &'a Table,
}

impl IndexColumns<'_> {
    pub async fn insert(&self, stmt: &mut Statement, obj: &IndexColumnObject) -> bool {
        let cols = vec![
            Val::from(obj.column_id),
            Val::from(obj.index_id),
            Val::from(obj.column_no),
            Val::from(obj.index_column_no),
            Val::from(obj.index_order as u8),
        ];
        self.table.insert_mvcc(stmt, cols).await.is_ok()
    }

    pub async fn delete_by_index(&self, _stmt: &mut Statement, _index_id: IndexID) -> bool {
        todo!()
    }

    pub async fn list_uncommitted_by_index_id(&self, index_id: IndexID) -> Vec<IndexColumnObject> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(0, |metadata, row| {
                let index_id_in_row = row
                    .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_ID)
                    .as_u64()
                    .unwrap();
                if index_id_in_row == index_id {
                    let obj = row_to_index_column_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }
}
