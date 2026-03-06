use crate::catalog::storage::CatalogDefinition;
use crate::catalog::storage::object::{IndexColumnObject, IndexObject};
use crate::catalog::table::TableMetadata;
use crate::catalog::{
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexOrder, IndexSpec, TableID,
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

pub const TABLE_ID_INDEXES: TableID = 2;
const COL_NO_INDEXES_TABLE_ID: usize = 0;
const COL_NAME_INDEXES_TABLE_ID: &str = "table_id";
const COL_NO_INDEXES_INDEX_NO: usize = 1;
const COL_NAME_INDEXES_INDEX_NO: &str = "index_no";
const COL_NO_INDEXES_INDEX_NAME: usize = 2;
const COL_NAME_INDEXES_INDEX_NAME: &str = "index_name";
const COL_NO_INDEXES_INDEX_ATTRIBUTES: usize = 3;
const COL_NAME_INDEXES_INDEX_ATTRIBUTES: &str = "index_attributes";
const PK_NO_INDEXES: usize = 0;
const PK_NAME_INDEXES: &str = "pk_indexes";

pub fn catalog_definition_of_indexes() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEXES,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsgined bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_NO),
                        column_type: ValKind::U16,
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
                    // primary key pk_indexes (table_id, index_no)
                    IndexSpec::new(
                        PK_NAME_INDEXES,
                        vec![IndexKey::new(0), IndexKey::new(1)],
                        IndexAttributes::PK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_index_object(metadata: &TableMetadata, row: Row<'_>) -> IndexObject {
    let table_id = row.val(metadata, COL_NO_INDEXES_TABLE_ID).as_u64().unwrap();
    let index_no = row.val(metadata, COL_NO_INDEXES_INDEX_NO).as_u16().unwrap();
    let index_name = row.str(COL_NO_INDEXES_INDEX_NAME).unwrap();
    let index_attributes = row
        .val(metadata, COL_NO_INDEXES_INDEX_ATTRIBUTES)
        .as_u32()
        .unwrap();
    IndexObject {
        table_id,
        index_no,
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
            Val::from(obj.table_id),
            Val::from(obj.index_no),
            Val::from(obj.index_name.as_str()),
            Val::from(obj.index_attributes.bits()),
        ];
        self.table.insert_mvcc(stmt, cols).await.is_ok()
    }

    /// Delete an index by (table_id, index_no).
    pub async fn delete_by_id(
        &self,
        stmt: &mut Statement,
        table_id: TableID,
        index_no: u16,
    ) -> bool {
        let key = SelectKey::new(
            PK_NO_INDEXES,
            vec![Val::from(table_id), Val::from(index_no)],
        );
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

pub const TABLE_ID_INDEX_COLUMNS: TableID = 3;
const COL_NO_INDEX_COLUMNS_TABLE_ID: usize = 0;
const COL_NAME_INDEX_COLUMNS_TABLE_ID: &str = "table_id";
const COL_NO_INDEX_COLUMNS_INDEX_NO: usize = 1;
const COL_NAME_INDEX_COLUMNS_INDEX_NO: &str = "index_no";
const COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO: usize = 2;
const COL_NAME_INDEX_COLUMNS_INDEX_COLUMN_NO: &str = "index_column_no";
const COL_NO_INDEX_COLUMNS_COLUMN_NO: usize = 3;
const COL_NAME_INDEX_COLUMNS_COLUMN_NO: &str = "column_no";

const COL_NO_INDEX_COLUMNS_INDEX_ORDER: usize = 4;
const COL_NAME_INDEX_COLUMNS_INDEX_ORDER: &str = "index_order";
#[expect(
    dead_code,
    reason = "reserved for future unique-key lookups on index_columns primary key"
)]
const PK_NO_INDEX_COLUMNS: usize = 0;
const PK_NAME_INDEX_COLUMNS: &str = "pk_index_columns";

pub fn catalog_definition_of_index_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEX_COLUMNS,
            metadata: TableMetadata::new(
                vec![
                    // table_id unsigned bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_TABLE_ID),
                        column_type: ValKind::U64,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_COLUMN_NO),
                        column_type: ValKind::U16,
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // column_no unsigned smallint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_COLUMN_NO),
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
                    // primary key pk_index_columns
                    // (table_id, index_no, index_column_no)
                    IndexSpec::new(
                        PK_NAME_INDEX_COLUMNS,
                        vec![IndexKey::new(0), IndexKey::new(1), IndexKey::new(2)],
                        IndexAttributes::PK,
                    ),
                ],
            ),
        }
    })
}

#[inline]
fn row_to_index_column_object(metadata: &TableMetadata, row: Row<'_>) -> IndexColumnObject {
    let table_id = row
        .val(metadata, COL_NO_INDEX_COLUMNS_TABLE_ID)
        .as_u64()
        .unwrap();
    let index_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_NO)
        .as_u16()
        .unwrap();
    let index_column_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_COLUMN_NO)
        .as_u16()
        .unwrap();
    let column_no = row
        .val(metadata, COL_NO_INDEX_COLUMNS_COLUMN_NO)
        .as_u16()
        .unwrap();
    let index_order = row
        .val(metadata, COL_NO_INDEX_COLUMNS_INDEX_ORDER)
        .as_u8()
        .unwrap();
    IndexColumnObject {
        table_id,
        index_no,
        index_column_no,
        column_no,
        index_order: IndexOrder::from(index_order),
    }
}

pub struct IndexColumns<'a> {
    pub(super) table: &'a Table,
}

impl IndexColumns<'_> {
    pub async fn insert(&self, stmt: &mut Statement, obj: &IndexColumnObject) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.index_no),
            Val::from(obj.index_column_no),
            Val::from(obj.column_no),
            Val::from(obj.index_order as u8),
        ];
        self.table.insert_mvcc(stmt, cols).await.is_ok()
    }

    pub async fn delete_by_index(
        &self,
        _stmt: &mut Statement,
        _table_id: TableID,
        _index_no: u16,
    ) -> bool {
        todo!()
    }

    pub async fn list_uncommitted_by_table_id(&self, table_id: TableID) -> Vec<IndexColumnObject> {
        let mut res = vec![];
        self.table
            .table_scan_uncommitted(0, |metadata, row| {
                let table_id_in_row = row
                    .val(metadata, COL_NO_INDEX_COLUMNS_TABLE_ID)
                    .as_u64()
                    .unwrap();
                if table_id_in_row == table_id {
                    let obj = row_to_index_column_object(metadata, row);
                    res.push(obj);
                }
                true
            })
            .await;
        res
    }
}
