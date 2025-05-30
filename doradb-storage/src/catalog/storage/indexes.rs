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
    ColumnAttributes, ColumnSpec, IndexAttributes, IndexColumnObject, IndexID, IndexKey,
    IndexObject, IndexOrder, IndexSpec, TableID,
};
use doradb_datatype::{Collation, PreciseType};
use semistr::SemiStr;
use std::sync::OnceLock;

/* Indexes table */

pub const TABLE_ID_INDEXES: TableID = 3;
const ROOT_PAGE_ID_INDEXES: PageID = 3;
const COL_NO_INDEXES_INDEX_ID: usize = 0;
const COL_NAME_INDEXES_INDEX_ID: &'static str = "index_id";
const COL_NO_INDEXES_TABLE_ID: usize = 1;
const COL_NAME_INDEXES_TABLE_ID: &'static str = "table_id";
const COL_NO_INDEXES_INDEX_NAME: usize = 2;
const COL_NAME_INDEXES_INDEX_NAME: &'static str = "index_name";
const COL_NO_INDEXES_INDEX_ATTRIBUTES: usize = 3;
const COL_NAME_INDEXES_INDEX_ATTRIBUTES: &'static str = "index_attributes";
const INDEX_NO_INDEXES_INDEX_ID: usize = 0;
const INDEX_NAME_INDEXES_INDEX_ID: &'static str = "idx_indexes_index_id";
const INDEX_NO_INDEXES_TABLE_ID: usize = 1;
const INDEX_NAME_INDEXES_TABLE_ID: &'static str = "idx_indexes_table_id";

pub fn catalog_definition_of_indexes() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEXES,
            root_page_id: ROOT_PAGE_ID_INDEXES,
            metadata: TableMetadata::new(
                vec![
                    // index_id bigint primary key not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // table_id bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_TABLE_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_name string unique not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_NAME),
                        column_type: PreciseType::Varchar(255, Collation::Utf8mb4),
                        column_attributes: ColumnAttributes::empty(),
                    },
                    // index_attributes integer not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEXES_INDEX_ATTRIBUTES),
                        column_type: PreciseType::Int(4, false),
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
fn row_to_index_object(row: Row<'_>) -> IndexObject {
    let index_id = row.user_val::<u64>(COL_NO_INDEXES_INDEX_ID);
    let table_id = row.user_val::<u64>(COL_NO_INDEXES_TABLE_ID);
    let index_name = row.user_str(COL_NO_INDEXES_INDEX_NAME);
    let index_attributes = row.user_val::<u32>(COL_NO_INDEXES_INDEX_ATTRIBUTES);
    IndexObject {
        index_id: *index_id,
        table_id: *table_id,
        index_name: SemiStr::new(index_name),
        index_attributes: IndexAttributes::from_bits_truncate(*index_attributes),
    }
}

pub struct Indexes<'a, P: BufferPool>(pub(super) &'a Table<P>);

impl<P: BufferPool> Indexes<'_, P> {
    /// Insert an index.
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        obj: &IndexObject,
    ) -> bool {
        let cols = vec![
            Val::from(obj.index_id),
            Val::from(obj.table_id),
            Val::from(obj.index_name.as_str()),
            Val::from(obj.index_attributes.bits()),
        ];
        self.0.insert_row(buf_pool, stmt, cols).await.is_ok()
    }

    /// Delete an index by id.
    pub async fn delete_by_id(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        id: IndexID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_INDEXES_INDEX_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }

    /// List all indexes by given table id.
    pub async fn list_uncommitted_by_table_id(
        &self,
        buf_pool: &'static P,
        table_id: TableID,
    ) -> Vec<IndexObject> {
        let mut res = vec![];
        self.0
            .scan_rows_uncommitted(buf_pool, |row| {
                // filter by table id before deserializing the whole object.
                let table_id_in_row = *row.user_val::<TableID>(COL_NO_INDEXES_TABLE_ID);
                if table_id_in_row == table_id {
                    let obj = row_to_index_object(row);
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
const ROOT_PAGE_ID_INDEX_COLUMNS: PageID = 4;
const COL_NO_INDEX_COLUMNS_COLUMN_ID: usize = 0;
const COL_NAME_INDEX_COLUMNS_COLUMN_ID: &'static str = "column_id";
const COL_NO_INDEX_COLUMNS_INDEX_ID: usize = 1;
const COL_NAME_INDEX_COLUMNS_INDEX_ID: &'static str = "index_id";
const COL_NO_INDEX_COLUMNS_INDEX_ORDER: usize = 2;
const COL_NAME_INDEX_COLUMNS_INDEX_ORDER: &'static str = "index_order";
const INDEX_NO_INDEX_COLUMNS_INDEX_ID: usize = 0;
const INDEX_NAME_INDEX_COLUMNS_INDEX_ID: &'static str = "idx_index_columns_index_id";

pub fn catalog_definition_of_index_columns() -> &'static CatalogDefinition {
    static DEF: OnceLock<CatalogDefinition> = OnceLock::new();
    DEF.get_or_init(|| {
        CatalogDefinition {
            table_id: TABLE_ID_INDEX_COLUMNS,
            root_page_id: ROOT_PAGE_ID_INDEX_COLUMNS,
            metadata: TableMetadata::new(
                vec![
                    // column_id bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_COLUMN_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // index_id bigint not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_ID),
                        column_type: PreciseType::Int(8, false),
                        column_attributes: ColumnAttributes::INDEX,
                    },
                    // descending boolean not null
                    ColumnSpec {
                        column_name: SemiStr::new(COL_NAME_INDEX_COLUMNS_INDEX_ORDER),
                        column_type: PreciseType::Bool,
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
fn row_to_index_column_object(row: Row<'_>) -> IndexColumnObject {
    let column_id = row.user_val::<u64>(COL_NO_INDEX_COLUMNS_COLUMN_ID);
    let index_id = row.user_val::<u64>(COL_NO_INDEX_COLUMNS_INDEX_ID);
    let index_order = row.user_val::<u8>(COL_NO_INDEX_COLUMNS_INDEX_ORDER);
    IndexColumnObject {
        column_id: *column_id,
        index_id: *index_id,
        index_order: IndexOrder::from(*index_order),
    }
}

pub struct IndexColumns<'a, P: BufferPool>(pub(super) &'a Table<P>);

impl<P: BufferPool> IndexColumns<'_, P> {
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement<P>,
        obj: &IndexColumnObject,
    ) -> bool {
        let cols = vec![
            Val::from(obj.column_id),
            Val::from(obj.index_id),
            Val::from(obj.index_order as u8),
        ];
        self.0.insert_row(buf_pool, stmt, cols).await.is_ok()
    }

    pub async fn delete_by_index(
        &self,
        _buf_pool: &'static P,
        _stmt: &mut Statement<P>,
        _index_id: IndexID,
    ) -> bool {
        todo!()
    }
}
