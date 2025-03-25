use crate::buffer::BufferPool;
use crate::catalog::table::TableMetadata;
use crate::row::ops::SelectKey;
use crate::row::Row;
use crate::row::RowRead;
use crate::stmt::Statement;
use crate::table::Table;
use crate::value::Val;
use doradb_catalog::{
    ColumnAttributes, ColumnID, ColumnObject, ColumnSpec, IndexAttributes, IndexColumnObject,
    IndexID, IndexKey, IndexObject, IndexOrder, IndexSpec, SchemaID, SchemaObject, TableID,
    TableObject,
};
use doradb_datatype::{Collation, PreciseType};
use semistr::SemiStr;

/* Schemas table */

pub const TABLE_ID_SCHEMAS: TableID = 0;
const COL_NO_SCHEMAS_SCHEMA_ID: usize = 0;
const COL_NAME_SCHEMAS_SCHEMA_ID: &'static str = "schema_id";
const COL_NO_SCHEMAS_SCHEMA_NAME: usize = 1;
const COL_NAME_SCHEMAS_SCHEMA_NAME: &'static str = "schema_name";
const INDEX_NO_SCHEMAS_SCHEMA_ID: usize = 0;
const INDEX_NAME_SCHEMAS_SCHEMA_ID: &'static str = "idx_schemas_schema_id";
const INDEX_NO_SCHEMAS_SCHEMA_NAME: usize = 1;
const INDEX_NAME_SCHEMAS_SCHEMA_NAME: &'static str = "idx_schemas_schema_name";

#[inline]
fn metadata_of_schemas() -> TableMetadata {
    TableMetadata::new(
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
    )
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

/* Tables table */

pub const TABLE_ID_TABLES: TableID = 1;
const COL_NO_TABLES_TABLE_ID: usize = 0;
const COL_NAME_TABLES_TABLE_ID: &'static str = "table_id";
const COL_NO_TABLES_SCHEMA_ID: usize = 1;
const COL_NAME_TABLES_SCHEMA_ID: &'static str = "schema_id";
const COL_NO_TABLES_TABLE_NAME: usize = 2;
const COL_NAME_TABLES_TABLE_NAME: &'static str = "table_name";
const INDEX_NO_TABLES_TABLE_ID: usize = 0;
const INDEX_NAME_TABLES_TABLE_ID: &'static str = "idx_tables_table_id";
const INDEX_NO_TABLES_TABLE_NAME: usize = 1;
const INDEX_NAME_TABLES_TABLE_NAME: &'static str = "idx_tables_table_name";

#[inline]
fn metadata_of_tables() -> TableMetadata {
    TableMetadata::new(
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
    )
}

#[inline]
fn row_to_table_object(row: Row<'_>) -> TableObject {
    let table_id = row.user_val::<u64>(COL_NO_TABLES_TABLE_ID);
    let schema_id = row.user_val::<u64>(COL_NO_TABLES_SCHEMA_ID);
    let table_name = row.user_str(COL_NO_TABLES_TABLE_NAME);
    TableObject {
        table_id: *table_id,
        schema_id: *schema_id,
        table_name: SemiStr::new(table_name),
    }
}

/* Columns table */

pub const TABLE_ID_COLUMNS: TableID = 2;
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

#[inline]
fn metadata_of_columns() -> TableMetadata {
    TableMetadata::new(
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
    )
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

/* Indexes table */

pub const TABLE_ID_INDEXES: TableID = 3;
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

#[inline]
fn metadata_of_indexes() -> TableMetadata {
    TableMetadata::new(
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
    )
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

/* Index columns table */

pub const TABLE_ID_INDEX_COLUMNS: TableID = 4;
const COL_NO_INDEX_COLUMNS_COLUMN_ID: usize = 0;
const COL_NAME_INDEX_COLUMNS_COLUMN_ID: &'static str = "column_id";
const COL_NO_INDEX_COLUMNS_INDEX_ID: usize = 1;
const COL_NAME_INDEX_COLUMNS_INDEX_ID: &'static str = "index_id";
const COL_NO_INDEX_COLUMNS_INDEX_ORDER: usize = 2;
const COL_NAME_INDEX_COLUMNS_INDEX_ORDER: &'static str = "index_order";
const INDEX_NO_INDEX_COLUMNS_INDEX_ID: usize = 0;
const INDEX_NAME_INDEX_COLUMNS_INDEX_ID: &'static str = "idx_index_columns_index_id";

#[inline]
fn metadata_of_index_columns() -> TableMetadata {
    TableMetadata::new(
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
    )
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

pub struct Schemas<P: BufferPool>(Table<P>);

impl<P: BufferPool> Schemas<P> {
    /// Create a new schemas table.
    #[inline]
    pub async fn new(buf_pool: &'static P) -> Self {
        let table = Table::new(buf_pool, TABLE_ID_SCHEMAS, metadata_of_schemas()).await;
        Schemas(table)
    }

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
        stmt: &mut Statement,
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
        stmt: &mut Statement,
        id: SchemaID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_SCHEMAS_SCHEMA_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }
}

pub struct Tables<P: BufferPool>(Table<P>);

impl<P: BufferPool> Tables<P> {
    /// Create a new tables table.
    pub async fn new(buf_pool: &'static P) -> Self {
        let table = Table::new(buf_pool, TABLE_ID_TABLES, metadata_of_tables()).await;
        Tables(table)
    }

    /// Find a table by name.
    #[inline]
    pub async fn find_uncommitted_by_name(
        &self,
        buf_pool: &'static P,
        schema_id: SchemaID,
        name: &str,
    ) -> Option<TableObject> {
        let key = SelectKey::new(
            INDEX_NO_TABLES_TABLE_NAME,
            vec![Val::from(schema_id), Val::from(name)],
        );
        self.0
            .select_row_uncommitted(buf_pool, &key, row_to_table_object)
            .await
    }

    /// Insert a table.
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        obj: &TableObject,
    ) -> bool {
        let cols = vec![
            Val::from(obj.table_id),
            Val::from(obj.schema_id),
            Val::from(obj.table_name.as_str()),
        ];
        self.0.insert_row(buf_pool, stmt, cols).await.is_ok()
    }

    /// Delete a table by id.
    pub async fn delete_by_id(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        id: TableID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_TABLES_TABLE_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }
}

pub struct Columns<P: BufferPool>(Table<P>);

impl<P: BufferPool> Columns<P> {
    pub async fn new(buf_pool: &'static P) -> Self {
        let table = Table::new(buf_pool, TABLE_ID_COLUMNS, metadata_of_columns()).await;
        Columns(table)
    }

    /// Insert a column.
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
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

    /// Delete a column by id.
    pub async fn delete_by_id(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
        id: ColumnID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_COLUMNS_COLUMN_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }
}

pub struct Indexes<P: BufferPool>(Table<P>);

impl<P: BufferPool> Indexes<P> {
    pub async fn new(buf_pool: &'static P) -> Self {
        let table = Table::new(buf_pool, TABLE_ID_INDEXES, metadata_of_indexes()).await;
        Indexes(table)
    }

    /// Insert an index.
    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
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
        stmt: &mut Statement,
        id: IndexID,
    ) -> bool {
        let key = SelectKey::new(INDEX_NO_INDEXES_INDEX_ID, vec![Val::from(id)]);
        self.0.delete_row(buf_pool, stmt, &key).await.is_ok()
    }
}

pub struct IndexColumns<P: BufferPool>(Table<P>);

impl<P: BufferPool> IndexColumns<P> {
    pub async fn new(buf_pool: &'static P) -> Self {
        let table = Table::new(
            buf_pool,
            TABLE_ID_INDEX_COLUMNS,
            metadata_of_index_columns(),
        )
        .await;
        IndexColumns(table)
    }

    pub async fn insert(
        &self,
        buf_pool: &'static P,
        stmt: &mut Statement,
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
        _stmt: &mut Statement,
        _index_id: IndexID,
    ) -> bool {
        todo!()
    }
}
