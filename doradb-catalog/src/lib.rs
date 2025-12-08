pub mod error;
pub mod mem_impl;
pub mod spec;

use crate::error::Result;
use bitflags::bitflags;
use doradb_datatype::PreciseType;
use semistr::SemiStr;
use std::hash::Hash;
use std::mem;

pub use spec::*;

pub type ObjID = u64;
pub type TableID = ObjID;
pub type SchemaID = ObjID;
pub type ColumnID = ObjID;
pub type IndexID = ObjID;

/// Catalog maintains metadata of all database objects.
/// It could be shared between threads.
pub trait Catalog: Send + Sync {
    fn create_schema(&self, schema: SchemaSpec) -> Result<SchemaID>;

    fn drop_schema(&self, schema_name: &str) -> Result<()>;

    fn all_schemas(&self) -> Vec<Schema>;

    fn exists_schema(&self, schema_name: &str) -> bool;

    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema>;

    fn find_schema(&self, schema_id: SchemaID) -> Option<Schema>;

    fn create_table(&self, schema_id: SchemaID, table_spec: TableSpec) -> Result<TableID>;

    fn drop_table(&self, schema_id: SchemaID, table_name: &str) -> Result<()>;

    fn all_tables_in_schema(&self, schema_id: SchemaID) -> Vec<Table>;

    fn exists_table(&self, schema_id: SchemaID, table_name: &str) -> bool;

    fn find_table_by_name(&self, schema_id: SchemaID, table_name: &str) -> Option<Table>;

    fn find_table(&self, table_id: TableID) -> Option<Table>;

    fn all_columns_in_table(&self, table_id: TableID) -> Vec<Column>;

    fn exists_column(&self, table_id: TableID, column_name: &str) -> bool;

    fn find_column_by_name(&self, table_id: TableID, column_name: &str) -> Option<Column>;

    fn find_keys(&self, table_id: TableID) -> Vec<Key>;

    fn create_index(&self, table_id: TableID, index: IndexSpec) -> Result<()>;

    fn drop_index(&self, table_id: TableID, index_name: &str) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub id: SchemaID,
    pub name: SemiStr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub id: TableID,
    pub schema_id: SchemaID,
    pub name: SemiStr,
}

pub enum Key {
    PrimaryKey(Vec<Column>),
    UniqueKey(Vec<Column>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub id: ColumnID,
    pub table_id: TableID,
    pub name: SemiStr,
    pub pty: PreciseType,
    pub idx: ColIndex,
    pub attr: ColumnAttributes,
}

/// ColIndex wraps u32 to be the index of column in current table/subquery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColIndex(u32);

impl ColIndex {
    #[inline]
    pub fn value(&self) -> u32 {
        self.0
    }
}

impl From<u32> for ColIndex {
    fn from(src: u32) -> Self {
        ColIndex(src)
    }
}

impl std::fmt::Display for ColIndex {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "c{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TblCol(pub TableID, pub ColIndex);

bitflags! {
    pub struct ColumnAttributes: u32 {
        // whether value can be null.
        const NULLABLE = 0x01;
        // whether it belongs to any index.
        const INDEX = 0x02;
    }
}

bitflags! {
    pub struct IndexAttributes: u32 {
        const PK = 0x01;
        const UK = 0x02;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexKey {
    // This is user_col_idx. RowID is not included.
    pub col_no: u16,
    pub order: IndexOrder,
}

impl IndexKey {
    #[inline]
    pub fn new(col_no: u16) -> Self {
        IndexKey {
            col_no,
            order: IndexOrder::Asc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexOrder {
    Asc = 0,
    Desc = 1,
}

impl From<u8> for IndexOrder {
    #[inline]
    fn from(value: u8) -> Self {
        unsafe { mem::transmute(value) }
    }
}
