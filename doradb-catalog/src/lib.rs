pub mod error;
pub mod mem_impl;
pub mod object;

pub use object::*;

use crate::error::Result;
use bitflags::bitflags;
use doradb_datatype::PreciseType;
use semistr::SemiStr;
use std::hash::Hash;
/// Catalog maintains metadata of all database objects.
/// It could be shared between threads.
pub trait Catalog: Send + Sync {
    fn create_schema(&self, schema_name: &str) -> Result<SchemaID>;

    fn drop_schema(&self, schema_name: &str) -> Result<()>;

    fn all_schemas(&self) -> Vec<Schema>;

    fn exists_schema(&self, schema_name: &str) -> bool;

    fn find_schema_by_name(&self, schema_name: &str) -> Option<Schema>;

    fn find_schema(&self, schema_id: &SchemaID) -> Option<Schema>;

    fn create_table(&self, table_spec: TableSpec) -> Result<TableID>;

    fn drop_table(&self, schema_name: &str, table_name: &str) -> Result<()>;

    fn all_tables_in_schema(&self, schema_id: &SchemaID) -> Vec<Table>;

    fn exists_table(&self, schema_id: &SchemaID, table_name: &str) -> bool;

    fn find_table_by_name(&self, schema_id: &SchemaID, table_name: &str) -> Option<Table>;

    fn find_table(&self, table_id: &TableID) -> Option<Table>;

    fn all_columns_in_table(&self, table_id: &TableID) -> Vec<Column>;

    fn exists_column(&self, table_id: &TableID, column_name: &str) -> bool;

    fn find_column_by_name(&self, table_id: &TableID, column_name: &str) -> Option<Column>;

    fn find_keys(&self, table_id: &TableID) -> Vec<Key>;
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

/// Table spec used in creating table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSpec {
    pub schema_name: SemiStr,
    pub table_name: SemiStr,
    pub columns: Vec<ColumnSpec>,
    // todo: index and constraint
}

impl TableSpec {
    #[inline]
    pub fn new(schema_name: &str, table_name: &str, columns: Vec<ColumnSpec>) -> Self {
        TableSpec {
            schema_name: SemiStr::new(schema_name),
            table_name: SemiStr::new(table_name),
            columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub id: ColumnID,
    pub table_id: TableID,
    pub name: SemiStr,
    pub pty: PreciseType,
    pub idx: ColIndex,
    pub attr: ColumnAttr,
}

/// Column spec used in creating table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: SemiStr,
    pub pty: PreciseType,
    pub attr: ColumnAttr,
}

impl ColumnSpec {
    #[inline]
    pub fn new(name: &str, pty: PreciseType, attr: ColumnAttr) -> Self {
        ColumnSpec {
            name: SemiStr::new(name),
            pty,
            attr,
        }
    }
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
    pub struct ColumnAttr: u8 {
        const PK = 0x01; // primary key
        const UK = 0x02; // unique key
        const FK = 0x04; // foreign key
        const SK = 0x08; // shard key
    }
}
