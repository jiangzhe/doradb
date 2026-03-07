use crate::catalog::{ColumnAttributes, IndexAttributes, IndexOrder, TableID};
use crate::value::ValKind;
use semistr::SemiStr;

/// One row object in `catalog.tables`.
#[derive(Debug)]
pub struct TableObject {
    pub table_id: TableID,
}

/// One row object in `catalog.columns`.
#[derive(Debug)]
pub struct ColumnObject {
    pub table_id: TableID,
    pub column_no: u16,
    pub column_name: SemiStr,
    pub column_type: ValKind,
    pub column_attributes: ColumnAttributes,
}

/// One row object in `catalog.indexes`.
#[derive(Debug)]
pub struct IndexObject {
    pub table_id: TableID,
    pub index_no: u16,
    pub index_name: SemiStr,
    pub index_attributes: IndexAttributes,
}

/// One row object in `catalog.index_columns`.
#[derive(Debug)]
pub struct IndexColumnObject {
    pub table_id: TableID,
    pub index_no: u16,
    // column position in index.
    pub index_column_no: u16,
    // column position in table.
    pub column_no: u16,
    pub index_order: IndexOrder,
}
