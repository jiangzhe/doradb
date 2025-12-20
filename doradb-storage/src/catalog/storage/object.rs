use crate::catalog::{
    ColumnAttributes, ColumnID, IndexAttributes, IndexID, IndexOrder, SchemaID, TableID,
};
use crate::value::ValKind;
use semistr::SemiStr;

#[derive(Debug)]
pub struct SchemaObject {
    pub schema_id: SchemaID,
    pub schema_name: SemiStr,
}

#[derive(Debug)]
pub struct TableObject {
    pub table_id: TableID,
    pub schema_id: SchemaID,
    pub table_name: SemiStr,
}

#[derive(Debug)]
pub struct ColumnObject {
    pub column_id: ColumnID,
    pub table_id: TableID,
    pub column_name: SemiStr,
    pub column_no: u16,
    pub column_type: ValKind,
    pub column_attributes: ColumnAttributes,
}

#[derive(Debug)]
pub struct IndexObject {
    pub index_id: IndexID,
    pub table_id: TableID,
    pub index_name: SemiStr,
    pub index_attributes: IndexAttributes,
}

#[derive(Debug)]
pub struct IndexColumnObject {
    pub column_id: ColumnID,
    pub index_id: IndexID,
    // column position in table.
    pub column_no: u16,
    // column position in index.
    pub index_column_no: u16,
    pub index_order: IndexOrder,
}
