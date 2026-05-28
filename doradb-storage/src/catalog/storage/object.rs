use crate::catalog::{ColumnAttributes, IndexAttributes, IndexOrder};
use crate::id::TableID;
use crate::value::ValKind;
use semistr::SemiStr;

/// One row object in `catalog.tables`.
#[derive(Debug)]
pub(crate) struct TableObject {
    pub(crate) table_id: TableID,
    pub(crate) next_index_no: u16,
}

/// One row object in `catalog.columns`.
#[derive(Debug)]
pub(crate) struct ColumnObject {
    pub(crate) table_id: TableID,
    pub(crate) column_no: u16,
    pub(crate) column_name: SemiStr,
    pub(crate) column_type: ValKind,
    pub(crate) column_attributes: ColumnAttributes,
}

/// One row object in `catalog.indexes`.
#[derive(Debug)]
pub(crate) struct IndexObject {
    pub(crate) table_id: TableID,
    pub(crate) index_no: u16,
    pub(crate) index_attributes: IndexAttributes,
}

/// One row object in `catalog.index_columns`.
#[derive(Debug)]
pub(crate) struct IndexColumnObject {
    pub(crate) table_id: TableID,
    pub(crate) index_no: u16,
    // column position in index.
    pub(crate) index_column_no: u16,
    // column position in table.
    pub(crate) column_no: u16,
    pub(crate) index_order: IndexOrder,
}
