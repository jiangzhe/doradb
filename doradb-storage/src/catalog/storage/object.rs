use crate::catalog::{ColumnAttributes, IndexAttributes, IndexOrder};
use crate::id::{TableID, TrxID};
use crate::value::ValKind;
use semistr::SemiStr;

/// One row object in `catalog.tables`.
#[derive(Debug)]
pub(crate) struct TableObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Next index number to assign for the table.
    pub(crate) next_index_no: u16,
}

/// One row object in `catalog.columns`.
#[derive(Debug)]
pub(crate) struct ColumnObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Column ordinal in the table.
    pub(crate) column_no: u16,
    /// Column name.
    pub(crate) column_name: SemiStr,
    /// Stored value kind.
    pub(crate) column_type: ValKind,
    /// Column attribute bitset.
    pub(crate) column_attributes: ColumnAttributes,
}

/// One row object in `catalog.indexes`.
#[derive(Debug)]
pub(crate) struct IndexObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Index number within the table.
    pub(crate) index_no: u16,
    /// Index attribute bitset.
    pub(crate) index_attributes: IndexAttributes,
}

/// One row object in `catalog.index_columns`.
#[derive(Debug)]
pub(crate) struct IndexColumnObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Index number within the table.
    pub(crate) index_no: u16,
    /// Column position in the index.
    pub(crate) index_column_no: u16,
    /// Column position in the table.
    pub(crate) column_no: u16,
    /// Sort order for the indexed column.
    pub(crate) index_order: IndexOrder,
}

/// One row object in `catalog.table_replay_silent_watermarks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SilentWatermarkObject {
    /// User table identifier.
    pub(crate) table_id: TableID,
    /// Silent heap replay lower bound.
    pub(crate) heap_redo_start_ts: TrxID,
    /// Silent deletion replay lower bound.
    pub(crate) deletion_cutoff_ts: TrxID,
}
