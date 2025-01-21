use crate::row::RowID;
use crate::table::TableID;
use crate::value::Val;

/// IndexUndo represent the undo operation of a index.
pub struct IndexUndo {
    pub table_id: TableID,
    pub kind: IndexUndoKind,
}

pub enum IndexUndoKind {
    // Insert key -> row_id into index.
    Insert(Val, RowID),
    // Update key -> old_row_id to key -> new_row_id.
    Update(Val, RowID, RowID),
    // Delete is not included in index undo.
    // Because the deletion of index will only be executed by GC thread,
    // not transaction thread. So DELETE undo is unneccessary.
}
