use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::row::ops::SelectKey;
use crate::row::RowID;
use crate::table::TableID;

#[derive(Default)]
pub struct IndexUndoLogs(Vec<IndexUndo>);

impl IndexUndoLogs {
    /// Create an empty index undo buffer.
    #[inline]
    pub fn empty() -> Self {
        IndexUndoLogs(vec![])
    }

    /// Returns whether the index undo buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns count of index undo logs.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Add a new index undo log at end of the buffer.
    #[inline]
    pub fn push(&mut self, value: IndexUndo) {
        self.0.push(value);
    }

    /// Rollback all index changes.
    ///
    /// This method has strong assertion to make sure it will not fail,
    /// because other transaction can not update the same index entry
    /// concurrently.
    #[inline]
    pub fn rollback<P: BufferPool>(&mut self, catalog: &Catalog<P>) {
        while let Some(entry) = self.0.pop() {
            let table = catalog.get_table(entry.table_id).unwrap();
            match entry.kind {
                IndexUndoKind::InsertUnique(key) => {
                    let res = table.sec_idx[key.index_no]
                        .unique()
                        .unwrap()
                        .compare_delete(&key.vals, entry.row_id);
                    assert!(res);
                }
                IndexUndoKind::UpdateUnique(key, old_row_id) => {
                    let new_row_id = entry.row_id;
                    let res = table.sec_idx[key.index_no]
                        .unique()
                        .unwrap()
                        .compare_exchange(&key.vals, new_row_id, old_row_id);
                    assert!(res);
                }
                IndexUndoKind::DeferDelete(_) => (), // do nothing.
            }
        }
    }

    /// Merge another undo log buffer.
    /// This is used when a statement succeeds, and statement-level index undo buffer
    /// should be merged into transaction-level index undo buffer.
    #[inline]
    pub fn merge(&mut self, other: &mut Self) {
        self.0.extend(other.0.drain(..));
    }

    /// Prepare index undo logs for GC.
    /// Index undo logs is mainly for proactive/passive rollback.
    /// And to support MVCC, index deletion is delayed to GC phase.
    /// So here we should only keep potential index deletions.
    #[inline]
    pub fn commit_for_gc(&mut self) -> Vec<IndexPurge> {
        self.0
            .drain(..)
            .filter_map(|entry| match entry.kind {
                IndexUndoKind::InsertUnique(_) | IndexUndoKind::UpdateUnique(..) => None,
                IndexUndoKind::DeferDelete(key) => Some(IndexPurge {
                    table_id: entry.table_id,
                    row_id: entry.row_id,
                    key,
                }),
            })
            .collect()
    }
}

/// IndexUndo represent the undo operation of a index.
pub struct IndexUndo {
    pub table_id: TableID,
    // The new row id of index change.
    pub row_id: RowID,
    pub kind: IndexUndoKind,
}

pub enum IndexUndoKind {
    /// Insert key.
    InsertUnique(SelectKey),
    /// Update key and old row id.
    UpdateUnique(SelectKey, RowID),
    /// Delete is not included in index undo,
    /// because transaction thread does not perform index deletion,
    /// in order to support MVCC.
    /// The actual deletion is performed solely by GC thread.
    /// This is what GC entry means.
    DeferDelete(SelectKey),
}

pub struct IndexPurge {
    pub table_id: TableID,
    pub row_id: RowID,
    pub key: SelectKey,
}
