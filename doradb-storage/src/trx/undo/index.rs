use crate::buffer::PoolGuards;
use crate::catalog::{TableCache, TableID};
use crate::error::Result;
use crate::row::RowID;
use crate::row::ops::SelectKey;
use crate::trx::TrxID;

/// Buffer of index undo entries accumulated for rollback and GC handoff.
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
    pub async fn rollback(
        &mut self,
        table_cache: &mut TableCache<'_>,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        while let Some(entry) = self.0.pop() {
            let table = table_cache.must_get_table(entry.table_id).await;
            table.rollback_index_entry(entry, guards, ts).await?;
        }
        Ok(())
    }

    /// Merge another undo log buffer.
    /// This is used when a statement succeeds, and statement-level index undo buffer
    /// should be merged into transaction-level index undo buffer.
    #[inline]
    pub fn merge(&mut self, other: &mut Self) {
        self.0.append(&mut other.0);
    }

    /// Prepare index undo logs for GC.
    /// Index undo logs is mainly for proactive/passive rollback.
    /// And to support MVCC, index deletion is delayed to GC phase.
    /// So here we should only keep potential index deletions.
    #[inline]
    pub fn commit_for_gc(&mut self) -> Vec<IndexPurgeEntry> {
        self.0
            .drain(..)
            .filter_map(|entry| match entry.kind {
                IndexUndoKind::InsertUnique(..)
                | IndexUndoKind::InsertNonUnique(..)
                | IndexUndoKind::UpdateUnique(..) => None,
                IndexUndoKind::DeferDelete(key, unique) => Some(IndexPurgeEntry {
                    table_id: entry.table_id,
                    row_id: entry.row_id,
                    key,
                    unique,
                }),
            })
            .collect()
    }
}

/// One reversible index change recorded for rollback.
pub struct IndexUndo {
    pub table_id: TableID,
    // The new row id of index change.
    pub row_id: RowID,
    pub kind: IndexUndoKind,
}

/// Kinds of index changes that can be rolled back.
pub enum IndexUndoKind {
    /// Insert unique key, merge flag(if overwrite delete flag)
    InsertUnique(SelectKey, bool),
    /// Insert non-unique key, merge flag(if overwrite delete flag).
    InsertNonUnique(SelectKey, bool),
    /// Update unique key, old row id, delete flag of old row.
    UpdateUnique(SelectKey, RowID, bool),
    /// Delete is not included in index undo,
    /// because transaction thread does not perform index deletion,
    /// in order to support MVCC.
    /// The actual deletion is performed solely by GC thread.
    /// This is what GC entry means.
    /// Second parameter indicates whether the index is unique.
    DeferDelete(SelectKey, bool),
}

/// Index entry scheduled for deferred GC-time deletion.
pub struct IndexPurgeEntry {
    pub table_id: TableID,
    pub row_id: RowID,
    pub key: SelectKey,
    pub unique: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::ops::SelectKey;

    fn create_test_key(index_no: usize) -> SelectKey {
        SelectKey {
            index_no,
            vals: vec![],
        }
    }

    #[test]
    fn test_index_undo_logs_merge() {
        let mut log1 = IndexUndoLogs::empty();
        let mut log2 = IndexUndoLogs::empty();

        // Test merging empty logs
        log1.merge(&mut log2);
        assert!(log1.is_empty());
        assert!(log2.is_empty());

        // Add entries to log1
        log1.push(IndexUndo {
            table_id: 1,
            row_id: 1,
            kind: IndexUndoKind::InsertUnique(create_test_key(1), false),
        });

        // Add entries to log2
        log2.push(IndexUndo {
            table_id: 2,
            row_id: 2,
            kind: IndexUndoKind::DeferDelete(create_test_key(2), true),
        });
        log2.push(IndexUndo {
            table_id: 3,
            row_id: 3,
            kind: IndexUndoKind::UpdateUnique(create_test_key(3), 4, false),
        });

        // Merge and verify
        let original_len = log1.len() + log2.len();
        log1.merge(&mut log2);
        assert_eq!(log1.len(), original_len);
        assert!(log2.is_empty());

        // Verify order is preserved
        match &log1.0[0].kind {
            IndexUndoKind::InsertUnique(..) => (),
            _ => panic!("First entry should be InsertUnique"),
        }
        match &log1.0[1].kind {
            IndexUndoKind::DeferDelete(..) => (),
            _ => panic!("Second entry should be DeferDelete"),
        }
    }
}
