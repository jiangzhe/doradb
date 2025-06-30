use crate::buffer::BufferPool;
use crate::catalog::Catalog;
use crate::index::RowLocation;
use crate::latch::LatchFallbackMode;
use crate::row::ops::SelectKey;
use crate::row::{RowID, RowPage, RowRead};
use crate::table::TableID;
use std::collections::HashMap;

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
    pub async fn rollback<P: BufferPool>(&mut self, data_pool: &'static P, catalog: &Catalog) {
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
                    // There is a race condition:
                    // Transaction A deleted a row{row_id=100, k=1} and committed.
                    // GC thread is trying to delete index entry.
                    // Meanwhile, transaction B is inserting a row{row_id=200, k=1}, and updates
                    // index entry {k=1,row_id=100} to {k=1, row_id=200}.
                    // After the index change, GC thread finds index value of transaction A
                    // does not match orignal value(row_id != 100), then skips the index deletion.
                    // After that, transaction B starts to rollback.
                    // If transaction B does not care about the status of original row, it will leave
                    // a leaked index entry {k=1, row_id=100}, and no GC will touch it again.
                    //
                    // To solve this, we need to re-check original row with row latch and delete
                    // index entry if it is deleted and does not have any old version (already GCed).
                    match table.blk_idx.find_row(old_row_id).await {
                        RowLocation::NotFound => unreachable!(),
                        RowLocation::ColSegment(..) => todo!(),
                        RowLocation::RowPage(page_id) => {
                            let page_guard = data_pool
                                .get_page::<RowPage>(page_id, LatchFallbackMode::Shared)
                                .await
                                .shared_async()
                                .await;
                            // acquire row latch to avoid race condition.
                            let access = page_guard.read_row_by_id(old_row_id);
                            if access.row().is_deleted() && !access.any_old_version_exists() {
                                // old row is invisible to all transactions.
                                let new_row_id = entry.row_id;
                                let res = table.sec_idx[key.index_no]
                                    .unique()
                                    .unwrap()
                                    .compare_delete(&key.vals, new_row_id);
                                assert!(res);
                            } else {
                                // old row must be seen for one transaction.
                                // rollback the index change.
                                let new_row_id = entry.row_id;
                                let res = table.sec_idx[key.index_no]
                                    .unique()
                                    .unwrap()
                                    .compare_exchange(&key.vals, new_row_id, old_row_id);
                                assert!(res.is_ok());
                            }
                        }
                    }
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

    // If one transaction update index back and forth, there might be
    // unneccessary index purge entries.
    // To avoid incorrect purge of index, here we analyze row undo to
    // find whether the index purge entry should be removed.
    // e.g. One transaction update one row(row_id=100) twice:
    // 1. Begin.
    // 2. Update k=1 to k=2.
    // 3. Update k=2 to k=1.
    // 4. Commit.
    // We will have two row undo:
    // update row row_id=100, k=1 => k=2
    // update row row_id=100, k=2 => k=1
    // four index undo:
    // delete index k=1, row_id=100
    // insert index k=2, row_id=100
    // delete index k=2, row_id=100
    // insert index k=1, row_id=100
    // When transaction commits, the index purge entries will be kept for GC.
    // So both k=1 and k=2 will be removed by GC thread.
    // This is wrong. we may either re-check if the row exists or pre-process
    // index purge entries when committing the transaction.
    #[inline]
    pub fn remove_unneccessary_purges(&mut self) {
        let mut deletes = HashMap::new();
        let mut to_remove = vec![];
        for (idx, undo) in self.0.iter().enumerate() {
            match &undo.kind {
                IndexUndoKind::DeferDelete(_) => {
                    let res = deletes.insert((undo.table_id, undo.row_id), idx);
                    assert!(res.is_none());
                }
                IndexUndoKind::InsertUnique(_) => {
                    // one delete is covered by a later insert, we can safely remove it from undo logs.
                    if let Some(del_idx) = deletes.remove(&(undo.table_id, undo.row_id)) {
                        to_remove.push(del_idx);
                    }
                }
                IndexUndoKind::UpdateUnique(..) => (),
            }
        }
        for idx in to_remove.iter().rev() {
            self.0.remove(*idx);
        }
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
            kind: IndexUndoKind::InsertUnique(create_test_key(1)),
        });

        // Add entries to log2
        log2.push(IndexUndo {
            table_id: 2,
            row_id: 2,
            kind: IndexUndoKind::DeferDelete(create_test_key(2)),
        });
        log2.push(IndexUndo {
            table_id: 3,
            row_id: 3,
            kind: IndexUndoKind::UpdateUnique(create_test_key(3), 4),
        });

        // Merge and verify
        let original_len = log1.len() + log2.len();
        log1.merge(&mut log2);
        assert_eq!(log1.len(), original_len);
        assert!(log2.is_empty());

        // Verify order is preserved
        match &log1.0[0].kind {
            IndexUndoKind::InsertUnique(_) => (),
            _ => panic!("First entry should be InsertUnique"),
        }
        match &log1.0[1].kind {
            IndexUndoKind::DeferDelete(_) => (),
            _ => panic!("Second entry should be DeferDelete"),
        }
    }

    #[test]
    fn test_index_undo_logs_remove_unneccessary_purges() {
        let mut logs = IndexUndoLogs::empty();

        // Add some purge entries that should remain
        logs.push(IndexUndo {
            table_id: 1,
            row_id: 1,
            kind: IndexUndoKind::DeferDelete(create_test_key(1)),
        });
        logs.push(IndexUndo {
            table_id: 2,
            row_id: 2,
            kind: IndexUndoKind::DeferDelete(create_test_key(2)),
        });

        // Add purge entries that will be removed
        logs.push(IndexUndo {
            table_id: 3,
            row_id: 3,
            kind: IndexUndoKind::DeferDelete(create_test_key(3)),
        });
        logs.push(IndexUndo {
            table_id: 3,
            row_id: 3,
            kind: IndexUndoKind::InsertUnique(create_test_key(3)),
        });

        // Add another purge that should remain
        logs.push(IndexUndo {
            table_id: 4,
            row_id: 4,
            kind: IndexUndoKind::DeferDelete(create_test_key(4)),
        });

        // Add update which shouldn't affect purge entries
        logs.push(IndexUndo {
            table_id: 5,
            row_id: 5,
            kind: IndexUndoKind::UpdateUnique(create_test_key(5), 6),
        });

        let original_len = logs.len();
        logs.remove_unneccessary_purges();

        // Only one purge entry should be removed (table_id=3, row_id=3)
        assert_eq!(logs.len(), original_len - 1);

        // Verify remaining entries
        let mut remaining_purges = 0;
        for entry in &logs.0 {
            if let IndexUndoKind::DeferDelete(_) = &entry.kind {
                remaining_purges += 1;
                // Verify the purge for table_id=3,row_id=3 was removed
                assert!(!(entry.table_id == 3 && entry.row_id == 3));
            }
        }
        assert_eq!(remaining_purges, 3); // Original 3 purges minus 1 removed
    }
}
