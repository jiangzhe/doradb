use crate::buffer::BufferPool;
use crate::catalog::{Catalog, TableID};
use crate::index::util::Maskable;
use crate::index::{NonUniqueIndex, RowLocation, UniqueIndex};
use crate::latch::LatchFallbackMode;
use crate::row::ops::SelectKey;
use crate::row::{RowID, RowPage, RowRead};
use crate::trx::TrxID;

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
    pub async fn rollback<P: BufferPool>(
        &mut self,
        data_pool: &'static P,
        catalog: &Catalog,
        ts: TrxID,
    ) {
        while let Some(entry) = self.0.pop() {
            let table = catalog.get_table(entry.table_id).await.unwrap();
            match entry.kind {
                IndexUndoKind::InsertUnique(key, merge_old_deleted) => {
                    let index = table.sec_idx[key.index_no].unique().unwrap();
                    if merge_old_deleted {
                        // this is actually a update from deleted to non-deleted.
                        // so we just mask it back to deleted.
                        let res = index.mask_as_deleted(&key.vals, entry.row_id, ts).await;
                        assert!(res);
                    } else {
                        let res = index
                            .compare_delete(&key.vals, entry.row_id, true, ts)
                            .await;
                        assert!(res);
                    }
                }
                IndexUndoKind::InsertNonUnique(key, merge_old_deleted) => {
                    let index = table.sec_idx[key.index_no].non_unique().unwrap();
                    if merge_old_deleted {
                        let res = index.mask_as_deleted(&key.vals, entry.row_id, ts).await;
                        assert!(res);
                    } else {
                        let res = index
                            .compare_delete(&key.vals, entry.row_id, true, ts)
                            .await;
                        assert!(res);
                    }
                }
                IndexUndoKind::UpdateUnique(key, old_row_id, deleted) => {
                    if !deleted {
                        // The version of previous row is not deleted, means current transaction
                        // must own the lock of old row.
                        // So directly rollback index should be fine.
                        let new_row_id = entry.row_id;
                        let index = table.sec_idx[key.index_no].unique().unwrap();
                        let res = index
                            .compare_exchange(&key.vals, new_row_id, old_row_id, ts)
                            .await;
                        debug_assert!(res.is_ok());
                        return;
                    }
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
                        RowLocation::LwcPage(..) => todo!("lwc page"),
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
                                let index = table.sec_idx[key.index_no].unique().unwrap();
                                let res =
                                    index.compare_delete(&key.vals, new_row_id, true, ts).await;
                                assert!(res);
                            } else {
                                // old row must be seen for one transaction.
                                // rollback the index change and let other take care of
                                // following GC.
                                let new_row_id = entry.row_id;
                                let index = table.sec_idx[key.index_no].unique().unwrap();
                                let res = index
                                    .compare_exchange(
                                        &key.vals,
                                        new_row_id,
                                        old_row_id.deleted(),
                                        ts,
                                    )
                                    .await;
                                assert!(res.is_ok());
                            }
                        }
                    }
                }
                IndexUndoKind::DeferDelete(key, unique) => {
                    // Because we always mask index entry as deleted for each defer delete undo log.
                    // We need to unmask it.
                    if unique {
                        let index = table.sec_idx[key.index_no].unique().unwrap();
                        let res = index
                            .compare_exchange(&key.vals, entry.row_id.deleted(), entry.row_id, ts)
                            .await;
                        assert!(res.is_ok());
                    } else {
                        let index = table.sec_idx[key.index_no].non_unique().unwrap();
                        let res = index.mask_as_active(&key.vals, entry.row_id, ts).await;
                        assert!(res);
                    }
                }
            }
        }
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

/// IndexUndo represent the undo operation of a index.
pub struct IndexUndo {
    pub table_id: TableID,
    // The new row id of index change.
    pub row_id: RowID,
    pub kind: IndexUndoKind,
}

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
