use crate::buffer::{BufferPool, PoolGuards};
use crate::catalog::{TableCache, TableHandle, TableID};
use crate::error::{BlockCorruptionCause, BlockKind, Error, FileKind, Result};
use crate::index::util::Maskable;
use crate::index::{NonUniqueIndex, RowLocation, UniqueIndex};
use crate::lwc::PersistedLwcBlock;
use crate::row::ops::SelectKey;
use crate::row::{RowID, RowRead};
use crate::table::{ColumnStorage, GenericMemTable};
use crate::trx::TrxID;
use crate::trx::row::RowReadAccess;

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
            Self::rollback_entry_in_table(entry, table, guards, ts).await?;
        }
        Ok(())
    }

    #[inline]
    async fn rollback_entry_in_table(
        entry: IndexUndo,
        table: &TableHandle,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        match table {
            TableHandle::User(table) => {
                Self::rollback_entry_in_mem_table(entry, table, Some(&table.storage), guards, ts)
                    .await
            }
            TableHandle::Catalog(table) => {
                Self::rollback_entry_in_mem_table(entry, table, None, guards, ts).await
            }
        }
    }

    async fn rollback_entry_in_mem_table<D: BufferPool, I: BufferPool>(
        entry: IndexUndo,
        table: &GenericMemTable<D, I>,
        storage: Option<&ColumnStorage>,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        let index_pool_guard = table.index_pool_guard(guards);
        match entry.kind {
            IndexUndoKind::InsertUnique(key, merge_old_deleted) => {
                let index = table.sec_idx()[key.index_no].unique().unwrap();
                if merge_old_deleted {
                    // this is actually a update from deleted to non-deleted.
                    // so we just mask it back to deleted.
                    let res = index
                        .mask_as_deleted(index_pool_guard, &key.vals, entry.row_id, ts)
                        .await?;
                    assert!(res);
                } else {
                    let res = index
                        .compare_delete(index_pool_guard, &key.vals, entry.row_id, true, ts)
                        .await?;
                    assert!(res);
                }
            }
            IndexUndoKind::InsertNonUnique(key, merge_old_deleted) => {
                let index = table.sec_idx()[key.index_no].non_unique().unwrap();
                if merge_old_deleted {
                    let res = index
                        .mask_as_deleted(index_pool_guard, &key.vals, entry.row_id, ts)
                        .await?;
                    assert!(res);
                } else {
                    let res = index
                        .compare_delete(index_pool_guard, &key.vals, entry.row_id, true, ts)
                        .await?;
                    assert!(res);
                }
            }
            IndexUndoKind::UpdateUnique(key, old_row_id, deleted) => {
                if !deleted {
                    // The version of previous row is not deleted, means current transaction
                    // must own the lock of old row.
                    // So directly rollback index should be fine.
                    let new_row_id = entry.row_id;
                    let index = table.sec_idx()[key.index_no].unique().unwrap();
                    let res = index
                        .compare_exchange(index_pool_guard, &key.vals, new_row_id, old_row_id, ts)
                        .await?;
                    debug_assert!(res.is_ok());
                } else {
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
                    match table.find_row(guards, old_row_id, storage).await {
                        RowLocation::NotFound => unreachable!(),
                        RowLocation::LwcBlock {
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                        } => {
                            // Rollback of a claimed cold unique owner cannot
                            // latch a row page. Re-read the persisted key and
                            // restore the index according to the CDB marker.
                            let new_row_id = entry.row_id;
                            let index = table.sec_idx()[key.index_no].unique().unwrap();
                            if !Self::lwc_row_matches_key(
                                table,
                                storage,
                                guards,
                                &key,
                                block_id,
                                row_idx,
                                row_shape_fingerprint,
                            )
                            .await?
                            {
                                let res = index
                                    .compare_delete(
                                        index_pool_guard,
                                        &key.vals,
                                        new_row_id,
                                        true,
                                        ts,
                                    )
                                    .await?;
                                assert!(res);
                                return Ok(());
                            }
                            // Restore the masked old owner first. If this
                            // transaction also recorded a deferred delete,
                            // that undo entry will unmask it next.
                            let old_row_id = old_row_id.deleted();
                            let res = index
                                .compare_exchange(
                                    index_pool_guard,
                                    &key.vals,
                                    new_row_id,
                                    old_row_id,
                                    ts,
                                )
                                .await?;
                            assert!(res.is_ok());
                        }
                        RowLocation::RowPage(page_id) => {
                            let Some(page_guard) =
                                table.get_row_page_shared(guards, page_id).await?
                            else {
                                return Ok(());
                            };
                            // acquire row latch to avoid race condition.
                            let (ctx, page) = page_guard.ctx_and_page();
                            let access = RowReadAccess::new(page, ctx, page.row_idx(old_row_id));
                            if access.row().is_deleted() && !access.any_old_version_exists() {
                                // old row is invisible to all transactions.
                                let new_row_id = entry.row_id;
                                let index = table.sec_idx()[key.index_no].unique().unwrap();
                                let res = index
                                    .compare_delete(
                                        index_pool_guard,
                                        &key.vals,
                                        new_row_id,
                                        true,
                                        ts,
                                    )
                                    .await?;
                                assert!(res);
                            } else {
                                // old row must be seen for one transaction.
                                // rollback the index change and let other take care of
                                // following GC.
                                let new_row_id = entry.row_id;
                                let index = table.sec_idx()[key.index_no].unique().unwrap();
                                let res = index
                                    .compare_exchange(
                                        index_pool_guard,
                                        &key.vals,
                                        new_row_id,
                                        old_row_id.deleted(),
                                        ts,
                                    )
                                    .await?;
                                assert!(res.is_ok());
                            }
                        }
                    }
                }
            }
            IndexUndoKind::DeferDelete(key, unique) => {
                // Because we always mask index entry as deleted for each defer delete undo log.
                // We need to unmask it.
                if unique {
                    let index = table.sec_idx()[key.index_no].unique().unwrap();
                    let res = index
                        .compare_exchange(
                            index_pool_guard,
                            &key.vals,
                            entry.row_id.deleted(),
                            entry.row_id,
                            ts,
                        )
                        .await?;
                    assert!(res.is_ok());
                } else {
                    let index = table.sec_idx()[key.index_no].non_unique().unwrap();
                    let res = index
                        .mask_as_active(index_pool_guard, &key.vals, entry.row_id, ts)
                        .await?;
                    assert!(res);
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn lwc_row_matches_key<D: BufferPool, I: BufferPool>(
        table: &GenericMemTable<D, I>,
        storage: Option<&ColumnStorage>,
        guards: &PoolGuards,
        key: &SelectKey,
        block_id: crate::file::BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> Result<bool> {
        let Some(storage) = storage else {
            return Ok(false);
        };
        let block = PersistedLwcBlock::load(
            storage.file().file_kind(),
            storage.file().sparse_file(),
            storage.disk_pool(),
            guards.disk_guard(),
            block_id,
        )
        .await?;
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(Error::block_corrupted(
                FileKind::TableFile,
                BlockKind::LwcBlock,
                block_id,
                BlockCorruptionCause::InvalidPayload,
            ));
        }
        let read_set = table.metadata().index_specs[key.index_no]
            .index_cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let vals = block.decode_row_values(table.metadata(), row_idx, &read_set)?;
        Ok(vals.as_slice() == key.vals.as_slice())
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
