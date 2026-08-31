use crate::buffer::PoolGuards;
use crate::catalog::{ResolvedIndexKey, TableCache};
use crate::error::RuntimeResult as Result;
use crate::id::{RowID, TableID, TrxID};
use crate::runtime::{POLL_BUDGET, yield_now};
use crate::table::IndexRollback;

/// Buffer of index undo entries accumulated for rollback and GC handoff.
#[derive(Default)]
pub(crate) struct IndexUndoLogs(Vec<IndexUndo>);

impl IndexUndoLogs {
    /// Create an empty index undo buffer.
    #[inline]
    pub(crate) fn empty() -> Self {
        IndexUndoLogs(vec![])
    }

    /// Returns whether the index undo buffer is empty.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns count of index undo logs.
    #[inline]
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved len"))]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Adds one resolved index undo at the end of the effect-ordered buffer.
    #[inline]
    pub(crate) fn push(&mut self, value: IndexUndo) {
        self.0.push(value);
    }

    /// Rollback all index changes.
    ///
    /// This method has strong assertion to make sure it will not fail,
    /// because other transaction can not update the same index entry
    /// concurrently.
    #[inline]
    pub(crate) async fn rollback(
        &mut self,
        table_cache: &mut TableCache<'_>,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> Result<()> {
        let mut budget = POLL_BUDGET;
        while !self.0.is_empty() {
            {
                // Keep the current entry vector-owned across every await. If
                // this rollback future is cancelled or fails, the entry stays
                // available for transaction rollback or fatal retention.
                // Successful rollback returns before the synchronous pop below.
                let entry = self
                    .0
                    .last()
                    .expect("non-empty index undo buffer must have a last entry");
                #[cfg(test)]
                {
                    use super::tests::maybe_pause_index_rollback;
                    maybe_pause_index_rollback().await;
                }
                if entry.table_id.is_catalog() {
                    let table = table_cache.must_get_catalog_table(entry.table_id);
                    table.rollback_index_entry(entry, guards, ts).await?;
                } else {
                    let table = table_cache.must_get_user_entry_mut(entry.table_id).await;
                    table.rollback_index_entry(entry, guards, ts).await?;
                }
            }
            self.0.pop();
            budget -= 1;
            if budget == 0 && !self.0.is_empty() {
                yield_now().await;
                budget = POLL_BUDGET;
            }
        }
        Ok(())
    }

    /// Merge another undo log buffer.
    /// This is used when a statement succeeds, and statement-level index undo buffer
    /// should be merged into transaction-level index undo buffer.
    #[inline]
    pub(crate) fn merge(&mut self, other: &mut Self) {
        self.0.append(&mut other.0);
    }

    /// Prepare index undo logs for GC.
    /// Index undo logs is mainly for proactive/passive rollback.
    /// And to support MVCC, index deletion is delayed to GC phase.
    /// So here we should only keep potential index deletions.
    #[inline]
    pub(in crate::trx) fn commit_for_gc(&mut self) -> Vec<IndexPurgeEntry> {
        self.0.drain(..).filter_map(IndexUndo::into_purge).collect()
    }
}

/// Kinds of index changes that can be rolled back.
pub(crate) enum IndexUndoKind {
    /// Insert unique key, merge flag(if overwrite delete flag)
    InsertUnique(ResolvedIndexKey, bool),
    /// Insert non-unique key, merge flag(if overwrite delete flag).
    InsertNonUnique(ResolvedIndexKey, bool),
    /// Update unique key, old row id, delete flag of old row.
    UpdateUnique(ResolvedIndexKey, RowID, bool),
    /// Delete is not included in index undo,
    /// because transaction thread does not perform index deletion,
    /// in order to support MVCC.
    /// The actual deletion is performed solely by purge workers.
    /// This is what GC entry means.
    /// Second parameter indicates whether the index is unique.
    DeferDelete(ResolvedIndexKey, bool),
}

/// One reversible index change recorded for rollback.
pub(crate) struct IndexUndo {
    /// Table whose index entry was changed.
    pub(crate) table_id: TableID,
    /// Row version referenced by the new index entry.
    pub(crate) row_id: RowID,
    /// Reversible index operation and rollback payload.
    pub(crate) kind: IndexUndoKind,
}

impl IndexUndo {
    #[inline]
    fn into_purge(self) -> Option<IndexPurgeEntry> {
        match self.kind {
            IndexUndoKind::InsertUnique(..)
            | IndexUndoKind::InsertNonUnique(..)
            | IndexUndoKind::UpdateUnique(..) => None,
            IndexUndoKind::DeferDelete(key, unique) => Some(IndexPurgeEntry {
                table_id: self.table_id,
                row_id: self.row_id,
                key,
                unique,
            }),
        }
    }
}

/// Index entry scheduled for deferred GC-time deletion.
pub(in crate::trx) struct IndexPurgeEntry {
    pub(in crate::trx) table_id: TableID,
    pub(in crate::trx) row_id: RowID,
    pub(in crate::trx) key: ResolvedIndexKey,
    pub(in crate::trx) unique: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        CATALOG_TABLE_ID_START, IndexID, IndexRef, IndexSlot, catalog_key_from_active_ordinal,
        resolve_catalog_key, user_key_from_index_ref,
    };

    fn create_test_key(index: IndexRef) -> ResolvedIndexKey {
        user_key_from_index_ref(index, vec![])
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
            table_id: TableID::new(1),
            row_id: RowID::new(1),
            kind: IndexUndoKind::InsertUnique(
                create_test_key(IndexRef::new(IndexID::new(1), IndexSlot::new(1))),
                false,
            ),
        });

        // Add entries to log2
        log2.push(IndexUndo {
            table_id: TableID::new(2),
            row_id: RowID::new(2),
            kind: IndexUndoKind::DeferDelete(
                create_test_key(IndexRef::new(IndexID::new(2), IndexSlot::new(2))),
                true,
            ),
        });
        log2.push(IndexUndo {
            table_id: TableID::new(3),
            row_id: RowID::new(3),
            kind: IndexUndoKind::UpdateUnique(
                create_test_key(IndexRef::new(IndexID::new(3), IndexSlot::new(3))),
                RowID::new(4),
                false,
            ),
        });

        // Merge and verify
        let original_len = log1.len() + log2.len();
        log1.merge(&mut log2);
        assert_eq!(log1.len(), original_len);
        assert!(log2.is_empty());

        // Verify order is preserved
        let first = &log1.0[0];
        match &first.kind {
            IndexUndoKind::InsertUnique(..) => (),
            _ => panic!("First entry should be InsertUnique"),
        }
        let second = &log1.0[1];
        match &second.kind {
            IndexUndoKind::DeferDelete(..) => (),
            _ => panic!("Second entry should be DeferDelete"),
        }
    }

    #[test]
    fn test_index_undo_and_purge_preserve_resolved_references() {
        let mut logs = IndexUndoLogs::empty();
        logs.push(IndexUndo {
            table_id: CATALOG_TABLE_ID_START,
            row_id: RowID::new(7),
            kind: IndexUndoKind::DeferDelete(
                resolve_catalog_key(catalog_key_from_active_ordinal(3, vec![])),
                true,
            ),
        });
        logs.push(IndexUndo {
            table_id: TableID::new(9),
            row_id: RowID::new(11),
            kind: IndexUndoKind::DeferDelete(
                create_test_key(IndexRef::new(IndexID::new(5), IndexSlot::new(5))),
                false,
            ),
        });

        let user = &logs.0[1];
        let IndexUndoKind::DeferDelete(key, false) = &user.kind else {
            panic!("user retained entry must preserve deferred-delete payload");
        };
        assert_eq!(key.index.id().get(), 5);
        assert_eq!(key.index.slot().get(), 5);

        let purge = logs.commit_for_gc();
        assert_eq!(purge[0].key.index.id().get(), 3);
        assert_eq!(purge[0].key.index.slot().get(), 3);
        assert_eq!(purge[1].key.index.id().get(), 5);
        assert_eq!(purge[1].key.index.slot().get(), 5);
    }
}
