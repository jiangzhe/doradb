use crate::buffer::PoolGuards;
use crate::catalog::{CatalogSelectKey, ResolvedUserIndexKey, TableCache};
use crate::error::RuntimeResult as Result;
use crate::id::{RowID, TableID, TrxID};
use crate::runtime::{POLL_BUDGET, yield_now};
use crate::table::IndexRollback;

/// Domain-tagged index undo entry stored in one effect-ordered log.
enum IndexUndoEntry {
    Catalog(IndexUndo<CatalogSelectKey>),
    User(IndexUndo<ResolvedUserIndexKey>),
}

/// Buffer of index undo entries accumulated for rollback and GC handoff.
#[derive(Default)]
pub(crate) struct IndexUndoLogs(Vec<IndexUndoEntry>);

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

    /// Add one fixed-ordinal catalog index undo at the end of the buffer.
    #[inline]
    pub(crate) fn push_catalog(&mut self, value: IndexUndo<CatalogSelectKey>) {
        assert!(
            value.table_id.is_catalog(),
            "catalog index undo requires a catalog table: table_id={}",
            value.table_id
        );
        self.0.push(IndexUndoEntry::Catalog(value));
    }

    /// Add one generation-qualified user index undo at the end of the buffer.
    #[inline]
    pub(crate) fn push_user(&mut self, value: IndexUndo<ResolvedUserIndexKey>) {
        assert!(
            value.table_id.is_user(),
            "user index undo requires a user table: table_id={}",
            value.table_id
        );
        self.0.push(IndexUndoEntry::User(value));
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
                match entry {
                    IndexUndoEntry::Catalog(entry) => {
                        let table = table_cache.must_get_catalog_table(entry.table_id);
                        table.rollback_index_entry(entry, guards, ts).await?;
                    }
                    IndexUndoEntry::User(entry) => {
                        let table = table_cache.must_get_user_entry_mut(entry.table_id).await;
                        table.rollback_index_entry(entry, guards, ts).await?;
                    }
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
        self.0
            .drain(..)
            .filter_map(|entry| match entry {
                IndexUndoEntry::Catalog(entry) => entry.into_purge().map(IndexPurgeEntry::Catalog),
                IndexUndoEntry::User(entry) => entry.into_purge().map(IndexPurgeEntry::User),
            })
            .collect()
    }
}

/// Kinds of index changes that can be rolled back.
pub(crate) enum IndexUndoKind<K> {
    /// Insert unique key, merge flag(if overwrite delete flag)
    InsertUnique(K, bool),
    /// Insert non-unique key, merge flag(if overwrite delete flag).
    InsertNonUnique(K, bool),
    /// Update unique key, old row id, delete flag of old row.
    UpdateUnique(K, RowID, bool),
    /// Delete is not included in index undo,
    /// because transaction thread does not perform index deletion,
    /// in order to support MVCC.
    /// The actual deletion is performed solely by purge workers.
    /// This is what GC entry means.
    /// Second parameter indicates whether the index is unique.
    DeferDelete(K, bool),
}

/// One reversible index change recorded for rollback.
pub(crate) struct IndexUndo<K> {
    /// Table whose index entry was changed.
    pub(crate) table_id: TableID,
    /// Row version referenced by the new index entry.
    pub(crate) row_id: RowID,
    /// Reversible index operation and rollback payload.
    pub(crate) kind: IndexUndoKind<K>,
}

impl<K> IndexUndo<K> {
    #[inline]
    fn into_purge(self) -> Option<IndexPurge<K>> {
        match self.kind {
            IndexUndoKind::InsertUnique(..)
            | IndexUndoKind::InsertNonUnique(..)
            | IndexUndoKind::UpdateUnique(..) => None,
            IndexUndoKind::DeferDelete(key, unique) => Some(IndexPurge {
                table_id: self.table_id,
                row_id: self.row_id,
                key,
                unique,
            }),
        }
    }
}

/// Domain-tagged index entry scheduled for deferred GC-time deletion.
pub(in crate::trx) enum IndexPurgeEntry {
    Catalog(IndexPurge<CatalogSelectKey>),
    User(IndexPurge<ResolvedUserIndexKey>),
}

/// Deferred deletion payload in one index-reference domain.
pub(in crate::trx) struct IndexPurge<K> {
    pub(in crate::trx) table_id: TableID,
    pub(in crate::trx) row_id: RowID,
    pub(in crate::trx) key: K,
    pub(in crate::trx) unique: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        CATALOG_TABLE_ID_START, IndexSlot, catalog_key_from_active_ordinal,
        user_key_from_active_slot,
    };

    fn create_test_key(index_slot: IndexSlot) -> ResolvedUserIndexKey {
        user_key_from_active_slot(index_slot, vec![])
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
        log1.push_user(IndexUndo {
            table_id: TableID::new(1),
            row_id: RowID::new(1),
            kind: IndexUndoKind::InsertUnique(create_test_key(IndexSlot::new(1)), false),
        });

        // Add entries to log2
        log2.push_user(IndexUndo {
            table_id: TableID::new(2),
            row_id: RowID::new(2),
            kind: IndexUndoKind::DeferDelete(create_test_key(IndexSlot::new(2)), true),
        });
        log2.push_user(IndexUndo {
            table_id: TableID::new(3),
            row_id: RowID::new(3),
            kind: IndexUndoKind::UpdateUnique(
                create_test_key(IndexSlot::new(3)),
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
        let IndexUndoEntry::User(first) = &log1.0[0] else {
            panic!("First entry should be user undo");
        };
        match &first.kind {
            IndexUndoKind::InsertUnique(..) => (),
            _ => panic!("First entry should be InsertUnique"),
        }
        let IndexUndoEntry::User(second) = &log1.0[1] else {
            panic!("Second entry should be user undo");
        };
        match &second.kind {
            IndexUndoKind::DeferDelete(..) => (),
            _ => panic!("Second entry should be DeferDelete"),
        }
    }

    #[test]
    fn test_index_undo_and_purge_preserve_typed_reference_domains() {
        let mut logs = IndexUndoLogs::empty();
        logs.push_catalog(IndexUndo {
            table_id: CATALOG_TABLE_ID_START,
            row_id: RowID::new(7),
            kind: IndexUndoKind::DeferDelete(catalog_key_from_active_ordinal(3, vec![]), true),
        });
        logs.push_user(IndexUndo {
            table_id: TableID::new(9),
            row_id: RowID::new(11),
            kind: IndexUndoKind::DeferDelete(create_test_key(IndexSlot::new(5)), false),
        });

        let IndexUndoEntry::User(user) = &logs.0[1] else {
            panic!("second retained entry must be user-qualified");
        };
        let IndexUndoKind::DeferDelete(key, false) = &user.kind else {
            panic!("user retained entry must preserve deferred-delete payload");
        };
        assert_eq!(key.index.id().get(), 5);
        assert_eq!(key.index.slot().get(), 5);

        let purge = logs.commit_for_gc();
        assert!(
            matches!(&purge[0], IndexPurgeEntry::Catalog(entry) if entry.key.index_slot.get() == 3)
        );
        assert!(matches!(&purge[1], IndexPurgeEntry::User(entry)
            if entry.key.index.id().get() == 5 && entry.key.index.slot().get() == 5));
    }
}
