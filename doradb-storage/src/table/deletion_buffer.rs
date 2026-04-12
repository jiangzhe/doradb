//! In-memory delete and ownership overlay for immutable column-store rows.
//!
//! `ColumnDeletionBuffer` is the table-level state that makes LWC rows mutable
//! without rewriting their persisted row images. Foreground cold-row delete and
//! update paths install a marker before masking index entries. Readers consult
//! the marker with their snapshot to decide whether the persisted row image is
//! still visible, while rollback, purge, checkpoints, and recovery use the same
//! marker as the transaction-owned delete state.
//!
//! The buffer is also the in-memory tail above persisted column delete bitmaps:
//! checkpointing can flush committed markers to disk, but markers may remain
//! here until they are no longer needed to make old snapshots visible.

use crate::row::RowID;
use crate::trx::{SharedTrxStatus, TrxID, trx_is_committed};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

/// Result of attempting to claim or seed a cold-row delete marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionError {
    /// The row is owned by another active transaction, or by a committed delete
    /// newer than the caller's snapshot.
    WriteConflict,
    /// A committed delete already exists and is visible to the caller.
    AlreadyDeleted,
}

/// Delete state stored for a logical row that currently belongs to column
/// storage.
///
/// The marker is MVCC and ownership metadata. The immutable LWC row image is
/// still read from disk when the marker is absent or newer than a reader's
/// snapshot.
#[derive(Clone)]
pub enum DeleteMarker {
    /// Marker backed by a shared transaction status.
    ///
    /// While uncommitted, this is the cold-row write ownership record for a
    /// delete/update, or a transition-captured row lock. After commit, the same
    /// status supplies the delete commit timestamp until purge or another
    /// maintenance path compacts it to [`DeleteMarker::Committed`].
    Ref(Arc<SharedTrxStatus>),
    /// Compact committed delete marker carrying the delete commit timestamp.
    Committed(TrxID),
}

/// Concurrent table-level map from cold row id to delete marker.
///
/// Column-store rows are immutable, so foreground modifications use this buffer
/// as both the row-level write lock and the MVCC delete version. Persistent
/// delete bitmaps are the durable base state; this buffer keeps active and
/// recent committed markers needed by transactions, rollback, purge,
/// checkpoint, and recovery replay.
pub struct ColumnDeletionBuffer {
    entries: DashMap<RowID, DeleteMarker>,
}

impl ColumnDeletionBuffer {
    /// Creates an empty column deletion buffer.
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        ColumnDeletionBuffer {
            entries: DashMap::new(),
        }
    }

    /// Claims a cold row for delete/update using the caller's transaction
    /// status.
    ///
    /// `snapshot_sts` is the caller's statement snapshot. The same transaction
    /// may claim the row idempotently. Another uncommitted owner is a write
    /// conflict. A committed delete at or before `snapshot_sts` means the row is
    /// already deleted for this caller; a committed delete after `snapshot_sts`
    /// is treated as a write conflict because the caller is racing with a newer
    /// version state it cannot safely overwrite.
    #[inline]
    pub fn put_ref(
        &self,
        row_id: RowID,
        status: Arc<SharedTrxStatus>,
        snapshot_sts: TrxID,
    ) -> Result<(), DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(entry) => match entry.get() {
                DeleteMarker::Ref(existing) => {
                    let ts = existing.ts();
                    if trx_is_committed(ts) {
                        // A committed Ref is semantically identical to a compact
                        // committed marker until a maintenance path promotes it.
                        if ts <= snapshot_sts {
                            return Err(DeletionError::AlreadyDeleted);
                        }
                        return Err(DeletionError::WriteConflict);
                    }
                    if Arc::ptr_eq(existing, &status) {
                        return Ok(());
                    }
                    // Any live Ref owned by another transaction is the cold-row
                    // write lock.
                    Err(DeletionError::WriteConflict)
                }
                DeleteMarker::Committed(ts) => {
                    if *ts <= snapshot_sts {
                        Err(DeletionError::AlreadyDeleted)
                    } else {
                        Err(DeletionError::WriteConflict)
                    }
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(DeleteMarker::Ref(status));
                Ok(())
            }
        }
    }

    /// Inserts a compact committed delete marker for a cold row.
    ///
    /// This is used by recovery, checkpoint/transition code, and other paths
    /// that already know the delete commit timestamp. If an existing Ref has
    /// already committed, the buffer compacts it using the timestamp observed
    /// from the shared transaction status. A timestamp mismatch reports
    /// [`DeletionError::AlreadyDeleted`] because the row is already represented
    /// by a different committed delete.
    #[inline]
    pub fn put_committed(&self, row_id: RowID, cts: TrxID) -> Result<(), DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(mut entry) => match entry.get() {
                DeleteMarker::Ref(status) => {
                    let ts = status.ts();
                    if !trx_is_committed(ts) {
                        return Err(DeletionError::WriteConflict);
                    }
                    // Use the shared status as the source of truth for an
                    // existing Ref; the caller-provided cts is only a
                    // consistency check.
                    entry.insert(DeleteMarker::Committed(ts));
                    if ts == cts {
                        Ok(())
                    } else {
                        Err(DeletionError::AlreadyDeleted)
                    }
                }
                DeleteMarker::Committed(ts) => {
                    if *ts == cts {
                        Ok(())
                    } else {
                        Err(DeletionError::AlreadyDeleted)
                    }
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(DeleteMarker::Committed(cts));
                Ok(())
            }
        }
    }

    /// Promotes an existing committed Ref marker to the compact committed form.
    ///
    /// Returns `true` only when a conversion happened. Missing markers,
    /// uncommitted refs, and already compact markers return `false`.
    #[inline]
    pub fn promote_delete_marker_if_committed(&self, row_id: RowID) -> bool {
        match self.entries.entry(row_id) {
            Entry::Occupied(mut entry) => match entry.get() {
                DeleteMarker::Ref(status) => {
                    let ts = status.ts();
                    if !trx_is_committed(ts) {
                        return false;
                    }
                    entry.insert(DeleteMarker::Committed(ts));
                    true
                }
                DeleteMarker::Committed(_) => false,
            },
            Entry::Vacant(_) => false,
        }
    }

    /// Clones the current marker for `row_id`.
    ///
    /// Callers are responsible for applying read or write visibility rules with
    /// their own transaction snapshot.
    #[inline]
    pub fn get(&self, row_id: RowID) -> Option<DeleteMarker> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    /// Returns whether a cold-row delete marker is safe for global physical
    /// purge at the supplied oldest active snapshot.
    ///
    /// A marker is purgeable only after its delete is committed and strictly
    /// older than `min_active_sts`. This proves every active or future
    /// transaction sees the cold row as deleted.
    #[inline]
    pub fn delete_marker_is_globally_purgeable(
        &self,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> bool {
        self.delete_marker_is_globally_purgeable_with(row_id, || min_active_sts)
    }

    /// Lazy variant of [`ColumnDeletionBuffer::delete_marker_is_globally_purgeable`].
    ///
    /// The `min_active_sts` closure is called only after the marker exists and
    /// has a committed delete timestamp.
    #[inline]
    pub fn delete_marker_is_globally_purgeable_with<F>(
        &self,
        row_id: RowID,
        min_active_sts: F,
    ) -> bool
    where
        F: FnOnce() -> TrxID,
    {
        let Some(marker) = self.get(row_id) else {
            return false;
        };
        let delete_cts = match marker {
            DeleteMarker::Committed(ts) => ts,
            DeleteMarker::Ref(status) => {
                let ts = status.ts();
                if !trx_is_committed(ts) {
                    // Active owners must keep both the write lock and the MVCC
                    // undo state in memory.
                    return false;
                }
                ts
            }
        };
        delete_cts < min_active_sts()
    }

    /// Collects row ids whose delete marker is committed and
    /// `previous_cutoff <= cts < current_cutoff`.
    ///
    /// Deletion checkpoint uses this to select the replay range not yet covered
    /// by the durable delete bitmap. Uncommitted refs are skipped; committed
    /// refs are included by reading the shared transaction status.
    pub fn collect_committed_in_range(
        &self,
        previous_cutoff: TrxID,
        current_cutoff: TrxID,
    ) -> Vec<RowID> {
        let mut row_ids = Vec::new();
        for entry in &self.entries {
            let cts = match entry.value() {
                DeleteMarker::Committed(ts) => *ts,
                DeleteMarker::Ref(status) => {
                    let ts = status.ts();
                    if !trx_is_committed(ts) {
                        // Checkpoint persists only committed delete state.
                        continue;
                    }
                    ts
                }
            };
            if cts >= previous_cutoff && cts < current_cutoff {
                row_ids.push(*entry.key());
            }
        }
        row_ids
    }

    /// Removes the marker for `row_id` without checking ownership.
    ///
    /// This is intended for rollback after the caller has proven the undo entry
    /// belongs to the transaction being undone. It is not a general committed
    /// marker GC policy.
    #[inline]
    pub fn remove(&self, row_id: RowID) {
        self.entries.remove(&row_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trx::{MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_delete_marker_is_globally_purgeable_with_is_lazy() {
        let buffer = ColumnDeletionBuffer::new();
        let calls = AtomicUsize::new(0);

        assert!(!buffer.delete_marker_is_globally_purgeable_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            100
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        buffer
            .put_ref(
                1,
                Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 1)),
                MAX_SNAPSHOT_TS,
            )
            .unwrap();
        assert!(!buffer.delete_marker_is_globally_purgeable_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            100
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        buffer.remove(1);
        buffer.put_committed(1, 10).unwrap();
        assert!(!buffer.delete_marker_is_globally_purgeable_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            10
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert!(buffer.delete_marker_is_globally_purgeable_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            11
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_put_ref_respects_snapshot_for_committed_markers() {
        let buffer = ColumnDeletionBuffer::new();
        let status = Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 1));

        buffer.put_committed(1, 20).unwrap();
        assert_eq!(
            buffer.put_ref(1, status.clone(), 20),
            Err(DeletionError::AlreadyDeleted)
        );
        assert_eq!(
            buffer.put_ref(1, status.clone(), 19),
            Err(DeletionError::WriteConflict)
        );

        let committed_ref = Arc::new(SharedTrxStatus::new(30));
        buffer.put_ref(2, committed_ref, MAX_SNAPSHOT_TS).unwrap();
        assert_eq!(
            buffer.put_ref(2, status.clone(), 30),
            Err(DeletionError::AlreadyDeleted)
        );
        assert_eq!(
            buffer.put_ref(2, status, 29),
            Err(DeletionError::WriteConflict)
        );
    }
}
