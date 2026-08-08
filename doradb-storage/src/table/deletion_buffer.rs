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

use crate::id::{RowID, TrxID};
use crate::map::FastDashMap;
use crate::poison::PoisonAwareListener;
use crate::trx::{PrepareListenerResult, SharedTrxStatus, trx_is_committed};
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

/// Result of attempting to claim or seed a cold-row delete marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeletionError {
    /// The row is owned by another active transaction, or by a committed delete
    /// newer than the caller's snapshot.
    WriteConflict,
    /// A committed delete already exists and is visible to the caller.
    AlreadyDeleted,
}

/// Result of a foreground cold-row ownership claim.
pub(crate) enum DeletionClaim {
    /// The caller installed or already owns the delete marker.
    Acquired,
    /// A foreign owner is preparing; consume the poison-aware token, then retry
    /// from authoritative row and marker state.
    Preparing(PoisonAwareListener),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingDeletion {
    Acquired,
    AlreadyDeleted,
    WriteConflict,
    ForeignActive,
}

/// Delete state stored for a logical row that currently belongs to column
/// storage.
///
/// The marker is MVCC and ownership metadata. The immutable LWC row image is
/// still read from disk when the marker is absent or newer than a reader's
/// snapshot.
#[derive(Clone)]
pub(crate) enum DeleteMarker {
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
pub(crate) struct ColumnDeletionBuffer {
    entries: FastDashMap<RowID, DeleteMarker>,
}

impl ColumnDeletionBuffer {
    /// Creates an empty column deletion buffer.
    #[inline]
    pub(crate) fn new() -> Self {
        ColumnDeletionBuffer {
            entries: FastDashMap::default(),
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
    pub(crate) fn put_ref(
        &self,
        row_id: RowID,
        status: Arc<SharedTrxStatus>,
        snapshot_sts: TrxID,
    ) -> Result<(), DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(entry) => match entry.get() {
                DeleteMarker::Ref(existing) => {
                    Self::no_wait_result(Self::classify_ref(existing, &status, snapshot_sts))
                }
                DeleteMarker::Committed(ts) => {
                    Self::no_wait_result(Self::classify_committed(*ts, snapshot_sts))
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(DeleteMarker::Ref(status));
                Ok(())
            }
        }
    }

    /// Claims a cold row for a foreground update or delete.
    ///
    /// Unlike [`ColumnDeletionBuffer::put_ref`], a foreign preparing owner
    /// returns opaque poison-aware retry authority. The returned token is owned
    /// and the deletion-buffer entry guard has already been released.
    #[inline]
    pub(crate) fn claim_ref(
        &self,
        row_id: RowID,
        status: Arc<SharedTrxStatus>,
        snapshot_sts: TrxID,
    ) -> Result<DeletionClaim, DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(entry) => match entry.get() {
                DeleteMarker::Ref(existing) => {
                    match Self::classify_ref(existing, &status, snapshot_sts) {
                        ExistingDeletion::Acquired => Ok(DeletionClaim::Acquired),
                        ExistingDeletion::AlreadyDeleted => Err(DeletionError::AlreadyDeleted),
                        ExistingDeletion::WriteConflict => Err(DeletionError::WriteConflict),
                        ExistingDeletion::ForeignActive => {
                            match existing.prepare_listener() {
                                PrepareListenerResult::NotPreparing => {
                                    // Commit can publish its timestamp just before
                                    // prepare completion clears the flag.
                                    Self::foreground_committed_result(existing.ts(), snapshot_sts)
                                        .unwrap_or(Err(DeletionError::WriteConflict))
                                }
                                PrepareListenerResult::Registered(listener) => {
                                    Ok(DeletionClaim::Preparing(listener))
                                }
                                PrepareListenerResult::Completed(listener) => {
                                    // Completion won registration. Reclassify a
                                    // committed owner under the CDB entry guard; an
                                    // active timestamp requires an immediate retry so
                                    // rollback removal or fatal poison can be observed.
                                    Self::foreground_committed_result(existing.ts(), snapshot_sts)
                                        .unwrap_or(Ok(DeletionClaim::Preparing(listener)))
                                }
                            }
                        }
                    }
                }
                DeleteMarker::Committed(ts) => {
                    Self::foreground_result(Self::classify_committed(*ts, snapshot_sts))
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(DeleteMarker::Ref(status));
                Ok(DeletionClaim::Acquired)
            }
        }
    }

    #[inline]
    fn classify_ref(
        existing: &Arc<SharedTrxStatus>,
        status: &Arc<SharedTrxStatus>,
        snapshot_sts: TrxID,
    ) -> ExistingDeletion {
        let ts = existing.ts();
        if trx_is_committed(ts) {
            // A committed Ref is semantically identical to a compact marker
            // until a maintenance path promotes it.
            return Self::classify_committed(ts, snapshot_sts);
        }
        if Arc::ptr_eq(existing, status) {
            ExistingDeletion::Acquired
        } else {
            ExistingDeletion::ForeignActive
        }
    }

    #[inline]
    fn classify_committed(ts: TrxID, snapshot_sts: TrxID) -> ExistingDeletion {
        if ts <= snapshot_sts {
            ExistingDeletion::AlreadyDeleted
        } else {
            ExistingDeletion::WriteConflict
        }
    }

    #[inline]
    fn no_wait_result(classification: ExistingDeletion) -> Result<(), DeletionError> {
        match classification {
            ExistingDeletion::Acquired => Ok(()),
            ExistingDeletion::AlreadyDeleted => Err(DeletionError::AlreadyDeleted),
            ExistingDeletion::WriteConflict | ExistingDeletion::ForeignActive => {
                Err(DeletionError::WriteConflict)
            }
        }
    }

    #[inline]
    fn foreground_result(classification: ExistingDeletion) -> Result<DeletionClaim, DeletionError> {
        match classification {
            ExistingDeletion::Acquired => Ok(DeletionClaim::Acquired),
            ExistingDeletion::AlreadyDeleted => Err(DeletionError::AlreadyDeleted),
            ExistingDeletion::WriteConflict | ExistingDeletion::ForeignActive => {
                Err(DeletionError::WriteConflict)
            }
        }
    }

    #[inline]
    fn foreground_committed_result(
        ts: TrxID,
        snapshot_sts: TrxID,
    ) -> Option<Result<DeletionClaim, DeletionError>> {
        trx_is_committed(ts)
            .then(|| Self::foreground_result(Self::classify_committed(ts, snapshot_sts)))
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
    pub(crate) fn put_committed(&self, row_id: RowID, cts: TrxID) -> Result<(), DeletionError> {
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
    pub(crate) fn promote_delete_marker_if_committed(&self, row_id: RowID) -> bool {
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
    pub(crate) fn get(&self, row_id: RowID) -> Option<DeleteMarker> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    /// Returns whether a cold-row delete marker is safe for global physical
    /// purge at the supplied oldest active snapshot.
    ///
    /// A marker is purgeable only after its delete is committed and strictly
    /// older than `min_active_sts`. This proves every active or future
    /// transaction sees the cold row as deleted.
    #[inline]
    pub(crate) fn delete_marker_is_globally_purgeable(
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
    pub(crate) fn delete_marker_is_globally_purgeable_with<F>(
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
    pub(crate) fn collect_committed_in_range(
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
    pub(crate) fn remove(&self, row_id: RowID) {
        self.entries.remove(&row_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trx::tests::{
        commit_preparing_shared_trx_status, install_prepare_listener_before_lock_hook,
        prepare_event_is_installed, prepare_shared_trx_status,
        rollback_preparing_shared_trx_status, shared_trx_status,
    };
    use crate::trx::{MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread::spawn;

    #[test]
    fn test_delete_marker_is_globally_purgeable_with_is_lazy() {
        let buffer = ColumnDeletionBuffer::new();
        let calls = AtomicUsize::new(0);

        assert!(
            !buffer.delete_marker_is_globally_purgeable_with(RowID::new(1), || {
                calls.fetch_add(1, Ordering::SeqCst);
                TrxID::new(100)
            })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        buffer
            .put_ref(
                RowID::new(1),
                Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 1)),
                MAX_SNAPSHOT_TS,
            )
            .unwrap();
        assert!(
            !buffer.delete_marker_is_globally_purgeable_with(RowID::new(1), || {
                calls.fetch_add(1, Ordering::SeqCst);
                TrxID::new(100)
            })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        buffer.remove(RowID::new(1));
        buffer.put_committed(RowID::new(1), TrxID::new(10)).unwrap();
        assert!(
            !buffer.delete_marker_is_globally_purgeable_with(RowID::new(1), || {
                calls.fetch_add(1, Ordering::SeqCst);
                TrxID::new(10)
            })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert!(
            buffer.delete_marker_is_globally_purgeable_with(RowID::new(1), || {
                calls.fetch_add(1, Ordering::SeqCst);
                TrxID::new(11)
            })
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_put_ref_respects_snapshot_for_committed_markers() {
        let buffer = ColumnDeletionBuffer::new();
        let status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 1));

        buffer.put_committed(RowID::new(1), TrxID::new(20)).unwrap();
        assert_eq!(
            buffer.put_ref(RowID::new(1), status.clone(), TrxID::new(20)),
            Err(DeletionError::AlreadyDeleted)
        );
        assert_eq!(
            buffer.put_ref(RowID::new(1), status.clone(), TrxID::new(19)),
            Err(DeletionError::WriteConflict)
        );

        let committed_ref = Arc::new(shared_trx_status(TrxID::new(30)));
        buffer
            .put_ref(RowID::new(2), committed_ref, MAX_SNAPSHOT_TS)
            .unwrap();
        assert_eq!(
            buffer.put_ref(RowID::new(2), status.clone(), TrxID::new(30)),
            Err(DeletionError::AlreadyDeleted)
        );
        assert_eq!(
            buffer.put_ref(RowID::new(2), status, TrxID::new(29)),
            Err(DeletionError::WriteConflict)
        );
    }

    #[test]
    fn test_foreground_claim_waits_for_shared_preparing_owner() {
        let buffer = ColumnDeletionBuffer::new();
        let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 10));
        let first_waiter = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 11));
        let second_waiter = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 12));
        buffer
            .put_ref(RowID::new(1), Arc::clone(&owner), MAX_SNAPSHOT_TS)
            .unwrap();
        prepare_shared_trx_status(&owner);
        assert!(!prepare_event_is_installed(&owner));

        let first = match buffer
            .claim_ref(RowID::new(1), first_waiter, MAX_SNAPSHOT_TS)
            .unwrap()
        {
            DeletionClaim::Preparing(listener) => listener,
            _ => panic!("preparing owner should return an installed listener"),
        };
        assert!(prepare_event_is_installed(&owner));
        let second = match buffer
            .claim_ref(RowID::new(1), second_waiter, MAX_SNAPSHOT_TS)
            .unwrap()
        {
            DeletionClaim::Preparing(listener) => listener,
            _ => panic!("later waiter should reuse the installed listener"),
        };

        let cts = TrxID::new(40);
        commit_preparing_shared_trx_status(&owner, cts);
        first.wait_primary_for_test();
        second.wait_primary_for_test();
        assert!(!prepare_event_is_installed(&owner));
        let requester = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 13));
        assert!(matches!(
            buffer.claim_ref(RowID::new(1), requester, cts),
            Err(DeletionError::AlreadyDeleted)
        ));
    }

    #[test]
    fn test_no_wait_claim_does_not_inject_prepare_event() {
        let buffer = ColumnDeletionBuffer::new();
        let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 20));
        let requester = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 21));
        buffer
            .put_ref(RowID::new(1), Arc::clone(&owner), MAX_SNAPSHOT_TS)
            .unwrap();
        assert!(matches!(
            buffer.claim_ref(RowID::new(1), Arc::clone(&requester), MAX_SNAPSHOT_TS),
            Err(DeletionError::WriteConflict)
        ));
        assert!(
            !prepare_event_is_installed(&owner),
            "ordinary active foreground conflict must not install an event"
        );
        prepare_shared_trx_status(&owner);

        assert_eq!(
            buffer.put_ref(RowID::new(1), requester, MAX_SNAPSHOT_TS),
            Err(DeletionError::WriteConflict)
        );
        assert!(!prepare_event_is_installed(&owner));
        rollback_preparing_shared_trx_status(&owner);
    }

    #[test]
    fn test_rollback_marker_removal_does_not_deadlock_listener_registration() {
        let buffer = Arc::new(ColumnDeletionBuffer::new());
        let row_id = RowID::new(1);
        let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 30));
        buffer
            .put_ref(row_id, Arc::clone(&owner), MAX_SNAPSHOT_TS)
            .unwrap();
        prepare_shared_trx_status(&owner);

        let (loaded_tx, loaded_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let claimant_buffer = Arc::clone(&buffer);
        let claimant = spawn(move || {
            install_prepare_listener_before_lock_hook(move || {
                loaded_tx
                    .send(())
                    .expect("claimant should report its optimistic prepare load");
                release_rx
                    .recv()
                    .expect("claimant registration should be released");
            });
            let requester = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 31));
            let claim = claimant_buffer
                .claim_ref(row_id, requester, MAX_SNAPSHOT_TS)
                .expect("preparing owner should produce a foreground wait");
            let DeletionClaim::Preparing(listener) = claim else {
                panic!("claimant should install the shared prepare listener")
            };
            listener.wait_primary_for_test();
        });

        loaded_rx
            .recv()
            .expect("claimant should hold the CDB entry before prepare registration");
        let (rollback_tx, rollback_rx) = mpsc::channel();
        let rollback_buffer = Arc::clone(&buffer);
        let rollback_owner = Arc::clone(&owner);
        let rollback = spawn(move || {
            rollback_tx
                .send(())
                .expect("rollback should report marker-removal attempt");
            rollback_buffer.remove(row_id);
            rollback_preparing_shared_trx_status(&rollback_owner);
        });
        rollback_rx
            .recv()
            .expect("rollback should start in production lock order");
        release_tx
            .send(())
            .expect("claimant registration should resume");

        claimant.join().expect("claimant should wake");
        rollback.join().expect("rollback should finish");
        assert!(buffer.get(row_id).is_none());
        assert!(!prepare_event_is_installed(&owner));
    }
}
