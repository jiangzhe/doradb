use crate::row::RowID;
use crate::trx::{SharedTrxStatus, TrxID, trx_is_committed};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionError {
    WriteConflict,
    AlreadyDeleted,
}

#[derive(Clone)]
pub enum DeleteMarker {
    Ref(Arc<SharedTrxStatus>),
    Committed(TrxID),
}

pub struct ColumnDeletionBuffer {
    entries: DashMap<RowID, DeleteMarker>,
}

impl ColumnDeletionBuffer {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        ColumnDeletionBuffer {
            entries: DashMap::new(),
        }
    }

    #[inline]
    pub fn put_ref(
        &self,
        row_id: RowID,
        status: Arc<SharedTrxStatus>,
    ) -> Result<(), DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(entry) => match entry.get() {
                DeleteMarker::Ref(existing) => {
                    let ts = existing.ts();
                    if trx_is_committed(ts) {
                        return Err(DeletionError::AlreadyDeleted);
                    }
                    if Arc::ptr_eq(existing, &status) {
                        return Ok(());
                    }
                    Err(DeletionError::WriteConflict)
                }
                DeleteMarker::Committed(_) => Err(DeletionError::AlreadyDeleted),
            },
            Entry::Vacant(entry) => {
                entry.insert(DeleteMarker::Ref(status));
                Ok(())
            }
        }
    }

    #[inline]
    pub fn put_committed(&self, row_id: RowID, cts: TrxID) -> Result<(), DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(mut entry) => match entry.get() {
                DeleteMarker::Ref(status) => {
                    let ts = status.ts();
                    if !trx_is_committed(ts) {
                        return Err(DeletionError::WriteConflict);
                    }
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

    #[inline]
    pub fn get(&self, row_id: RowID) -> Option<DeleteMarker> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    #[inline]
    pub fn delete_marker_is_globally_purgeable(
        &self,
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> bool {
        self.delete_marker_is_globally_purgeable_with(row_id, || min_active_sts)
    }

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
                    return false;
                }
                ts
            }
        };
        delete_cts < min_active_sts()
    }

    /// Snapshot row ids whose delete marker is committed and
    /// `previous_cutoff <= cts < current_cutoff`.
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

    #[inline]
    pub fn remove(&self, row_id: RowID) {
        self.entries.remove(&row_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trx::MIN_ACTIVE_TRX_ID;
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
            .put_ref(1, Arc::new(SharedTrxStatus::new(MIN_ACTIVE_TRX_ID + 1)))
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
}
