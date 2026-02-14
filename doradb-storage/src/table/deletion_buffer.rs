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
    pub fn promote_ref_to_committed(
        &self,
        row_id: RowID,
        expected_ref: &Arc<SharedTrxStatus>,
        cts: TrxID,
    ) -> bool {
        let Some(mut entry) = self.entries.get_mut(&row_id) else {
            return false;
        };
        let DeleteMarker::Ref(current_ref) = &*entry else {
            return false;
        };
        if !Arc::ptr_eq(current_ref, expected_ref) {
            return false;
        }
        *entry = DeleteMarker::Committed(cts);
        true
    }

    #[inline]
    pub fn get(&self, row_id: RowID) -> Option<DeleteMarker> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    #[inline]
    pub fn remove(&self, row_id: RowID) {
        self.entries.remove(&row_id);
    }
}
