use crate::row::RowID;
use crate::trx::{SharedTrxStatus, trx_is_committed};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionError {
    WriteConflict,
    AlreadyDeleted,
}

pub struct ColumnDeletionBuffer {
    entries: DashMap<RowID, Arc<SharedTrxStatus>>,
}

impl ColumnDeletionBuffer {
    #[inline]
    pub fn new() -> Self {
        ColumnDeletionBuffer {
            entries: DashMap::new(),
        }
    }

    #[inline]
    pub fn put(
        &self,
        row_id: RowID,
        status: Arc<SharedTrxStatus>,
    ) -> Result<(), DeletionError> {
        match self.entries.entry(row_id) {
            Entry::Occupied(entry) => {
                let existing = entry.get();
                let ts = existing.ts();
                if trx_is_committed(ts) {
                    return Err(DeletionError::AlreadyDeleted);
                }
                if Arc::ptr_eq(existing, &status) {
                    return Ok(());
                }
                Err(DeletionError::WriteConflict)
            }
            Entry::Vacant(entry) => {
                entry.insert(status);
                Ok(())
            }
        }
    }

    #[inline]
    pub fn get(&self, row_id: RowID) -> Option<Arc<SharedTrxStatus>> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    #[inline]
    pub fn remove(&self, row_id: RowID) {
        self.entries.remove(&row_id);
    }
}
