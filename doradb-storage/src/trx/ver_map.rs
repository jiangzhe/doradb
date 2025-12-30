use crate::catalog::TableMetadata;
use crate::trx::TrxID;
use crate::trx::undo::RowUndoHead;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// RowVersionMap is a page-level hash map to store
/// old versions of rows in row page.
/// It also contains modification counter and max STS
/// to speed up table scan with MVCC.
pub struct RowVersionMap {
    // Fixed size array to store undo chains.
    // It wastes 16 bytes(one lock and one pointer)
    // for each row if no undo associated.
    entries: Box<[RwLock<Option<Box<RowUndoHead>>>]>,
    // Metadata fixed when the version map is created.
    // Currently it should be same as metadata of table
    // file because we do not support change of table
    // structure.
    pub metadata: Arc<TableMetadata>,
    // Monotonically increasing version number.
    // indicates whether the undo map is changed.
    mod_counter: AtomicU64,
    // Maximum STS of transactions that have modified
    // current page.
    // This is used to bypass version chain traversal
    // for fast table scan.
    // map.len()==0 is more strict than trx.sts > max_sts,
    // because it depends on efficient GC to remove undo
    // logs as fast as possible.
    //
    // The implementation uses guard to automatically maintain
    // this field. There might be some cases that this field
    // does not reflect the actual status and precent fast scan
    // by mistake, e.g. a transaction modifies one row, then
    // rollback.
    max_sts: AtomicU64,
}

impl RowVersionMap {
    /// Create a new version map.
    #[inline]
    pub fn new(metadata: Arc<TableMetadata>, max_size: usize) -> Self {
        let vec: Vec<_> = (0..max_size).map(|_| RwLock::new(None)).collect();
        RowVersionMap {
            entries: vec.into_boxed_slice(),
            metadata,
            mod_counter: AtomicU64::new(0),
            max_sts: AtomicU64::new(0),
        }
    }

    /// Acquire a read latch on given row.
    #[inline]
    pub fn read_latch(&self, row_idx: usize) -> RowVersionReadGuard<'_> {
        let g = self.entries[row_idx].read();
        RowVersionReadGuard { m: self, g }
    }

    /// Acquire a write latch on given row.
    #[inline]
    pub fn write_latch(&self, row_idx: usize, sts: Option<TrxID>) -> RowVersionWriteGuard<'_> {
        let g = self.entries[row_idx].write();
        // If STS is not provided, there should be no modification
        // on current page, so we don't need to bump the counter.
        // Currently this is invoked for purge(GC) thread.
        if sts.is_some() {
            self.mod_counter.fetch_add(1, Ordering::AcqRel);
        }
        RowVersionWriteGuard { m: self, g, sts }
    }

    /// Acquire exclusive latch as self is exclusive.
    #[inline]
    pub fn write_exclusive(&mut self, row_idx: usize) -> &mut Option<Box<RowUndoHead>> {
        self.entries[row_idx].get_mut()
    }
}

pub struct RowVersionReadGuard<'a> {
    m: &'a RowVersionMap,
    g: RwLockReadGuard<'a, Option<Box<RowUndoHead>>>,
}

impl<'a> Deref for RowVersionReadGuard<'a> {
    type Target = Option<Box<RowUndoHead>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

pub struct RowVersionWriteGuard<'a> {
    m: &'a RowVersionMap,
    g: RwLockWriteGuard<'a, Option<Box<RowUndoHead>>>,
    sts: Option<TrxID>,
}

impl<'a> Drop for RowVersionWriteGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        // Only update sts and counter for user transaction.
        if let Some(sts) = self.sts {
            // First we update max sts of this map.
            self.m.max_sts.fetch_max(sts, Ordering::AcqRel);
            // Then we bump the modification counter.
            self.m.mod_counter.fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl<'a> Deref for RowVersionWriteGuard<'a> {
    type Target = Option<Box<RowUndoHead>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.g
    }
}

impl<'a> DerefMut for RowVersionWriteGuard<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.g
    }
}
