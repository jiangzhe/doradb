use crate::catalog::TableMetadata;
use crate::trx::TrxID;
use crate::trx::undo::RowUndoHead;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
    // Commit timestamp when this row page is created.
    create_cts: AtomicU64,
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
    // Maximum STS of transactions that has insertion or update
    // on current page. This can speed up version check on
    // frozen pages, if `global_min_active_sts > max_ins_sts`,
    // we can make sure the content of row page can be safely
    // transformed to LWC block.
    max_ins_sts: AtomicU64,
    // Frozen flag indicates the row page is frozen and no
    // more insert or update can be applied to it.
    // No lock is used because frozen is just a transitional
    // period to restrict incoming transactions.
    frozen: AtomicBool,
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
            create_cts: AtomicU64::new(0),
            max_sts: AtomicU64::new(0),
            max_ins_sts: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
        }
    }

    /// Returns whether this page is frozen.
    #[inline]
    pub fn frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    /// Freeze current row page.
    #[inline]
    pub fn set_frozen(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    /// Set commit timestamp of page creation.
    #[inline]
    pub fn set_create_cts(&self, cts: TrxID) {
        self.create_cts.store(cts, Ordering::Release);
    }

    /// Returns commit timestamp of page creation.
    #[inline]
    pub fn create_cts(&self) -> TrxID {
        self.create_cts.load(Ordering::Acquire)
    }

    /// Acquire a read latch on given row.
    #[inline]
    pub fn read_latch(&self, row_idx: usize) -> RowVersionReadGuard<'_> {
        let g = self.entries[row_idx].read();
        RowVersionReadGuard { m: self, g }
    }

    /// Acquire a write latch on given row.
    #[inline]
    pub fn write_latch(
        &self,
        row_idx: usize,
        sts: Option<TrxID>,
        ins_or_update: bool,
    ) -> RowVersionWriteGuard<'_> {
        let g = self.entries[row_idx].write();
        // If STS is not provided, there should be no modification
        // on current page, so we don't need to bump the counter.
        // Currently this is invoked for purge(GC) thread.
        if sts.is_some() {
            self.mod_counter.fetch_add(1, Ordering::AcqRel);
        }
        RowVersionWriteGuard {
            m: self,
            g,
            sts,
            ins_or_update,
        }
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
    ins_or_update: bool,
}

impl RowVersionWriteGuard<'_> {
    #[inline]
    pub fn enable_ins_or_update(&mut self) {
        self.ins_or_update = true;
    }
}

impl<'a> Drop for RowVersionWriteGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        // Only update sts and counter for user transaction.
        if let Some(sts) = self.sts {
            // First we update max sts of this map.
            self.m.max_sts.fetch_max(sts, Ordering::Relaxed);
            // Then we bump the modification counter.
            self.m.mod_counter.fetch_add(1, Ordering::AcqRel);
            if self.ins_or_update {
                self.m.max_ins_sts.fetch_max(sts, Ordering::Relaxed);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableMetadata;
    use crate::catalog::spec::{ColumnAttributes, ColumnSpec, IndexSpec};
    use crate::value::ValKind;

    #[test]
    fn test_row_version_map_create_cts() {
        let metadata = TableMetadata::new(
            vec![ColumnSpec::new(
                "id",
                ValKind::I64,
                ColumnAttributes::empty(),
            )],
            Vec::<IndexSpec>::new(),
        );
        let map = RowVersionMap::new(Arc::new(metadata), 1);
        assert_eq!(map.create_cts(), 0);
        map.set_create_cts(42);
        assert_eq!(map.create_cts(), 42);
    }
}
