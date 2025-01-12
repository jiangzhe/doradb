use crate::buffer::page::PageID;
use crate::row::ops::UpdateCol;
use crate::row::RowID;
use crate::table::TableID;
use crate::trx::{SharedTrxStatus, TrxID, GLOBAL_VISIBLE_COMMIT_TS};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub struct UndoMap {
    entries: Box<[RwLock<Option<UndoHead>>]>,
    // occupied: usize,
}

impl UndoMap {
    #[inline]
    pub fn new(len: usize) -> Self {
        let vec: Vec<_> = (0..len).map(|_| RwLock::new(None)).collect();
        UndoMap {
            entries: vec.into_boxed_slice(),
            // occupied: 0,
        }
    }

    #[inline]
    pub fn occupied(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| if entry.read().is_some() { 1 } else { 0 })
            .sum()
    }

    #[inline]
    pub fn read(&self, row_idx: usize) -> RwLockReadGuard<'_, Option<UndoHead>> {
        self.entries[row_idx].read()
    }

    #[inline]
    pub fn write(&self, row_idx: usize) -> RwLockWriteGuard<'_, Option<UndoHead>> {
        self.entries[row_idx].write()
    }

    #[inline]
    pub fn ptr(&self, row_idx: usize) -> UndoHeadPtr {
        unsafe {
            let ptr = &self.entries[row_idx] as *const _ as *mut _;
            UndoHeadPtr(NonNull::new_unchecked(ptr))
        }
    }
}

/// UndoKind represents the kind of original operation.
/// So the actual undo action should be opposite of the kind.
/// There is one special UndoKind *Move*, due to the design of DoraDB.
pub enum UndoKind {
    /// Insert a new row.
    /// Before-image is empty for insert, so we do not need to copy values.
    ///
    /// # Possbile chains:
    ///
    /// 1. Insert -> null.
    ///
    /// This is common scenario. The insert is first version of a row. And it
    /// does not have older(next) version.
    Insert,
    /// Move is a special kind.
    ///
    /// It's not directly mapping to a user operation, e.g insert, update, delete.
    /// But it's a internal action which is triggered by user operation.
    ///
    /// # Scenarios:
    ///
    /// 1. insert into a table with unique constraint(primary key or unique key).
    ///
    /// A row with original key is deleted and then new row with the same key is
    /// inserted.
    /// Our index design is to make all secondary indexes to point to newest version.
    /// Therefore, we need to build a version chain from the insert log to delete log.
    ///
    /// For example, old(deleted) row has RowID 100, and new(inserted) row has
    /// RowID 200. they have KeyColumn with same value(k=1).
    /// In such case, for deleted row, we have a secondary index with an entry
    /// (key=1 => RowID=100).
    /// To insert the new row, we first perform the insertion directly on data
    /// page, and get new row inserted(RowID=200). It's necessary because RowID should
    /// be first generated before updating index.
    /// Then we try to update secondary index, and find there is already an entry,
    /// but the entry is deleted(otherwrise, we will fail with an error of duplicate
    /// key and rollback the insert).
    /// So we lock the deleted row and add undo entry *Move* at the head of its undo
    /// chain, then link new row's undo log head to "Move" entry.
    /// Finally, we update index to point k=1 to RowID=200.
    /// This is safe because we already locked the deleted row. Any concurrent operation
    /// will see original row locked, and wait or die according to concurrency control
    /// protocol.
    ///
    /// If we perform index lookup, we always land at the newest version.
    /// And then go through the version chain to locate visible version.
    /// A key validation is also required because key column might be changed and
    /// two key index entries pointing to the same new version. MVCC visible check must
    /// ensure key column matches the visible version built from version chain.
    ///
    /// If we scan the table(skipping secondary index), we may find deleted rows in
    /// data page with undo header of MOVE entry. We just eliminate such rows, because
    /// we can always find it again from other page through the complete undo chain.
    ///  
    /// 2. fail to in-place update.
    ///
    /// This can happen when data page's free space is not enough for the update.
    /// The original row is moved to a new page, so should be marked as *MOVE*d.
    ///
    /// 3. update on rows in a freezed row page or column-store.
    ///
    /// As DoraDB supports integrated column-store and row-store. To convert row page
    /// to column file, we need to freeze the pages.
    /// The fronzed pages does not support insert, delete or update, but it supports
    /// move rows to new pages.
    ///
    /// # Possible chains:
    ///
    /// 1. Move -> Insert.
    ///
    /// 2. Move -> Update.
    ///
    /// 3. Move -> Delete.
    ///
    /// 4. Move -> null.
    ///
    /// Note: Move always marks the row in data page as deleted. so we need to record
    /// delete flag of previous version. And undo if necessary.
    Move(bool),
    /// Delete an existing row.
    /// We optimize to not have row values in delete log entry, because we always can
    /// find the version in row page.
    ///
    /// Possible chains:
    ///
    /// 1. Delete -> null.
    ///
    /// It can happen when GC is executed and the insert transaction is cleaned.
    /// This means if we can not see the delete version, we should unmark latest
    /// version in data page.
    ///
    /// 2. Delete -> Insert.
    ///
    /// 3. Delete -> Update.
    ///
    Delete,
    /// Copy old versions of updated columns.
    ///
    /// Possible chains:
    ///
    /// 1. Update -> null.
    ///
    /// 2. Update -> Insert.
    ///
    /// 3. Update -> Update.
    ///
    /// 4. Update -> Delete.
    ///
    /// Dervied from an insert operation.
    /// We'd like to reuse the deleted row(RowID and data) and link
    /// update(instead of insert) entry to it.
    /// In this way, we may not need to change secondary index.
    ///
    /// 4. Update -> Move.
    ///
    /// Note: Update -> Delete is impossible. Even if we re-insert
    /// a deleted row, we will first *move* the deleted row to
    /// other place and then perform update.
    Update(Vec<UpdateCol>),
}

/// The transaction generates undo log, and page-level undo
/// map will also hold its shared copy to track all visible
/// versions of modified rows.
pub struct SharedUndoEntry(UndoEntryPtr);

unsafe impl Send for SharedUndoEntry {}

impl Deref for SharedUndoEntry {
    type Target = UndoEntryPtr;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SharedUndoEntry {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl SharedUndoEntry {
    #[inline]
    pub fn new(table_id: TableID, page_id: PageID, row_id: RowID, kind: UndoKind) -> Self {
        let entry = UndoEntry {
            table_id,
            page_id,
            row_id,
            kind,
            chain: RwLock::new(UndoChain {
                prev: None,
                next: None,
            }),
            ref_count: AtomicU32::new(1),
        };
        let ptr = Box::leak(Box::new(entry));
        SharedUndoEntry(UndoEntryPtr(unsafe { NonNull::new_unchecked(ptr) }))
    }

    #[inline]
    pub fn clone(this: &SharedUndoEntry) -> SharedUndoEntry {
        this.as_ref().ref_count.fetch_add(1, Ordering::Relaxed);
        SharedUndoEntry(this.leak())
    }
}

impl Drop for SharedUndoEntry {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let ref_count = self.as_ref().ref_count.fetch_sub(1, Ordering::Relaxed);
            if ref_count == 1 {
                // last referenece, call destructor now
                drop(Box::from_raw(self.as_mut()));
            }
        }
    }
}

/// UndoEntryPtr is an atomic pointer to UndoEntry.
#[repr(transparent)]
#[derive(Clone)]
pub struct UndoEntryPtr(NonNull<UndoEntry>);

impl UndoEntryPtr {
    #[inline]
    pub(crate) fn as_ref(&self) -> &UndoEntry {
        unsafe { self.0.as_ref() }
    }

    #[inline]
    pub(crate) unsafe fn as_mut(&mut self) -> &mut UndoEntry {
        self.0.as_mut()
    }

    /// Leak a not reference counted pointer of the undo entry.
    /// Caller must make sure it will not outlive the entry.
    ///
    /// This is used in MVCC forward processing.
    /// We only count the pointers from old version to new version
    /// to enable fast GC processing.
    /// The validity of pointer lifetime is guaranteed by GC logic.
    #[inline]
    pub fn leak(&self) -> Self {
        UndoEntryPtr(self.0)
    }
}

pub struct UndoEntry {
    /// This field stores uncommitted TrxID, committed timestamp.
    /// Or preparing status, which may block read.
    /// It uses shared pointer and atomic variable to support
    /// fast backfill.
    // pub status: Arc<SharedTrxStatus>,
    pub table_id: TableID,
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: UndoKind,
    pub chain: RwLock<UndoChain>,
    ref_count: AtomicU32,
}

/// UndoChain stores prev pointer, next pointer, next status, and current ref count.
/// Prev chain is used for garbage collection.
/// Next chain is used for visibility check.
pub struct UndoChain {
    /// Pointer to the newer version.
    /// This pointer is reference counted.
    pub prev: Option<PrevUndoEntry>,
    /// Status of older version and pointer to the older version.
    pub next: Option<NextUndoEntry>,
}

pub enum PrevUndoEntry {
    Head(UndoHeadPtr),
    Entry(SharedUndoEntry),
}

pub struct NextUndoEntry {
    pub status: NextUndoStatus,
    pub entry: UndoEntryPtr,
}

pub enum NextUndoStatus {
    // If transaction modify a row multiple times.
    // It will link multiple undo entries with the
    // same timestamp.
    // One optimization is to compact such entries.
    // In another way, we only keep the timestmap
    // on top of the entry, and mark other entries
    // as SameAsPrev.
    SameAsPrev,
    CTS(TrxID),
}

pub struct UndoHead {
    pub status: Arc<SharedTrxStatus>,
    pub entry: Option<UndoEntryPtr>,
}

/// A not reference counted pointer to undo head.
/// Because all undo heads are stored in continuous memory
/// area allocated in heap and no re-allocation is allowed.
/// So we can make sure pointer is always valid.
#[derive(Clone)]
pub struct UndoHeadPtr(NonNull<RwLock<Option<UndoHead>>>);

impl UndoHeadPtr {
    #[inline]
    pub fn as_ref(&self) -> &RwLock<Option<UndoHead>> {
        unsafe { self.0.as_ref() }
    }
}

#[derive(Default, Clone, Copy)]
pub enum NextTrxCTS {
    #[default]
    None,
    Value(TrxID),
    Myself,
}

impl NextTrxCTS {
    #[inline]
    pub fn undo_status(self) -> NextUndoStatus {
        match self {
            NextTrxCTS::None => NextUndoStatus::CTS(GLOBAL_VISIBLE_COMMIT_TS),
            NextTrxCTS::Value(cts) => NextUndoStatus::CTS(cts),
            NextTrxCTS::Myself => NextUndoStatus::SameAsPrev,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_head_size() {
        println!(
            "size of RwLock<Option<UndoHead>> is {}",
            std::mem::size_of::<RwLock<Option<UndoHead>>>()
        );
    }
}
