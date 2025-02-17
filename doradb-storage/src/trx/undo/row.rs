use crate::buffer::guard::PageGuard;
use crate::buffer::page::PageID;
use crate::buffer::BufferPool;
use crate::latch::LatchFallbackMode;
use crate::row::ops::SelectKey;
use crate::row::ops::UndoCol;
use crate::row::{RowID, RowPage};
use crate::table::TableID;
use crate::trx::{SharedTrxStatus, TrxID, GLOBAL_VISIBLE_COMMIT_TS};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

pub struct UndoMap {
    entries: Box<[RwLock<Option<RowUndoHead>>]>,
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
    pub fn read(&self, row_idx: usize) -> RwLockReadGuard<'_, Option<RowUndoHead>> {
        self.entries[row_idx].read()
    }

    #[inline]
    pub fn write(&self, row_idx: usize) -> RwLockWriteGuard<'_, Option<RowUndoHead>> {
        self.entries[row_idx].write()
    }
}

/// RowUndoKind represents the kind of original operation.
/// So the actual undo action should be opposite of the kind.
/// There is one special UndoKind *Move*, due to the design of DoraDB.
pub enum RowUndoKind {
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
    /// If we scan the table(skipping secondary index), we can find deleted row before
    /// MOVE and new row after MOVE. In case of visiting MOVE entry on old page,
    /// if current transaction is newer, we do not undo MOVE, and deleted flag on page
    /// is true - as it's *moved*, so we don't see the data. In case of visiting MOVE
    /// entry in version chain from new page. we have to stop the version traversal,
    /// in order to avoid double read(see the old version twice).
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
    Update(Vec<UndoCol>),
}

/// RowUndoLogs is a collection of row undo logs.
/// It owns the logs until GC clean them all at transaction level.
#[derive(Default)]
pub struct RowUndoLogs(Vec<OwnedRowUndo>);

impl RowUndoLogs {
    #[inline]
    pub fn empty() -> Self {
        RowUndoLogs(vec![])
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn push(&mut self, value: OwnedRowUndo) {
        self.0.push(value)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<OwnedRowUndo> {
        self.0.pop()
    }

    #[inline]
    pub fn merge(&mut self, other: &mut Self) {
        self.0.extend(other.0.drain(..));
    }

    #[inline]
    pub fn rollback<P: BufferPool>(&mut self, buf_pool: P) {
        while let Some(entry) = self.0.pop() {
            let page_guard: PageGuard<'_, RowPage> =
                buf_pool.get_page(entry.page_id, LatchFallbackMode::Shared);
            let page_guard = page_guard.block_until_shared();
            let row_idx = page_guard.page().row_idx(entry.row_id);
            let mut access = page_guard.write_row(row_idx);
            access.rollback_first_undo(entry);
        }
    }
}

/// OwnedRowUndo is the old version of a row.
/// It is stored in transaction undo buffer.
/// Page level undo map will also hold pointers to the entries.
/// We do not share ownership between them.
/// Instead, we require the undo buffer owns all entries.
/// Garbage collector will make sure the deletion of entries is
/// safe, because no transaction will access entries that is
/// supposed to be deleted.
pub struct OwnedRowUndo(Box<RowUndo>);

impl Deref for OwnedRowUndo {
    type Target = RowUndo;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl DerefMut for OwnedRowUndo {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl OwnedRowUndo {
    #[inline]
    pub fn new(table_id: TableID, page_id: PageID, row_id: RowID, kind: RowUndoKind) -> Self {
        let entry = RowUndo {
            table_id,
            page_id,
            row_id,
            kind,
            next: vec![],
        };
        OwnedRowUndo(Box::new(entry))
    }

    #[inline]
    pub fn leak(&self) -> RowUndoRef {
        unsafe {
            RowUndoRef(NonNull::new_unchecked(
                self.0.as_ref() as *const _ as *mut RowUndo
            ))
        }
    }
}

/// RowUndoRef is a reference to RowUndoEntry.
/// It does not share ownership with RowUndoEntry.
///
/// The safety is guaranteed by MVCC design and GC logic.
/// The modification of undo log is always guarded by row lock.
/// And the non-locking consistent read will not access
/// log entries that are deleted(GCed).
#[repr(transparent)]
#[derive(Clone)]
pub struct RowUndoRef(NonNull<RowUndo>);

unsafe impl Send for RowUndoRef {}
unsafe impl Sync for RowUndoRef {}

impl RowUndoRef {
    #[inline]
    pub(crate) fn as_ref(&self) -> &RowUndo {
        unsafe { self.0.as_ref() }
    }
}

pub struct RowUndo {
    pub table_id: TableID,
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: RowUndoKind,
    pub next: Vec<NextRowUndo>,
}

impl RowUndo {
    #[inline]
    pub fn find_next_version(&self, search_key: &SelectKey) -> Option<&NextRowUndo> {
        if self.next.is_empty() {
            return None;
        }
        // prefer key match, otherwise, choose main
        let mut main = None;
        for next in &self.next {
            match &next.branch {
                RowUndoBranch::Main => {
                    main = Some(next);
                }
                RowUndoBranch::Reborn(key) => {
                    if key == search_key {
                        return Some(next);
                    }
                }
            }
        }
        main
    }

    #[inline]
    pub fn remove_next_main_version(&mut self) -> Option<NextRowUndo> {
        if let Some(pos) = self
            .next
            .iter()
            .position(|next| matches!(next.branch, RowUndoBranch::Main))
        {
            return Some(self.next.swap_remove(pos));
        }
        None
    }
}

pub struct NextRowUndo {
    pub branch: RowUndoBranch,
    pub status: NextRowUndoStatus,
    pub entry: RowUndoRef,
}

#[derive(Clone)]
pub enum RowUndoBranch {
    /// Main branch starts from insertion and ends at deletion.
    Main,
    /// Reborn branch is created once an insertion with a key which
    /// is shared by a deleted row on a unique index.
    /// To make index uniqueness, we point the index entry to latest
    /// insert version and link new version to deleted version.
    /// That is the meaning of reborn(of the dead version).
    ///
    /// MVCC read can skip this branch if the index key provided for
    /// search is not same as the reborn key.
    /// Because only such key should be searched in the reborn branch.
    Reborn(SelectKey),
}

impl From<Option<SelectKey>> for RowUndoBranch {
    #[inline]
    fn from(value: Option<SelectKey>) -> Self {
        match value {
            Some(key) => RowUndoBranch::Reborn(key),
            None => RowUndoBranch::Main,
        }
    }
}

pub enum NextRowUndoStatus {
    // If transaction modify a row multiple times.
    // It will link multiple undo entries with the
    // same timestamp.
    // One optimization is to compact such entries.
    // In another way, we only keep the timestamp
    // on top of the entry, and mark other entries
    // as SameAsPrev.
    SameAsPrev,
    CTS(TrxID),
}

pub struct RowUndoHead {
    pub status: Arc<SharedTrxStatus>,
    pub entry: Option<RowUndoRef>,
}

unsafe impl Send for RowUndoHead {}

#[derive(Default, Clone, Copy)]
pub enum NextTrxCTS {
    #[default]
    None,
    Value(TrxID),
    Myself,
}

impl NextTrxCTS {
    #[inline]
    pub fn undo_status(self) -> NextRowUndoStatus {
        match self {
            NextTrxCTS::None => NextRowUndoStatus::CTS(GLOBAL_VISIBLE_COMMIT_TS),
            NextTrxCTS::Value(cts) => NextRowUndoStatus::CTS(cts),
            NextTrxCTS::Myself => NextRowUndoStatus::SameAsPrev,
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
            std::mem::size_of::<RwLock<Option<RowUndoHead>>>()
        );
    }
}
