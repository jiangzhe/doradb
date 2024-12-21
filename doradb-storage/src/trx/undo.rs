use crate::buffer::page::PageID;
use crate::row::ops::UpdateCol;
use crate::row::RowID;
use crate::table::TableID;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

/// UndoMap is page level hash map to store undo chain of rows.
/// If UndoMap is empty, this page has all data visible to all transactions.
pub type UndoMap = HashMap<RowID, SharedUndoEntry>;

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
    /// 4. Delete -> Move.
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

/// SharedUndoEntry is a reference-counted pointer to UndoEntry.
/// The transaction generates undo log, and page-level undo
/// map will also hold its shared copy to track all visible
/// versions of modified rows.
pub type SharedUndoEntry = Arc<UndoEntry>;

/// PrevUndoEntry is used for garbage collector to unlink its
/// previous entry to itself.
pub type PrevUndoEntry = Weak<UndoEntry>;

pub struct UndoEntry {
    /// This field stores uncommitted TrxID, or committed timestamp.
    /// It uses shared pointer and atomic variable to support
    /// fast backfill.
    pub ts: Arc<AtomicU64>,
    pub table_id: TableID,
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: UndoKind,
    pub chain: Mutex<UndoChain>,
}

/// UndoChain stores prev chain, and next chain.
/// Prev chain is used for garbage collection.
/// Next chain is used for visibility check.
#[derive(Default)]
pub struct UndoChain {
    /// Pointer to the newer version.
    pub prev: Option<PrevUndoEntry>,
    /// Pointer to the older version.
    pub next: Option<SharedUndoEntry>,
}
