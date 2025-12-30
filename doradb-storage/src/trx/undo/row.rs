use crate::buffer::BufferPool;
use crate::buffer::page::PageID;
use crate::catalog::TableID;
use crate::latch::LatchFallbackMode;
use crate::row::ops::{SelectKey, UndoCol, UpdateCol};
use crate::row::{RowID, RowPage};
use crate::trx::row::RowWriteAccess;
use crate::trx::{MIN_SNAPSHOT_TS, SharedTrxStatus, TrxID, trx_is_committed};
use event_listener::EventListener;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

/// RowUndoKind represents the kind of original operation.
/// So the actual undo action should be opposite of the kind.
/// There is one special UndoKind *Move*, due to the design of DoraDB.
pub enum RowUndoKind {
    /// Lock a row.
    /// Before insert/delete/update, we add a lock record to version chain
    /// to indicate other transactions that the row is locked by someone.
    /// After done the change, update the LOCK kind to specific kind.
    Lock,
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

impl fmt::Debug for RowUndoKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowUndoKind::Delete => f.pad("Delete"),
            RowUndoKind::Insert => f.pad("Insert"),
            RowUndoKind::Lock => f.pad("Lock"),
            RowUndoKind::Move(..) => f.pad("Move"),
            RowUndoKind::Update(_) => f.pad("Update"),
        }
    }
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
    pub fn push(&mut self, value: OwnedRowUndo) {
        self.0.push(value)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<OwnedRowUndo> {
        self.0.pop()
    }

    #[inline]
    pub fn merge(&mut self, other: &mut Self) {
        self.0.append(&mut other.0);
    }

    #[inline]
    pub async fn rollback<P: BufferPool>(&mut self, buf_pool: &'static P, sts: Option<TrxID>) {
        while let Some(entry) = self.0.pop() {
            let page_guard = buf_pool
                .get_page::<RowPage>(entry.page_id, LatchFallbackMode::Shared)
                .await
                .shared_async()
                .await;
            let (ctx, page) = page_guard.ctx_and_page();
            let metadata = &*ctx.row_ver().unwrap().metadata;
            let row_idx = page.row_idx(entry.row_id);
            let mut access = RowWriteAccess::new(page, ctx, row_idx, sts);
            access.rollback_first_undo(metadata, entry);
        }
    }
}

impl Deref for RowUndoLogs {
    type Target = [OwnedRowUndo];
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RowUndoLogs {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
        &self.0
    }
}

impl DerefMut for OwnedRowUndo {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
            next: None,
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
/// The modification of undo log is always guarded by row lock,
/// including GC operation.
/// And the non-locking consistent read will not access
/// log entries that are deleted(GCed).
#[repr(transparent)]
pub struct RowUndoRef(NonNull<RowUndo>);

unsafe impl Send for RowUndoRef {}
unsafe impl Sync for RowUndoRef {}

impl RowUndoRef {
    /// Returns reference of underlying undo log.
    /// This method is safe because GC operation always clear this reference
    /// from next undo list.
    /// So we won't have chance to access a deleted undo log.
    #[inline]
    pub(crate) fn as_ref(&self) -> &RowUndo {
        unsafe { self.0.as_ref() }
    }

    /// Returns mutable reference of underlying undo log.
    ///
    /// The caller must guarantee there is no other thread to modify it
    /// concurrently.
    /// The current design is to only allow calling this method in GC process.
    /// And only one thread can write because row lock is required before
    /// access the version chain.
    #[inline]
    pub(crate) fn as_mut(&mut self) -> &mut RowUndo {
        unsafe { self.0.as_mut() }
    }
}

impl Clone for RowUndoRef {
    #[inline]
    fn clone(&self) -> Self {
        RowUndoRef(unsafe { NonNull::new_unchecked(self.0.as_ptr()) })
    }
}

pub struct RowUndo {
    pub table_id: TableID,
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: RowUndoKind,
    pub next: Option<NextRowUndo>,
}

/// NextRowUndo stores status and reference of next undo log.
/// main branch is its own lifecycle.
/// index branches contains links to versions of another row
/// with same unique key.
///
/// Timestamp of Main branch is always larger than those of indexes,
/// because the link is generated when main is uncommitted but
/// index is committed.
pub struct NextRowUndo {
    pub main: MainBranch,
    pub indexes: Vec<IndexBranch>,
}

impl NextRowUndo {
    /// Create a new next undo with only main branch.
    #[inline]
    pub fn new(main: MainBranch) -> Self {
        NextRowUndo {
            main,
            indexes: vec![],
        }
    }

    /// Returns next index branch.
    #[inline]
    pub fn index_branch(&self, key: Option<&SelectKey>) -> Option<&IndexBranch> {
        key.and_then(|k| self.indexes.iter().find(|&ib| &ib.key == k))
    }
}

/// Main branch starts from insertion and ends at deletion.
pub struct MainBranch {
    pub entry: RowUndoRef,
    pub status: UndoStatus,
}

/// UndoStatus represents status of any undo log,
/// including uncommitted transactions.
pub enum UndoStatus {
    Ref(Arc<SharedTrxStatus>),
    CTS(TrxID),
}

impl UndoStatus {
    #[inline]
    pub fn ts(&self) -> TrxID {
        match self {
            UndoStatus::Ref(status) => status.ts(),
            UndoStatus::CTS(cts) => *cts,
        }
    }

    #[inline]
    pub fn can_purge(&mut self, min_active_sts: TrxID) -> bool {
        match self {
            UndoStatus::Ref(status) => {
                let ts = status.ts();
                if ts < min_active_sts {
                    return true;
                }
                if trx_is_committed(ts) {
                    // convert from reference to integer.
                    *self = UndoStatus::CTS(ts);
                    return false;
                }
                false
            }
            UndoStatus::CTS(ts) => *ts < min_active_sts,
        }
    }
}

/// Index branch is created if new version conflicts with old
/// version on same key of unique index.
/// In our design, we point the index entry to latest version
/// and link new version to old(deleted or updated) version.
/// The advantage is making index concise, especially for unique
/// index.
/// The disadvantage is making version chain complicated.
/// But in our assumption, most transactions are short and in-memory
/// version chain can be easily purged than out-of-memory index
/// maintainance.
///
/// MVCC read can skip this branch if the index key provided for
/// search is not same as the reborn key.
/// Because only such key should be searched in the Index branch.
/// Table scan should skip such branch.
///
/// Below is a sample data flow of the undo branch maintainance.
///
/// ```text
///  ┌──────────────────────────────────────────────────────────┐                     
///  │t1: insert {rowid=100,k=1,v=1}                            │                     
///  └──────────────────────────────────────────────────────────┘                     
///   unique index            row page                                                
///   ┌───────────┐          ┌─────────────────┐                                      
///   │k=1────►100├─────────►│rowid=100,k=1,v=1│                                      
///   └───────────┘          └─────────────────┘                                      
///                                                                                   
///  ┌──────────────────────────────────────────────────────────┐                     
///  │t2: update {k=1,v=1} to {k=9,v=9}                         │                     
///  └──────────────────────────────────────────────────────────┘                     
///   unique index            row page              version chain                     
///   ┌───────────┐          ┌─────────────────┐   ┌───────┐                          
///   │k=1────►100├─────┬───►│rowid=100,k=9,v=9├──►│k=1,v=1│                          
///   │           │     │    └─────────────────┘   └───────┘                          
///   │k=9────►100├─────┘                                                             
///   └───────────┘                                                                   
///                                                                                   
///  ┌──────────────────────────────────────────────────────────┐                     
///  │t3: insert {rowid=200,k=1,v=2}                            │                     
///  └──────────────────────────────────────────────────────────┘                     
///   unique index            row page              version chain                     
///   ┌───────────┐          ┌─────────────────┐   ┌───────┐                          
///   │k=1────►100├───┐  ┌──►│rowid=100,k=9,v=9├──►│k=1,v=1│                          
///   │           │   │  │   └─────────────────┘   └─▲─────┘                          
///   │k=9────►100├───┼──┘                           │                                
///   └───────────┘   │                              │Index(k=1)(delta)                      
///                   │      ┌─────────────────┐     │                                
///                   └─────►│rowid=200,k=1,v=2├─────┘                                
///                          └─────────────────┘                                      
/// ┌───────────────────────────────────────────────────────────┐                     
/// │t4: update {k=1,v=2} to {k=3,v=4}                          │                     
/// └───────────────────────────────────────────────────────────┘                     
///   unique index            row page              version chain                     
///   ┌───────────┐          ┌─────────────────┐                ┌───────┐             
///   │k=1────►200├──┐  ┌───►│rowid=100,k=9,v=9├───────────────►│k=1,v=1│             
///   │           │  │  │    └─────────────────┘                └─▲─────┘             
///   │k=9────►100├──┼──┘                                         │                   
///   │           │  │                                            │Index(k=1)(delta)         
///   │k=3────►200├──┤       ┌─────────────────┐   ┌───────┐      │                   
///   └───────────┘  └──────►│rowid=200,k=3,v=4├──►│k=1,v=2├──────┘                   
///                          └─────────────────┘   └───────┘                          
///                                                                                   
/// ┌───────────────────────────────────────────────────────────┐                     
/// │t5: update {k=9,v=9} to {k=1,v=5}                          │                     
/// └───────────────────────────────────────────────────────────┘                     
///   unique index            row page              version chain                     
///   ┌───────────┐          ┌──────────────────┐            ┌───────┐   ┌───────┐    
///   │k=1────►100├───┬─────►│rowid=100,k=1,v=5 ├───────────►│k=9,v=9├──►│k=1,v=1│    
///   │           │   │      └─────────────┬────┘            └───────┘   └─▲─────┘    
///   │k=9────►100├───┘                    └─────────┐                     │          
///   │           │                 Index(k=1)(delta)│                     │Index(k=1)(delta)
///   │k=3────►200├───┐      ┌─────────────────┐   ┌─▼─────┐               │          
///   └───────────┘   └─────►│rowid=200,k=3,v=4├──►│k=1,v=2├───────────────┘          
///                          └─────────────────┘   └───────┘                          
/// ```
pub struct IndexBranch {
    pub key: SelectKey,
    pub cts: TrxID,
    pub entry: RowUndoRef,
    pub undo_vals: Vec<UpdateCol>,
}

pub struct RowUndoHead {
    pub next: NextRowUndo,
    // If a purge thread purge some logs from this chain,
    // it will increase this field, so another thread may
    // skip if it has been already processed.
    pub purge_ts: TrxID,
}

impl RowUndoHead {
    #[inline]
    pub fn new(status: Arc<SharedTrxStatus>, entry: RowUndoRef) -> Self {
        RowUndoHead {
            next: NextRowUndo {
                main: MainBranch {
                    entry,
                    status: UndoStatus::Ref(status),
                },
                indexes: vec![],
            },
            purge_ts: MIN_SNAPSHOT_TS,
        }
    }

    /// Returns timestamp of undo head.
    #[inline]
    pub fn ts(&self) -> TrxID {
        self.next.main.status.ts()
    }

    #[inline]
    pub fn preparing(&self) -> bool {
        match &self.next.main.status {
            UndoStatus::Ref(status) => status.preparing(),
            _ => false,
        }
    }

    #[inline]
    pub fn prepare_listener(&self) -> Option<EventListener> {
        match &self.next.main.status {
            UndoStatus::Ref(status) => status.prepare_listener(),
            _ => None,
        }
    }
}

unsafe impl Send for RowUndoHead {}
