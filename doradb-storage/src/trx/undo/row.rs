use crate::buffer::BufferPool;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{Catalog, TableID};
use crate::latch::LatchFallbackMode;
use crate::row::ops::{SelectKey, UndoCol, UpdateCol};
use crate::row::{RowID, RowPage};
use crate::table::Table;
use crate::trx::row::RowWriteAccess;
use crate::trx::{MIN_SNAPSHOT_TS, SharedTrxStatus, TrxID, trx_is_committed};
use event_listener::EventListener;
use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

/// RowUndoKind represents the kind of original operation.
/// So the actual undo action should be opposite of the kind.
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
    Update(Vec<UndoCol>),
}

impl fmt::Debug for RowUndoKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowUndoKind::Delete => f.pad("Delete"),
            RowUndoKind::Insert => f.pad("Insert"),
            RowUndoKind::Lock => f.pad("Lock"),
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
    pub async fn rollback<P: BufferPool>(
        &mut self,
        buf_pool: &'static P,
        catalog: &Catalog,
        sts: Option<TrxID>,
    ) {
        let mut table_cache: HashMap<TableID, (RowID, Table)> = HashMap::new();
        while let Some(entry) = self.0.pop() {
            let (pivot_row_id, table) = match table_cache.get(&entry.table_id) {
                Some((pivot_row_id, table)) => (*pivot_row_id, table.clone()),
                None => {
                    let table = catalog
                        .get_table(entry.table_id)
                        .await
                        .expect("table exists");
                    let pivot_row_id = table.pivot_row_id();
                    table_cache.insert(entry.table_id, (pivot_row_id, table.clone()));
                    (pivot_row_id, table)
                }
            };
            if entry.page_id.is_none() || entry.row_id < pivot_row_id {
                table.deletion_buffer().remove(entry.row_id);
                continue;
            }
            let Some(page_guard) = buf_pool
                .try_get_page_versioned::<RowPage>(
                    entry.page_id.unwrap(),
                    LatchFallbackMode::Shared,
                )
                .await
            else {
                continue;
            };
            let page_guard = page_guard.shared_async().await;
            let (ctx, page) = page_guard.ctx_and_page();
            let metadata = &*ctx.row_ver().unwrap().metadata;
            // TODO: we should retry or wait for notification if rollback happens on a page
            // in transition state. This will be handled in a future task.
            let row_idx = page.row_idx(entry.row_id);
            let mut access = RowWriteAccess::new(page, ctx, row_idx, sts, false);
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
    pub fn new(
        table_id: TableID,
        page_id: Option<VersionedPageID>,
        row_id: RowID,
        kind: RowUndoKind,
    ) -> Self {
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
    pub page_id: Option<VersionedPageID>,
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
