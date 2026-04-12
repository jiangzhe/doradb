use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{TableCache, TableHandle, TableID};
use crate::error::Result;
use crate::row::RowID;
use crate::row::ops::{SelectKey, UndoCol, UpdateCol};
use crate::trx::row::RowWriteAccess;
use crate::trx::{MIN_SNAPSHOT_TS, SharedTrxStatus, TrxID, trx_is_committed};
use event_listener::EventListener;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;

/// RowUndoKind records the foreground operation that produced an undo entry.
///
/// Hot-row MVCC and rollback both interpret the entry in reverse: an `Insert`
/// becomes invisible to older readers, a `Delete` restores the previous
/// visible row image, and an `Update` carries the before-images needed to
/// reconstruct the older version.
pub enum RowUndoKind {
    /// Provisional row-page write lock.
    ///
    /// Hot updates and deletes first install a `Lock` entry at the undo head.
    /// That entry is the write-conflict point for other transactions. After
    /// the row-page mutation succeeds, the same transaction rewrites the entry
    /// into the actual operation kind.
    Lock,
    /// Insert a new hot row.
    ///
    /// The row page holds the newly inserted image. No before-image values are
    /// stored because older snapshots must treat the row as non-existent once
    /// this entry is reached.
    ///
    /// For move updates, the inserted row may also carry unique-index runtime
    /// branches to the previous hot or cold owner.
    ///
    /// # Possible chains
    ///
    /// 1. Insert -> null.
    ///
    /// This is the common scenario: the insert is the first version of a row
    /// and does not have an older next version.
    Insert,
    /// Delete an existing hot row.
    ///
    /// The row-page delete bit is the newest image. The undo entry does not
    /// copy row values because older snapshots can still read the row image
    /// from the page and flip the delete state while traversing the chain.
    ///
    /// Possible chains:
    ///
    /// 1. Delete -> null.
    ///
    /// It can happen when GC is executed and the insert transaction is cleaned.
    /// This means if we cannot see the delete version, we should unmark latest
    /// version in data page.
    ///
    /// 2. Delete -> Insert.
    ///
    /// 3. Delete -> Update.
    ///
    Delete,
    /// Update a hot row in place.
    ///
    /// Only changed columns are copied as before-images. Readers that cannot
    /// see the latest page image apply these values while walking the main
    /// branch. Rollback applies the same values to the row page.
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
    /// Derived from an insert operation.
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
    pub async fn rollback(
        &mut self,
        table_cache: &mut TableCache<'_>,
        guards: &PoolGuards,
        sts: Option<TrxID>,
    ) -> Result<()> {
        while let Some(entry) = self.0.pop() {
            let table = table_cache.must_get_table(entry.table_id).await;
            Self::rollback_entry_in_table(entry, table, guards, sts).await?;
        }
        Ok(())
    }

    #[inline]
    async fn rollback_entry_in_table(
        entry: OwnedRowUndo,
        table: &TableHandle,
        guards: &PoolGuards,
        sts: Option<TrxID>,
    ) -> Result<()> {
        let pivot_row_id = table.pivot_row_id();
        if entry.page_id.is_none() || entry.row_id < pivot_row_id {
            if let Some(deletion_buffer) = table.deletion_buffer() {
                deletion_buffer.remove(entry.row_id);
            }
            return Ok(());
        }
        let Some(page_guard) = table
            .get_row_page_versioned_shared(guards, entry.page_id.unwrap())
            .await?
        else {
            return Ok(());
        };
        let (ctx, page) = page_guard.ctx_and_page();
        let metadata = &*ctx.row_ver().unwrap().metadata;
        // TODO: we should retry or wait for notification if rollback happens on a page
        // in transition state. This will be handled in a future task.
        let row_idx = page.row_idx(entry.row_id);
        let mut access = RowWriteAccess::new(page, ctx, row_idx, sts, false);
        access.rollback_first_undo(metadata, entry);
        Ok(())
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
        RowUndoRef(NonNull::from(self.0.as_ref()))
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

// SAFETY: version-chain lifetime and row-lock/GC rules guarantee the pointed
// undo entry remains valid while a `RowUndoRef` is reachable.
unsafe impl Send for RowUndoRef {}
// SAFETY: sharing references to `RowUndoRef` only shares access to the same
// version-chain node governed by those MVCC/GC invariants.
unsafe impl Sync for RowUndoRef {}

impl RowUndoRef {
    /// Returns reference of underlying undo log.
    /// This method is safe because GC operation always clear this reference
    /// from next undo list.
    /// So we won't have chance to access a deleted undo log.
    #[inline]
    pub(crate) fn as_ref(&self) -> &RowUndo {
        // SAFETY: `RowUndoRef` invariants guarantee the pointed entry stays valid while
        // reachable from version chains.
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
        // SAFETY: mutable access is restricted to GC/row-lock-protected paths,
        // so aliasing mutable references are not created.
        unsafe { self.0.as_mut() }
    }
}

impl Clone for RowUndoRef {
    #[inline]
    fn clone(&self) -> Self {
        RowUndoRef(self.0)
    }
}

pub struct RowUndo {
    /// Table containing the hot row or cold deletion marker.
    pub table_id: TableID,
    /// Row page for hot-row undo. `None` is reserved for cold-row deletion
    /// buffer undo, which has no row page to latch during rollback.
    pub page_id: Option<VersionedPageID>,
    /// Physical row version affected by this undo entry.
    pub row_id: RowID,
    /// Operation whose inverse reconstructs the previous MVCC state.
    pub kind: RowUndoKind,
    /// Older version state reachable from this entry.
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
///
/// Unique-index branches are runtime MVCC bridges. They are needed when the
/// latest unique-key mapping points to a row whose ordinary undo chain cannot
/// reach an older visible owner of the same logical key. The branch target may
/// be a hot row undo chain or a terminal cold row image reconstructed from the
/// branch's undo values.
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

/// Main branch stores older versions of the same hot RowID.
///
/// It is the normal path for table scans and point reads that already routed
/// to the row. Unique-index branches are separate because a latest unique-key
/// mapping may need to reach an older owner with a different RowID.
pub struct MainBranch {
    pub entry: RowUndoRef,
    pub status: UndoStatus,
}

/// UndoStatus represents status of any undo log,
/// including uncommitted transactions.
pub enum UndoStatus {
    /// Shared transaction status while the owning transaction is active or has
    /// not yet been compacted to a plain commit timestamp.
    Ref(Arc<SharedTrxStatus>),
    /// Stable committed timestamp kept after the shared status is no longer
    /// needed for visibility.
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
/// maintenance.
///
/// MVCC read can skip this branch if the index key provided for
/// search is not same as the reborn key.
/// Because only such key should be searched in the Index branch.
/// Table scan should skip such branch.
///
/// A branch can target either a hot owner with a row-page undo continuation, or
/// a cold terminal owner. Cold terminal branches are used by LWC unique update
/// and unique-key claim paths: the persisted old row has no row-page undo
/// chain, so `undo_vals` reconstructs that old image and the optional delete
/// timestamp decides whether the reconstructed image is visible to a reader.
///
/// Below is a sample data flow of the undo branch maintenance.
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
    pub target: IndexBranchTarget,
    pub undo_vals: Vec<UpdateCol>,
}

/// Target of a runtime unique-index branch.
pub enum IndexBranchTarget {
    /// Branch to another hot row's undo chain.
    ///
    /// `cts` is the delete/update timestamp at which the old hot owner stopped
    /// being visible. Readers at or before that timestamp continue through
    /// `entry` to find the older same-key version.
    Hot { cts: TrxID, entry: RowUndoRef },
    /// Branch to a persisted cold row reconstructed from `undo_vals`.
    ///
    /// Cold rows are immutable and have no row-page undo chain. `delete_cts`
    /// is the committed CDB delete timestamp when the cold row was already
    /// deleted by an earlier transaction; readers after that timestamp must not
    /// see the reconstructed image. `None` means the transaction containing the
    /// new hot row owns the cold delete marker, which covers the same-row
    /// cold-to-hot update case before that transaction commits.
    ColdTerminal { delete_cts: Option<TrxID> },
}

impl IndexBranchTarget {
    #[inline]
    pub(crate) fn purge_cts(&self) -> Option<TrxID> {
        match self {
            IndexBranchTarget::Hot { cts, .. } => Some(*cts),
            IndexBranchTarget::ColdTerminal { delete_cts } => *delete_cts,
        }
    }
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
