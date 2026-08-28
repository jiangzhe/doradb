use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{
    CatalogSelectKey, ResolvedUserIndexKey, TableCache, catalog_key_from_active_ordinal,
    user_key_from_active_slot,
};
use crate::error::RuntimeOrFatalResult as Result;
use crate::id::{RowID, TableID, TrxID};
use crate::poison::EnginePoisoner;
use crate::row::ops::{SelectKey, UndoCol, UpdateCol};
use crate::runtime::{POLL_BUDGET, yield_now};
use crate::trx::{
    MIN_SNAPSHOT_TS, PrepareListenerResult, SharedTrxStatus, StmtNo, trx_is_committed,
};
use crate::value::Val;
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
pub(crate) enum RowUndoKind {
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

/// Outcome of one exact-page hot row-undo rollback attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowUndoRollbackAttempt {
    /// The exact undo was synchronously unlinked from the hot row.
    Applied,
    /// The undo's original page generation is no longer resident.
    PageMissing,
    /// The original page is retained by checkpoint transition.
    Transition,
}

/// Borrowed engine authority required by row-undo rollback.
#[derive(Clone, Copy)]
pub(crate) struct RowUndoRollbackContext<'a> {
    pool_guards: &'a PoolGuards,
    poisoner: &'a EnginePoisoner,
}

impl<'a> RowUndoRollbackContext<'a> {
    /// Build rollback authority from the terminal or statement owner.
    #[inline]
    pub(crate) fn new(pool_guards: &'a PoolGuards, poisoner: &'a EnginePoisoner) -> Self {
        Self {
            pool_guards,
            poisoner,
        }
    }
}

/// RowUndoLogs is a collection of row undo logs.
/// It owns the logs until GC clean them all at transaction level.
#[derive(Default)]
pub(crate) struct RowUndoLogs(Vec<OwnedRowUndo>);

impl RowUndoLogs {
    /// Create an empty row undo buffer.
    #[inline]
    pub(crate) fn empty() -> Self {
        RowUndoLogs(vec![])
    }

    /// Append a row undo entry to the transaction undo buffer.
    #[inline]
    pub(crate) fn push(&mut self, value: OwnedRowUndo) {
        self.0.push(value)
    }

    /// Remove the newest entry after its row-version reference was unlinked.
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<OwnedRowUndo> {
        self.0.pop()
    }

    /// Move all row undo entries from another buffer into this one.
    #[inline]
    pub(crate) fn merge(&mut self, other: &mut Self) {
        self.0.append(&mut other.0);
    }

    /// Roll back row changes in reverse undo-log order.
    #[inline]
    pub(crate) async fn rollback(
        &mut self,
        table_cache: &mut TableCache<'_>,
        context: RowUndoRollbackContext<'_>,
    ) -> Result<()> {
        let mut budget = POLL_BUDGET;
        while !self.0.is_empty() {
            {
                // Keep the current entry vector-owned across every await. Its
                // stable Box continues to own any pointer reachable from the
                // row undo chain if this future is cancelled or fails. Pop it
                // only after rollback synchronously unlinks that chain entry.
                let entry = self
                    .0
                    .last_mut()
                    .expect("non-empty row undo buffer must have a last entry");
                #[cfg(test)]
                {
                    use super::tests::maybe_pause_row_rollback;
                    maybe_pause_row_rollback().await;
                }
                if entry.table_id.is_catalog() {
                    let table = table_cache.must_get_catalog_table(entry.table_id);
                    if entry.page_id.is_some() {
                        match table
                            .mem
                            .try_rollback_hot_row_undo(entry, context.pool_guards)
                            .await?
                        {
                            RowUndoRollbackAttempt::Applied
                            | RowUndoRollbackAttempt::PageMissing => (),
                            RowUndoRollbackAttempt::Transition => {
                                panic!(
                                    "catalog row page cannot enter checkpoint transition: \
                                     table_id={}, row_id={}",
                                    entry.table_id, entry.row_id
                                );
                            }
                        }
                    }
                } else {
                    let table = table_cache.must_get_user_table(entry.table_id).await;
                    loop {
                        if entry.page_id.is_none() {
                            table.deletion_buffer().remove(entry.row_id);
                            break;
                        }
                        if entry.row_id < table.mem.pivot_row_id() {
                            table.deletion_buffer().remove(entry.row_id);
                            break;
                        }
                        match table
                            .mem
                            .try_rollback_hot_row_undo(entry, context.pool_guards)
                            .await?
                        {
                            RowUndoRollbackAttempt::Applied => break,
                            RowUndoRollbackAttempt::PageMissing
                            | RowUndoRollbackAttempt::Transition => {
                                table
                                    .wait_transition_route_or_poison(context.poisoner, entry.row_id)
                                    .await?;
                            }
                        }
                    }
                }
            }
            self.0.pop();
            budget -= 1;
            if budget == 0 && !self.0.is_empty() {
                yield_now().await;
                budget = POLL_BUDGET;
            }
        }
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
pub(crate) struct OwnedRowUndo(Box<RowUndo>);

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
    /// Create an owned row undo entry for a single row-page change.
    #[inline]
    pub(crate) fn new(
        stmt_no: StmtNo,
        table_id: TableID,
        page_id: Option<VersionedPageID>,
        row_id: RowID,
        kind: RowUndoKind,
    ) -> Self {
        let entry = RowUndo {
            stmt_no,
            table_id,
            page_id,
            row_id,
            kind,
            next: None,
        };
        OwnedRowUndo(Box::new(entry))
    }

    /// Return a non-owning reference that can be stored in row version chains.
    #[inline]
    pub(crate) fn leak(&self) -> RowUndoRef {
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
pub(crate) struct RowUndoRef(NonNull<RowUndo>);

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

/// Undo entry for one hot-row or cold-delete-buffer row version.
pub(crate) struct RowUndo {
    /// Transaction-local statement that installed this foreground version.
    pub(crate) stmt_no: StmtNo,
    /// Table containing the hot row or cold deletion marker.
    pub(crate) table_id: TableID,
    /// Row page for hot-row undo. `None` is reserved for cold-row deletion
    /// buffer undo, which has no row page to latch during rollback.
    pub(crate) page_id: Option<VersionedPageID>,
    /// Physical row version affected by this undo entry.
    pub(crate) row_id: RowID,
    /// Operation whose inverse reconstructs the previous MVCC state.
    pub(crate) kind: RowUndoKind,
    /// Older version state reachable from this entry.
    pub(crate) next: Option<NextRowUndo>,
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
pub(crate) struct NextRowUndo {
    /// Main undo branch for older versions of the same hot row.
    pub(crate) main: MainBranch,
    /// Runtime unique-index branches to older owners of matching keys.
    pub(crate) indexes: Vec<IndexBranch>,
}

impl NextRowUndo {
    /// Create a new next undo with only main branch.
    #[inline]
    pub(crate) fn new(main: MainBranch) -> Self {
        NextRowUndo {
            main,
            indexes: vec![],
        }
    }

    /// Returns next index branch.
    #[inline]
    pub(crate) fn index_branch(&self, key: Option<(usize, &[Val])>) -> Option<&IndexBranch> {
        key.and_then(|(index_no, key_vals)| {
            self.indexes
                .iter()
                .find(|branch| branch.matches(index_no, key_vals))
        })
    }
}

/// Main branch stores older versions of the same hot RowID.
///
/// It is the normal path for table scans and point reads that already routed
/// to the row. Unique-index branches are separate because a latest unique-key
/// mapping may need to reach an older owner with a different RowID.
pub(crate) struct MainBranch {
    /// Next undo entry in the main row-version chain.
    pub(crate) entry: RowUndoRef,
    /// Commit or active transaction status for the next undo entry.
    pub(crate) status: UndoStatus,
}

/// UndoStatus represents status of any undo log,
/// including uncommitted transactions.
pub(crate) enum UndoStatus {
    /// Shared transaction status while the owning transaction is active or has
    /// not yet been compacted to a plain commit timestamp.
    Ref(Arc<SharedTrxStatus>),
    /// Stable committed timestamp kept after the shared status is no longer
    /// needed for visibility.
    Committed(TrxID),
}

impl UndoStatus {
    /// Return the current transaction or commit timestamp represented here.
    #[inline]
    pub(crate) fn ts(&self) -> TrxID {
        match self {
            UndoStatus::Ref(status) => status.ts(),
            UndoStatus::Committed(cts) => *cts,
        }
    }

    /// Return whether an undo entry with this status is older than all readers.
    #[inline]
    pub(crate) fn can_purge(&mut self, min_active_sts: TrxID) -> bool {
        match self {
            UndoStatus::Ref(status) => {
                let ts = status.ts();
                if ts < min_active_sts {
                    return true;
                }
                if trx_is_committed(ts) {
                    // convert from reference to integer.
                    *self = UndoStatus::Committed(ts);
                    return false;
                }
                false
            }
            UndoStatus::Committed(ts) => *ts < min_active_sts,
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
pub(crate) struct IndexBranchPayload<K> {
    /// Unique index key that requires this alternate version branch.
    pub(crate) key: K,
    /// Hot or cold owner reached by this branch.
    pub(crate) target: IndexBranchTarget,
    /// Before-image values used to reconstruct a cold terminal owner.
    pub(crate) undo_vals: Vec<UpdateCol>,
}

/// Domain-tagged transaction-owned unique-index MVCC branch.
pub(crate) enum IndexBranch {
    /// Branch on a fixed catalog index ordinal.
    Catalog(IndexBranchPayload<CatalogSelectKey>),
    /// Branch on a generation-qualified user index reference.
    User(IndexBranchPayload<ResolvedUserIndexKey>),
}

impl IndexBranch {
    /// Creates a catalog branch from one metadata-proven active ordinal.
    #[inline]
    pub(crate) fn catalog(
        key: SelectKey,
        target: IndexBranchTarget,
        undo_vals: Vec<UpdateCol>,
    ) -> Self {
        Self::Catalog(IndexBranchPayload {
            key: catalog_key_from_active_ordinal(key.index_no, key.vals),
            target,
            undo_vals,
        })
    }

    /// Creates a user branch from one layout-proven active slot.
    #[inline]
    pub(crate) fn user(
        key: SelectKey,
        target: IndexBranchTarget,
        undo_vals: Vec<UpdateCol>,
    ) -> Self {
        Self::User(IndexBranchPayload {
            key: user_key_from_active_slot(key.index_no, key.vals),
            target,
            undo_vals,
        })
    }

    /// Returns whether this branch matches a transient execution slot and key.
    #[inline]
    pub(crate) fn matches(&self, index_no: usize, key_vals: &[Val]) -> bool {
        match self {
            Self::Catalog(branch) => {
                branch.key.index.as_usize() == index_no && branch.key.vals == key_vals
            }
            Self::User(branch) => {
                branch.key.index.slot().as_usize() == index_no && branch.key.vals == key_vals
            }
        }
    }

    /// Returns the transient execution slot and logical values for matching.
    #[inline]
    pub(crate) fn key_parts(&self) -> (usize, &[Val]) {
        match self {
            Self::Catalog(branch) => (branch.key.index.as_usize(), &branch.key.vals),
            Self::User(branch) => (branch.key.index.slot().as_usize(), &branch.key.vals),
        }
    }

    /// Returns this branch's target.
    #[inline]
    pub(crate) fn target(&self) -> &IndexBranchTarget {
        match self {
            Self::Catalog(branch) => &branch.target,
            Self::User(branch) => &branch.target,
        }
    }

    /// Returns the row before-images carried by this branch.
    #[inline]
    pub(crate) fn undo_vals(&self) -> &[UpdateCol] {
        match self {
            Self::Catalog(branch) => &branch.undo_vals,
            Self::User(branch) => &branch.undo_vals,
        }
    }

    /// Returns the timestamp controlling whether this branch can be purged.
    #[inline]
    pub(crate) fn purge_cts(&self) -> Option<TrxID> {
        self.target().purge_cts()
    }
}

/// Qualifies a shared positional key before it enters retained row-undo state.
pub(crate) trait IndexBranchDomain {
    /// Builds one branch in the selected index-reference domain.
    fn branch(key: SelectKey, target: IndexBranchTarget, undo_vals: Vec<UpdateCol>) -> IndexBranch;
}

/// Compile-time selector used by shared hot-row code to qualify catalog branches.
pub(crate) struct CatalogIndexBranchDomain;

impl IndexBranchDomain for CatalogIndexBranchDomain {
    #[inline]
    fn branch(key: SelectKey, target: IndexBranchTarget, undo_vals: Vec<UpdateCol>) -> IndexBranch {
        IndexBranch::catalog(key, target, undo_vals)
    }
}

/// Compile-time selector used by shared hot-row code to qualify user branches.
pub(crate) struct UserIndexBranchDomain;

impl IndexBranchDomain for UserIndexBranchDomain {
    #[inline]
    fn branch(key: SelectKey, target: IndexBranchTarget, undo_vals: Vec<UpdateCol>) -> IndexBranch {
        IndexBranch::user(key, target, undo_vals)
    }
}

/// Target of a runtime unique-index branch.
pub(crate) enum IndexBranchTarget {
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
    /// Return the timestamp that determines when this branch can be purged.
    #[inline]
    pub(crate) fn purge_cts(&self) -> Option<TrxID> {
        match self {
            IndexBranchTarget::Hot { cts, .. } => Some(*cts),
            IndexBranchTarget::ColdTerminal { delete_cts } => *delete_cts,
        }
    }
}

/// Current undo-chain head stored on a row page.
pub(crate) struct RowUndoHead {
    /// Branches reachable from the newest row version.
    pub(crate) next: NextRowUndo,
    /// Newest purge timestamp already processed for this chain.
    ///
    /// Purge workers advance this value after trimming logs so later workers
    /// can skip chains they have already covered.
    pub(crate) purge_ts: TrxID,
}

impl RowUndoHead {
    /// Create a row undo head for a newly installed undo entry.
    #[inline]
    pub(crate) fn new(status: Arc<SharedTrxStatus>, entry: RowUndoRef) -> Self {
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
    pub(crate) fn ts(&self) -> TrxID {
        self.next.main.status.ts()
    }

    /// Returns the transaction-local statement tag on the current main entry.
    #[inline]
    pub(crate) fn stmt_no(&self) -> StmtNo {
        self.next.main.entry.as_ref().stmt_no
    }

    /// Register a listener for the owning transaction's prepare completion.
    #[inline]
    pub(crate) fn prepare_listener(&self) -> PrepareListenerResult {
        match &self.next.main.status {
            UndoStatus::Ref(status) => status.prepare_listener(),
            _ => PrepareListenerResult::NotPreparing,
        }
    }
}
