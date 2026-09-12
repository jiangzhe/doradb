use crate::buffer::PoolGuards;
use crate::buffer::page::VersionedPageID;
use crate::catalog::{IndexRef, ResolvedIndexKey, TableCache};
use crate::error::RuntimeOrFatalResult as Result;
use crate::id::{RowID, TableID, TrxID};
use crate::poison::EnginePoisoner;
use crate::row::ops::{UndoCol, UpdateCol};
use crate::runtime::{POLL_BUDGET, yield_now};
use crate::trx::{
    MIN_SNAPSHOT_TS, PrepareListenerResult, SharedTrxStatus, StmtNo, TrxContext, trx_is_committed,
};
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
    Delete(ForwardLinks),
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
    Update(UpdateUndo),
}

impl RowUndoKind {
    /// Creates Delete undo with no allocated successor storage.
    #[inline]
    pub(crate) fn delete() -> Self {
        Self::Delete(ForwardLinks::default())
    }

    /// Creates Update undo with immutable before-images and no forward allocation.
    #[inline]
    pub(crate) fn update(cols: Vec<UndoCol>) -> Self {
        Self::Update(UpdateUndo {
            cols,
            forward: ForwardLinks::default(),
        })
    }
}

impl fmt::Debug for RowUndoKind {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowUndoKind::Delete(_) => f.pad("Delete"),
            RowUndoKind::Insert => f.pad("Insert"),
            RowUndoKind::Lock => f.pad("Lock"),
            RowUndoKind::Update(_) => f.pad("Update"),
        }
    }
}

/// Surviving destination for one departed key in an exact index generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ForwardHint {
    /// Index identity and physical slot; slots alone can be reused after DROP.
    pub(crate) index: IndexRef,
    /// Successor whose latest ownership, key and deletion state must be checked.
    pub(crate) row_id: RowID,
}

/// Plain per-index successors for one exact Delete or Update departure.
/// The source writer publishes and restores slots under the source row latch.
/// Snapshot views exclude this storage; departure before-images supply the key.
#[derive(Default)]
pub(crate) struct ForwardLinks {
    successors: Option<Box<[ForwardHint]>>,
}

impl ForwardLinks {
    /// Copies the previous slot for destination-owned rollback bookkeeping.
    #[inline]
    pub(crate) fn hint(&self, index: IndexRef) -> Option<ForwardHint> {
        self.successors
            .as_deref()?
            .iter()
            .find(|hint| hint.index == index)
            .copied()
    }

    /// Returns one exact-index successor without allocating.
    #[inline]
    pub(crate) fn successor(&self, index: IndexRef) -> Option<RowID> {
        self.successors
            .as_deref()?
            .iter()
            .find_map(|hint| (hint.index == index).then_some(hint.row_id))
    }

    /// Updates one link through exclusive access to its writer-owned departure.
    /// The caller serializes source-row access and publishes only after index undo.
    #[inline]
    pub(crate) fn set_successor(&mut self, hint: ForwardHint) {
        if let Some(stored) = self
            .successors
            .as_deref_mut()
            .and_then(|slots| slots.iter_mut().find(|stored| stored.index == hint.index))
        {
            *stored = hint;
        } else {
            let mut slots = self.successors.take().map_or_else(Vec::new, Vec::from);
            slots.push(hint);
            self.successors = Some(slots.into_boxed_slice());
        }
    }

    /// Restores an overwritten slot or removes a slot created by a failed effect.
    #[inline]
    pub(crate) fn restore(&mut self, index: IndexRef, previous: Option<ForwardHint>) {
        if let Some(hint) = previous {
            assert_eq!(
                hint.index, index,
                "forward rollback index must match its before-image"
            );
            self.set_successor(hint);
        } else if let Some(slots) = self.successors.take() {
            let mut slots = slots.into_vec();
            slots.retain(|hint| hint.index != index);
            self.successors = (!slots.is_empty()).then(|| slots.into_boxed_slice());
        }
    }
}

/// In-place before-images and separately borrowed current-write successors.
pub(crate) struct UpdateUndo {
    /// Immutable column before-images used by snapshots and rollback.
    pub(crate) cols: Vec<UndoCol>,
    forward: ForwardLinks,
}

impl UpdateUndo {
    /// Reads successors under the same source row latch as its before-images.
    #[inline]
    pub(crate) fn forward(&self) -> &ForwardLinks {
        &self.forward
    }
}

/// Identity and ownership of one exact hot departure, including a buried Update.
#[derive(Clone)]
pub(crate) struct HotForwardSource {
    /// Exact source page generation to pin before the index exchange.
    pub(crate) page_id: VersionedPageID,
    /// Source physical row whose write latch protects direct link access.
    pub(crate) row_id: RowID,
    entry: RowUndoRef,
    owner: Arc<SharedTrxStatus>,
}

impl HotForwardSource {
    /// Captures an exact Delete/Update entry owned by this still-active writer.
    /// The source row latch protects discovery; transaction ownership protects
    /// the entry until the operation finishes, including across insertion awaits.
    #[inline]
    pub(crate) fn new(ctx: &TrxContext, head: &RowUndoHead, entry: &RowUndoRef) -> Option<Self> {
        if !ctx.is_same_trx(head) {
            return None;
        }
        let mut main = &head.next.main;
        loop {
            if main.entry.0 == entry.0 {
                break;
            }
            main = &main.entry.as_ref().next.as_ref()?.main;
        }
        let UndoStatus::Ref(owner) = &main.status else {
            return None;
        };
        if !Arc::ptr_eq(owner, ctx.status())
            || !matches!(
                entry.as_ref().kind,
                RowUndoKind::Delete(_) | RowUndoKind::Update(_)
            )
        {
            return None;
        }
        Some(Self {
            page_id: entry.as_ref().page_id?,
            row_id: entry.as_ref().row_id,
            entry: entry.clone(),
            owner: Arc::clone(owner),
        })
    }

    /// Checks source identity without borrowing the shared undo payload.
    #[inline]
    pub(crate) fn matches(&self, entry: &RowUndoRef) -> bool {
        self.entry.0 == entry.0
    }

    /// Confirms that a reachable source version still has its publishing owner.
    #[inline]
    pub(crate) fn owns(&self, status: &UndoStatus) -> bool {
        !trx_is_committed(self.owner.ts())
            && matches!(status, UndoStatus::Ref(owner) if Arc::ptr_eq(owner, &self.owner))
    }
}

/// Before-image of a source slot, retained by the destination's undo owner.
pub(crate) struct ForwardLinkUndo {
    /// Exact source whose writer outlives reverse destination rollback.
    pub(crate) source: HotForwardSource,
    /// Index slot to remove when there was no previous link.
    pub(crate) index: IndexRef,
    /// Previous source link, restored before undoing the destination.
    pub(crate) previous: Option<ForwardHint>,
}

/// Snapshot operation view that never borrows mutable successor storage.
pub(crate) enum RowUndoKindView<'a> {
    /// Provisional write with no inverse row change.
    Lock,
    /// Insertion whose inverse hides the row.
    Insert,
    /// Deletion whose inverse restores visibility without reading its hints.
    Delete,
    /// Immutable before-images for an in-place update.
    Update(&'a [UndoCol]),
}

/// Disjoint operation and older-chain fields needed by backward snapshot traversal.
pub(crate) struct RowUndoView<'a> {
    /// Inverse operation, excluding mutable routing metadata.
    pub(crate) kind: RowUndoKindView<'a>,
    /// Older version state protected by the existing MVCC lifetime protocol.
    pub(crate) next: Option<&'a NextRowUndo>,
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
                    while let Some(undo) = entry.forward_undo.last() {
                        let result = table
                            .row_store
                            .try_restore_forward_link(undo, context.pool_guards)
                            .await?;
                        assert_eq!(
                            result,
                            RowUndoRollbackAttempt::Applied,
                            "catalog forward source must remain hot during rollback"
                        );
                        entry.forward_undo.pop();
                    }
                    if entry.page_id.is_some() {
                        match table
                            .row_store
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
                    let table = table_cache.must_get_user_table(entry.table_id);
                    while let Some(undo) = entry.forward_undo.last() {
                        let source_id = undo.source.row_id;
                        if source_id < table.row_store.pivot_row_id() {
                            // Cold routing has no forward fields. The old hot
                            // payload is no longer reachable by current selection.
                            entry.forward_undo.pop();
                            continue;
                        }
                        match table
                            .row_store
                            .try_restore_forward_link(undo, context.pool_guards)
                            .await?
                        {
                            RowUndoRollbackAttempt::Applied => {
                                entry.forward_undo.pop();
                            }
                            RowUndoRollbackAttempt::PageMissing
                            | RowUndoRollbackAttempt::Transition => {
                                table
                                    .wait_transition_route_or_poison(context.poisoner, source_id)
                                    .await?;
                            }
                        }
                    }
                    loop {
                        if entry.page_id.is_none() {
                            table.deletion_buffer().remove(entry.row_id);
                            break;
                        }
                        if entry.row_id < table.row_store.pivot_row_id() {
                            table.deletion_buffer().remove(entry.row_id);
                            break;
                        }
                        match table
                            .row_store
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
pub(crate) struct OwnedRowUndo {
    entry: Box<RowUndo>,
    // This owner-only journal is never reachable through snapshot RowUndoRef.
    forward_undo: Vec<ForwardLinkUndo>,
}

impl Deref for OwnedRowUndo {
    type Target = RowUndo;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl DerefMut for OwnedRowUndo {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entry
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
        OwnedRowUndo {
            entry: Box::new(entry),
            forward_undo: Vec::new(),
        }
    }

    /// Return a non-owning reference that can be stored in row version chains.
    #[inline]
    pub(crate) fn leak(&self) -> RowUndoRef {
        RowUndoRef(NonNull::from(self.entry.as_ref()))
    }

    /// Registers a source before-image without borrowing snapshot-visible fields.
    #[inline]
    pub(crate) fn push_forward_undo(&mut self, undo: ForwardLinkUndo) {
        self.forward_undo.push(undo);
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
    /// Reads only the fields needed by snapshots following another row's branch.
    /// A shared reference to the whole undo would also borrow its mutable hints.
    #[inline]
    pub(crate) fn snapshot_view(&self) -> RowUndoView<'_> {
        let entry = self.0.as_ptr();
        // SAFETY: MVCC keeps this entry and the traversed older chain alive. The
        // source row latch protects main-branch reads; published cross-row branches
        // target finalized operation kinds. A source writer changes only its
        // forward field, never its discriminant or before-images. The projections
        // borrow Update columns and `next`, leaving both link stores unborrowed.
        unsafe {
            let kind = match (*entry).kind {
                RowUndoKind::Lock => RowUndoKindView::Lock,
                RowUndoKind::Insert => RowUndoKindView::Insert,
                RowUndoKind::Delete(_) => RowUndoKindView::Delete,
                RowUndoKind::Update(UpdateUndo { ref cols, .. }) => RowUndoKindView::Update(cols),
            };
            RowUndoView {
                kind,
                next: (*entry).next.as_ref(),
            }
        }
    }

    /// Projects only forward storage without borrowing before-images or the chain.
    ///
    /// # Safety
    /// The caller must hold the source row's write latch and prove this is the
    /// active writer's exact reachable Delete/Update entry. Cross-row snapshot
    /// readers must use `snapshot_view`, which does not borrow forward storage.
    #[inline]
    pub(in crate::trx) unsafe fn forward_mut(&mut self) -> Option<&mut ForwardLinks> {
        // SAFETY: the caller establishes exclusive access to the forward field.
        // Match the raw place directly so neither RowUndo nor RowUndoKind is
        // borrowed mutably alongside snapshot references to disjoint fields.
        unsafe {
            match (*self.0.as_ptr()).kind {
                RowUndoKind::Delete(ref mut delete) => Some(delete),
                RowUndoKind::Update(UpdateUndo {
                    ref mut forward, ..
                }) => Some(forward),
                _ => None,
            }
        }
    }

    /// Returns the whole undo while its source row latch protects shared access.
    /// Cross-row snapshot traversal must use `snapshot_view` instead because
    /// the source writer can still update forward links. GC clears unreachable
    /// references before freeing their entries.
    #[inline]
    pub(crate) fn as_ref(&self) -> &RowUndo {
        // SAFETY: the source row latch serializes access, and `RowUndoRef`
        // invariants keep the entry alive while reachable from version chains.
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

    /// Returns the first index branch accepted by the caller's exact-reference matcher.
    #[inline]
    pub(crate) fn index_branch(
        &self,
        mut matches: impl FnMut(&IndexBranch) -> bool,
    ) -> Option<&IndexBranch> {
        self.indexes.iter().find(|branch| matches(branch))
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
pub(crate) struct IndexBranch {
    /// Unique index key that requires this alternate version branch.
    pub(crate) key: ResolvedIndexKey,
    /// Hot or cold owner reached by this branch.
    pub(crate) target: IndexBranchTarget,
    /// Before-image values used to reconstruct a cold terminal owner.
    pub(crate) undo_vals: Vec<UpdateCol>,
}

impl IndexBranch {
    /// Creates a branch from one exact runtime index reference.
    #[inline]
    pub(crate) fn new(
        key: ResolvedIndexKey,
        target: IndexBranchTarget,
        undo_vals: Vec<UpdateCol>,
    ) -> Self {
        Self {
            key,
            target,
            undo_vals,
        }
    }

    /// Returns the timestamp controlling whether this branch can be purged.
    #[inline]
    pub(crate) fn purge_cts(&self) -> Option<TrxID> {
        self.target.purge_cts()
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

#[cfg(test)]
mod tests {
    use super::{ForwardHint, ForwardLinks, OwnedRowUndo, RowUndo, RowUndoKind};
    use crate::catalog::{IndexID, IndexRef, IndexSlot};
    use crate::id::RowID;
    use std::mem::size_of;

    #[test]
    fn test_delete_successors_are_lazy_exact_and_bounded() {
        let mut delete = ForwardLinks::default();
        let first = IndexRef::new(IndexID::new(7), IndexSlot::new(0));
        let second = IndexRef::new(IndexID::new(8), IndexSlot::new(1));
        let recycled = IndexRef::new(IndexID::new(9), IndexSlot::new(0));
        assert_eq!(delete.successor(first), None);
        assert!(delete.successors.is_none());
        delete.set_successor(ForwardHint {
            index: first,
            row_id: RowID::new(10),
        });
        delete.set_successor(ForwardHint {
            index: second,
            row_id: RowID::new(20),
        });
        let allocated = delete.successors.as_deref().unwrap().as_ptr();
        let previous = delete.hint(first);
        delete.set_successor(ForwardHint {
            index: first,
            row_id: RowID::new(30),
        });
        assert_eq!(delete.successor(first), Some(RowID::new(30)));
        assert_eq!(delete.successor(second), Some(RowID::new(20)));
        assert_eq!(delete.successor(recycled), None);
        assert_eq!(delete.successors.as_deref().unwrap().len(), 2);
        assert_eq!(delete.successors.as_deref().unwrap().as_ptr(), allocated);
        delete.restore(first, previous);
        assert_eq!(delete.successor(first), Some(RowID::new(10)));
        assert_eq!(delete.successor(second), Some(RowID::new(20)));
        delete.restore(first, None);
        assert_eq!(delete.successor(first), None);
        assert_eq!(delete.successor(second), Some(RowID::new(20)));
        delete.restore(second, None);
        assert!(delete.successors.is_none());
        assert_eq!(
            size_of::<RowUndoKind>(),
            40,
            "Update contains before-images and lazy forward storage"
        );
        assert_eq!(size_of::<ForwardLinks>(), 16);
        assert_eq!(size_of::<ForwardHint>(), 16);
        assert_eq!(
            size_of::<RowUndo>(),
            136,
            "snapshot-visible undo excludes the restoration journal"
        );
        assert_eq!(
            size_of::<OwnedRowUndo>(),
            32,
            "the owner retains a Box and a lazily allocated Vec"
        );
    }
}
