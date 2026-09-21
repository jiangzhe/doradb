//! Borrowed foreground insertion and hot-row mutation execution.
//!
//! Physical rows and exact index layouts remain owned by their table. User
//! roots, cold ownership, and persisted-index completeness stay in access.rs.

use super::access::{
    InsertedRow, OwnedOldIndexEntry, OwnedRowIndexSetProof, UniqueIndexLinkTarget,
    UserTableAccessor,
};
use super::hot::{
    HotRowMutator, InsertRowIntoPage, RowInserter, UpdateRowInplace, publish_forward_hint,
};
use super::index_key::{WriteIndexKey, WriteIndexKeySet};
use super::{
    MemTable, RowStore, TableRootSnapshot, TableRuntimeLayout, index_key_is_changed,
    index_key_vals_replace, read_latest_index_key, row_len,
};
use crate::buffer::guard::PageSharedGuard;
use crate::buffer::{BufferPool, EvictableBufferPool, PoolGuards, PoolRole};
use crate::catalog::{IndexRef, ResolvedIndexKey, TableMetadata};
use crate::error::{
    FatalResult, MultiDomainResultExt, OperationError, QuadResult, RuntimeOrFatalError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::{RowID, TableID};
use crate::index::util::Maskable;
use crate::index::{
    GuardedNonUniqueMemIndex, GuardedUniqueMemIndex, InMemorySecondaryIndex, IndexCompareExchange,
    IndexInsert, IndexMask, LwcRowLocation, NonUniqueMemIndex, RowLocation, SecondaryIndex,
    UniqueInsertAttempt, UniqueLookupObservation, UniqueMemIndex, UniqueOwnerObservation,
    UniqueSecondaryIndex,
};
use crate::map::FastHashMap;
use crate::row::ops::{LinkForUniqueIndex, UpdateCol};
use crate::row::{RowPage, RowRead, estimate_max_row_count};
use crate::trx::row::{FindOldVersion, RowWriteAccess};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{ForwardHint, HotForwardSource, IndexBranch, RowUndoKind};
use crate::trx::{TrxContext, TrxRuntime};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::sync::Arc;

#[derive(Clone, Copy)]
struct RowIdMove<'a> {
    pub(super) source: &'a HotForwardSource,
    pub(super) old: RowID,
    pub(super) new: RowID,
}

impl<'a> RowIdMove<'a> {
    #[inline]
    const fn new(old: RowID, new: RowID, source: &'a HotForwardSource) -> Self {
        Self { source, old, new }
    }
}

/// Infallible projection of the mutable index owned by an exact layout entry.
pub(super) trait MemIndexRuntime {
    /// Buffer pool owning the projected index pages.
    type Pool: BufferPool + 'static;

    /// Borrows the existing mutable runtime without allocating or resolving roots.
    fn mem_index(&self) -> MemIndexRef<'_, Self::Pool>;
}

/// Borrowed mutable half of a secondary index.
pub(super) enum MemIndexRef<'a, P: 'static> {
    Unique(&'a UniqueMemIndex<P>),
    NonUnique(&'a NonUniqueMemIndex<P>),
}

impl<P: BufferPool> MemIndexRuntime for InMemorySecondaryIndex<P> {
    type Pool = P;

    #[inline]
    fn mem_index(&self) -> MemIndexRef<'_, P> {
        match self {
            Self::Unique(index) => MemIndexRef::Unique(index),
            Self::NonUnique(index) => MemIndexRef::NonUnique(index),
        }
    }
}

impl<P: BufferPool> MemIndexRuntime for Arc<SecondaryIndex<P>> {
    type Pool = P;

    #[inline]
    fn mem_index(&self) -> MemIndexRef<'_, P> {
        match self.as_ref() {
            SecondaryIndex::Unique { mem, .. } => MemIndexRef::Unique(mem),
            SecondaryIndex::NonUnique { mem, .. } => MemIndexRef::NonUnique(mem),
        }
    }
}

/// Concrete family binding established only from a complete table owner.
enum MutationFamily<'op> {
    Memory,
    User(&'op UserTableAccessor<'op>),
}

/// Root captured and validated by the admitted user accessor for one attempt.
pub(super) struct UserMutationAttempt<'op, 'ctx> {
    pub(super) accessor: &'op UserTableAccessor<'op>,
    pub(super) root: TableRootSnapshot<'ctx>,
}

/// Attempt-local routing and index authority, without synthetic memory roots.
pub(super) enum MutationAttempt<'op, 'ctx> {
    Memory,
    User(UserMutationAttempt<'op, 'ctx>),
}

impl MutationAttempt<'_, '_> {
    /// Borrows the user root when the attempt owns one.
    #[inline]
    pub(super) fn root(&self) -> Option<&TableRootSnapshot<'_>> {
        match self {
            Self::Memory => None,
            Self::User(attempt) => Some(&attempt.root),
        }
    }
}

/// Unique selection transfers its page pin; scan cursors keep their own pin.
pub(super) enum HotUpdatePage<'a> {
    Owned(PageSharedGuard<RowPage>),
    Borrowed(&'a PageSharedGuard<RowPage>),
}

impl HotUpdatePage<'_> {
    #[inline]
    fn page(&self) -> &PageSharedGuard<RowPage> {
        match self {
            Self::Owned(page) => page,
            Self::Borrowed(page) => page,
        }
    }
}

/// Shared execution borrows physical rows and the owner's existing exact layout.
pub(super) struct MutationExecutor<'op, D: 'static, R> {
    rows: &'op RowStore<D>,
    layout: &'op TableRuntimeLayout<R>,
    index_pool_role: PoolRole,
    family: MutationFamily<'op>,
}

impl<'op, D: BufferPool, I: BufferPool> MutationExecutor<'op, D, InMemorySecondaryIndex<I>> {
    /// Borrows a complete fixed-layout memory table.
    #[inline]
    pub(super) fn memory(table: &'op MemTable<D, I>) -> Self {
        Self {
            rows: &table.row_store,
            layout: &table.layout,
            index_pool_role: table.index_pool_role,
            family: MutationFamily::Memory,
        }
    }
}

impl<'op> MutationExecutor<'op, EvictableBufferPool, Arc<SecondaryIndex<EvictableBufferPool>>> {
    /// Borrows an admitted user accessor and its exact runtime layout.
    #[inline]
    pub(super) fn user(accessor: &'op UserTableAccessor<'op>) -> Self {
        Self {
            rows: accessor.row_store(),
            layout: accessor.layout(),
            index_pool_role: PoolRole::Index,
            family: MutationFamily::User(accessor),
        }
    }
}

impl<'op, D: BufferPool, R: MemIndexRuntime> MutationExecutor<'op, D, R> {
    /// Returns this exact owner's row store.
    #[inline]
    pub(super) fn row_store(&self) -> &'op RowStore<D> {
        self.rows
    }

    /// Returns the operation's existing runtime layout.
    #[inline]
    pub(super) fn layout(&self) -> &'op TableRuntimeLayout<R> {
        self.layout
    }

    /// Returns the bound immutable metadata.
    #[inline]
    pub(super) fn metadata(&self) -> &'op TableMetadata {
        self.layout.metadata()
    }

    /// Returns the physical table identity established by construction.
    #[inline]
    pub(super) fn table_id(&self) -> TableID {
        self.rows.table_id()
    }

    /// Captures user roots at the owning accessor boundary.
    #[inline]
    pub(super) fn begin_attempt<'ctx>(&self, ctx: &'ctx TrxContext) -> MutationAttempt<'op, 'ctx> {
        match self.family {
            MutationFamily::Memory => MutationAttempt::Memory,
            MutationFamily::User(accessor) => {
                MutationAttempt::User(accessor.begin_mutation_attempt(ctx))
            }
        }
    }

    /// Binds this family's unique index while retaining the user snapshot lifetime.
    #[inline]
    pub(super) fn bind_unique<'a, 'g>(
        &'a self,
        guards: &'g PoolGuards,
        index: IndexRef,
        root: Option<&'g TableRootSnapshot<'_>>,
    ) -> RuntimeResult<MutationIndex<'a, 'g, R::Pool>> {
        match self.family {
            MutationFamily::Memory => {
                assert!(
                    root.is_none(),
                    "memory mutation cannot bind a user root: table_id={}, index={index}",
                    self.table_id()
                );
                Ok(MutationIndex::Memory(self.unique_mem(guards, index)))
            }
            MutationFamily::User(accessor) => {
                Ok(MutationIndex::User(accessor.snapshot_unique_index(
                    guards,
                    root.expect("user mutation requires its captured root"),
                    index,
                )?))
            }
        }
    }

    /// Uses the existing user route-publication wait after releasing attempt resources.
    #[inline]
    pub(super) async fn wait_transition(
        &self,
        rt: TrxRuntime<'_>,
        row_id: RowID,
    ) -> FatalResult<()> {
        match self.family {
            MutationFamily::Memory => unreachable!(
                "memory mutation cannot observe TRANSITION: table_id={}, row_id={row_id}",
                self.table_id()
            ),
            MutationFamily::User(accessor) => {
                accessor
                    .table()
                    .wait_transition_route_or_poison(&rt.engine().poisoner, row_id)
                    .await
            }
        }
    }

    #[inline]
    fn unique_mem<'g>(
        &self,
        guards: &'g PoolGuards,
        index: IndexRef,
    ) -> GuardedUniqueMemIndex<'_, 'g, R::Pool> {
        match self.layout.expect_index_entry(index).runtime().mem_index() {
            MemIndexRef::Unique(mem) => mem.bind(guards.guard(self.index_pool_role)),
            MemIndexRef::NonUnique(_) => panic!(
                "unique mutation requires unique runtime: table_id={}, index={index}",
                self.table_id()
            ),
        }
    }

    #[inline]
    fn non_unique_mem<'g>(
        &self,
        guards: &'g PoolGuards,
        index: IndexRef,
    ) -> GuardedNonUniqueMemIndex<'_, 'g, R::Pool> {
        match self.layout.expect_index_entry(index).runtime().mem_index() {
            MemIndexRef::NonUnique(mem) => mem.bind(guards.guard(self.index_pool_role)),
            MemIndexRef::Unique(_) => panic!(
                "non-unique mutation requires non-unique runtime: table_id={}, index={index}",
                self.table_id()
            ),
        }
    }

    #[inline]
    fn retained_key(&self, index: IndexRef, vals: Vec<Val>) -> ResolvedIndexKey {
        // Keys already retain an exact admitted entry. Preserve its binding
        // without re-entering external selector validation or the IndexID map.
        self.layout.expect_index_entry(index);
        ResolvedIndexKey::new(index, vals)
    }

    #[inline]
    fn debug_assert_table_write_lock_held(&self, rt: TrxRuntime<'_>) {
        rt.debug_assert_table_write_lock_held(self.table_id());
    }

    #[inline]
    fn sec_idx_is_unique(&self, index: IndexRef) -> bool {
        matches!(
            self.layout.expect_index_entry(index).runtime().mem_index(),
            MemIndexRef::Unique(_)
        )
    }

    /// Routes memory candidates directly and delegates user routing to its owner.
    #[inline]
    pub(super) async fn resolve_row_location(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
    ) -> RuntimeOrFatalResult<RowLocation> {
        match self.family {
            MutationFamily::Memory => match self.rows.find_row(guards, row_id).await? {
                RowLocation::LwcBlock(_) => panic!(
                    "memory mutation cannot route to LWC: table_id={}, row_id={row_id}",
                    self.table_id()
                ),
                location => Ok(location),
            },
            MutationFamily::User(accessor) => accessor.resolve_row_location(guards, row_id).await,
        }
    }

    /// Rolls back only the current invocation's provisional hot lock.
    #[inline]
    pub(super) fn cancel_owned_hot_row(
        &self,
        effects: &mut StmtEffects,
        mut access: RowWriteAccess<'_>,
    ) {
        effects.cancel_last_row_undo_lock(|undo| {
            access.rollback_first_undo(self.rows.column_layout(), undo);
        });
    }

    /// Binds a complete stable hot key set; user construction consumes RowPage proof.
    #[inline]
    pub(super) fn owned_hot_index_set<'snapshot, 'ctx>(
        &self,
        effects: &StmtEffects,
        row_id: RowID,
        keys: WriteIndexKeySet<'op>,
        root: Option<&'snapshot TableRootSnapshot<'ctx>>,
    ) -> OwnedHotIndexSet<'op, 'snapshot, 'ctx> {
        self.assert_owned_hot_effect(effects, row_id);
        match self.family {
            MutationFamily::Memory => {
                assert!(root.is_none());
                OwnedHotIndexSet {
                    row_id,
                    keys,
                    root_snapshot: None,
                }
            }
            MutationFamily::User(accessor) => {
                OwnedHotIndexSet::from_user(accessor.owned_row_page_index_set_proof(
                    row_id,
                    keys,
                    root.expect("user hot authority requires root"),
                ))
            }
        }
    }

    #[inline]
    fn owned_hot_index_entry<'snapshot, 'ctx>(
        &self,
        effects: &StmtEffects,
        row_id: RowID,
        key: WriteIndexKey<'op>,
        root: Option<&'snapshot TableRootSnapshot<'ctx>>,
    ) -> OwnedHotIndexEntry<'op, 'snapshot, 'ctx> {
        self.assert_owned_hot_effect(effects, row_id);
        match self.family {
            MutationFamily::Memory => {
                assert!(root.is_none());
                OwnedHotIndexEntry {
                    row_id,
                    key,
                    root_snapshot: None,
                }
            }
            MutationFamily::User(accessor) => {
                OwnedHotIndexEntry::from_user(accessor.owned_row_page_index_entry(
                    row_id,
                    key,
                    root.expect("user hot authority requires root"),
                ))
            }
        }
    }

    #[inline]
    fn assert_owned_hot_effect(&self, effects: &StmtEffects, row_id: RowID) {
        // Synchronous hot undo conversion is the authority boundary. Binding
        // before insertion replaces the newest effect prevents a destination
        // or a CDB marker from authorizing the source's complete hot index set.
        let undo = effects.last_row_undo();
        assert!(
            undo.table_id == self.table_id()
                && undo.row_id == row_id
                && undo.page_id.is_some()
                && matches!(undo.kind, RowUndoKind::Delete(_) | RowUndoKind::Update(_)),
            "hot index authority requires this row's converted hot undo: table_id={}, row_id={row_id}, undo_table_id={}, undo_row_id={}, undo_kind={:?}",
            self.table_id(),
            undo.table_id,
            undo.row_id,
            undo.kind
        );
    }

    #[inline]
    fn assert_new_hot_row(&self, row_id: RowID, root: Option<&TableRootSnapshot<'_>>) {
        if let MutationFamily::User(accessor) = self.family {
            accessor.assert_new_hot_row(row_id, root.expect("user hot insertion requires root"));
        }
    }

    #[inline]
    async fn get_insert_page(
        &self,
        rt: TrxRuntime<'_>,
        row_count: usize,
    ) -> RuntimeOrFatalResult<PageSharedGuard<RowPage>> {
        match self.family {
            MutationFamily::Memory => Ok(self
                .rows
                .try_get_insert_page(rt.pool_guards(), row_count)
                .await?),
            MutationFamily::User(accessor) => accessor.get_insert_page(rt, row_count).await,
        }
    }

    #[inline]
    fn push_insert_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        index: IndexRef,
        vals: Vec<Val>,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_insert_unique_index_undo(
            self.table_id(),
            row_id,
            self.retained_key(index, vals),
            merge_old_deleted,
        );
    }

    #[inline]
    fn push_insert_non_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        index: IndexRef,
        vals: Vec<Val>,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_insert_non_unique_index_undo(
            self.table_id(),
            row_id,
            self.retained_key(index, vals),
            merge_old_deleted,
        );
    }

    #[inline]
    fn push_delete_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        index: IndexRef,
        vals: Vec<Val>,
        unique: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_delete_index_undo(
            self.table_id(),
            row_id,
            self.retained_key(index, vals),
            unique,
        );
    }

    #[inline]
    fn push_update_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        key: ResolvedIndexKey,
        old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_update_unique_index_undo(
            self.table_id(),
            old_row_id,
            new_row_id,
            key,
            old_deleted,
        );
    }

    /// Retries physical insertion with the same owned row and backward branches.
    pub(super) async fn insert_row(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        mut insert: Vec<Val>,
        mut undo_kind: RowUndoKind,
        mut index_branches: Vec<IndexBranch>,
    ) -> RuntimeOrFatalResult<(RowID, PageSharedGuard<RowPage>)> {
        let metadata = self.metadata();
        let row_len = row_len(metadata, &insert);
        let row_count = estimate_max_row_count(row_len, metadata.col.col_count());
        let inserter = RowInserter::new(self.table_id(), metadata, rt);
        loop {
            let page_guard = self.get_insert_page(rt, row_count).await?;
            match inserter.insert_to_page(effects, page_guard, insert, undo_kind, index_branches) {
                InsertRowIntoPage::Ok(row_id, page_guard) => {
                    match self.family {
                        MutationFamily::Memory => self
                            .rows
                            .cache_insert_page_version(page_guard.versioned_page_id()),
                        MutationFamily::User(_) => rt.save_active_insert_page(
                            self.table_id(),
                            page_guard.versioned_page_id(),
                        ),
                    }
                    return Ok((row_id, page_guard));
                }
                // this page cannot be inserted any more, just leave it and retry another page.
                InsertRowIntoPage::NoSpaceOrFrozen(ins, uk, ib) => {
                    insert = ins;
                    undo_kind = uk;
                    index_branches = ib;
                }
            }
        }
    }

    async fn move_update_for_space(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row: Vec<Val>,
        update: Vec<UpdateCol>,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> RuntimeOrFatalResult<(
        RowID,
        FastHashMap<usize, Val>,
        PageSharedGuard<RowPage>,
        HotForwardSource,
    )> {
        let prepared = HotRowMutator::new(self.table_id(), self.metadata(), rt, &old_guard, old_id)
            .prepare_move_update(old_row, update, |key, target, undo_vals| {
                let key = WriteIndexKey::new(self.layout(), key);
                let (index, vals) = key.into_parts();
                IndexBranch::new(self.retained_key(index, vals), target, undo_vals)
            });
        // Release the old row page before awaiting replacement-row insertion.
        drop(old_guard);
        let (new_row_id, new_guard) = self
            .insert_row(
                rt,
                effects,
                prepared.row,
                RowUndoKind::Insert,
                prepared.index_branches,
            )
            .await?;
        // do not unlock the page because we may need to update index
        Ok((
            new_row_id,
            prepared.index_change_cols,
            new_guard,
            prepared.source,
        ))
    }

    async fn update_indexes_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &FastHashMap<usize, Val>,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> QuadResult<()> {
        let metadata = self.metadata();
        for (index_schema, entry) in self.layout.active_indexes() {
            let index_slot = entry.index_ref().slot();
            if index_key_is_changed(index_schema, index_change_cols) {
                let new_key = WriteIndexKey::new(
                    self.layout(),
                    read_latest_index_key(metadata, index_slot, page_guard, row_id),
                );
                debug_assert_eq!(
                    self.sec_idx_is_unique(new_key.index_ref()),
                    index_schema.unique()
                );
                let old_entry = self.owned_hot_index_entry(
                    effects,
                    row_id,
                    new_key.with_vals(index_key_vals_replace(
                        index_schema,
                        new_key.vals(),
                        index_change_cols,
                    )),
                    root_snapshot,
                );
                // First we need to insert new entry to index due to key change.
                // There might be conflict we will try to fix (if old one is already deleted).
                // Once the insert is done, we also need to defer deletion of original key.
                if index_schema.unique() {
                    self.update_unique_index_only_key_change(
                        rt, effects, old_entry, new_key, page_guard,
                    )
                    .await?;
                } else {
                    self.update_non_unique_index_only_key_change(rt, effects, old_entry, new_key)
                        .await?;
                }
            } // otherwise, in-place update do not change row id, so we do nothing
        }
        Ok(())
    }

    async fn update_indexes_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        proof: OwnedHotIndexSet<'_, '_, '_>,
        source: &HotForwardSource,
    ) -> RuntimeOrFatalResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        let source_page = self
            .row_store()
            .pin_forward_source(rt, Some(source))
            .await?;
        assert_eq!(
            proof.row_id,
            old_row_id,
            "owned-row index-set invariant violated: move proof RowID mismatch, table_id={}, proof_row_id={}, old_row_id={old_row_id}, new_row_id={new_row_id}",
            self.table_id(),
            proof.row_id
        );
        for old_entry in proof.into_entries() {
            let index_schema = metadata.idx.expect_index_spec(old_entry.key.index_ref());
            debug_assert_eq!(
                self.sec_idx_is_unique(old_entry.key.index_ref()),
                index_schema.unique()
            );
            if index_schema.unique() {
                self.update_unique_index_only_row_id_change(rt, effects, old_entry, new_row_id)
                    .await?;
                publish_forward_hint(
                    rt.ctx(),
                    effects,
                    Some(source),
                    source_page.as_ref(),
                    ForwardHint {
                        index: index_schema.index,
                        row_id: new_row_id,
                    },
                );
            } else {
                self.update_non_unique_index_only_row_id_change(rt, effects, old_entry, new_row_id)
                    .await?;
            }
        }
        Ok(())
    }

    async fn update_indexes_may_both_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id_move: RowIdMove<'_>,
        index_change_cols: &FastHashMap<usize, Val>,
        page_guard: &PageSharedGuard<RowPage>,
        proof: OwnedHotIndexSet<'_, '_, '_>,
    ) -> QuadResult<()> {
        debug_assert!(row_id_move.old != row_id_move.new);
        let metadata = self.metadata();
        let source_page = self
            .row_store()
            .pin_forward_source(rt, Some(row_id_move.source))
            .await?;
        assert_eq!(
            proof.row_id,
            row_id_move.old,
            "owned-row index-set invariant violated: move proof RowID mismatch, table_id={}, proof_row_id={}, old_row_id={}, new_row_id={}",
            self.table_id(),
            proof.row_id,
            row_id_move.old,
            row_id_move.new
        );
        for old_entry in proof.into_entries() {
            let index_slot = old_entry.key.index_slot();
            let index_schema = metadata.idx.expect_index_spec(old_entry.key.index_ref());
            debug_assert_eq!(
                self.sec_idx_is_unique(old_entry.key.index_ref()),
                index_schema.unique()
            );
            if index_key_is_changed(index_schema, index_change_cols) {
                let key = WriteIndexKey::new(
                    self.layout(),
                    read_latest_index_key(metadata, index_slot, page_guard, row_id_move.new),
                );
                debug_assert_eq!(old_entry.key.index_ref(), key.index_ref());
                debug_assert_eq!(
                    old_entry.key.vals(),
                    index_key_vals_replace(index_schema, key.vals(), index_change_cols)
                );
                // key change and row id change.
                if index_schema.unique() {
                    self.update_unique_index_key_and_row_id_change(
                        rt,
                        effects,
                        old_entry,
                        key,
                        row_id_move.new,
                        page_guard,
                    )
                    .await?;
                } else {
                    self.update_non_unique_index_key_and_row_id_change(
                        rt,
                        effects,
                        old_entry,
                        key,
                        row_id_move.new,
                    )
                    .await?;
                }
            } else {
                // only row id change.
                if index_schema.unique() {
                    self.update_unique_index_only_row_id_change(
                        rt,
                        effects,
                        old_entry,
                        row_id_move.new,
                    )
                    .await?;
                    publish_forward_hint(
                        rt.ctx(),
                        effects,
                        Some(row_id_move.source),
                        source_page.as_ref(),
                        ForwardHint {
                            index: index_schema.index,
                            row_id: row_id_move.new,
                        },
                    );
                } else {
                    self.update_non_unique_index_only_row_id_change(
                        rt,
                        effects,
                        old_entry,
                        row_id_move.new,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn link_for_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        old_id: RowID,
        index_ref: IndexRef,
        key_vals: &[Val],
        target: UniqueIndexLinkTarget<'_>,
        resolved_lwc: Option<LwcRowLocation>,
    ) -> QuadResult<LinkForUniqueIndex> {
        let index_slot = index_ref.slot();
        debug_assert!(old_id != target.row_id);
        let mut resolved_lwc = resolved_lwc;
        let (old_guard, old_id) = loop {
            let location = match resolved_lwc.take() {
                Some(location) => Ok(RowLocation::LwcBlock(location)),
                None => self.resolve_row_location(rt.pool_guards(), old_id).await,
            };
            match location {
                Ok(RowLocation::NotFound) => return Ok(LinkForUniqueIndex::NotNeeded),
                Ok(RowLocation::LwcBlock(location)) => {
                    let MutationFamily::User(accessor) = self.family else {
                        unreachable!("memory unique owner cannot be cold");
                    };
                    return accessor
                        .link_for_unique_index_lwc(
                            rt, old_id, index_ref, key_vals, target, location,
                        )
                        .await;
                }
                Ok(RowLocation::RowPage(page_id)) => {
                    // A hot duplicate candidate must be inspected through its
                    // row-page undo chain. It may be a stale latest mapping, a
                    // deleted owner that older snapshots still need, or a true
                    // duplicate visible to this transaction.
                    let Some(old_guard) = self
                        .row_store()
                        .try_get_validated_row_page_shared_result(rt.pool_guards(), page_id, old_id)
                        .await?
                    else {
                        continue;
                    };
                    break (old_guard, old_id);
                }
                Err(err) => return Err(err.into()),
            }
        };
        // Find a non-deleted old hot version that matches the unique key. If
        // this transaction cannot see that version, a runtime branch from the
        // new owner to the old owner's undo chain preserves it for older
        // snapshots. If this transaction can see it, the new claim is a real
        // duplicate.
        let metadata = self.metadata();
        let old_access = old_guard.read_row_by_id(old_id);
        match old_access
            .find_old_version_for_unique_key(metadata, index_slot, key_vals, rt.ctx())
            .attach_with(|| format!("operation=link_for_unique_index, index={index_ref}"))?
        {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::Found(old_row, end_cts, old_entry) => {
                let source = old_access
                    .undo_head()
                    .and_then(|head| HotForwardSource::new(rt.ctx(), head, &old_entry));
                // row latch is enough, because row lock is already acquired.
                let mut new_access = target.guard.write_row_by_id(target.row_id);
                assert!(
                    new_access.owned_by_trx(rt.ctx()),
                    "unique hint publication requires writer-owned destination"
                );
                let undo_vals = new_access.row().calc_delta(metadata.col.as_ref(), &old_row);
                new_access.link_for_unique_index(
                    self.retained_key(index_ref, key_vals.to_vec()),
                    end_cts,
                    old_entry,
                    undo_vals,
                );
                Ok(LinkForUniqueIndex::Linked(source))
            }
        }
    }

    /// Inserts a complete row using one initial key derivation and one claim per index.
    pub(super) async fn insert_in_attempt(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        vals: Vec<Val>,
        root: Option<&TableRootSnapshot<'_>>,
    ) -> QuadResult<(RowID, PageSharedGuard<RowPage>)> {
        let keys = WriteIndexKeySet::from_full_row(self.layout, &vals);
        let (row_id, page) = self
            .insert_row(rt, effects, vals, RowUndoKind::Insert, Vec::new())
            .await?;
        self.assert_new_hot_row(row_id, root);
        for key in keys.into_keys() {
            if self
                .metadata()
                .idx
                .expect_index_spec(key.index_ref())
                .unique()
            {
                self.insert_unique_index(
                    rt,
                    effects,
                    key,
                    UniqueIndexLinkTarget::new(row_id, &page),
                    root,
                    false,
                )
                .await?;
            } else {
                self.insert_non_unique_index(rt, effects, key, row_id, root, false)
                    .await?;
            }
        }
        Ok((row_id, page))
    }

    /// Begins a fresh insertion attempt, including for a selected missing entry.
    #[inline]
    pub(super) async fn insert_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        vals: Vec<Val>,
    ) -> QuadResult<RowID> {
        let attempt = self.begin_attempt(rt.ctx());
        let (row_id, _) = self
            .insert_in_attempt(rt, effects, vals, attempt.root())
            .await?;
        Ok(row_id)
    }

    /// Claims a destination key, retaining exact previous-owner evidence across inspection.
    async fn insert_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: WriteIndexKey<'_>,
        target: UniqueIndexLinkTarget<'_>,
        root: Option<&TableRootSnapshot<'_>>,
        merge: bool,
    ) -> QuadResult<()> {
        let row_id = target.row_id;
        let page = target.guard;
        self.assert_new_hot_row(row_id, root);
        let (index_ref, vals) = key.into_parts();
        let index = self.bind_unique(rt.pool_guards(), index_ref, root)?;
        loop {
            let claim = match index.claim(&vals, row_id, merge, rt).await? {
                ClaimAttempt::Inserted(merged) => {
                    self.push_insert_unique_index_undo(
                        rt, effects, row_id, index_ref, vals, merged,
                    );
                    return Ok(());
                }
                ClaimAttempt::Occupied(claim) => claim,
            };
            let (old_row_id, deleted) = claim.owner();
            let resolved_lwc = if deleted {
                None
            } else {
                let location = match self.family {
                    MutationFamily::Memory => None,
                    MutationFamily::User(accessor) => {
                        accessor
                            .resolve_unmasked_lwc_duplicate(rt, old_row_id)
                            .await?
                    }
                };
                Some(location.ok_or_else(|| {
                    Report::new(OperationError::DuplicateKey).attach(format!(
                        "unique key claim: table_id={}, index={index_ref}, row_id={row_id}",
                        self.table_id()
                    ))
                })?)
            };
            let link = self
                .link_for_unique_index(
                    rt,
                    old_row_id,
                    index_ref,
                    &vals,
                    UniqueIndexLinkTarget::new(row_id, page),
                    resolved_lwc,
                )
                .await?;
            // Pin the actual departure before exchange. Index undo precedes the
            // synchronous forward publication and its restoration journal.
            let source_page = self.rows.pin_forward_source(rt, link.source()).await?;
            match claim.replace(row_id, rt).await? {
                IndexCompareExchange::Ok => {
                    self.push_update_unique_index_undo(rt, effects, old_row_id, row_id, self.retained_key(index_ref, vals), deleted);
                    publish_forward_hint(rt.ctx(), effects, link.source(), source_page.as_ref(), ForwardHint { index: index_ref, row_id });
                    return Ok(());
                }
                IndexCompareExchange::NotExists => (), // Purge won; retry the claim, never the callback.
                IndexCompareExchange::Mismatch => return Err(Report::new(OperationError::WriteConflict).attach(format!("unique owner changed during claim: table_id={}, index={index_ref}, row_id={row_id}", self.table_id())).into()),
            }
        }
    }

    #[inline]
    async fn insert_non_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: WriteIndexKey<'_>,
        row_id: RowID,
        root: Option<&TableRootSnapshot<'_>>,
        merge: bool,
    ) -> RuntimeOrFatalResult<()> {
        self.assert_new_hot_row(row_id, root);
        let (index, vals) = key.into_parts();
        match self
            .non_unique_mem(rt.pool_guards(), index)
            .insert_if_not_exists(&vals, row_id, merge, rt.sts())
            .await
            .map_err(Into::<RuntimeOrFatalError>::into)?
        {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(rt, effects, row_id, index, vals, merged)
            }
            IndexInsert::DuplicateKey(..) => {
                unreachable!("writer-owned hot destination has no active non-unique entry")
            }
        }
        Ok(())
    }

    #[inline]
    async fn defer_delete_owned_old_index_entry(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
    ) -> RuntimeOrFatalResult<()> {
        let row_id = entry.row_id;
        let (index, vals) = entry.key.into_parts();
        let unique = self.sec_idx_is_unique(index);
        // Stable hot ownership requires the exact active entry. Persisted-row
        // optional copies are consumed separately by the user CDB owner.
        if unique {
            let result = self
                .unique_mem(rt.pool_guards(), index)
                .compare_exchange(&vals, row_id, row_id.deleted(), rt.sts())
                .await
                .map_err(Into::<RuntimeOrFatalError>::into)?;
            assert_eq!(
                result,
                IndexCompareExchange::Ok,
                "owned hot unique mask requires exact owner: table_id={}, index={index}, row_id={row_id}",
                self.table_id()
            );
        } else {
            let result = self
                .non_unique_mem(rt.pool_guards(), index)
                .mask_if_present(&vals, row_id, rt.sts())
                .await
                .map_err(Into::<RuntimeOrFatalError>::into)?;
            assert_eq!(
                result,
                IndexMask::Masked,
                "owned hot non-unique mask requires exact owner: table_id={}, index={index}, row_id={row_id}",
                self.table_id()
            );
        }
        self.push_delete_index_undo(rt, effects, row_id, index, vals, unique);
        Ok(())
    }

    /// Consumes every old hot entry after physical deletion and page release.
    #[inline]
    pub(super) async fn defer_delete_owned_row_index_set(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        proof: OwnedHotIndexSet<'_, '_, '_>,
    ) -> RuntimeOrFatalResult<()> {
        for entry in proof.into_entries() {
            self.defer_delete_owned_old_index_entry(rt, effects, entry)
                .await?;
        }
        Ok(())
    }

    #[inline]
    async fn update_unique_index_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
        new_row_id: RowID,
    ) -> RuntimeOrFatalResult<()> {
        self.assert_new_hot_row(new_row_id, entry.root_snapshot);
        let old_row_id = entry.row_id;
        let (index, vals) = entry.key.into_parts();
        let result = self
            .unique_mem(rt.pool_guards(), index)
            .compare_exchange(&vals, old_row_id, new_row_id, rt.sts())
            .await
            .map_err(Into::<RuntimeOrFatalError>::into)?;
        assert_eq!(
            result,
            IndexCompareExchange::Ok,
            "owned hot unique replacement requires exact owner: table_id={}, index={index}, old_row_id={old_row_id}, new_row_id={new_row_id}",
            self.table_id()
        );
        self.push_update_unique_index_undo(
            rt,
            effects,
            old_row_id,
            new_row_id,
            self.retained_key(index, vals),
            false,
        );
        Ok(())
    }

    #[inline]
    async fn update_non_unique_index_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
        new_row_id: RowID,
    ) -> RuntimeOrFatalResult<()> {
        let new_key = entry.key.with_vals(entry.key.vals().to_vec());
        self.update_non_unique_index_key_and_row_id_change(rt, effects, entry, new_key, new_row_id)
            .await
    }

    #[inline]
    async fn update_unique_index_key_and_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
        key: WriteIndexKey<'_>,
        new_row_id: RowID,
        page: &PageSharedGuard<RowPage>,
    ) -> QuadResult<()> {
        self.insert_unique_index(
            rt,
            effects,
            key,
            UniqueIndexLinkTarget::new(new_row_id, page),
            entry.root_snapshot,
            false,
        )
        .await?;
        self.defer_delete_owned_old_index_entry(rt, effects, entry)
            .await?;
        Ok(())
    }

    #[inline]
    async fn update_non_unique_index_key_and_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
        key: WriteIndexKey<'_>,
        new_row_id: RowID,
    ) -> RuntimeOrFatalResult<()> {
        self.insert_non_unique_index(rt, effects, key, new_row_id, entry.root_snapshot, false)
            .await?;
        self.defer_delete_owned_old_index_entry(rt, effects, entry)
            .await
    }

    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
        key: WriteIndexKey<'_>,
        page: &PageSharedGuard<RowPage>,
    ) -> QuadResult<()> {
        self.insert_unique_index(
            rt,
            effects,
            key,
            UniqueIndexLinkTarget::new(entry.row_id, page),
            entry.root_snapshot,
            true,
        )
        .await?;
        self.defer_delete_owned_old_index_entry(rt, effects, entry)
            .await?;
        Ok(())
    }

    #[inline]
    async fn update_non_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedHotIndexEntry<'_, '_, '_>,
        key: WriteIndexKey<'_>,
    ) -> RuntimeOrFatalResult<()> {
        self.insert_non_unique_index(rt, effects, key, entry.row_id, entry.root_snapshot, true)
            .await?;
        self.defer_delete_owned_old_index_entry(rt, effects, entry)
            .await
    }

    /// Applies an owned hot update and its shared in-place or move effects.
    pub(super) async fn continue_hot_update(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        page: HotUpdatePage<'_>,
        result: UpdateRowInplace,
        root_snapshot: Option<&TableRootSnapshot<'_>>,
    ) -> QuadResult<(RowID, Option<InsertedRow>)> {
        let accessor = self;
        let page_guard = page.page();
        match result {
            UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                let row_id = new_row_id;
                if !index_change_cols.is_empty() {
                    accessor
                        .update_indexes_only_key_change(
                            rt,
                            effects,
                            row_id,
                            page_guard,
                            &index_change_cols,
                            root_snapshot,
                        )
                        .await
                        .attach("index-driven mutation hot key change")?;
                }
                Ok((new_row_id, None))
            }
            UpdateRowInplace::NoFreeSpaceOrFrozen(old_row_id, old_row, update) => {
                let old_index_keys = WriteIndexKeySet::from_full_row(accessor.layout(), &old_row);
                let proof = accessor.owned_hot_index_set(
                    effects,
                    old_row_id,
                    old_index_keys,
                    root_snapshot,
                );
                let move_guard = match page {
                    HotUpdatePage::Owned(page) => page,
                    HotUpdatePage::Borrowed(page) => {
                        accessor
                            .row_store()
                            .must_get_row_page_shared(rt.pool_guards(), page.page_id())
                            .await?
                    }
                };
                let (new_row_id, index_change_cols, new_guard, source) = accessor
                    .move_update_for_space(rt, effects, old_row, update, old_row_id, move_guard)
                    .await?;
                if index_change_cols.is_empty() {
                    accessor
                        .update_indexes_only_row_id_change(
                            rt, effects, old_row_id, new_row_id, proof, &source,
                        )
                        .await
                        .attach("index-driven mutation hot move index update")?;
                } else {
                    accessor
                        .update_indexes_may_both_change(
                            rt,
                            effects,
                            RowIdMove::new(old_row_id, new_row_id, &source),
                            &index_change_cols,
                            &new_guard,
                            proof,
                        )
                        .await
                        .attach("index-driven mutation hot move index update")?;
                }
                Ok((
                    new_row_id,
                    Some(InsertedRow::new(new_guard.page_id(), new_row_id)),
                ))
            }
            UpdateRowInplace::RowDeleted
            | UpdateRowInplace::RowNotFound
            | UpdateRowInplace::RetryInTransition => {
                unreachable!("retained owned hot row changed before physical update")
            }
        }
    }
}

/// Consuming authority for a complete old hot index set in active slot order.
pub(super) struct OwnedHotIndexSet<'op, 'snapshot, 'ctx> {
    row_id: RowID,
    keys: WriteIndexKeySet<'op>,
    root_snapshot: Option<&'snapshot TableRootSnapshot<'ctx>>,
}

impl<'op, 'snapshot, 'ctx> OwnedHotIndexSet<'op, 'snapshot, 'ctx> {
    /// Converts a user RowPage proof after the user owner checks its authority.
    #[inline]
    pub(super) fn from_user(proof: OwnedRowIndexSetProof<'op, 'snapshot, 'ctx>) -> Self {
        let (row_id, keys, root_snapshot) = proof.into_hot_parts();
        Self {
            row_id,
            keys,
            root_snapshot: Some(root_snapshot),
        }
    }

    #[inline]
    fn into_entries(self) -> impl Iterator<Item = OwnedHotIndexEntry<'op, 'snapshot, 'ctx>> {
        self.keys.into_keys().map(move |key| OwnedHotIndexEntry {
            row_id: self.row_id,
            key,
            root_snapshot: self.root_snapshot,
        })
    }
}

/// Selective old hot authority for one affected in-place index entry.
pub(super) struct OwnedHotIndexEntry<'op, 'snapshot, 'ctx> {
    row_id: RowID,
    key: WriteIndexKey<'op>,
    root_snapshot: Option<&'snapshot TableRootSnapshot<'ctx>>,
}

impl<'op, 'snapshot, 'ctx> OwnedHotIndexEntry<'op, 'snapshot, 'ctx> {
    /// Converts a selective user RowPage proof without admitting CDB ownership.
    #[inline]
    pub(super) fn from_user(entry: OwnedOldIndexEntry<'op, 'snapshot, 'ctx>) -> Self {
        let (row_id, key, root_snapshot) = entry.into_hot_parts();
        Self {
            row_id,
            key,
            root_snapshot: Some(root_snapshot),
        }
    }
}

/// Bound unique lookup/claim view; both variants borrow existing runtimes.
pub(super) enum MutationIndex<'a, 'g, P: 'static> {
    Memory(GuardedUniqueMemIndex<'a, 'g, P>),
    User(UniqueSecondaryIndex<'a, 'g, EvictableBufferPool>),
}

impl<'a, 'g, P: BufferPool> MutationIndex<'a, 'g, P> {
    /// Retains the original lookup observation for the complete forward walk.
    #[inline]
    pub(super) async fn lookup_observed<'lookup>(
        &'lookup self,
        key: &'lookup [Val],
    ) -> RuntimeOrFatalResult<(Option<(RowID, bool)>, UniqueLookupObservation<'lookup>)> {
        match self {
            Self::Memory(index) => index.lookup_observed(key).await.map_err(Into::into),
            Self::User(index) => index.lookup_observed(key).await,
        }
    }

    #[inline]
    async fn claim<'k>(
        &self,
        key: &'k [Val],
        row_id: RowID,
        merge: bool,
        rt: TrxRuntime<'_>,
    ) -> RuntimeOrFatalResult<ClaimAttempt<'a, 'g, 'k, P>> {
        Ok(match self {
            Self::Memory(index) => match index
                .insert_if_not_exists(key, row_id, merge, rt.sts())
                .await
                .map_err(Into::<RuntimeOrFatalError>::into)?
            {
                IndexInsert::Ok(merged) => ClaimAttempt::Inserted(merged),
                IndexInsert::DuplicateKey(owner, deleted) => {
                    ClaimAttempt::Occupied(MutationClaim::Memory {
                        index: *index,
                        key,
                        owner,
                        deleted,
                    })
                }
            },
            Self::User(index) => match index
                .insert_if_not_exists_observed(key, row_id, merge, rt.sts())
                .await?
            {
                UniqueInsertAttempt::Inserted { merged } => ClaimAttempt::Inserted(merged),
                UniqueInsertAttempt::Occupied(observation) => {
                    ClaimAttempt::Occupied(MutationClaim::User(observation))
                }
            },
        })
    }
}

/// Exact previous-owner evidence consumed by a successful replacement.
enum MutationClaim<'a, 'g, 'k, P: 'static> {
    Memory {
        index: GuardedUniqueMemIndex<'a, 'g, P>,
        key: &'k [Val],
        owner: RowID,
        deleted: bool,
    },
    User(UniqueOwnerObservation<'a, 'g, 'k, EvictableBufferPool>),
}

impl<P: BufferPool> MutationClaim<'_, '_, '_, P> {
    #[inline]
    fn owner(&self) -> (RowID, bool) {
        match self {
            Self::Memory { owner, deleted, .. } => (*owner, *deleted),
            Self::User(observation) => (observation.owner_row_id(), observation.deleted()),
        }
    }

    #[inline]
    async fn replace(
        self,
        row_id: RowID,
        rt: TrxRuntime<'_>,
    ) -> RuntimeOrFatalResult<IndexCompareExchange> {
        match self {
            Self::Memory {
                index,
                key,
                owner,
                deleted,
            } => index
                .compare_exchange(
                    key,
                    if deleted { owner.deleted() } else { owner },
                    row_id,
                    rt.sts(),
                )
                .await
                .map_err(Into::into),
            Self::User(observation) => observation.replace(row_id, rt.sts()).await,
        }
    }
}

enum ClaimAttempt<'a, 'g, 'k, P: 'static> {
    Inserted(bool),
    Occupied(MutationClaim<'a, 'g, 'k, P>),
}
