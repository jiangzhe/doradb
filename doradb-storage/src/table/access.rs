#[path = "index_mutate.rs"]
mod index_mutate;

use super::hot::{
    DeleteInternal, HotRowMutator, InsertRowIntoPage, RowInserter, UpdateRowInplace,
    read_hot_row_mvcc,
};
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::buffer::{EvictableBufferPool, PoolGuards};
use crate::catalog::{TableColumnLayout, TableMetadata};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, DiscloseError, DiscloseResultExt, FatalResult,
    InternalError, MultiDomainResultExt, OperationError, OperationOrFatalResult,
    OperationOrRuntimeError, OperationOrRuntimeResult, OperationResult, QuadResult, Result,
    RuntimeError, RuntimeOrFatalResult, RuntimeResult,
};
use crate::file::FileKind;
use crate::file::cow_file::SUPER_BLOCK_ID;
use crate::file::table_file::ActiveRoot;
use crate::id::{BlockID, PageID, RowID, TableID, TrxID};
use crate::index::util::{Maskable, RowPageCreateRedoCtx};
use crate::index::{
    BorrowedIndexMutationStream, ColumnBlockIndex, ColumnLeafEntry, CurrentIndexReadHandle,
    IndexBatchStream, IndexCompareExchange, IndexInsert, IndexMask, KeyRange, LwcRowLocation,
    NonUniqueSecondaryIndex, OwnedCurrentIndexReadHandle, OwnedIndexCandidateStream, RowLocation,
    SecondaryIndex, UniqueInsertAttempt, UniqueSecondaryIndex,
};
use crate::log::redo::{RowRedo, RowRedoKind};
use crate::lwc::LwcBlock;
use crate::map::{FastHashMap, FastHashSet};
use crate::poison::PoisonAwareListener;
use crate::row::ops::{
    DeleteMvcc, LinkForUniqueIndex, ReadRow, RowMutation, RowUpdateInput, ScanMvcc, SelectKey,
    SelectMvcc, TableMutationOutcome, UpdateCol, UpdateMvcc, UpsertMvcc,
};
use crate::row::{Row, RowPage, RowRead, estimate_max_row_count};
use crate::table::{
    ColumnDeletionBuffer, ColumnStorage, DeleteMarker, DeletionClaim, DeletionError, DmlValidator,
    MemTable, RowPageDescriptor, Table, TableRootSnapshot, TableRuntimeLayout, UpdateUniqueMvcc,
    index_key_is_changed, index_key_replace, read_latest_index_key,
    read_physical_index_keys_for_delete, row_len, unique_key_from_full_row,
};
use crate::trx::row::{
    BoundIndexCandidate, FindOldVersion, ReadLatestRow, RowReadAccess, RowWriteAccess,
};
use crate::trx::stmt::StmtEffects;
use crate::trx::undo::{IndexBranch, OwnedRowUndo, RowUndoKind};
use crate::trx::{
    MIN_SNAPSHOT_TS, PrepareListenerResult, SharedTrxStatus, TrxContext, TrxRuntime,
    trx_is_committed,
};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use futures::FutureExt;
use index_mutate::IndexMutator;
use std::marker::PhantomData;
use std::mem;
use std::ops::RangeBounds;
use std::ptr::addr_eq;
use std::sync::Arc;

enum LazyRowSource<'row> {
    Cold {
        block: &'row LwcBlock,
        column_layout: &'row TableColumnLayout,
        row_idx: usize,
        file_kind: FileKind,
        block_id: BlockID,
    },
    Hot {
        access: RowReadAccess<'row>,
        column_layout: &'row TableColumnLayout,
    },
    HotWrite {
        access: RowWriteAccess<'row>,
        column_layout: &'row TableColumnLayout,
    },
}

impl LazyRowSource<'_> {
    #[inline]
    fn load_latest(&self, column_no: usize) -> DataIntegrityResult<Val> {
        match self {
            LazyRowSource::Cold {
                block,
                column_layout,
                row_idx,
                file_kind,
                block_id,
            } => Ok(block
                .decode_value(column_layout, *row_idx, column_no)
                .attach_with(|| {
                    format!("file={file_kind}, block=lwc_block, block_id={block_id}")
                })?),
            LazyRowSource::Hot {
                access,
                column_layout,
            } => Ok(access.read_latest_value(column_layout, column_no)),
            LazyRowSource::HotWrite {
                access,
                column_layout,
            } => Ok(access.row().val(column_layout, column_no)),
        }
    }
}

/// Lazy accessor for one latest modifiable row in a table mutation callback.
///
/// The accessor hides whether the row is stored in a persisted column block or
/// an in-memory row page. Requested values are cached for the callback
/// invocation. Cold values are decoded only after the persisted image is
/// confirmed to remain the current logical row. Hot values come from the latest
/// physical page image after row ownership validation; they are not
/// reconstructed from MVCC undo for an older snapshot. Index-driven mutation
/// retains definitive row ownership for the callback, while full-table
/// mutation retains its existing table-exclusive traversal protocol.
/// References returned by [`LazyRow::val`] remain tied to the mutable accessor
/// borrow.
pub struct LazyRow<'row> {
    source: LazyRowSource<'row>,
    values: Vec<Val>,
    ready: Vec<bool>,
    ready_columns: Vec<usize>,
}

impl<'row> LazyRow<'row> {
    #[inline]
    fn new(source: LazyRowSource<'row>, mut values: Vec<Val>, column_count: usize) -> Self {
        if values.len() != column_count {
            values = vec![Val::default(); column_count];
        }
        Self {
            source,
            values,
            ready: vec![false; column_count],
            ready_columns: Vec::new(),
        }
    }

    /// Returns the number of columns in this row.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.values.len()
    }

    /// Returns one latest modifiable column value, loading and caching it on demand.
    #[inline]
    pub fn val(&mut self, column_no: usize) -> Result<&Val> {
        if column_no >= self.values.len() {
            return Err(Report::new(OperationError::InvalidDmlInput)
                .attach(format!(
                    "lazy row column out of range: column_no={column_no}, column_count={}",
                    self.values.len()
                ))
                .disclose());
        }
        self.val_inner(column_no).disclose()
    }

    /// Returns one valid latest modifiable column value without crossing the
    /// public error boundary.
    #[inline]
    pub(crate) fn val_inner(&mut self, column_no: usize) -> DataIntegrityResult<&Val> {
        assert!(
            column_no < self.values.len(),
            "internal lazy-row column must be in range: column_no={column_no}, column_count={}",
            self.values.len()
        );
        if !self.ready[column_no] {
            self.values[column_no] = self.source.load_latest(column_no)?;
            self.ready[column_no] = true;
            self.ready_columns.push(column_no);
        }
        Ok(&self.values[column_no])
    }

    #[inline]
    fn into_reusable_buffer(mut self) -> Vec<Val> {
        for column_no in self.ready_columns {
            self.values[column_no] = Val::default();
        }
        self.values
    }

    #[inline]
    fn into_hot_write_reusable_buffer(mut self) -> (RowWriteAccess<'row>, Vec<Val>) {
        for column_no in self.ready_columns {
            self.values[column_no] = Val::default();
        }
        let LazyRowSource::HotWrite { access, .. } = self.source else {
            unreachable!("hot mutation callback must retain row write access")
        };
        (access, self.values)
    }

    #[inline]
    fn into_hot_write_index_keys<'op>(
        mut self,
        accessor: &UserTableAccessor<'op>,
    ) -> DataIntegrityResult<(RowWriteAccess<'row>, WriteIndexKeySet<'op>, Vec<Val>)> {
        let metadata = accessor.metadata();
        for &column_no in metadata.idx.index_columns() {
            let _ = self.val_inner(column_no)?;
        }
        let keys = WriteIndexKeySet::from_full_row(accessor, &self.values);
        let (access, values) = self.into_hot_write_reusable_buffer();
        Ok((access, keys, values))
    }

    #[inline]
    fn into_full_row(mut self) -> DataIntegrityResult<(Vec<Val>, Vec<Val>)> {
        for column_no in 0..self.values.len() {
            let _ = self.val_inner(column_no)?;
        }
        let column_count = self.values.len();
        Ok((
            mem::take(&mut self.values),
            vec![Val::default(); column_count],
        ))
    }

    #[inline]
    fn into_index_keys<'op>(
        mut self,
        accessor: &UserTableAccessor<'op>,
    ) -> DataIntegrityResult<(WriteIndexKeySet<'op>, Vec<Val>)> {
        let metadata = accessor.metadata();
        for &column_no in metadata.idx.index_columns() {
            let _ = self.val_inner(column_no)?;
        }
        let keys = WriteIndexKeySet::from_full_row(accessor, &self.values);
        Ok((keys, self.into_reusable_buffer()))
    }
}

#[derive(Clone, Copy)]
pub(super) struct RowIdMove {
    old: RowID,
    new: RowID,
}

impl RowIdMove {
    #[inline]
    const fn new(old: RowID, new: RowID) -> Self {
        Self { old, new }
    }
}

#[derive(Clone, Copy)]
struct InsertedRow {
    page_id: PageID,
    row_id: RowID,
}

impl InsertedRow {
    #[inline]
    const fn new(page_id: PageID, row_id: RowID) -> Self {
        Self { page_id, row_id }
    }
}

#[derive(Clone, Copy)]
struct UniqueIndexLinkTarget<'a> {
    row_id: RowID,
    guard: &'a PageSharedGuard<RowPage>,
}

impl<'a> UniqueIndexLinkTarget<'a> {
    #[inline]
    const fn new(row_id: RowID, guard: &'a PageSharedGuard<RowPage>) -> Self {
        Self { row_id, guard }
    }
}

struct ScanBoundaryTracker {
    upper_bound: RowID,
    pages: Vec<RowPageDescriptor>,
    next_page: usize,
    boundaries: FastHashMap<PageID, RowID>,
}

impl ScanBoundaryTracker {
    #[inline]
    fn new(upper_bound: RowID, pages: Vec<RowPageDescriptor>) -> Self {
        Self {
            upper_bound,
            pages,
            next_page: 0,
            boundaries: FastHashMap::default(),
        }
    }

    #[inline]
    fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[inline]
    fn page(&self, page_idx: usize) -> RowPageDescriptor {
        self.pages[page_idx]
    }

    #[inline]
    fn start_page(&mut self, page_idx: usize, current_end: RowID) -> RowID {
        debug_assert_eq!(self.next_page, page_idx);
        self.next_page += 1;
        self.boundaries
            .remove(&self.pages[page_idx].page_id)
            .unwrap_or(current_end)
    }

    #[inline]
    fn observe_insert(&mut self, insert: InsertedRow) {
        let InsertedRow { page_id, row_id } = insert;
        if row_id >= self.upper_bound || self.pages.is_empty() {
            return;
        }
        if let Some(boundary) = self.boundaries.get_mut(&page_id) {
            *boundary = (*boundary).min(row_id);
            return;
        }
        let partition = self
            .pages
            .partition_point(|page| page.start_row_id <= row_id);
        if partition == 0 {
            return;
        }
        let page_idx = partition - 1;
        let page = self.pages[page_idx];
        if page.page_id != page_id || row_id >= page.end_row_id || page_idx < self.next_page {
            return;
        }
        self.boundaries.insert(page_id, row_id);
    }
}

struct TableMutationState {
    tracker: ScanBoundaryTracker,
    column_count: usize,
    value_buffer: Vec<Val>,
    outcome: TableMutationOutcome,
}

enum PendingColdMutation<'op> {
    Delete {
        row_id: RowID,
        index_keys: WriteIndexKeySet<'op>,
    },
    Update {
        row_id: RowID,
        old_row: Vec<Val>,
        update: Vec<UpdateCol>,
    },
}

/// One mutation key derived from an admitted foreground write accessor.
///
/// The lifetime marker keeps the key within the admitted layout lifetime. The
/// raw key is exposed only when `UserTableAccessor` consumes it at leaf index,
/// undo, or runtime-link boundaries.
struct WriteIndexKey<'op> {
    key: SelectKey,
    _layout: PhantomData<&'op TableRuntimeLayout>,
}

impl<'op> WriteIndexKey<'op> {
    #[inline]
    fn new(_accessor: &UserTableAccessor<'op>, key: SelectKey) -> Self {
        Self {
            key,
            _layout: PhantomData,
        }
    }

    #[inline]
    fn index_no(&self) -> usize {
        self.key.index_no
    }

    #[inline]
    fn as_select_key(&self) -> &SelectKey {
        &self.key
    }

    #[inline]
    fn into_select_key(self) -> SelectKey {
        self.key
    }
}

/// Complete mutation keys derived from one admitted foreground write accessor.
///
/// Private constructors preserve active stable-index order. The accessor
/// lifetime remains attached from construction through either new-side claims
/// or old-row ownership proof construction.
struct WriteIndexKeySet<'op> {
    keys: Vec<SelectKey>,
    _layout: PhantomData<&'op TableRuntimeLayout>,
}

impl<'op> WriteIndexKeySet<'op> {
    #[inline]
    fn from_full_row(accessor: &UserTableAccessor<'op>, row: &[Val]) -> Self {
        Self {
            keys: accessor.metadata().idx.keys_for_insert(row),
            _layout: PhantomData,
        }
    }

    #[inline]
    fn from_physical_row(
        accessor: &UserTableAccessor<'op>,
        page_guard: &PageSharedGuard<RowPage>,
        row_id: RowID,
    ) -> Self {
        Self {
            keys: read_physical_index_keys_for_delete(accessor.metadata(), page_guard, row_id),
            _layout: PhantomData,
        }
    }

    #[inline]
    fn from_indexed_values(
        accessor: &UserTableAccessor<'op>,
        read_set: &[usize],
        vals: Vec<Val>,
    ) -> Self {
        let table_id = accessor.table_id();
        assert_eq!(
            read_set.len(),
            vals.len(),
            "indexed-column read must return one value per requested column: table_id={table_id}, read_set_len={}, value_count={}",
            read_set.len(),
            vals.len()
        );
        let indexed_vals = read_set
            .iter()
            .copied()
            .zip(vals)
            .collect::<FastHashMap<_, _>>();
        let keys = accessor
            .metadata()
            .idx
            .active_indexes()
            .map(|(index_no, index)| {
                let vals = index
                    .cols
                    .iter()
                    .map(|key| {
                        indexed_vals
                            .get(&(key.col_no as usize))
                            .cloned()
                            .unwrap_or_else(|| {
                                panic!(
                                    "active index column must be present in the metadata-derived read set: table_id={table_id}, index_no={index_no}, column_no={}",
                                    key.col_no
                                )
                            })
                    })
                    .collect();
                SelectKey::new(index_no, vals)
            })
            .collect();
        Self {
            keys,
            _layout: PhantomData,
        }
    }

    #[inline]
    fn as_slice(&self) -> &[SelectKey] {
        &self.keys
    }

    #[inline]
    fn into_keys(self) -> impl Iterator<Item = WriteIndexKey<'op>> {
        let Self { keys, _layout: _ } = self;
        keys.into_iter().map(move |key| WriteIndexKey {
            key,
            _layout: PhantomData,
        })
    }
}

/// Root-relative storage contract for one owned current row's old index set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedRowIndexStorage {
    /// The RowID is at or above the captured pivot.
    ///
    /// The captured DiskTree roots cannot contain the row, so every old index
    /// entry reconstructed after write ownership must still exist in MemIndex.
    /// MemIndex absence or an incompatible value is an invariant violation.
    MemRequired,
    /// The RowID is below the captured pivot.
    ///
    /// Companion publication placed the current row in the captured column
    /// root and its complete index set in the captured DiskTree roots. A
    /// matching MemIndex copy may exist, but absence is valid because the CDB
    /// owns visibility for the durable row.
    Persisted,
}

/// Write authority that made the row's complete old index set stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedRowMutationOwnership {
    /// Latest-row write ownership is held on a validated stable RowPage.
    RowPage,
    /// The current LWC image was validated before acquiring its CDB marker.
    ColumnDeletionBuffer,
}

/// Opaque, single-use proof for every old index entry of one owned current row.
///
/// Construction requires write authority over the current row, exactly one
/// reconstructed key for every active index in the pinned layout, and a
/// compatible captured root. Storage mode depends only on the RowID relative
/// to that root's pivot; the layer that originally selected the row is not part
/// of this proof.
///
/// Consuming the proof mutates only MemIndex because DiskTree is immutable.
/// `MemRequired` and `Persisted` differ in whether an absent MemIndex copy is an
/// invariant violation or a successful no-op. Keeping the fields private and
/// exposing only consuming iteration prevents rebinding or partial reuse.
struct OwnedRowIndexSetProof<'op, 'snapshot, 'ctx> {
    /// Current owned row whose old index membership was reconstructed.
    row_id: RowID,
    /// Pivot copied from the root against which storage mode was classified.
    pivot_row_id: RowID,
    /// Complete old key set in pinned active-index slot order.
    keys: WriteIndexKeySet<'op>,
    storage: OwnedRowIndexStorage,
    ownership: OwnedRowMutationOwnership,
    /// Root whose column and secondary-index completeness contract is used.
    root_snapshot: &'snapshot TableRootSnapshot<'ctx>,
}

impl<'op, 'snapshot, 'ctx> OwnedRowIndexSetProof<'op, 'snapshot, 'ctx> {
    /// Consume the complete proof into independently processed old entries.
    #[inline]
    fn into_entries(self) -> impl Iterator<Item = OwnedOldIndexEntry<'op, 'snapshot, 'ctx>> {
        let Self {
            row_id,
            pivot_row_id,
            keys,
            storage,
            ownership,
            root_snapshot,
        } = self;
        keys.into_keys().map(move |key| OwnedOldIndexEntry {
            row_id,
            pivot_row_id,
            key,
            storage,
            ownership,
            root_snapshot,
        })
    }
}

/// One old entry carrying every root, layout, storage, and ownership binding.
///
/// Full-set operations obtain entries by consuming an
/// [`OwnedRowIndexSetProof`]. A RowID-stable in-place update may bind only an
/// affected entry after RowPage write ownership, avoiding reconstruction of
/// unchanged index keys.
struct OwnedOldIndexEntry<'op, 'snapshot, 'ctx> {
    row_id: RowID,
    pivot_row_id: RowID,
    key: WriteIndexKey<'op>,
    storage: OwnedRowIndexStorage,
    ownership: OwnedRowMutationOwnership,
    root_snapshot: &'snapshot TableRootSnapshot<'ctx>,
}

pub(super) enum IndexPurgeDecision {
    Delete,
    Keep,
    RowPage(PageID),
}

pub(super) enum ColdRowUpdateRead {
    Ok(Vec<Val>),
    NotFound,
    WriteConflict,
    Preparing(PoisonAwareListener),
}

enum ColdLatestRow {
    Readable,
    NotFound,
    WriteConflict,
    Preparing(PoisonAwareListener),
}

enum PointMutationAttempt<'ctx> {
    Hot {
        root_snapshot: TableRootSnapshot<'ctx>,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
    },
    Preparing(PoisonAwareListener),
}

/// Operation accessor for user tables.
///
/// Construction is intentionally explicit: statement paths must acquire the
/// appropriate logical locks, check table lifecycle, capture one
/// `TableRuntimeLayout`, and then build this accessor with both column storage
/// and the pinned layout. That keeps user metadata/index runtime binding
/// independent from catalog fixed-schema access.
pub(crate) struct UserTableAccessor<'op> {
    table: &'op Table,
    storage: &'op ColumnStorage,
    layout: &'op TableRuntimeLayout,
}

impl<'op> UserTableAccessor<'op> {
    /// Create a user-table accessor over an externally pinned layout snapshot.
    #[inline]
    pub(crate) fn new(table: &'op Table, layout: &'op TableRuntimeLayout) -> Self {
        UserTableAccessor {
            table,
            storage: &table.storage,
            layout,
        }
    }

    #[inline]
    fn layout(&self) -> &TableRuntimeLayout {
        self.layout
    }

    #[inline]
    fn user_sec_idx(&self) -> &[Option<Arc<SecondaryIndex<EvictableBufferPool>>>] {
        self.layout().secondary_indexes()
    }

    #[inline]
    fn mem(&self) -> &MemTable<EvictableBufferPool, EvictableBufferPool> {
        &self.table.mem
    }

    #[inline]
    fn metadata(&self) -> &TableMetadata {
        self.layout().metadata()
    }

    #[inline]
    fn sec_idx_len(&self) -> usize {
        self.layout().index_slot_count()
    }

    #[inline]
    fn sec_idx_is_active(&self, index_no: usize) -> bool {
        self.user_sec_idx()
            .get(index_no)
            .is_some_and(Option::is_some)
    }

    #[inline]
    fn sec_idx_is_unique(&self, index_no: usize) -> bool {
        self.require_sec_idx(index_no)
            .expect("active user index slot")
            .is_unique()
    }

    #[inline]
    async fn wait_transition_route_or_poison(
        &self,
        rt: TrxRuntime<'_>,
        row_id: RowID,
    ) -> FatalResult<()> {
        let engine = rt.engine();
        loop {
            engine.poisoner.ensure_healthy()?;
            if row_id < self.mem().blk_idx().pivot_row_id() {
                return Ok(());
            }
            let route_epoch = self.mem().blk_idx().route_epoch();
            let poison_listener = engine.poisoner.listener();
            if row_id < self.mem().blk_idx().pivot_row_id() {
                return Ok(());
            }
            engine.poisoner.ensure_healthy()?;

            // Row pages in TRANSITION need either a newly published cold route
            // or storage poison. Without the poison wake, writers could sleep
            // after the checkpoint producer failed before route publication.
            let route_wait = self.mem().blk_idx().wait_route_since(route_epoch).fuse();
            let poison_wait = poison_listener.fuse();
            futures::pin_mut!(route_wait);
            futures::pin_mut!(poison_wait);
            futures::select! {
                () = route_wait => (),
                () = poison_wait => (),
            }
            engine.poisoner.ensure_healthy()?;
        }
    }

    #[inline]
    fn require_sec_idx(
        &self,
        index_no: usize,
    ) -> RuntimeResult<&SecondaryIndex<EvictableBufferPool>> {
        self.user_sec_idx()
            .get(index_no)
            .and_then(Option::as_deref)
            .ok_or_else(|| {
                Report::new(InternalError::SecondaryIndexOutOfBounds)
                    .attach(format!(
                        "index_no={index_no}, index_count={}",
                        self.user_sec_idx().len()
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=resolve_table_secondary_index")
            })
    }

    #[inline]
    fn require_sec_idx_arc(
        &self,
        index_no: usize,
    ) -> RuntimeResult<Arc<SecondaryIndex<EvictableBufferPool>>> {
        self.user_sec_idx()
            .get(index_no)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| {
                Report::new(InternalError::SecondaryIndexOutOfBounds)
                    .attach(format!(
                        "index_no={index_no}, index_count={}",
                        self.user_sec_idx().len()
                    ))
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=resolve_table_secondary_index")
            })
    }

    #[inline]
    fn require_unique_index_unchecked<'g>(
        &self,
        guards: &'g PoolGuards,
        index_no: usize,
        root: BlockID,
    ) -> RuntimeResult<UniqueSecondaryIndex<'_, 'g, EvictableBufferPool>> {
        self.require_sec_idx(index_no)?
            .bind_unique_unchecked(guards, root)
    }

    #[inline]
    fn require_non_unique_index_unchecked<'g>(
        &self,
        guards: &'g PoolGuards,
        index_no: usize,
        root: BlockID,
    ) -> RuntimeResult<NonUniqueSecondaryIndex<'_, 'g, EvictableBufferPool>> {
        self.require_sec_idx(index_no)?
            .bind_non_unique_unchecked(guards, root)
    }

    #[inline]
    fn current_index_read_handle<'g, 'idx>(
        &'idx self,
        rt: TrxRuntime<'g>,
        index_no: usize,
    ) -> RuntimeResult<CurrentIndexReadHandle<'g, 'idx, EvictableBufferPool>> {
        // User accessors pin metadata/runtime layout, while secondary DiskTree
        // operations bind to the latest proof-gated root for the same stable
        // slot. Checkpoint publication may advance roots without changing
        // layout shape.
        let index = self.require_sec_idx(index_no)?;
        let proof = rt.read_proof();
        let root = self
            .storage
            .with_active_root(&proof, |root| root.secondary_index_roots[index_no]);
        Ok(CurrentIndexReadHandle::new(
            index,
            rt.pool_guards(),
            root,
            &proof,
        ))
    }

    #[inline]
    fn owned_current_index_read_handle(
        &self,
        rt: TrxRuntime<'_>,
        index_no: usize,
    ) -> RuntimeResult<OwnedCurrentIndexReadHandle<EvictableBufferPool>> {
        let index = self.require_sec_idx_arc(index_no)?;
        let proof = rt.read_proof();
        let root = self
            .storage
            .with_active_root(&proof, |root| root.secondary_index_roots[index_no]);
        let pool_guards = rt.pool_guards();
        Ok(OwnedCurrentIndexReadHandle::new(
            index,
            pool_guards.index_guard().clone(),
            pool_guards.disk_guard().clone(),
            root,
            &proof,
        ))
    }

    #[inline]
    fn unchecked_secondary_root(&self, index_no: usize) -> BlockID {
        // Unchecked internal callers share the same layout/root compatibility
        // contract as proof-gated foreground reads. Purge additionally treats
        // inactive layout slots as no-ops before reaching this binding point.
        self.storage
            .file()
            .active_root_unchecked()
            .secondary_index_roots[index_no]
    }

    #[inline]
    fn root_snapshot<'ctx>(&self, ctx: &'ctx TrxContext) -> TableRootSnapshot<'ctx> {
        let proof = ctx.read_proof();
        self.storage.with_active_root(&proof, |root| {
            self.assert_foreground_root_layout_compatible(root);
            TableRootSnapshot::from_active_root(root, &proof)
        })
    }

    /// Assert in constant time that the pinned layout belongs to this root.
    ///
    /// `TableRuntimeLayout::assert_valid` checks the immutable runtime vector
    /// when the layout is constructed and installed. Table-root creation,
    /// publication, and loading separately validate the sparse root shape.
    /// Metadata pointer identity therefore binds those validated structures to
    /// the same index specification, while the slot-count check prevents an
    /// incompatible root vector from being indexed through this layout.
    ///
    /// Foreground metadata locks exclude the index-DDL interval between root
    /// publication and layout installation, so no per-index revalidation is
    /// needed on this hot path.
    #[inline]
    fn assert_foreground_root_layout_compatible(&self, root: &ActiveRoot) {
        let layout = self.layout();
        assert!(
            Arc::ptr_eq(&root.metadata, layout.metadata_arc()),
            "foreground root/layout invariant violated: metadata identity mismatch, table_id={}, layout_generation={}",
            self.table_id(),
            layout.generation()
        );
        assert_eq!(
            root.secondary_index_roots.len(),
            layout.index_slot_count(),
            "foreground root/layout invariant violated: root_slots={}, layout_slots={}, table_id={}, layout_generation={}",
            root.secondary_index_roots.len(),
            layout.index_slot_count(),
            self.table_id(),
            layout.generation()
        );
    }

    #[inline]
    fn owned_row_index_set_proof<'snapshot, 'ctx>(
        &self,
        row_id: RowID,
        keys: WriteIndexKeySet<'op>,
        root_snapshot: &'snapshot TableRootSnapshot<'ctx>,
        ownership: OwnedRowMutationOwnership,
    ) -> OwnedRowIndexSetProof<'op, 'snapshot, 'ctx> {
        // The complete ordered key set retains its admitted-layout lifetime;
        // row ownership adds authority without rebinding metadata.
        let pivot_row_id = root_snapshot.pivot_row_id();
        // Storage mode is a statement-local fact about the captured root, not
        // about whether the row was initially found through Mem or Disk. A row
        // that moved to LWC after root capture remains MemRequired when its
        // RowID is outside that root's persisted range.
        let storage = if row_id >= pivot_row_id {
            OwnedRowIndexStorage::MemRequired
        } else {
            OwnedRowIndexStorage::Persisted
        };
        // A stable RowPage belongs to the captured root's Mem-required range.
        // Persisted ownership must instead be established through the CDB path.
        if ownership == OwnedRowMutationOwnership::RowPage {
            assert_eq!(
                storage,
                OwnedRowIndexStorage::MemRequired,
                "owned-row index-set invariant violated: RowPage-owned row is persisted in captured root, table_id={}, row_id={row_id}, pivot_row_id={pivot_row_id}",
                self.table_id()
            );
        }
        OwnedRowIndexSetProof {
            row_id,
            pivot_row_id,
            keys,
            storage,
            ownership,
            root_snapshot,
        }
    }

    #[inline]
    fn owned_row_page_index_set_proof<'snapshot, 'ctx>(
        &self,
        row_id: RowID,
        keys: WriteIndexKeySet<'op>,
        root_snapshot: &'snapshot TableRootSnapshot<'ctx>,
    ) -> OwnedRowIndexSetProof<'op, 'snapshot, 'ctx> {
        self.owned_row_index_set_proof(
            row_id,
            keys,
            root_snapshot,
            OwnedRowMutationOwnership::RowPage,
        )
    }

    #[inline]
    fn owned_row_page_index_entry<'snapshot, 'ctx>(
        &self,
        row_id: RowID,
        key: WriteIndexKey<'op>,
        root_snapshot: &'snapshot TableRootSnapshot<'ctx>,
    ) -> OwnedOldIndexEntry<'op, 'snapshot, 'ctx> {
        let pivot_row_id = root_snapshot.pivot_row_id();
        assert!(
            row_id >= pivot_row_id,
            "owned-row index-set invariant violated: individually bound RowPage entry is persisted in captured root, table_id={}, row_id={row_id}, index_no={}, pivot_row_id={pivot_row_id}",
            self.table_id(),
            key.index_no()
        );
        OwnedOldIndexEntry {
            row_id,
            pivot_row_id,
            key,
            storage: OwnedRowIndexStorage::MemRequired,
            ownership: OwnedRowMutationOwnership::RowPage,
            root_snapshot,
        }
    }

    #[inline]
    async fn owned_cdb_index_set_proof<'snapshot, 'ctx>(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
        keys: WriteIndexKeySet<'op>,
        root_snapshot: &'snapshot TableRootSnapshot<'ctx>,
    ) -> RuntimeResult<OwnedRowIndexSetProof<'op, 'snapshot, 'ctx>> {
        let proof = self.owned_row_index_set_proof(
            row_id,
            keys,
            root_snapshot,
            OwnedRowMutationOwnership::ColumnDeletionBuffer,
        );
        if proof.storage == OwnedRowIndexStorage::Persisted {
            // The pivot classifies the row as durable, but proof construction
            // also re-resolves it through the same captured column root. This
            // verifies that companion publication persisted the exact current
            // key set before MemIndex absence is accepted during consumption.
            let column_root = root_snapshot.column_block_index_root();
            assert_ne!(
                column_root,
                SUPER_BLOCK_ID,
                "owned-row index-set invariant violated: persisted row has no captured column root, table_id={}, row_id={row_id}, pivot_row_id={}",
                self.table_id(),
                proof.pivot_row_id
            );
            let storage = self.column_storage();
            let column_index = ColumnBlockIndex::new(
                column_root,
                proof.pivot_row_id,
                storage.file().file_kind(),
                storage.file().sparse_file(),
                storage.disk_pool(),
                guards.disk_guard(),
            );
            let resolved = column_index
                .locate_and_resolve_row(row_id)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=construct_owned_cdb_index_set_proof, table_id={}, row_id={row_id}, pivot_row_id={}",
                        self.table_id(),
                        proof.pivot_row_id
                    )
                })?
                .unwrap_or_else(|| {
                    panic!(
                        "owned-row index-set invariant violated: persisted row missing from captured column root, table_id={}, row_id={row_id}, pivot_row_id={}, column_root={column_root}",
                        self.table_id(),
                        proof.pivot_row_id
                    )
                });
            let captured_keys = self
                .read_lwc_index_keys(
                    guards,
                    resolved.block_id(),
                    resolved.row_idx(),
                    resolved.row_shape_fingerprint(),
                )
                .await?;
            assert_eq!(
                captured_keys.as_slice(),
                proof.keys.as_slice(),
                "owned-row index-set invariant violated: current LWC keys differ from captured-root LWC keys, table_id={}, row_id={row_id}, pivot_row_id={}",
                self.table_id(),
                proof.pivot_row_id
            );
        }
        Ok(proof)
    }

    #[inline]
    fn snapshot_index_read_handle<'g, 'idx>(
        &'idx self,
        guards: &'g PoolGuards,
        snapshot: &'g TableRootSnapshot<'_>,
        index_no: usize,
    ) -> RuntimeResult<CurrentIndexReadHandle<'g, 'idx, EvictableBufferPool>> {
        Ok(CurrentIndexReadHandle::from_snapshot(
            self.require_sec_idx(index_no)?,
            guards,
            snapshot,
            index_no,
        ))
    }

    #[inline]
    async fn resolve_row_location(
        &self,
        guards: &PoolGuards,
        row_id: RowID,
    ) -> RuntimeResult<RowLocation> {
        self.table.find_row(guards, row_id).await
    }

    #[inline]
    fn row_page_create_redo_ctx<'b>(&self, rt: TrxRuntime<'b>) -> RowPageCreateRedoCtx<'b> {
        let engine = rt.engine();
        RowPageCreateRedoCtx::new(&engine.trx_sys, self.table_id())
    }

    #[inline]
    fn column_storage(&self) -> &ColumnStorage {
        self.storage
    }

    #[inline]
    fn lwc_deletion_buffer(&self) -> &ColumnDeletionBuffer {
        self.storage.deletion_buffer()
    }
    #[inline]
    fn table_id(&self) -> TableID {
        self.mem().table_id()
    }

    #[inline]
    async fn mem_scan_from<F>(
        &self,
        guards: &PoolGuards,
        start_row_id: RowID,
        page_action: F,
    ) -> RuntimeResult<()>
    where
        F: FnMut(PageSharedGuard<RowPage>) -> bool,
    {
        self.mem()
            .scan_from(guards, start_row_id, page_action)
            .await
    }

    #[inline]
    fn debug_assert_table_write_lock_held(&self, rt: TrxRuntime<'_>) {
        rt.debug_assert_table_write_lock_held(self.table_id());
    }

    #[inline]
    fn push_insert_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_insert_unique_index_undo(self.table_id(), row_id, key, merge_old_deleted);
    }

    #[inline]
    fn push_insert_non_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        merge_old_deleted: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_insert_non_unique_index_undo(self.table_id(), row_id, key, merge_old_deleted);
    }

    #[inline]
    fn push_delete_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        key: SelectKey,
        unique: bool,
    ) {
        self.debug_assert_table_write_lock_held(rt);
        effects.push_delete_index_undo(self.table_id(), row_id, key, unique);
    }

    #[inline]
    fn push_update_unique_index_undo(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        key: SelectKey,
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

    #[inline]
    async fn index_lookup_unique_row_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
        row_id: RowID,
    ) -> RuntimeResult<SelectMvcc> {
        loop {
            let location = self.resolve_row_location(rt.pool_guards(), row_id).await?;
            match location {
                RowLocation::NotFound => return Ok(SelectMvcc::NotFound),
                RowLocation::LwcBlock(LwcRowLocation {
                    block_id,
                    row_idx,
                    row_shape_fingerprint,
                    durable_deleted,
                }) => {
                    if !cold_row_visible_mvcc(
                        self.lwc_deletion_buffer(),
                        rt.sts(),
                        rt.status().as_ref(),
                        row_id,
                        durable_deleted,
                    ) {
                        return Ok(SelectMvcc::NotFound);
                    }
                    let vals = self
                        .read_lwc_row(
                            rt.pool_guards(),
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                            user_read_set,
                        )
                        .await?;
                    return Ok(SelectMvcc::Found(vals));
                }
                RowLocation::RowPage(page_id) => {
                    let Some(page_guard) = self
                        .mem()
                        .try_get_validated_row_page_shared_result(rt.pool_guards(), page_id, row_id)
                        .await?
                    else {
                        continue;
                    };
                    return Ok(read_hot_row_mvcc(
                        rt,
                        self.metadata(),
                        &page_guard,
                        row_id,
                        Some((index_no, key_vals)),
                        user_read_set,
                    ));
                }
            }
        }
    }

    /// Resolves one index scan candidate through row MVCC and exact key recheck.
    #[inline]
    pub(crate) async fn index_lookup_candidate_row_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        candidate: BoundIndexCandidate<'_>,
        read_set: &[usize],
    ) -> RuntimeResult<SelectMvcc> {
        let index_no = candidate.index_no;
        loop {
            let location = self
                .resolve_row_location(rt.pool_guards(), candidate.row_id)
                .await?;
            match location {
                RowLocation::NotFound => return Ok(SelectMvcc::NotFound),
                RowLocation::LwcBlock(LwcRowLocation {
                    block_id,
                    row_idx,
                    row_shape_fingerprint,
                    durable_deleted,
                }) => {
                    if !cold_row_visible_mvcc(
                        self.lwc_deletion_buffer(),
                        rt.sts(),
                        rt.status().as_ref(),
                        candidate.row_id,
                        durable_deleted,
                    ) {
                        return Ok(SelectMvcc::NotFound);
                    }
                    let storage = self.column_storage();
                    let file_kind = storage.file().file_kind();
                    let persisted = storage
                        .load_lwc_block(rt.pool_guards().disk_guard(), block_id)
                        .await?;
                    let block = persisted.block();
                    if block.row_shape_fingerprint() != row_shape_fingerprint {
                        return Err(Report::new(DataIntegrityError::InvalidPayload)
                            .attach(format!(
                                "file={file_kind}, block=lwc_block, block_id={block_id}, row shape fingerprint mismatch"
                            ))
                            .change_context(RuntimeError::TableAccess)
                            .attach(format!(
                                "operation=index_lookup_candidate_row_mvcc, table_id={}, index_no={index_no}, row_id={}",
                                self.table_id(),
                                candidate.row_id
                            )));
                    }
                    let index_spec = self
                        .metadata()
                        .idx
                        .require_index_spec(candidate.index_no)
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=index_lookup_candidate_row_mvcc, table_id={}, index_no={index_no}, row_id={}",
                                self.table_id(),
                                candidate.row_id
                            )
                        })?;
                    let key_vals = block
                        .decode_index_key_values(self.metadata().col.as_ref(), index_spec, row_idx)
                        .attach_with(|| {
                            format!("file={file_kind}, block=lwc_block, block_id={block_id}")
                        })
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=index_lookup_candidate_row_mvcc, table_id={}, index_no={index_no}, row_id={}",
                                self.table_id(),
                                candidate.row_id
                            )
                        })?;
                    if !candidate.matches_key(&key_vals) {
                        return Ok(SelectMvcc::NotFound);
                    }
                    let vals = block
                        .decode_row_values(self.metadata().col.as_ref(), row_idx, read_set)
                        .attach_with(|| {
                            format!("file={file_kind}, block=lwc_block, block_id={block_id}")
                        })
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=index_lookup_candidate_row_mvcc, table_id={}, index_no={index_no}, row_id={}",
                                self.table_id(),
                                candidate.row_id
                            )
                        })?;
                    return Ok(SelectMvcc::Found(vals));
                }
                RowLocation::RowPage(page_id) => {
                    let Some(page_guard) = self
                        .mem()
                        .try_get_validated_row_page_shared_result(
                            rt.pool_guards(),
                            page_id,
                            candidate.row_id,
                        )
                        .await?
                    else {
                        continue;
                    };
                    let access = page_guard.read_row_by_id(candidate.row_id);
                    return Ok(
                        match access.read_row_mvcc_index_candidate(
                            rt.ctx(),
                            self.metadata(),
                            read_set,
                            &candidate,
                        ) {
                            ReadRow::Ok(vals) => SelectMvcc::Found(vals),
                            ReadRow::InvalidIndex | ReadRow::NotFound => SelectMvcc::NotFound,
                        },
                    );
                }
            }
        }
    }

    /// Create an index-derived candidate stream for a public scan range.
    pub(crate) fn index_scan_candidates(
        &self,
        rt: TrxRuntime<'_>,
        index_no: usize,
        range: KeyRange,
    ) -> RuntimeResult<OwnedIndexCandidateStream<EvictableBufferPool>> {
        debug_assert!(index_no < self.sec_idx_len());
        let handle = self.owned_current_index_read_handle(rt, index_no)?;
        Ok(OwnedIndexCandidateStream::new(handle, range))
    }

    #[inline]
    async fn read_lwc_row(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
        read_set: &[usize],
    ) -> RuntimeResult<Vec<Val>> {
        let storage = self.column_storage();
        let file_kind = storage.file().file_kind();
        let persisted = storage
            .load_lwc_block(guards.disk_guard(), block_id)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=read_lwc_row, table_id={}, block_id={block_id}, row_idx={row_idx}",
                    self.table_id()
                )
            })?;
        let block = persisted.block();
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, row shape fingerprint mismatch"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach(format!(
                    "operation=read_lwc_row, table_id={}, row_idx={row_idx}",
                    self.table_id()
                )));
        }
        block
            .decode_row_values(self.metadata().col.as_ref(), row_idx, read_set)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=read_lwc_row, table_id={}, row_idx={row_idx}, file={file_kind}, block=lwc_block, block_id={block_id}",
                    self.table_id()
                )
            })
    }

    #[inline]
    async fn read_lwc_full_row(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> RuntimeResult<Vec<Val>> {
        let storage = self.column_storage();
        let file_kind = storage.file().file_kind();
        let persisted = storage
            .load_lwc_block(guards.disk_guard(), block_id)
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=read_lwc_full_row, table_id={}, block_id={block_id}, row_idx={row_idx}",
                    self.table_id()
                )
            })?;
        let block = persisted.block();
        if block.row_shape_fingerprint() != row_shape_fingerprint {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={block_id}, row shape fingerprint mismatch"
                ))
                .change_context(RuntimeError::TableAccess)
                .attach(format!(
                    "operation=read_lwc_full_row, table_id={}, row_idx={row_idx}",
                    self.table_id()
                )));
        }
        block
            .decode_full_row_values(self.metadata().col.as_ref(), row_idx)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=read_lwc_full_row, table_id={}, row_idx={row_idx}, file={file_kind}, block=lwc_block, block_id={block_id}",
                    self.table_id()
                )
            })
    }

    async fn scan_cold_lwc_mvcc<F>(
        &self,
        guards: &PoolGuards,
        rt: TrxRuntime<'_>,
        read_set: &[usize],
        root_snapshot: &TableRootSnapshot<'_>,
        row_action: &mut F,
    ) -> RuntimeResult<bool>
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        let column_root = root_snapshot.column_block_index_root();
        let pivot_row_id = root_snapshot.pivot_row_id();
        if column_root == SUPER_BLOCK_ID || pivot_row_id == RowID::new(0) {
            return Ok(true);
        }

        let storage = self.column_storage();
        let deletion_buffer = self.lwc_deletion_buffer();
        let column_layout = self.metadata().col.as_ref();
        let reader_sts = rt.sts();
        let reader_status = rt.status();
        let file_kind = storage.file().file_kind();
        let disk_guard = guards.disk_guard();
        let column_index = ColumnBlockIndex::new(
            column_root,
            pivot_row_id,
            file_kind,
            storage.file().sparse_file(),
            storage.disk_pool(),
            disk_guard,
        );
        for entry in column_index.collect_leaf_entries().await? {
            let (delete_deltas, row_ids) =
                column_index.load_delete_deltas_and_row_ids(&entry).await?;
            let persisted = storage.load_lwc_block(disk_guard, entry.block_id()).await?;
            let block = persisted.block();
            validate_cold_scan_entry(file_kind, &entry, block, &row_ids)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=scan_cold_lwc_mvcc, table_id={}, block_id={}",
                        self.table_id(),
                        entry.block_id()
                    )
                })?;
            let durable_deleted = persisted_delete_set_for_scan(file_kind, &entry, delete_deltas)
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=scan_cold_lwc_mvcc, table_id={}, block_id={}",
                        self.table_id(),
                        entry.block_id()
                    )
                })?;
            let has_durable_deletes = !durable_deleted.is_empty();
            for (row_idx, row_id) in row_ids.into_iter().enumerate() {
                if !cold_row_visible_mvcc(
                    deletion_buffer,
                    reader_sts,
                    reader_status.as_ref(),
                    row_id,
                    has_durable_deletes && durable_deleted.contains(&row_id),
                ) {
                    continue;
                }
                let vals = block
                    .decode_row_values(column_layout, row_idx, read_set)
                    .attach_with(|| {
                        format!(
                            "file={file_kind}, block=lwc_block, block_id={}",
                            entry.block_id()
                        )
                    })
                    .change_context(RuntimeError::TableAccess)
                    .attach_with(|| {
                        format!(
                            "operation=scan_cold_lwc_mvcc, table_id={}, block_id={}, row_idx={row_idx}",
                            self.table_id(),
                            entry.block_id()
                        )
                    })?;
                if !row_action(vals) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    #[inline]
    async fn read_lwc_index_keys(
        &self,
        guards: &PoolGuards,
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> RuntimeResult<WriteIndexKeySet<'op>> {
        let mut read_set = self
            .metadata()
            .idx
            .index_columns()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        read_set.sort_unstable();
        let vals = self
            .read_lwc_row(guards, block_id, row_idx, row_shape_fingerprint, &read_set)
            .await?;
        Ok(WriteIndexKeySet::from_indexed_values(self, &read_set, vals))
    }

    #[inline]
    async fn read_lwc_row_for_update<F>(
        &self,
        rt: TrxRuntime<'_>,
        row_id: RowID,
        location: LwcRowLocation,
        key_matches: F,
    ) -> RuntimeResult<ColdRowUpdateRead>
    where
        F: FnOnce(&[Val]) -> bool,
    {
        let deletion_buffer = self.lwc_deletion_buffer();
        // Cold rows are immutable, so their write visibility is tracked by the
        // column deletion buffer rather than by a row-page undo chain. A marker
        // committed at or before this writer's snapshot means the row is gone
        // for this statement. An uncommitted marker owned by this transaction
        // means this statement already consumed the cold row. A marker owned by
        // another active transaction is a write conflict.
        let marker = deletion_buffer.get(row_id);
        if let Some(marker) = marker.as_ref() {
            match marker {
                DeleteMarker::Committed(ts) => {
                    if *ts <= rt.sts() {
                        return Ok(ColdRowUpdateRead::NotFound);
                    }
                }
                DeleteMarker::Ref(status) => {
                    let ts = status.ts();
                    if trx_is_committed(ts) {
                        if ts <= rt.sts() {
                            return Ok(ColdRowUpdateRead::NotFound);
                        }
                    } else if Arc::ptr_eq(status, rt.status()) {
                        // This transaction already consumed the cold row.
                        return Ok(ColdRowUpdateRead::NotFound);
                    } else {
                        match status.prepare_listener() {
                            PrepareListenerResult::Registered(listener) => {
                                return Ok(ColdRowUpdateRead::Preparing(listener));
                            }
                            PrepareListenerResult::Completed(listener) => {
                                // Completion won registration. Reclassify commit now;
                                // rollback marker removal or fatal poison is observed by
                                // the caller's immediate authoritative retry.
                                let ts = status.ts();
                                if trx_is_committed(ts) {
                                    if ts <= rt.sts() {
                                        return Ok(ColdRowUpdateRead::NotFound);
                                    }
                                } else {
                                    return Ok(ColdRowUpdateRead::Preparing(listener));
                                }
                            }
                            PrepareListenerResult::NotPreparing => {
                                let ts = status.ts();
                                if trx_is_committed(ts) {
                                    if ts <= rt.sts() {
                                        return Ok(ColdRowUpdateRead::NotFound);
                                    }
                                } else {
                                    return Ok(ColdRowUpdateRead::WriteConflict);
                                }
                            }
                        }
                    }
                }
            }
        }
        // A surviving marker carries the timestamp/ownership information
        // needed to distinguish a newer delete from an already-visible one.
        // Without that in-memory bridge, the durable delete bit proves the
        // cold image is no longer a writable latest row.
        if marker.is_none() && location.durable_deleted {
            return Ok(ColdRowUpdateRead::NotFound);
        }
        // Decode after the deletion-buffer visibility check, then revalidate
        // the caller's key predicate. The index candidate can be stale while
        // delete/index cleanup catches up with a cold-row delete.
        let vals = self
            .read_lwc_full_row(
                rt.pool_guards(),
                location.block_id,
                location.row_idx,
                location.row_shape_fingerprint,
            )
            .await?;
        if !key_matches(&vals) {
            return Ok(ColdRowUpdateRead::NotFound);
        }
        Ok(ColdRowUpdateRead::Ok(vals))
    }

    #[inline]
    async fn persisted_lwc_key_differs(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        block_id: BlockID,
        row_idx: usize,
        row_shape_fingerprint: u128,
    ) -> RuntimeResult<bool> {
        let read_set = self
            .metadata()
            .idx
            .require_index_spec(index_no)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=compare_persisted_lwc_key, table_id={}, index_no={index_no}, block_id={block_id}, row_idx={row_idx}",
                    self.table_id()
                )
            })?
            .cols
            .iter()
            .map(|key| key.col_no as usize)
            .collect::<Vec<_>>();
        let vals = self
            .read_lwc_row(guards, block_id, row_idx, row_shape_fingerprint, &read_set)
            .await?;
        Ok(vals.as_slice() != key_vals)
    }

    #[inline]
    async fn index_purge_decision(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> RuntimeResult<IndexPurgeDecision> {
        // This path is physical GC cleanup for a previously delete-marked
        // secondary-index entry. A cold delete marker proves that every key for
        // the row is unreachable only after its transaction is committed and
        // older than the current purge horizon.
        // A present CDB marker remains authoritative even after its delete is
        // durable. Only a globally purgeable marker proves deletion here;
        // otherwise the durable bit must be ignored until the marker is gone.
        let marker_present = match self
            .lwc_deletion_buffer()
            .delete_marker_purgeability(row_id, min_active_sts)
        {
            Some(true) => return Ok(IndexPurgeDecision::Delete),
            Some(false) => true,
            None => false,
        };

        match self.resolve_row_location(guards, row_id).await? {
            RowLocation::NotFound => Ok(IndexPurgeDecision::Delete),
            RowLocation::LwcBlock(LwcRowLocation {
                block_id,
                row_idx,
                row_shape_fingerprint,
                durable_deleted,
            }) => {
                // LWC rows are immutable persisted images. If no globally
                // purgeable marker proves the whole row invisible, decode only
                // the indexed columns and delete the purge key only when it no
                // longer matches the persisted current key.
                if (!marker_present && durable_deleted)
                    || self
                        .persisted_lwc_key_differs(
                            guards,
                            index_no,
                            key_vals,
                            block_id,
                            row_idx,
                            row_shape_fingerprint,
                        )
                        .await?
                {
                    Ok(IndexPurgeDecision::Delete)
                } else {
                    Ok(IndexPurgeDecision::Keep)
                }
            }
            RowLocation::RowPage(page_id) => Ok(IndexPurgeDecision::RowPage(page_id)),
        }
    }

    #[inline]
    async fn insert_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: WriteIndexKey<'op>,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> OperationOrRuntimeResult<()> {
        let key = key.into_select_key();
        if self
            .metadata()
            .idx
            .require_index_spec(key.index_no)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_index, table_id={}, index_no={}, row_id={row_id}",
                    self.table_id(),
                    key.index_no
                )
            })?
            .unique()
        {
            self.insert_unique_index(rt, effects, key, row_id, page_guard, root_snapshot)
                .await?;
        } else {
            self.insert_non_unique_index(rt, effects, key, row_id, root_snapshot)
                .await?;
        }
        Ok(())
    }

    #[inline]
    async fn insert_index_set(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        keys: WriteIndexKeySet<'op>,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> OperationOrRuntimeResult<()> {
        for key in keys.into_keys() {
            self.insert_index(rt, effects, key, row_id, page_guard, root_snapshot)
                .await?;
        }
        Ok(())
    }

    #[inline]
    async fn insert_row_internal(
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
                    rt.save_active_insert_page(self.table_id(), page_guard.versioned_page_id());
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

    #[inline]
    async fn move_update_for_space(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row: Vec<Val>,
        update: RowUpdateInput,
        old_id: RowID,
        old_guard: PageSharedGuard<RowPage>,
    ) -> RuntimeOrFatalResult<(RowID, FastHashMap<usize, Val>, PageSharedGuard<RowPage>)> {
        let prepared = HotRowMutator::new(self.table_id(), self.metadata(), rt, &old_guard, old_id)
            .prepare_move_update(old_row, update);
        // Release the old row page before awaiting replacement-row insertion.
        drop(old_guard);
        let (new_row_id, new_guard) = self
            .insert_row_internal(
                rt,
                effects,
                prepared.row,
                RowUndoKind::Insert,
                prepared.index_branches,
            )
            .await?;
        // do not unlock the page because we may need to update index
        Ok((new_row_id, prepared.index_change_cols, new_guard))
    }

    #[inline]
    fn build_cold_update_row(&self, mut vals: Vec<Val>, update: RowUpdateInput) -> Vec<Val> {
        match update {
            RowUpdateInput::Sparse(cols) => {
                for UpdateCol { idx, val } in cols {
                    let old_val = &mut vals[idx];
                    if old_val != &val {
                        *old_val = val;
                    }
                }
                vals
            }
            RowUpdateInput::FullRow(vals) => vals,
        }
    }

    #[inline]
    async fn update_indexes_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        index_change_cols: &FastHashMap<usize, Val>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> OperationOrRuntimeResult<()> {
        let metadata = self.metadata();
        for (index_no, index_schema) in metadata.idx.active_indexes() {
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            if index_key_is_changed(index_schema, index_change_cols) {
                let new_key = WriteIndexKey::new(
                    self,
                    read_latest_index_key(metadata, index_no, page_guard, row_id),
                );
                let old_entry = self.owned_row_page_index_entry(
                    row_id,
                    WriteIndexKey::new(
                        self,
                        index_key_replace(index_schema, new_key.as_select_key(), index_change_cols),
                    ),
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

    #[inline]
    async fn update_indexes_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_row_id: RowID,
        new_row_id: RowID,
        proof: OwnedRowIndexSetProof<'_, '_, '_>,
    ) -> RuntimeResult<()> {
        debug_assert!(old_row_id != new_row_id);
        let metadata = self.metadata();
        assert_eq!(
            proof.row_id,
            old_row_id,
            "owned-row index-set invariant violated: move proof RowID mismatch, table_id={}, proof_row_id={}, old_row_id={old_row_id}, new_row_id={new_row_id}",
            self.table_id(),
            proof.row_id
        );
        for old_entry in proof.into_entries() {
            let index_no = old_entry.key.index_no();
            let index_schema = metadata
                .idx
                .index_spec(index_no)
                .expect("owned proof contains active index keys");
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            if index_schema.unique() {
                self.update_unique_index_only_row_id_change(rt, effects, old_entry, new_row_id)
                    .await?;
            } else {
                self.update_non_unique_index_only_row_id_change(rt, effects, old_entry, new_row_id)
                    .await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn update_indexes_may_both_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id_move: RowIdMove,
        index_change_cols: &FastHashMap<usize, Val>,
        page_guard: &PageSharedGuard<RowPage>,
        proof: OwnedRowIndexSetProof<'_, '_, '_>,
    ) -> OperationOrRuntimeResult<()> {
        debug_assert!(row_id_move.old != row_id_move.new);
        let metadata = self.metadata();
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
            let index_no = old_entry.key.index_no();
            let index_schema = metadata
                .idx
                .index_spec(index_no)
                .expect("owned proof contains active index keys");
            debug_assert_eq!(self.sec_idx_is_unique(index_no), index_schema.unique());
            if index_key_is_changed(index_schema, index_change_cols) {
                let key = WriteIndexKey::new(
                    self,
                    read_latest_index_key(metadata, index_no, page_guard, row_id_move.new),
                );
                debug_assert_eq!(
                    old_entry.key.as_select_key(),
                    &index_key_replace(index_schema, key.as_select_key(), index_change_cols)
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

    #[inline]
    async fn delete_unique_index(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        let (page_guard, row_id, index) = loop {
            let root = self.unchecked_secondary_root(index_no);
            let index = self.require_unique_index_unchecked(guards, index_no, root)?;
            match index.lookup(key_vals, MIN_SNAPSHOT_TS).await? {
                None => return Ok(false), // Another thread deleted this entry.
                Some((index_row_id, deleted)) => {
                    if !deleted || index_row_id != row_id {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return Ok(false);
                    }
                    match self
                        .index_purge_decision(guards, index_no, key_vals, row_id, min_active_sts)
                        .await?
                    {
                        IndexPurgeDecision::Delete => {
                            return index
                                .compare_delete_mem(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                        }
                        IndexPurgeDecision::Keep => return Ok(false),
                        IndexPurgeDecision::RowPage(page_id) => {
                            let Some(page_guard) = self
                                .mem()
                                .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                                .await?
                            else {
                                continue;
                            };
                            break (page_guard, row_id, index);
                        }
                    }
                }
            }
        };
        let access = page_guard.read_row_by_id(row_id);
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain. Hot row pages still have undo chains, unlike
        // LWC rows whose persisted image is the only current key material.
        if !access.any_version_matches_key(self.metadata(), index_no, key_vals) {
            return index
                .compare_delete_mem(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    #[inline]
    async fn delete_non_unique_index(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        let (page_guard, row_id, index) = loop {
            let root = self.unchecked_secondary_root(index_no);
            let index = self.require_non_unique_index_unchecked(guards, index_no, root)?;
            match index
                .lookup_unique(key_vals, row_id, MIN_SNAPSHOT_TS)
                .await?
            {
                None => return Ok(false), // Another thread deleted this entry.
                Some(active) => {
                    if active {
                        // 1. Delete flag is unset by other transaction,
                        // so we skip to delete it.
                        // 2. Row id changed, means another transaction inserted
                        // new row with same key and reused this index entry.
                        // So we skip to delete it.
                        return Ok(false);
                    }
                    match self
                        .index_purge_decision(guards, index_no, key_vals, row_id, min_active_sts)
                        .await?
                    {
                        IndexPurgeDecision::Delete => {
                            return index
                                .compare_delete_mem(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await;
                        }
                        IndexPurgeDecision::Keep => return Ok(false),
                        IndexPurgeDecision::RowPage(page_id) => {
                            let Some(page_guard) = self
                                .mem()
                                .try_get_validated_row_page_shared_result(guards, page_id, row_id)
                                .await?
                            else {
                                continue;
                            };
                            break (page_guard, row_id, index);
                        }
                    }
                }
            }
        };
        let access = page_guard.read_row_by_id(row_id);
        // To safely delete an index entry, we need to make sure
        // no version with matched keys can be found in either page
        // data or version chain. Hot row pages still have undo chains, unlike
        // LWC rows whose persisted image is the only current key material.
        if !access.any_version_matches_key(self.metadata(), index_no, key_vals) {
            return index
                .compare_delete_mem(key_vals, row_id, false, MIN_SNAPSHOT_TS)
                .await;
        }
        Ok(false)
    }

    #[inline]
    async fn get_insert_page(
        &self,
        rt: TrxRuntime<'_>,
        row_count: usize,
    ) -> RuntimeOrFatalResult<PageSharedGuard<RowPage>> {
        if let Some(page_id) = rt.load_active_insert_page(self.table_id()) {
            let page_guard = self
                .mem()
                .get_row_page_versioned_shared(rt.pool_guards(), page_id)
                .await?;
            if let Some(page_guard) = page_guard {
                return Ok(page_guard);
            }
        }
        let redo_ctx = self.row_page_create_redo_ctx(rt);
        self.mem()
            .try_get_insert_page_with_redo(rt.pool_guards(), row_count, redo_ctx)
            .await
    }

    #[inline]
    async fn resolve_unmasked_lwc_duplicate(
        &self,
        rt: TrxRuntime<'_>,
        row_id: RowID,
    ) -> RuntimeResult<Option<LwcRowLocation>> {
        // The normal LWC delete/update path first writes the CDB marker and
        // then masks index entries. Another transaction can observe the small
        // window before masking completes. A marker is the newest authority;
        // only when it is absent may durable delete membership admit the stale
        // duplicate to link_for_unique_index_lwc().
        match self.resolve_row_location(rt.pool_guards(), row_id).await? {
            RowLocation::LwcBlock(location) => match self.lwc_deletion_buffer().get(row_id) {
                Some(_) => Ok(Some(location)),
                None if location.durable_deleted => Ok(Some(location)),
                None => Ok(None),
            },
            RowLocation::RowPage(_) | RowLocation::NotFound => Ok(None),
        }
    }

    #[inline]
    async fn link_for_unique_index_lwc(
        &self,
        rt: TrxRuntime<'_>,
        old_id: RowID,
        index_no: usize,
        key_vals: &[Val],
        target: UniqueIndexLinkTarget<'_>,
        location: LwcRowLocation,
    ) -> OperationOrRuntimeResult<LinkForUniqueIndex> {
        let deletion_buffer = self.lwc_deletion_buffer();
        // Convert the CDB marker into the delete timestamp carried by a cold
        // terminal unique branch. `None` below means the old cold owner is
        // still visible to this statement, either because there is no marker or
        // because the committed delete is newer than the statement snapshot. If
        // the persisted row still matches the key, that remains a duplicate.
        // `Some(None)` means this transaction itself installed the cold delete
        // marker, so the old cold image must be reachable only through the new
        // hot row's runtime unique branch. `Some(Some(ts))` means an earlier
        // committed delete is visible to this statement, and older snapshots
        // before `ts` may still need the old cold image.
        // The CDB marker is newer authority even when the resolved durable bit
        // is set. In particular, a marker committed after this statement's
        // snapshot keeps the old image visible and therefore a duplicate.
        let delete_cts = match deletion_buffer.get(old_id) {
            None if location.durable_deleted => return Ok(LinkForUniqueIndex::NotNeeded),
            None => None,
            Some(DeleteMarker::Committed(ts)) => {
                if ts <= rt.sts() {
                    Some(Some(ts))
                } else {
                    None
                }
            }
            Some(DeleteMarker::Ref(status)) => {
                let ts = status.ts();
                if trx_is_committed(ts) {
                    if ts <= rt.sts() { Some(Some(ts)) } else { None }
                } else if Arc::ptr_eq(&status, rt.status()) {
                    Some(None)
                } else {
                    return Err(Report::new(OperationError::WriteConflict)
                        .attach("unique-index LWC owner is modified by another transaction")
                        .into());
                }
            }
        };
        let old_row = self
            .read_lwc_full_row(
                rt.pool_guards(),
                location.block_id,
                location.row_idx,
                location.row_shape_fingerprint,
            )
            .await?;
        // The unique index entry may be stale while purge is catching up, so
        // verify the persisted row still owns the key before linking it.
        if !self.metadata().idx.match_key(index_no, key_vals, &old_row) {
            return Ok(LinkForUniqueIndex::NotNeeded);
        }
        let Some(delete_cts) = delete_cts else {
            return Err(Report::new(OperationError::DuplicateKey)
                .attach("visible LWC row owns the unique key")
                .into());
        };
        let metadata = self.metadata();
        let mut new_access = target.guard.write_row_by_id(target.row_id);
        let undo_vals = new_access.row().calc_delta(metadata.col.as_ref(), &old_row);
        // The new hot row owns the key now. The terminal branch preserves the
        // old cold image for snapshots that still need to see it. The branch is
        // runtime-only; recovery restores only the latest committed mapping.
        new_access.link_for_unique_index_cold_terminal(
            SelectKey::new(index_no, key_vals.to_vec()),
            delete_cts,
            undo_vals,
        );
        Ok(LinkForUniqueIndex::Linked)
    }

    /// Link old version for index.
    /// This is a special operation for unique index maintenance.
    /// It's triggered by duplicate key finding when updating index.
    ///
    /// There are scenarios as below:
    /// 1. The old row not found. Just skip it.
    /// 2. The old row is being modified. Just throw write conflict.
    /// 3. Then we search from row page through version chain,
    ///    try to find one version that is not deleted and matches
    ///    the index key.
    ///    a) we find it, then link it.
    ///    b) no version found, we skip this row.
    #[inline]
    async fn link_for_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        old_id: RowID,
        index_no: usize,
        key_vals: &[Val],
        target: UniqueIndexLinkTarget<'_>,
        resolved_lwc: Option<LwcRowLocation>,
    ) -> OperationOrRuntimeResult<LinkForUniqueIndex> {
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
                    return self
                        .link_for_unique_index_lwc(rt, old_id, index_no, key_vals, target, location)
                        .await;
                }
                Ok(RowLocation::RowPage(page_id)) => {
                    // A hot duplicate candidate must be inspected through its
                    // row-page undo chain. It may be a stale latest mapping, a
                    // deleted owner that older snapshots still need, or a true
                    // duplicate visible to this transaction.
                    let Some(old_guard) = self
                        .mem()
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
            .find_old_version_for_unique_key(metadata, index_no, key_vals, rt.ctx())
            .attach_with(|| format!("operation=link_for_unique_index, index_no={index_no}"))?
        {
            FindOldVersion::None => Ok(LinkForUniqueIndex::NotNeeded),
            FindOldVersion::Found(old_row, cts, old_entry) => {
                // row latch is enough, because row lock is already acquired.
                let mut new_access = target.guard.write_row_by_id(target.row_id);
                let undo_vals = new_access.row().calc_delta(metadata.col.as_ref(), &old_row);
                new_access.link_for_unique_index(
                    SelectKey::new(index_no, key_vals.to_vec()),
                    cts,
                    old_entry,
                    undo_vals,
                );
                Ok(LinkForUniqueIndex::Linked)
            }
        }
    }

    #[inline]
    async fn insert_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        page_guard: &PageSharedGuard<RowPage>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> OperationOrRuntimeResult<()> {
        let sts = rt.sts();
        let handle = self
            .snapshot_index_read_handle(rt.pool_guards(), root_snapshot, key.index_no)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_unique_index, table_id={}, index_no={}, row_id={row_id}",
                    self.table_id(),
                    key.index_no
                )
            })?;
        let index = handle.bind_unique()?;
        loop {
            match index
                .insert_if_not_exists_observed(&key.vals, row_id, false, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=insert_unique_index, table_id={}, index_no={}, row_id={row_id}",
                        self.table_id(),
                        key.index_no
                    )
                })? {
                UniqueInsertAttempt::Inserted { merged } => {
                    // insert index success.
                    self.push_insert_unique_index_undo(rt, effects, row_id, key, merged);
                    return Ok(());
                }
                UniqueInsertAttempt::Occupied(observation) => {
                    let old_row_id = observation.owner_row_id();
                    let deleted = observation.deleted();
                    // A unique key already has a latest mapping. A live
                    // non-deleted owner is a duplicate. A delete-masked or
                    // cold-marked owner may instead be a stale/old owner that
                    // should be linked for snapshots before this new claim.
                    debug_assert!(old_row_id != row_id);
                    let resolved_lwc = if deleted {
                        None
                    } else if let Some(location) = self
                        .resolve_unmasked_lwc_duplicate(rt, old_row_id)
                        .await
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=insert_unique_index, phase=check_duplicate_owner, table_id={}, index_no={}, row_id={old_row_id}",
                                self.table_id(), key.index_no
                            )
                        })?
                    {
                        Some(location)
                    } else {
                        return Err(Report::new(OperationError::DuplicateKey)
                            .attach(format!(
                                "operation=insert_unique_index, index_no={}",
                                key.index_no
                            ))
                            .into());
                    };
                    match self
                        .link_for_unique_index(
                            rt,
                            old_row_id,
                            key.index_no,
                            &key.vals,
                            UniqueIndexLinkTarget::new(row_id, page_guard),
                            resolved_lwc,
                        )
                        .await?
                    {
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            // Claim the latest mapping if it still points to
                            // the owner inspected above. A concurrent purge may
                            // remove the entry first, so retry insertion.
                            match observation
                                .replace(row_id, sts)
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=insert_unique_index, phase=claim_latest_owner, table_id={}, index_no={}, row_id={row_id}",
                                        self.table_id(), key.index_no
                                    )
                                })?
                            {
                                IndexCompareExchange::Ok => {
                                    self.push_update_unique_index_undo(
                                        rt, effects, old_row_id, row_id, key, deleted,
                                    );
                                    return Ok(());
                                }
                                IndexCompareExchange::NotExists => {}
                                IndexCompareExchange::Mismatch => {
                                    return Err(Report::new(OperationError::WriteConflict)
                                        .attach(format!(
                                            "operation=insert_unique_index, index_no={}",
                                            key.index_no
                                        ))
                                        .into());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn insert_non_unique_index_mem_required(
        &self,
        rt: TrxRuntime<'_>,
        key: &SelectKey,
        row_id: RowID,
        merge_if_match_deleted: bool,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> RuntimeResult<IndexInsert> {
        let pivot_row_id = root_snapshot.pivot_row_id();
        assert!(
            row_id >= pivot_row_id,
            "captured-pivot non-unique insertion invariant violated: table_id={}, index_no={}, row_id={row_id}, pivot_row_id={pivot_row_id}",
            self.table_id(),
            key.index_no
        );
        let handle =
            self.snapshot_index_read_handle(rt.pool_guards(), root_snapshot, key.index_no)?;
        handle
            .bind_non_unique()?
            .insert_mem_if_not_exists(&key.vals, row_id, merge_if_match_deleted, rt.sts())
            .await
    }

    #[inline]
    async fn insert_non_unique_index(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        key: SelectKey,
        row_id: RowID,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> RuntimeResult<()> {
        // For non-unique index, it's guaranteed to be success.
        match self
            .insert_non_unique_index_mem_required(
                rt,
                &key,
                row_id,
                false,
                root_snapshot,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=insert_non_unique_index, phase=insert_entry, table_id={}, index_no={}, row_id={row_id}",
                    self.table_id(),
                    key.index_no
                )
            })?
        {
            IndexInsert::Ok(merged) => {
                // insert index success.
                self.push_insert_non_unique_index_undo(rt, effects, row_id, key, merged);
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn defer_delete_owned_old_index_entry(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedOldIndexEntry<'_, '_, '_>,
    ) -> RuntimeResult<()> {
        let OwnedOldIndexEntry {
            row_id,
            pivot_row_id,
            key,
            storage,
            ownership,
            root_snapshot,
        } = entry;
        let key = key.into_select_key();
        let index_spec = self
            .metadata()
            .idx
            .index_spec(key.index_no)
            .unwrap_or_else(|| {
                panic!(
                    "owned-row index-set invariant violated: proof key became inactive, table_id={}, row_id={row_id}, index_no={}, pivot_row_id={pivot_row_id}, storage={storage:?}, ownership={ownership:?}",
                    self.table_id(),
                    key.index_no
                )
            });
        let handle =
            self.snapshot_index_read_handle(rt.pool_guards(), root_snapshot, key.index_no)?;
        // Both storage modes conditionally transition an existing MemIndex
        // copy; neither probes or mutates DiskTree here. Persisted mode alone
        // may accept absence because the CDB already hides the durable row and
        // its DiskTree candidates. Every incompatible present value remains an
        // invariant violation after ownership.
        if index_spec.unique() {
            let result = handle
                .bind_unique()?
                .compare_exchange_mem(&key.vals, row_id, row_id.deleted(), rt.sts())
                .await?;
            match (storage, result) {
                (
                    OwnedRowIndexStorage::MemRequired | OwnedRowIndexStorage::Persisted,
                    IndexCompareExchange::Ok,
                ) => {
                    self.push_delete_index_undo(rt, effects, row_id, key, true);
                    Ok(())
                }
                (OwnedRowIndexStorage::Persisted, IndexCompareExchange::NotExists) => {
                    // No optional MemIndex copy existed, so there is no write,
                    // undo record, purge record, or delete overlay to create.
                    Ok(())
                }
                (_, result) => {
                    panic!(
                        "owned-row index-set invariant violated: illegal unique mask result, table_id={}, index_no={}, key={:?}, row_id={row_id}, pivot_row_id={pivot_row_id}, storage={storage:?}, ownership={ownership:?}, result={result:?}",
                        self.table_id(),
                        key.index_no,
                        key.vals
                    );
                }
            }
        } else {
            let result = handle
                .bind_non_unique()?
                .mask_mem_if_present(&key.vals, row_id, rt.sts())
                .await?;
            match (storage, result) {
                (
                    OwnedRowIndexStorage::MemRequired | OwnedRowIndexStorage::Persisted,
                    IndexMask::Masked,
                ) => {
                    self.push_delete_index_undo(rt, effects, row_id, key, false);
                    Ok(())
                }
                (OwnedRowIndexStorage::Persisted, IndexMask::NotFound) => {
                    // The durable entry remains governed by the CDB until
                    // deletion checkpoint updates the immutable DiskTree.
                    Ok(())
                }
                (_, result) => {
                    panic!(
                        "owned-row index-set invariant violated: illegal non-unique mask result, table_id={}, index_no={}, key={:?}, row_id={row_id}, pivot_row_id={pivot_row_id}, storage={storage:?}, ownership={ownership:?}, result={result:?}",
                        self.table_id(),
                        key.index_no,
                        key.vals
                    );
                }
            }
        }
    }

    #[inline]
    async fn defer_delete_owned_row_index_set(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        proof: OwnedRowIndexSetProof<'_, '_, '_>,
    ) -> RuntimeResult<()> {
        for entry in proof.into_entries() {
            self.defer_delete_owned_old_index_entry(rt, effects, entry)
                .await?;
        }
        Ok(())
    }

    #[inline]
    async fn replace_owned_unique_index_row_id(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_row_id: RowID,
    ) -> RuntimeResult<()> {
        let OwnedOldIndexEntry {
            row_id: old_row_id,
            pivot_row_id,
            key,
            storage,
            ownership,
            root_snapshot,
        } = entry;
        let key = key.into_select_key();
        assert_eq!(
            storage,
            OwnedRowIndexStorage::MemRequired,
            "owned-row index-set invariant violated: direct unique RowID replacement requires MemRequired, table_id={}, index_no={}, old_row_id={old_row_id}, new_row_id={new_row_id}, pivot_row_id={pivot_row_id}, storage={storage:?}",
            self.table_id(),
            key.index_no
        );
        assert_eq!(
            ownership,
            OwnedRowMutationOwnership::RowPage,
            "owned-row index-set invariant violated: direct unique RowID replacement requires RowPage ownership, table_id={}, index_no={}, old_row_id={old_row_id}, new_row_id={new_row_id}, pivot_row_id={pivot_row_id}, ownership={ownership:?}",
            self.table_id(),
            key.index_no
        );
        let handle =
            self.snapshot_index_read_handle(rt.pool_guards(), root_snapshot, key.index_no)?;
        let result = handle
            .bind_unique()?
            .compare_exchange_mem(&key.vals, old_row_id, new_row_id, rt.sts())
            .await?;
        assert_eq!(
            result,
            IndexCompareExchange::Ok,
            "owned-row index-set invariant violated: illegal unique replacement result, table_id={}, index_no={}, key={:?}, old_row_id={old_row_id}, new_row_id={new_row_id}, pivot_row_id={pivot_row_id}, storage={storage:?}, ownership={ownership:?}, result={result:?}",
            self.table_id(),
            key.index_no,
            key.vals
        );
        self.push_update_unique_index_undo(rt, effects, old_row_id, new_row_id, key, false);
        Ok(())
    }

    #[inline]
    async fn update_unique_index_key_and_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_key: WriteIndexKey<'_>,
        new_row_id: RowID,
        new_guard: &PageSharedGuard<RowPage>,
    ) -> OperationOrRuntimeResult<()> {
        let old_row_id = old_entry.row_id;
        debug_assert!(old_row_id != new_row_id);
        let new_key = new_key.into_select_key();
        let index_no = new_key.index_no;
        debug_assert_eq!(old_entry.key.index_no(), index_no);
        let sts = rt.sts();
        let handle = self
            .snapshot_index_read_handle(rt.pool_guards(), old_entry.root_snapshot, index_no)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=update_unique_index_key_and_row_id, table_id={}, index_no={}, old_row_id={old_row_id}, new_row_id={new_row_id}",
                    self.table_id(), index_no
                )
            })?;
        let index = handle.bind_unique()?;
        loop {
            // Move update with a unique-key change. The new RowID cannot
            // already be in the index; duplicate handling below decides
            // whether an existing logical-key owner is visible, stale, or
            // should be linked for older snapshots.
            match index
                .insert_if_not_exists_observed(&new_key.vals, new_row_id, false, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=update_unique_index_key_and_row_id, phase=insert_new_key, table_id={}, index_no={}, new_row_id={new_row_id}",
                        self.table_id(), index_no
                    )
                })?
            {
                UniqueInsertAttempt::Inserted { merged } => {
                    debug_assert!(!merged);
                    // New key insert succeed.
                    self.push_insert_unique_index_undo(
                        rt, effects, new_row_id, new_key, false,
                    );
                    // mark index of old row as deleted and defer delete.
                    self.defer_delete_owned_old_index_entry(
                        rt,
                        effects,
                        old_entry,
                    )
                        .await
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=update_unique_index_key_and_row_id, phase=defer_old_key_delete, table_id={}, index_no={}, old_row_id={old_row_id}",
                                self.table_id(), index_no
                            )
                        })?;
                    return Ok(());
                }
                UniqueInsertAttempt::Occupied(observation) => {
                    let index_row_id = observation.owner_row_id();
                    let deleted = observation.deleted();
                    // The new row id is the insert id, so a duplicate points
                    // to another latest or delete-masked owner.
                    debug_assert!(index_row_id != new_row_id);
                    let resolved_lwc = if deleted {
                        None
                    } else if let Some(location) = self
                        .resolve_unmasked_lwc_duplicate(rt, index_row_id)
                        .await
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=update_unique_index_key_and_row_id, phase=check_duplicate_owner, table_id={}, index_no={}, row_id={index_row_id}",
                                self.table_id(), index_no
                            )
                        })?
                    {
                        Some(location)
                    } else {
                        return Err(OperationOrRuntimeError::from(
                            Report::new(OperationError::DuplicateKey).attach(format!(
                                "operation=update_unique_index_key_and_row_id_change, table_id={}, index_no={index_no}, new_row_id={new_row_id}",
                                self.table_id()
                            )),
                        ));
                    };
                    // todo: change the logic.
                    // If we treat move-update just as delete and insert,
                    // with an extra linking step. then, we don't need to
                    // care about if index_row_id equal to old_row_id.
                    if deleted && index_row_id == old_row_id {
                        // This is possible.
                        // For example, transaction update row(RowID=100) key=1 to key=2.
                        //
                        // Then index has following entries:
                        // key=1 -> RowID=100 (old version)
                        // key=2 -> RowID=100 (latest version)
                        //
                        // Then we update key=2 to key=1 again.
                        // And page does not have enough space, so move+update with RowID=200.
                        // Now we should have:
                        // key=1 -> RowID=200 (latest version)
                        // key=2 -> RowID=100 (old version)
                        //
                        // In this case, we can just update index to point to new version.
                        //
                        // There can be an optimization to combine the update into insert.
                        // e.g. add a new method BTree::insert_if_not_exists_or_merge_match_value().
                        // But I think the case is rare so keep as is.
                        match observation
                            .replace(new_row_id, sts)
                            .await
                            .change_context(RuntimeError::TableAccess)
                            .attach_with(|| {
                                format!(
                                    "operation=update_unique_index_key_and_row_id, phase=restore_reused_key, table_id={}, index_no={}, new_row_id={new_row_id}",
                                    self.table_id(), index_no
                                )
                            })?
                        {
                            IndexCompareExchange::Ok => {
                                // New key update succeed.
                                self.push_update_unique_index_undo(
                                    rt, effects, old_row_id, new_row_id, new_key, deleted,
                                );
                                // mark index of old row as deleted and defer delete.
                                self.defer_delete_owned_old_index_entry(
                                    rt,
                                    effects,
                                    old_entry,
                                )
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=update_unique_index_key_and_row_id, phase=defer_old_key_delete, table_id={}, index_no={}, old_row_id={old_row_id}",
                                        self.table_id(), index_no
                                    )
                                })?;
                                return Ok(());
                            }
                            IndexCompareExchange::Mismatch => {
                                unreachable!();
                            }
                            IndexCompareExchange::NotExists => {
                                // re-insert index entry.
                                continue;
                            }
                        }
                    }
                    // A conflicting key points to another row. Inspect that
                    // hot/cold owner before deciding whether this is a true
                    // duplicate, a write conflict, or a linkable old owner.
                    match self
                        .link_for_unique_index(
                            rt,
                            index_row_id,
                            index_no,
                            &new_key.vals,
                            UniqueIndexLinkTarget::new(new_row_id, new_guard),
                            resolved_lwc,
                        )
                        .await?
                    {
                        LinkForUniqueIndex::NotNeeded => {
                            // No visible old version matched the key, so the
                            // existing index entry is stale and can be claimed
                            // if it has not changed concurrently.
                            match observation
                                .replace(new_row_id, sts)
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=update_unique_index_key_and_row_id, phase=claim_stale_owner, table_id={}, index_no={}, new_row_id={new_row_id}",
                                        self.table_id(), index_no
                                    )
                                })?
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeed.
                                    self.push_update_unique_index_undo(
                                        rt,
                                        effects,
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_owned_old_index_entry(
                                        rt,
                                        effects,
                                        old_entry,
                                    )
                                    .await
                                    .change_context(RuntimeError::TableAccess)
                                    .attach_with(|| {
                                        format!(
                                            "operation=update_unique_index_key_and_row_id, phase=defer_old_key_delete, table_id={}, index_no={}, old_row_id={old_row_id}",
                                            self.table_id(), index_no
                                        )
                                    })?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch => {
                                    // This may happen when another transaction insert/update with same key.
                                    return Err(OperationOrRuntimeError::from(
                                        Report::new(OperationError::WriteConflict).attach(format!(
                                            "operation=update_unique_index_key_and_row_id_change, table_id={}, index_no={index_no}, new_row_id={new_row_id}",
                                            self.table_id()
                                        )),
                                    ));
                                }
                                IndexCompareExchange::NotExists => {
                                    // Purge thread may delete the index entry before we update,
                                    // we should re-insert.
                                }
                            }
                        }
                        LinkForUniqueIndex::Linked => {
                            // The older owner was preserved through a runtime
                            // branch. The compare_exchange publishes the new
                            // latest owner while ensuring the entry still
                            // points at the owner we inspected.
                            match observation
                                .replace(new_row_id, sts)
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=update_unique_index_key_and_row_id, phase=claim_linked_owner, table_id={}, index_no={}, new_row_id={new_row_id}",
                                        self.table_id(), index_no
                                    )
                                })?
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeeds.
                                    self.push_update_unique_index_undo(
                                        rt,
                                        effects,
                                        index_row_id,
                                        new_row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_owned_old_index_entry(
                                        rt,
                                        effects,
                                        old_entry,
                                    )
                                    .await
                                    .change_context(RuntimeError::TableAccess)
                                    .attach_with(|| {
                                        format!(
                                            "operation=update_unique_index_key_and_row_id, phase=defer_old_key_delete, table_id={}, index_no={}, old_row_id={old_row_id}",
                                            self.table_id(), index_no
                                        )
                                    })?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch
                                | IndexCompareExchange::NotExists => {
                                    unreachable!()
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_key_and_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_key: WriteIndexKey<'_>,
        new_row_id: RowID,
    ) -> RuntimeResult<()> {
        let old_row_id = old_entry.row_id;
        debug_assert!(old_row_id != new_row_id);
        let new_key = new_key.into_select_key();
        let index_no = new_key.index_no;
        debug_assert_eq!(old_entry.key.index_no(), index_no);
        // Non-unique indexes store exact `(key, row_id)` entries, so a move
        // update inserts the new exact entry and masks the old one.
        match self
            .insert_non_unique_index_mem_required(
                rt,
                &new_key,
                new_row_id,
                false,
                old_entry.root_snapshot,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=update_non_unique_index_key_and_row_id, phase=insert_new_key, table_id={}, index_no={}, new_row_id={new_row_id}",
                    self.table_id(), index_no
                )
            })?
        {
            IndexInsert::Ok(merged) => {
                debug_assert!(!merged);
                // New key insert succeed.
                self.push_insert_non_unique_index_undo(
                    rt, effects, new_row_id, new_key, false,
                );
                // mark index of old row as deleted and defer delete.
                self.defer_delete_owned_old_index_entry(
                    rt,
                    effects,
                    old_entry,
                )
                    .await
                    .change_context(RuntimeError::TableAccess)
                    .attach_with(|| {
                        format!(
                            "operation=update_non_unique_index_key_and_row_id, phase=defer_old_key_delete, table_id={}, index_no={}, old_row_id={old_row_id}",
                            self.table_id(), index_no
                        )
                    })?;
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    #[inline]
    async fn update_unique_index_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_row_id: RowID,
    ) -> RuntimeResult<()> {
        let old_row_id = old_entry.row_id;
        debug_assert!(old_row_id != new_row_id);
        self.replace_owned_unique_index_row_id(rt, effects, old_entry, new_row_id)
            .await
    }

    #[inline]
    async fn update_non_unique_index_only_row_id_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_row_id: RowID,
    ) -> RuntimeResult<()> {
        let old_row_id = old_entry.row_id;
        debug_assert!(old_row_id != new_row_id);
        let key = old_entry.key.as_select_key().clone();
        // Non-unique key unchanged but RowID changed: publish the replacement
        // exact entry, then mask the old exact entry for rollback/GC.
        let res = self
            .insert_non_unique_index_mem_required(
                rt,
                &key,
                new_row_id,
                false,
                old_entry.root_snapshot,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=update_non_unique_index_row_id, phase=insert_new_entry, table_id={}, index_no={}, new_row_id={new_row_id}",
                    self.table_id(), key.index_no
                )
            })?;
        debug_assert!(res.is_ok());
        self.push_insert_non_unique_index_undo(rt, effects, new_row_id, key, false);
        // defer delete old entry.
        self.defer_delete_owned_old_index_entry(
            rt,
            effects,
            old_entry,
        )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=update_non_unique_index_row_id, phase=defer_old_entry_delete, table_id={}, old_row_id={old_row_id}",
                    self.table_id()
                )
            })?;
        Ok(())
    }

    /// Update unique index due to key change.
    /// In this scenario, we only need to insert pair of new key and row id
    /// into index. Keep old index entry as is.
    #[inline]
    async fn update_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_key: WriteIndexKey<'_>,
        page_guard: &PageSharedGuard<RowPage>,
    ) -> OperationOrRuntimeResult<()> {
        let row_id = old_entry.row_id;
        let new_key = new_key.into_select_key();
        let index_no = new_key.index_no;
        debug_assert_eq!(old_entry.key.index_no(), index_no);
        let sts = rt.sts();
        let handle = self
            .snapshot_index_read_handle(rt.pool_guards(), old_entry.root_snapshot, index_no)
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=update_unique_index_key, table_id={}, index_no={}, row_id={row_id}",
                    self.table_id(),
                    index_no
                )
            })?;
        let index = handle.bind_unique()?;
        loop {
            // In-place unique-key change keeps the same RowID. Repeated key
            // changes in the same transaction can encounter this RowID already
            // delete-masked under the new key, so insert may merge by flipping
            // the delete flag back to active.
            //
            // This is case for one transaction or multiple transactions to update
            // key of the same row back and forth.
            // e.g. update k=1 to k=2, then update k=2 to k=1, ...
            //
            // Each update will mask old index entry as deleted, and try to insert a new
            // entry, with same row id(Because it's the same row).
            // And all old versions are also linked from the same row.
            // That mean we can just merge the new index entry into the deleted entry(flip
            // the delete flag) if key and row id all match.
            // So we set merge_if_match_deleted to true.
            match index
                .insert_if_not_exists_observed(&new_key.vals, row_id, true, sts)
                .await
                .change_context(RuntimeError::TableAccess)
                .attach_with(|| {
                    format!(
                        "operation=update_unique_index_key, phase=insert_new_key, table_id={}, index_no={}, row_id={row_id}",
                        self.table_id(), index_no
                    )
                })?
            {
                UniqueInsertAttempt::Inserted { merged } => {
                    // Insert new key success.
                    self.push_insert_unique_index_undo(
                        rt, effects, row_id, new_key, merged,
                    );
                    // Defer delete old key.
                    self.defer_delete_owned_old_index_entry(
                        rt,
                        effects,
                        old_entry,
                    )
                        .await
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=update_unique_index_key, phase=defer_old_key_delete, table_id={}, index_no={}, row_id={row_id}",
                                self.table_id(), index_no
                            )
                        })?;
                    return Ok(());
                }
                UniqueInsertAttempt::Occupied(observation) => {
                    let index_row_id = observation.owner_row_id();
                    let deleted = observation.deleted();
                    // Another owner is mapped to the new key. Inspect it
                    // before deciding whether this is a duplicate, a stale
                    // mapping, or an old owner to preserve through a runtime
                    // unique branch.
                    let resolved_lwc = if deleted {
                        None
                    } else if let Some(location) = self
                        .resolve_unmasked_lwc_duplicate(rt, index_row_id)
                        .await
                        .change_context(RuntimeError::TableAccess)
                        .attach_with(|| {
                            format!(
                                "operation=update_unique_index_key, phase=check_duplicate_owner, table_id={}, index_no={}, row_id={index_row_id}",
                                self.table_id(), index_no
                            )
                        })?
                    {
                        Some(location)
                    } else {
                        return Err(OperationOrRuntimeError::from(
                            Report::new(OperationError::DuplicateKey).attach(format!(
                                "operation=update_unique_index_only_key_change, table_id={}, index_no={index_no}, row_id={row_id}",
                                self.table_id()
                            )),
                        ));
                    };
                    match self
                        .link_for_unique_index(
                            rt,
                            index_row_id,
                            index_no,
                            &new_key.vals,
                            UniqueIndexLinkTarget::new(row_id, page_guard),
                            resolved_lwc,
                        )
                        .await?
                    {
                        LinkForUniqueIndex::NotNeeded | LinkForUniqueIndex::Linked => {
                            // Claim the latest mapping if it still points to
                            // the owner inspected above. A concurrent purge may
                            // remove the entry first, so retry insertion.
                            match observation
                                .replace(row_id, sts)
                                .await
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=update_unique_index_key, phase=claim_latest_owner, table_id={}, index_no={}, row_id={row_id}",
                                        self.table_id(), index_no
                                    )
                                })?
                            {
                                IndexCompareExchange::Ok => {
                                    // New key update succeeds.
                                    self.push_update_unique_index_undo(
                                        rt,
                                        effects,
                                        index_row_id,
                                        row_id,
                                        new_key,
                                        deleted,
                                    );
                                    self.defer_delete_owned_old_index_entry(
                                        rt,
                                        effects,
                                        old_entry,
                                    )
                                    .await
                                    .change_context(RuntimeError::TableAccess)
                                    .attach_with(|| {
                                        format!(
                                            "operation=update_unique_index_key, phase=defer_old_key_delete, table_id={}, index_no={}, row_id={row_id}",
                                            self.table_id(), index_no
                                        )
                                    })?;
                                    return Ok(());
                                }
                                IndexCompareExchange::Mismatch => {
                                    return Err(OperationOrRuntimeError::from(
                                        Report::new(OperationError::WriteConflict).attach(format!(
                                            "operation=update_unique_index_only_key_change, table_id={}, index_no={index_no}, row_id={row_id}",
                                            self.table_id()
                                        )),
                                    ));
                                }
                                IndexCompareExchange::NotExists => {}
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline]
    async fn update_non_unique_index_only_key_change(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        old_entry: OwnedOldIndexEntry<'_, '_, '_>,
        new_key: WriteIndexKey<'_>,
    ) -> RuntimeResult<()> {
        let row_id = old_entry.row_id;
        let new_key = new_key.into_select_key();
        let index_no = new_key.index_no;
        debug_assert_eq!(old_entry.key.index_no(), index_no);
        // This is case for one transaction or multiple transactions to update
        // key of the same row back and forth.
        // e.g. update k=1 to k=2, then update k=2 to k=1, ...
        //
        // Each update will mask old index entry as deleted, and try to insert a new
        // entry, with same row id(Because it's the same row).
        // And all old versions are also linked from the same row.
        // That mean we can just merge the new index entry into the deleted entry(flip
        // the delete flag) if key and row id all match.
        // So we set merge_if_match_deleted to true.
        match self
            .insert_non_unique_index_mem_required(
                rt,
                &new_key,
                row_id,
                true,
                old_entry.root_snapshot,
            )
            .await
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=update_non_unique_index_key, phase=insert_new_key, table_id={}, index_no={}, row_id={row_id}",
                    self.table_id(), index_no
                )
            })?
        {
            IndexInsert::Ok(merged) => {
                self.push_insert_non_unique_index_undo(
                    rt, effects, row_id, new_key, merged,
                );
                // Defer delete old key.
                self.defer_delete_owned_old_index_entry(
                    rt,
                    effects,
                    old_entry,
                )
                    .await
                    .change_context(RuntimeError::TableAccess)
                    .attach_with(|| {
                        format!(
                            "operation=update_non_unique_index_key, phase=defer_old_key_delete, table_id={}, index_no={}, row_id={row_id}",
                            self.table_id(), index_no
                        )
                    })?;
                Ok(())
            }
            IndexInsert::DuplicateKey(..) => unreachable!(),
        }
    }

    /// Scans raw latest row versions from in-memory row-store pages only.
    ///
    /// This helper is for current-state internal users that already account
    /// for the cold/hot split elsewhere. For example, CREATE INDEX builds the
    /// cold DiskTree from a captured active root, then uses this helper to
    /// collect the current hot rows for the new MemIndex while DDL locks and
    /// the table metadata-change lease prevent DML and checkpoint root
    /// movement.
    ///
    /// It includes rows marked deleted and intentionally does not visit
    /// persisted column-store rows. Foreground logical reads must use
    /// `table_scan_mvcc`, which binds cold and hot phases to one root snapshot.
    #[cfg(test)]
    pub(crate) async fn mem_scan_uncommitted<F>(
        &self,
        guards: &PoolGuards,
        row_action: F,
    ) -> RuntimeResult<()>
    where
        F: for<'m, 'p> FnMut(&'m TableColumnLayout, Row<'p>) -> bool,
    {
        self.mem_scan_uncommitted_from(guards, self.mem().pivot_row_id(), row_action)
            .await
    }

    /// Scans raw latest row versions from an explicit captured hot-row boundary.
    ///
    /// CREATE INDEX uses this form so its row-page scan is bound to the same
    /// pivot as its captured column-block root.
    pub(crate) async fn mem_scan_uncommitted_from<F>(
        &self,
        guards: &PoolGuards,
        start_row_id: RowID,
        mut row_action: F,
    ) -> RuntimeResult<()>
    where
        F: for<'m, 'p> FnMut(&'m TableColumnLayout, Row<'p>) -> bool,
    {
        self.mem_scan_from(guards, start_row_id, |page_guard| {
            let col_layout = page_guard.unwrap_vmap().column_layout.as_ref();
            for row_access in page_guard.read_all_rows() {
                if !row_action(col_layout, row_access.row()) {
                    return false;
                }
            }
            true
        })
        .await
    }

    /// Scan cold and hot table rows visible to the transaction snapshot.
    pub(crate) async fn table_scan_mvcc<F>(
        &self,
        rt: TrxRuntime<'_>,
        read_set: &[usize],
        mut row_action: F,
    ) -> RuntimeResult<()>
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        let guards = rt.pool_guards();
        let root_snapshot = self.root_snapshot(rt.ctx());
        if !self
            .scan_cold_lwc_mvcc(guards, rt, read_set, &root_snapshot, &mut row_action)
            .await?
        {
            return Ok(());
        }
        let metadata = self.metadata();
        self.mem_scan_from(guards, root_snapshot.pivot_row_id(), |page_guard| {
            for row_access in page_guard.read_all_rows() {
                match row_access.read_row_mvcc(rt.ctx(), metadata, read_set, None) {
                    ReadRow::InvalidIndex => unreachable!(),
                    ReadRow::NotFound => (),
                    ReadRow::Ok(vals) => {
                        if !row_action(vals) {
                            return false;
                        }
                    }
                }
            }
            true
        })
        .await
    }

    /// Mutate latest rows selected by one secondary-index logical-key range.
    pub(crate) async fn table_index_mutate_mvcc<'r, R, F>(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        range: R,
        validate_updates: bool,
        mut mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        R: RangeBounds<&'r [Val]>,
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        let root_snapshot = self.root_snapshot(rt.ctx());
        let index = self.require_sec_idx(index_no).disclose()?;
        let unique = index.is_unique();
        let encoder = index.key_encoder();
        let encoded_range = if unique {
            encoder.encode_range(range)
        } else {
            encoder.encode_non_unique_range(range)
        };
        let mut stream = BorrowedIndexMutationStream::new(
            index,
            rt.pool_guards(),
            &root_snapshot,
            &encoded_range,
        )
        .disclose()?;
        let column_count = self.metadata().col.col_count();
        let mut value_buffer = vec![Val::default(); column_count];
        let mut outcome = TableMutationOutcome::default();
        let mut mutator = IndexMutator::new(self, rt, effects, &root_snapshot, validate_updates);

        while let Some(batch) = stream.next_batch().await.disclose()? {
            for candidate in batch {
                let candidate = BoundIndexCandidate::new(index_no, unique, encoder, candidate);
                mutator
                    .mutate_index_candidate(
                        candidate,
                        &mut value_buffer,
                        &mut outcome,
                        &mut mutate_row,
                    )
                    .await?;
            }
        }
        mutator.apply_deferred_index_updates().await?;
        Ok(outcome)
    }

    /// Mutate callback-selected rows from one original latest-read worklist.
    ///
    /// This public-result contract is retained solely to transport an arbitrary
    /// public error returned by the caller-owned mutation callback.
    pub(crate) async fn table_mutate_mvcc<F>(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        validate_updates: bool,
        mut mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        // Step 1: Freeze the statement's cold root and original hot-page shape
        // before any replacement rows can change scan boundaries.
        let root_snapshot = self.root_snapshot(rt.ctx());
        let (upper_bound, original_pages) = self
            .mem()
            .snapshot_original_row_pages_from(rt.pool_guards(), root_snapshot.pivot_row_id())
            .await
            .disclose()?;
        let validator = validate_updates.then(|| DmlValidator::new(self.metadata()));
        let column_count = self.metadata().col.col_count();
        let mut state = TableMutationState {
            tracker: ScanBoundaryTracker::new(upper_bound, original_pages),
            column_count,
            value_buffer: vec![Val::default(); column_count],
            outcome: TableMutationOutcome::default(),
        };

        self.mutate_cold_rows_mvcc(
            rt,
            effects,
            &root_snapshot,
            validator.as_ref(),
            &mut state,
            &mut mutate_row,
        )
        .await?;
        self.mutate_hot_rows_mvcc(
            rt,
            effects,
            &root_snapshot,
            validator.as_ref(),
            &mut state,
            &mut mutate_row,
        )
        .await?;
        Ok(state.outcome)
    }

    /// Mutate callback-selected rows from the statement's persisted region.
    ///
    /// This helper retains public `Result` only while merging typed storage
    /// failures with an arbitrary public error from the mutation callback.
    async fn mutate_cold_rows_mvcc<F>(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        root_snapshot: &TableRootSnapshot<'_>,
        validator: Option<&DmlValidator<'_>>,
        state: &mut TableMutationState,
        mutate_row: &mut F,
    ) -> Result<()>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        let column_root = root_snapshot.column_block_index_root();
        let pivot_row_id = root_snapshot.pivot_row_id();
        if column_root == SUPER_BLOCK_ID || pivot_row_id == RowID::new(0) {
            return Ok(());
        }

        let storage = self.column_storage();
        let deletion_buffer = self.lwc_deletion_buffer();
        let column_layout = self.metadata().col.as_ref();
        let reader_status = rt.status();
        let file_kind = storage.file().file_kind();
        let disk_guard = rt.pool_guards().disk_guard();
        let entries = {
            let column_index = ColumnBlockIndex::new(
                column_root,
                pivot_row_id,
                file_kind,
                storage.file().sparse_file(),
                storage.disk_pool(),
                disk_guard,
            );
            column_index.collect_leaf_entries().await.disclose()?
        };
        for entry in entries {
            let (delete_deltas, row_ids) = {
                let column_index = ColumnBlockIndex::new(
                    column_root,
                    pivot_row_id,
                    file_kind,
                    storage.file().sparse_file(),
                    storage.disk_pool(),
                    disk_guard,
                );
                column_index
                    .load_delete_deltas_and_row_ids(&entry)
                    .await
                    .disclose()?
            };
            let durable_deleted =
                persisted_delete_set_for_scan(file_kind, &entry, delete_deltas).disclose()?;
            let has_durable_deletes = !durable_deleted.is_empty();
            // A wait before callback selection leaves the cursor on this row so
            // the persisted image and current marker are reloaded. Staged
            // callback output is drained exactly once before any wait.
            let mut row_idx = 0;
            let mut pending = Vec::new();
            while row_idx < row_ids.len() {
                let persisted = storage
                    .load_lwc_block(disk_guard, entry.block_id())
                    .await
                    .disclose()?;
                let block = persisted.block();
                validate_cold_scan_entry(file_kind, &entry, block, &row_ids).disclose()?;
                let mut prepare_wait: Option<PoisonAwareListener> = None;
                while row_idx < row_ids.len() {
                    let row_id = row_ids[row_idx];
                    match read_latest_cold_row(
                        deletion_buffer,
                        reader_status.as_ref(),
                        row_id,
                        has_durable_deletes && durable_deleted.contains(&row_id),
                    ) {
                        ColdLatestRow::Readable => (),
                        ColdLatestRow::NotFound => {
                            row_idx += 1;
                            continue;
                        }
                        ColdLatestRow::WriteConflict => {
                            return Err(Report::new(OperationError::WriteConflict)
                                .attach(format!(
                                    "full-table mutation latest cold-row read: row_id={row_id}"
                                ))
                                .disclose());
                        }
                        ColdLatestRow::Preparing(listener) => {
                            prepare_wait = Some(listener);
                            break;
                        }
                    }
                    let source = LazyRowSource::Cold {
                        block,
                        column_layout,
                        row_idx,
                        file_kind,
                        block_id: entry.block_id(),
                    };
                    let mut lazy_row = LazyRow::new(
                        source,
                        mem::take(&mut state.value_buffer),
                        state.column_count,
                    );
                    match mutate_row(&mut lazy_row)? {
                        RowMutation::Skip => {
                            state.value_buffer = lazy_row.into_reusable_buffer();
                        }
                        RowMutation::Delete => {
                            state.outcome.delete_count += 1;
                            let (index_keys, reusable) =
                                lazy_row.into_index_keys(self).disclose()?;
                            state.value_buffer = reusable;
                            pending.push(PendingColdMutation::Delete { row_id, index_keys });
                        }
                        RowMutation::Update(update) => {
                            state.outcome.update_count += 1;
                            self.validate_table_mutation_update(validator, &update)
                                .disclose()?;
                            if update.is_empty() {
                                state.value_buffer = lazy_row.into_reusable_buffer();
                            } else {
                                let (old_row, reusable) = lazy_row.into_full_row().disclose()?;
                                state.value_buffer = reusable;
                                pending.push(PendingColdMutation::Update {
                                    row_id,
                                    old_row,
                                    update,
                                });
                            }
                        }
                    }
                    row_idx += 1;
                }
                // Release the read-only block before applying staged mutations
                // or waiting. A staged action may itself wait on its definitive
                // CDB claim, but owns all callback output needed to retry
                // without invoking user code again.
                drop(persisted);
                for mutation in pending.drain(..) {
                    match mutation {
                        PendingColdMutation::Delete { row_id, index_keys } => {
                            self.delete_known_cold_row(
                                rt,
                                effects,
                                row_id,
                                index_keys,
                                root_snapshot,
                            )
                            .await
                            .disclose()?;
                        }
                        PendingColdMutation::Update {
                            row_id,
                            old_row,
                            update,
                        } => {
                            let inserted = self
                                .update_known_cold_row(
                                    rt,
                                    effects,
                                    row_id,
                                    old_row,
                                    update,
                                    root_snapshot,
                                )
                                .await
                                .disclose()?;
                            state.tracker.observe_insert(inserted);
                        }
                    }
                }
                // `None` means the scan completed without interruption. `Some`
                // carries opaque retry authority whose internal state records
                // whether primary completion still needs a wait or only a
                // sticky poison recheck.
                if let Some(listener) = prepare_wait {
                    rt.wait_prepare_or_poison(listener).await.disclose()?;
                }
            }
        }
        Ok(())
    }

    /// Mutate callback-selected rows from the statement's original hot pages.
    ///
    /// This helper retains public `Result` only while merging typed storage
    /// failures with an arbitrary public error from the mutation callback.
    async fn mutate_hot_rows_mvcc<F>(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        root_snapshot: &TableRootSnapshot<'_>,
        validator: Option<&DmlValidator<'_>>,
        state: &mut TableMutationState,
        mutate_row: &mut F,
    ) -> Result<()>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        // Visit only the hot pages captured at statement start; pages created
        // by cold or hot replacements are outside this descriptor list.
        for page_idx in 0..state.tracker.page_count() {
            let descriptor = state.tracker.page(page_idx);
            let page_guard = self
                .mem()
                .must_get_row_page_shared(rt.pool_guards(), descriptor.page_id)
                .await
                .disclose()?;
            let page = page_guard.page();
            let actual_end = page.header.start_row_id + u64::from(page.header.max_row_count);
            assert!(
                page.header.start_row_id == descriptor.start_row_id
                    && actual_end == descriptor.end_row_id,
                "TableData(X) must preserve the original full-table-mutation row-page identity: table_id={}, page_id={}, expected_range={}..{}, actual_range={}..{}",
                self.table_id(),
                descriptor.page_id,
                descriptor.start_row_id,
                descriptor.end_row_id,
                page.header.start_row_id,
                actual_end
            );
            // Bound this page at its original tail or at the first replacement
            // inserted before the page was reached.
            let current_end = descriptor.start_row_id + page.header.row_count() as u64;
            drop(page_guard);
            let scan_end = state.tracker.start_page(page_idx, current_end);
            debug_assert!(scan_end >= descriptor.start_row_id && scan_end <= current_end);
            for row_id_raw in descriptor.start_row_id.as_u64()..scan_end.as_u64() {
                let row_id = RowID::new(row_id_raw);
                let page_guard = self
                    .mem()
                    .must_get_row_page_shared(rt.pool_guards(), descriptor.page_id)
                    .await
                    .disclose()?;
                let access = page_guard.read_row_by_id(row_id);
                match access.read_latest(rt.ctx()) {
                    ReadLatestRow::Readable => (),
                    ReadLatestRow::NotFound => continue,
                    ReadLatestRow::WriteConflict => {
                        return Err(Report::new(OperationError::WriteConflict)
                            .attach(format!(
                                "full-table mutation latest hot-row read: row_id={row_id}"
                            ))
                            .disclose());
                    }
                }
                // Retain one latest-row read guard through the callback, count
                // matches, and validate the update before changing data.
                let source = LazyRowSource::Hot {
                    access,
                    column_layout: self.metadata().col.as_ref(),
                };
                let mut lazy_row = LazyRow::new(
                    source,
                    mem::take(&mut state.value_buffer),
                    state.column_count,
                );
                match mutate_row(&mut lazy_row)? {
                    RowMutation::Skip => {
                        state.value_buffer = lazy_row.into_reusable_buffer();
                    }
                    RowMutation::Delete => {
                        state.outcome.delete_count += 1;
                        let (index_keys, reusable) = lazy_row.into_index_keys(self).disclose()?;
                        state.value_buffer = reusable;
                        self.delete_known_hot_row(
                            rt,
                            effects,
                            page_guard,
                            row_id,
                            index_keys,
                            root_snapshot,
                        )
                        .await
                        .disclose()?;
                    }
                    RowMutation::Update(update) => {
                        state.outcome.update_count += 1;
                        self.validate_table_mutation_update(validator, &update)
                            .disclose()?;
                        state.value_buffer = lazy_row.into_reusable_buffer();
                        if update.is_empty() {
                            continue;
                        }
                        // Register out-of-place replacements so later page scans
                        // retain their original bounds.
                        let inserted = self
                            .update_known_hot_row(
                                rt,
                                effects,
                                page_guard,
                                row_id,
                                update,
                                root_snapshot,
                            )
                            .await
                            .disclose()?;
                        if let Some(inserted) = inserted {
                            state.tracker.observe_insert(inserted);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn validate_table_mutation_update(
        &self,
        validator: Option<&DmlValidator<'_>>,
        update: &[UpdateCol],
    ) -> OperationResult<()> {
        if let Some(validator) = validator {
            validator
                .validate_sparse_update(update)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation=table_mutate_mvcc, table_id={}", self.table_id())
                })?;
        }
        Ok(())
    }

    #[inline]
    async fn install_cold_delete_effects(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        index_keys: WriteIndexKeySet<'op>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> RuntimeResult<()> {
        effects.push_row_undo(OwnedRowUndo::new(
            effects.stmt_no(),
            self.table_id(),
            None,
            row_id,
            RowUndoKind::Delete,
        ));
        effects.insert_row_redo(
            self.table_id(),
            RowRedo {
                row_id,
                kind: RowRedoKind::Delete(None),
            },
        );
        let proof = self
            .owned_cdb_index_set_proof(rt.pool_guards(), row_id, index_keys, root_snapshot)
            .await?;
        self.defer_delete_owned_row_index_set(rt, effects, proof)
            .await
    }

    /// Convert a provisional cold-row lock into definitive delete effects.
    #[inline]
    async fn finish_owned_cold_delete_effects(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        index_keys: WriteIndexKeySet<'op>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> RuntimeResult<()> {
        effects.update_last_row_undo(RowUndoKind::Delete);
        effects.insert_row_redo(
            self.table_id(),
            RowRedo {
                row_id,
                kind: RowRedoKind::Delete(None),
            },
        );
        let proof = self
            .owned_cdb_index_set_proof(rt.pool_guards(), row_id, index_keys, root_snapshot)
            .await?;
        self.defer_delete_owned_row_index_set(rt, effects, proof)
            .await
    }

    #[inline]
    async fn update_owned_cold_row(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        old_row: Vec<Val>,
        update: Vec<UpdateCol>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> QuadResult<InsertedRow> {
        let old_index_keys = WriteIndexKeySet::from_full_row(self, &old_row);
        self.finish_owned_cold_delete_effects(rt, effects, row_id, old_index_keys, root_snapshot)
            .await
            .attach("index-driven mutation cold update delete effects")?;
        let new_row = self.build_cold_update_row(old_row, RowUpdateInput::Sparse(update));
        let new_index_keys = WriteIndexKeySet::from_full_row(self, &new_row);
        let (new_row_id, new_guard) = self
            .insert_row_internal(rt, effects, new_row, RowUndoKind::Insert, Vec::new())
            .await?;
        self.insert_index_set(
            rt,
            effects,
            new_index_keys,
            new_row_id,
            &new_guard,
            root_snapshot,
        )
        .await
        .attach("index-driven mutation cold replacement index claim")?;
        Ok(InsertedRow::new(new_guard.page_id(), new_row_id))
    }

    #[inline]
    async fn claim_known_cold_row(
        &self,
        rt: TrxRuntime<'_>,
        row_id: RowID,
    ) -> OperationOrFatalResult<()> {
        let deletion_buffer = self.lwc_deletion_buffer();
        self.debug_assert_table_write_lock_held(rt);
        loop {
            match deletion_buffer.claim_ref(row_id, Arc::clone(rt.status()), rt.sts(), false) {
                Ok(DeletionClaim::Acquired) => return Ok(()),
                Ok(DeletionClaim::Preparing(listener)) => {
                    rt.wait_prepare_or_poison(listener).await?;
                }
                Err(DeletionError::WriteConflict) => {
                    return Err(Report::new(OperationError::WriteConflict).into());
                }
                Err(DeletionError::AlreadyDeleted) => {
                    return Err(Report::new(OperationError::WriteConflict)
                        .attach("full-table mutation cold row changed after visibility")
                        .into());
                }
            }
        }
    }

    #[inline]
    async fn delete_known_cold_row(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        index_keys: WriteIndexKeySet<'op>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> QuadResult<()> {
        self.claim_known_cold_row(rt, row_id)
            .await
            .attach("full-table mutation cold delete marker ownership")?;
        self.install_cold_delete_effects(rt, effects, row_id, index_keys, root_snapshot)
            .await
            .attach("full-table mutation cold delete index masking")?;
        Ok(())
    }

    #[inline]
    async fn update_known_cold_row(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        row_id: RowID,
        old_row: Vec<Val>,
        update: Vec<UpdateCol>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> QuadResult<InsertedRow> {
        self.claim_known_cold_row(rt, row_id)
            .await
            .attach("full-table mutation cold update marker ownership")?;
        let old_index_keys = WriteIndexKeySet::from_full_row(self, &old_row);
        self.install_cold_delete_effects(rt, effects, row_id, old_index_keys, root_snapshot)
            .await
            .attach("full-table mutation cold update delete effects")?;
        let new_row = self.build_cold_update_row(old_row, RowUpdateInput::Sparse(update));
        let new_index_keys = WriteIndexKeySet::from_full_row(self, &new_row);
        let (new_row_id, new_guard) = self
            .insert_row_internal(rt, effects, new_row, RowUndoKind::Insert, Vec::new())
            .await?;
        self.insert_index_set(
            rt,
            effects,
            new_index_keys,
            new_row_id,
            &new_guard,
            root_snapshot,
        )
        .await
        .attach("full-table mutation cold replacement index claim")?;
        let inserted = InsertedRow::new(new_guard.page_id(), new_row_id);
        Ok(inserted)
    }

    #[inline]
    async fn delete_known_hot_row(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        index_keys: WriteIndexKeySet<'op>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> QuadResult<()> {
        let result = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
            .delete_known_row(effects)
            .await?;
        match result {
            DeleteInternal::Ok => {
                let proof = self.owned_row_page_index_set_proof(row_id, index_keys, root_snapshot);
                // Row undo ownership keeps the reconstructed key set stable;
                // release the page latch before awaiting MemIndex mutations.
                drop(page_guard);
                self.defer_delete_owned_row_index_set(rt, effects, proof)
                    .await
                    .attach("full-table mutation hot delete index masking")?;
                Ok(())
            }
            DeleteInternal::NotFound => Err(Report::new(OperationError::WriteConflict)
                .attach("full-table mutation hot row changed after visibility")
                .into()),
            DeleteInternal::RetryInTransition => {
                unreachable!(
                    "full-table mutation observed TRANSITION while holding TableData(X): table_id={}, row_id={row_id}",
                    self.table_id()
                )
            }
        }
    }

    #[inline]
    async fn update_known_hot_row(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        page_guard: PageSharedGuard<RowPage>,
        row_id: RowID,
        update: Vec<UpdateCol>,
        root_snapshot: &TableRootSnapshot<'_>,
    ) -> QuadResult<Option<InsertedRow>> {
        let result = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
            .update_known_row(effects, RowUpdateInput::Sparse(update))
            .await?;
        match result {
            UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                debug_assert_eq!(row_id, new_row_id);
                if !index_change_cols.is_empty() {
                    self.update_indexes_only_key_change(
                        rt,
                        effects,
                        row_id,
                        &page_guard,
                        &index_change_cols,
                        root_snapshot,
                    )
                    .await
                    .attach("full-table mutation hot key change")?;
                }
                Ok(None)
            }
            UpdateRowInplace::RowDeleted(_) | UpdateRowInplace::RowNotFound(_) => {
                Err(Report::new(OperationError::WriteConflict)
                    .attach("full-table mutation hot row changed after visibility")
                    .into())
            }
            UpdateRowInplace::RetryInTransition(_) => {
                // Checkpoint transition holds TableData(IS), which is
                // incompatible with the TableData(X) held by full-table mutation.
                unreachable!(
                    "full-table mutation observed TRANSITION while holding TableData(X): table_id={}, row_id={row_id}",
                    self.table_id()
                )
            }
            UpdateRowInplace::NoFreeSpaceOrFrozen(old_row_id, old_row, update) => {
                // The move result already owns the complete old row image.
                // Capture its full old index set before replacement preparation
                // consumes the values.
                let old_index_keys = WriteIndexKeySet::from_full_row(self, &old_row);
                let (new_row_id, index_change_cols, new_guard) = self
                    .move_update_for_space(rt, effects, old_row, update, old_row_id, page_guard)
                    .await?;
                let proof =
                    self.owned_row_page_index_set_proof(old_row_id, old_index_keys, root_snapshot);
                let result = if index_change_cols.is_empty() {
                    self.update_indexes_only_row_id_change(
                        rt, effects, old_row_id, new_row_id, proof,
                    )
                    .await
                    .map_err(OperationOrRuntimeError::from)
                } else {
                    self.update_indexes_may_both_change(
                        rt,
                        effects,
                        RowIdMove::new(old_row_id, new_row_id),
                        &index_change_cols,
                        &new_guard,
                        proof,
                    )
                    .await
                };
                let inserted = InsertedRow::new(new_guard.page_id(), new_row_id);
                result.attach("full-table mutation hot move index update")?;
                Ok(Some(inserted))
            }
        }
    }

    /// Lookup one visible row through a unique secondary index.
    pub(crate) async fn index_lookup_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> RuntimeResult<SelectMvcc> {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        debug_assert!({
            !user_read_set.is_empty()
                && user_read_set
                    .iter()
                    .zip(user_read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        let handle = self.current_index_read_handle(rt, index_no)?;
        let index = handle.bind_unique()?;
        match index.lookup(key_vals, rt.sts()).await? {
            None => Ok(SelectMvcc::NotFound),
            Some((row_id, _)) => {
                self.index_lookup_unique_row_mvcc(rt, index_no, key_vals, user_read_set, row_id)
                    .await
            }
        }
    }

    /// Lookup visible rows matching one non-unique secondary-index key.
    pub(crate) async fn index_lookup_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        index_no: usize,
        key_vals: &[Val],
        read_set: &[usize],
    ) -> RuntimeResult<ScanMvcc> {
        debug_assert!(index_no < self.sec_idx_len());
        // Index scan should be applied to non-unique index.
        // todo: support partial key scan on unique index.
        debug_assert!(
            !self
                .metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        debug_assert!({
            !read_set.is_empty()
                && read_set
                    .iter()
                    .zip(read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        let mut res = vec![];
        let handle = self.current_index_read_handle(rt, index_no)?;
        let encoder = handle.key_encoder();
        let index = handle.bind_non_unique()?;
        let range = encoder.encode_non_unique_equal_range(key_vals);
        let mut stream = index.equal_scan_candidates(&range, rt.sts())?;
        while let Some(batch) = stream.next_batch().await? {
            for candidate in batch {
                let candidate = BoundIndexCandidate::new(index_no, false, encoder, candidate);
                match self
                    .index_lookup_candidate_row_mvcc(rt, candidate, read_set)
                    .await?
                {
                    SelectMvcc::NotFound => (),
                    SelectMvcc::Found(vals) => {
                        res.push(vals);
                    }
                }
            }
        }
        Ok(ScanMvcc::Rows(res))
    }

    /// Scan visible rows matching a secondary-index key range.
    pub(crate) async fn index_scan_mvcc<'r, R>(
        &self,
        rt: TrxRuntime<'_>,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> RuntimeResult<ScanMvcc>
    where
        R: RangeBounds<&'r [Val]>,
    {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!({
            !read_set.is_empty()
                && read_set
                    .iter()
                    .zip(read_set.iter().skip(1))
                    .all(|(l, r)| l < r)
        });
        let mut res = vec![];
        let handle = self.current_index_read_handle(rt, index_no)?;
        let unique = handle.is_unique();
        let encoder = handle.key_encoder();
        let range = if unique {
            encoder.encode_range(range)
        } else {
            encoder.encode_non_unique_range(range)
        };
        if unique {
            let index = handle.bind_unique()?;
            let mut stream = index.index_scan_candidates(&range, rt.sts())?;
            while let Some(batch) = stream.next_batch().await? {
                for candidate in batch {
                    let candidate = BoundIndexCandidate::new(index_no, true, encoder, candidate);
                    match self
                        .index_lookup_candidate_row_mvcc(rt, candidate, read_set)
                        .await?
                    {
                        SelectMvcc::NotFound => (),
                        SelectMvcc::Found(vals) => {
                            res.push(vals);
                        }
                    }
                }
            }
        } else {
            let index = handle.bind_non_unique()?;
            let mut stream = index.index_scan_candidates(&range, rt.sts())?;
            while let Some(batch) = stream.next_batch().await? {
                for candidate in batch {
                    let candidate = BoundIndexCandidate::new(index_no, false, encoder, candidate);
                    match self
                        .index_lookup_candidate_row_mvcc(rt, candidate, read_set)
                        .await?
                    {
                        SelectMvcc::NotFound => (),
                        SelectMvcc::Found(vals) => {
                            res.push(vals);
                        }
                    }
                }
            }
        }
        Ok(ScanMvcc::Rows(res))
    }

    /// Insert a new MVCC row and claim all secondary-index entries.
    pub(crate) async fn insert_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        cols: Vec<Val>,
    ) -> QuadResult<RowID> {
        let metadata = self.metadata();
        debug_assert!(cols.len() == metadata.col.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| self.metadata().col.col_type_match(idx, val))
        });
        let keys = WriteIndexKeySet::from_full_row(self, &cols);
        let root_snapshot = self.root_snapshot(rt.ctx());
        // Insert always creates a hot RowStore row. The insert undo head makes
        // the new row invisible to older snapshots and is also the rollback
        // handle if any following index insert fails.
        let (row_id, page_guard) = self
            .insert_row_internal(rt, effects, cols, RowUndoKind::Insert, Vec::new())
            .await?;
        // This foreground method is a genuine mixed seam: row allocation above
        // can already contribute Runtime-or-Fatal, while index claims contribute
        // Operation-or-Runtime. Convert each native carrier only here.
        // Secondary-index claims are made after the row exists so unique
        // duplicate handling can link this new hot row to older owners if
        // needed for MVCC visibility.
        self.insert_index_set(rt, effects, keys, row_id, &page_guard, &root_snapshot)
            .await
            .attach("insert MVCC secondary index claim")?;
        Ok(row_id)
    }

    /// Insert or replace one MVCC row selected by a unique key derived from the row.
    pub(crate) async fn upsert_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        unique_index_no: usize,
        cols: Vec<Val>,
        log_by_key: bool,
    ) -> QuadResult<UpsertMvcc> {
        let key = unique_key_from_full_row(
            self.metadata(),
            unique_index_no,
            &cols,
            "upsert_unique_mvcc",
        );
        let input = RowUpdateInput::FullRow(cols);
        match self
            .update_unique_mvcc_input(rt, effects, key.index_no, &key.vals, input, log_by_key)
            .await?
        {
            UpdateUniqueMvcc::Updated(row_id) => Ok(UpsertMvcc::Updated(row_id)),
            UpdateUniqueMvcc::NotFound(input) => {
                let cols = input
                    .into_full_row()
                    .expect("upsert update input must preserve the full row");
                self.insert_mvcc(rt, effects, cols)
                    .await
                    .map(UpsertMvcc::Inserted)
            }
        }
    }

    /// Update the visible row found through a unique secondary-index key.
    pub(crate) async fn update_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
        log_by_key: bool,
    ) -> QuadResult<UpdateMvcc> {
        let input = RowUpdateInput::Sparse(update);
        match self
            .update_unique_mvcc_input(rt, effects, index_no, key_vals, input, log_by_key)
            .await?
        {
            UpdateUniqueMvcc::Updated(row_id) => Ok(UpdateMvcc::Updated(row_id)),
            UpdateUniqueMvcc::NotFound(_) => Ok(UpdateMvcc::NotFound),
        }
    }

    #[inline]
    async fn update_unique_mvcc_input(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
        mut input: RowUpdateInput,
        log_by_key: bool,
    ) -> QuadResult<UpdateUniqueMvcc> {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        debug_assert!(
            input.as_view().is_valid_for(self.metadata().col.as_ref()),
            "row update values must be ordered, in range, and type-compatible"
        );
        'retry: loop {
            let attempt = 'attempt: {
                let root_snapshot = self.root_snapshot(rt.ctx());
                let handle =
                    self.snapshot_index_read_handle(rt.pool_guards(), &root_snapshot, index_no)?;
                let index = handle.bind_unique()?;
                match index.lookup(key_vals, rt.sts()).await? {
                    None => return Ok(UpdateUniqueMvcc::NotFound(input)),
                    Some((row_id, _)) => match self
                        .resolve_row_location(rt.pool_guards(), row_id)
                        .await
                    {
                        Ok(RowLocation::NotFound) => {
                            return Ok(UpdateUniqueMvcc::NotFound(input));
                        }
                        Ok(RowLocation::LwcBlock(location)) => {
                            // LWC rows are immutable. A cold update is represented
                            // as an owned CDB delete marker for the old row plus a
                            // new hot RowStore row containing the updated values.
                            // read_lwc_row_for_update() checks snapshot visibility
                            // before decoding, then delegates key revalidation.
                            let metadata = self.metadata();
                            let old_vals = match self
                                .read_lwc_row_for_update(rt, row_id, location, |vals| {
                                    metadata.idx.match_key(index_no, key_vals, vals)
                                })
                                .await?
                            {
                                ColdRowUpdateRead::Ok(vals) => vals,
                                ColdRowUpdateRead::NotFound => {
                                    return Ok(UpdateUniqueMvcc::NotFound(input));
                                }
                                ColdRowUpdateRead::WriteConflict => {
                                    return Err(Report::new(OperationError::WriteConflict)
                                        .attach("update MVCC cold row read")
                                        .into());
                                }
                                ColdRowUpdateRead::Preparing(listener) => {
                                    break 'attempt PointMutationAttempt::Preparing(listener);
                                }
                            };
                            let deletion_buffer = self.lwc_deletion_buffer();
                            // The read above is only validation. This claim is
                            // the definitive ownership operation and rechecks
                            // CDB state under the map entry to catch races with
                            // other cold delete/update transactions.
                            self.debug_assert_table_write_lock_held(rt);
                            match deletion_buffer.claim_ref(
                                row_id,
                                Arc::clone(rt.status()),
                                rt.sts(),
                                location.durable_deleted,
                            ) {
                                Ok(DeletionClaim::Acquired) => (),
                                Ok(DeletionClaim::Preparing(listener)) => {
                                    break 'attempt PointMutationAttempt::Preparing(listener);
                                }
                                Err(DeletionError::WriteConflict) => {
                                    return Err(Report::new(OperationError::WriteConflict)
                                        .attach("update MVCC cold delete marker ownership")
                                        .into());
                                }
                                Err(DeletionError::AlreadyDeleted) => {
                                    return Ok(UpdateUniqueMvcc::NotFound(input));
                                }
                            }
                            let old_index_keys = WriteIndexKeySet::from_full_row(self, &old_vals);
                            self.install_cold_delete_effects(
                                rt,
                                effects,
                                row_id,
                                old_index_keys,
                                &root_snapshot,
                            )
                            .await?;

                            let new_row = self.build_cold_update_row(old_vals, input);
                            let new_index_keys = WriteIndexKeySet::from_full_row(self, &new_row);
                            let (new_row_id, new_guard) = self
                                .insert_row_internal(
                                    rt,
                                    effects,
                                    new_row,
                                    RowUndoKind::Insert,
                                    Vec::new(),
                                )
                                .await?;
                            // Row allocation can already contribute Runtime-or-Fatal;
                            // keep index mutation typed until this mixed seam.
                            self.insert_index_set(
                                rt,
                                effects,
                                new_index_keys,
                                new_row_id,
                                &new_guard,
                                &root_snapshot,
                            )
                            .await
                            .attach("update MVCC cold replacement index claim")?;
                            return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                        }
                        Ok(RowLocation::RowPage(page_id)) => {
                            // Hot-row update proceeds through row-page locking and
                            // undo-chain visibility. A stale index location is
                            // retried or rejected before any row mutation.
                            let Some(page_guard) = self
                                .mem()
                                .try_get_validated_row_page_shared_result(
                                    rt.pool_guards(),
                                    page_id,
                                    row_id,
                                )
                                .await?
                            else {
                                continue 'retry;
                            };
                            PointMutationAttempt::Hot {
                                root_snapshot,
                                page_guard,
                                row_id,
                            }
                        }
                        Err(err) => return Err(err.into()),
                    },
                }
            };
            let (root_snapshot, page_guard, row_id) = match attempt {
                PointMutationAttempt::Hot {
                    root_snapshot,
                    page_guard,
                    row_id,
                } => (root_snapshot, page_guard, row_id),
                PointMutationAttempt::Preparing(listener) => {
                    rt.wait_prepare_or_poison(listener).await?;
                    continue;
                }
            };
            let res = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
                .update_inplace(effects, index_no, key_vals, input, log_by_key)
                .await?;
            match res {
                UpdateRowInplace::Ok(new_row_id, index_change_cols) => {
                    debug_assert!(row_id == new_row_id);
                    if !index_change_cols.is_empty() {
                        // RowID is unchanged, but logical keys may have moved.
                        // Update MemIndex after the page mutation so rollback
                        // can restore both row data and index visibility.
                        self.update_indexes_only_key_change(
                            rt,
                            effects,
                            row_id,
                            &page_guard,
                            &index_change_cols,
                            &root_snapshot,
                        )
                        .await
                        .attach("update MVCC key-change index update")?;
                        return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                    } // otherwise, do nothing
                    return Ok(UpdateUniqueMvcc::Updated(row_id));
                }
                UpdateRowInplace::RowDeleted(input) | UpdateRowInplace::RowNotFound(input) => {
                    return Ok(UpdateUniqueMvcc::NotFound(input));
                }
                UpdateRowInplace::RetryInTransition(returned_input) => {
                    input = returned_input;
                    // Release the row page so the checkpoint transition can complete.
                    drop(page_guard);
                    self.wait_transition_route_or_poison(rt, row_id).await?;
                }
                UpdateRowInplace::NoFreeSpaceOrFrozen(old_row_id, old_row, returned_input) => {
                    // In-place update failed after the old row was locked and
                    // marked deleted. Finish the move update by inserting the
                    // replacement row and then update indexes for any RowID or
                    // key movement.
                    // Capture the complete old set from the returned old image;
                    // later helpers read the replacement only for changed keys.
                    let old_index_keys = WriteIndexKeySet::from_full_row(self, &old_row);
                    let (new_row_id, index_change_cols, new_guard) = self
                        .move_update_for_space(
                            rt,
                            effects,
                            old_row,
                            returned_input,
                            old_row_id,
                            page_guard,
                        )
                        .await?;
                    let proof = self.owned_row_page_index_set_proof(
                        old_row_id,
                        old_index_keys,
                        &root_snapshot,
                    );
                    if !index_change_cols.is_empty() {
                        // old guard is already marked inside.
                        self.update_indexes_may_both_change(
                            rt,
                            effects,
                            RowIdMove::new(old_row_id, new_row_id),
                            &index_change_cols,
                            &new_guard,
                            proof,
                        )
                        .await
                        .attach("update MVCC moved-row index update")?;
                        return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                    } else {
                        self.update_indexes_only_row_id_change(
                            rt, effects, old_row_id, new_row_id, proof,
                        )
                        .await
                        .attach("update MVCC moved-row index update")?;
                        return Ok(UpdateUniqueMvcc::Updated(new_row_id));
                    }
                }
            }
        }
    }

    /// Delete the visible row found through a unique secondary-index key.
    pub(crate) async fn delete_unique_mvcc(
        &self,
        rt: TrxRuntime<'_>,
        effects: &mut StmtEffects,
        index_no: usize,
        key_vals: &[Val],
    ) -> QuadResult<DeleteMvcc> {
        debug_assert!(index_no < self.sec_idx_len());
        debug_assert!(
            self.metadata()
                .idx
                .require_index_spec(index_no)
                .unwrap()
                .unique()
        );
        debug_assert!(self.metadata().idx.index_type_match(
            self.metadata().col.as_ref(),
            index_no,
            key_vals
        ));
        'retry: loop {
            let attempt = 'attempt: {
                let root_snapshot = self.root_snapshot(rt.ctx());
                let handle =
                    self.snapshot_index_read_handle(rt.pool_guards(), &root_snapshot, index_no)?;
                let index = handle.bind_unique()?;
                match index.lookup(key_vals, rt.sts()).await? {
                    None => return Ok(DeleteMvcc::NotFound),
                    Some((row_id, _)) => {
                        match self.resolve_row_location(rt.pool_guards(), row_id).await {
                            Ok(RowLocation::NotFound) => return Ok(DeleteMvcc::NotFound),
                            Ok(RowLocation::LwcBlock(LwcRowLocation {
                                block_id,
                                row_idx,
                                row_shape_fingerprint,
                                durable_deleted,
                            })) => {
                                // Delete only needs old secondary-index keys, so read
                                // indexed columns instead of decoding the whole row.
                                // The key recheck prevents acting on stale DiskTree or
                                // MemIndex state after another path already moved the
                                // logical key away from this cold row.
                                let index_keys = self
                                    .read_lwc_index_keys(
                                        rt.pool_guards(),
                                        block_id,
                                        row_idx,
                                        row_shape_fingerprint,
                                    )
                                    .await?;
                                if !index_key_matches(index_keys.as_slice(), index_no, key_vals) {
                                    return Ok(DeleteMvcc::NotFound);
                                }
                                let deletion_buffer = self.lwc_deletion_buffer();
                                self.debug_assert_table_write_lock_held(rt);
                                match deletion_buffer.claim_ref(
                                    row_id,
                                    Arc::clone(rt.status()),
                                    rt.sts(),
                                    durable_deleted,
                                ) {
                                    Ok(DeletionClaim::Acquired) => {
                                        // The marker is statement-owned delete state
                                        // until success. Row undo removes it on
                                        // rollback; redo rebuilds it as a cold delete
                                        // during recovery.
                                        // Mask old index entries immediately; physical
                                        // deletion remains deferred to index GC.
                                        self.install_cold_delete_effects(
                                            rt,
                                            effects,
                                            row_id,
                                            index_keys,
                                            &root_snapshot,
                                        )
                                        .await?;
                                        return Ok(DeleteMvcc::Deleted);
                                    }
                                    Ok(DeletionClaim::Preparing(listener)) => {
                                        break 'attempt PointMutationAttempt::Preparing(listener);
                                    }
                                    Err(DeletionError::WriteConflict) => {
                                        return Err(Report::new(OperationError::WriteConflict)
                                            .attach("delete MVCC cold delete marker ownership")
                                            .into());
                                    }
                                    Err(DeletionError::AlreadyDeleted) => {
                                        return Ok(DeleteMvcc::NotFound);
                                    }
                                }
                            }
                            Ok(RowLocation::RowPage(page_id)) => {
                                // Hot delete is an in-page delete bit guarded by row
                                // undo. Index entries are masked after the row mutation
                                // and restored by index undo on rollback.
                                let Some(page_guard) = self
                                    .mem()
                                    .try_get_validated_row_page_shared_result(
                                        rt.pool_guards(),
                                        page_id,
                                        row_id,
                                    )
                                    .await?
                                else {
                                    continue 'retry;
                                };
                                PointMutationAttempt::Hot {
                                    root_snapshot,
                                    page_guard,
                                    row_id,
                                }
                            }
                            Err(err) => return Err(err.into()),
                        }
                    }
                }
            };
            let (root_snapshot, page_guard, row_id) = match attempt {
                PointMutationAttempt::Hot {
                    root_snapshot,
                    page_guard,
                    row_id,
                } => (root_snapshot, page_guard, row_id),
                PointMutationAttempt::Preparing(listener) => {
                    rt.wait_prepare_or_poison(listener).await?;
                    continue;
                }
            };
            let res = HotRowMutator::new(self.table_id(), self.metadata(), rt, &page_guard, row_id)
                .delete(effects, index_no, key_vals, false)
                .await?;
            match res {
                DeleteInternal::NotFound => return Ok(DeleteMvcc::NotFound),
                DeleteInternal::RetryInTransition => {
                    // Release the row page so the checkpoint transition can complete.
                    drop(page_guard);
                    self.wait_transition_route_or_poison(rt, row_id).await?;
                }
                DeleteInternal::Ok => {
                    // Successful row undo ownership excludes another writer,
                    // and deletion changed only the row bit. Copy every key
                    // with one read guard, then release the page latch and
                    // buffer pin before awaiting secondary-index masking.
                    let index_keys = WriteIndexKeySet::from_physical_row(self, &page_guard, row_id);
                    let proof =
                        self.owned_row_page_index_set_proof(row_id, index_keys, &root_snapshot);
                    drop(page_guard);
                    // Physical index entries remain until rollback unmasks
                    // them or index GC removes them after they are invisible.
                    self.defer_delete_owned_row_index_set(rt, effects, proof)
                        .await?;
                    return Ok(DeleteMvcc::Deleted);
                }
            }
        }
    }

    /// Delete an obsolete secondary-index entry from a purge path.
    pub(crate) async fn delete_index(
        &self,
        guards: &PoolGuards,
        index_no: usize,
        key_vals: &[Val],
        row_id: RowID,
        unique: bool,
        min_active_sts: TrxID,
    ) -> RuntimeResult<bool> {
        // Undo can outlive the secondary index that produced it. Once the
        // index slot is inactive, row-level purge has no per-entry cleanup to do.
        let Some(index_schema) = self.metadata().idx.index_spec(index_no) else {
            return Ok(false);
        };
        if !self.sec_idx_is_active(index_no) {
            return Ok(false);
        }
        debug_assert_eq!(unique, index_schema.unique());
        if unique {
            self.delete_unique_index(guards, index_no, key_vals, row_id, min_active_sts)
                .await
        } else {
            self.delete_non_unique_index(guards, index_no, key_vals, row_id, min_active_sts)
                .await
        }
    }
}

#[inline]
fn index_key_matches(keys: &[SelectKey], index_no: usize, key_vals: &[Val]) -> bool {
    let old_key = keys
        .iter()
        .find(|old_key| old_key.index_no == index_no)
        .unwrap_or_else(|| {
            panic!(
                "target index key must be present in metadata-derived row keys: index_no={index_no}, key_count={}",
                keys.len()
            )
        });
    old_key.vals.as_slice() == key_vals
}

fn persisted_delete_set_for_scan(
    file_kind: FileKind,
    entry: &ColumnLeafEntry,
    delete_deltas: Vec<u32>,
) -> DataIntegrityResult<FastHashSet<RowID>> {
    let mut deleted = FastHashSet::default();
    for delta in delete_deltas {
        let row_id = entry
            .start_row_id
            .checked_add(u64::from(delta))
            .ok_or_else(|| {
                Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                    "file={file_kind}, block=lwc_block, block_id={}, delete delta overflows row id: start_row_id={}, delta={delta}",
                    entry.block_id(),
                    entry.start_row_id
                ))
            })?;
        if row_id >= entry.end_row_id() {
            return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "file={file_kind}, block=lwc_block, block_id={}, delete delta outside entry range: row_id={row_id}, start_row_id={}, end_row_id={}",
                entry.block_id(),
                entry.start_row_id,
                entry.end_row_id()
            )));
        }
        deleted.insert(row_id);
    }
    Ok(deleted)
}

fn validate_cold_scan_entry(
    file_kind: FileKind,
    entry: &ColumnLeafEntry,
    block: &LwcBlock,
    row_ids: &[RowID],
) -> DataIntegrityResult<()> {
    if usize::from(entry.row_count()) != row_ids.len() || block.row_count() != row_ids.len() {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "file={file_kind}, block=lwc_block, block_id={}, LWC row count mismatch: entry_rows={}, block_rows={}, row_ids={}",
            entry.block_id(),
            entry.row_count(),
            block.row_count(),
            row_ids.len()
        )));
    }
    if block.row_shape_fingerprint() != entry.row_shape_fingerprint() {
        return Err(
            Report::new(DataIntegrityError::InvalidPayload).attach(format!(
                "file={file_kind}, block=lwc_block, block_id={}, row shape fingerprint mismatch",
                entry.block_id()
            )),
        );
    }
    if row_ids.windows(2).any(|window| window[0] >= window[1])
        || row_ids
            .iter()
            .any(|row_id| *row_id < entry.start_row_id || *row_id >= entry.end_row_id())
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload).attach(format!(
            "file={file_kind}, block=lwc_block, block_id={}, invalid persisted row id set: start_row_id={}, end_row_id={}, row_ids={}",
            entry.block_id(),
            entry.start_row_id,
            entry.end_row_id(),
            row_ids.len()
        )));
    }
    Ok(())
}

#[inline]
fn cold_row_visible_mvcc(
    deletion_buffer: &ColumnDeletionBuffer,
    reader_sts: TrxID,
    reader_status: &SharedTrxStatus,
    row_id: RowID,
    durable_deleted: bool,
) -> bool {
    // The CDB marker is the newest MVCC authority: among other things, a
    // committed marker newer than the reader snapshot is the undo fact that
    // keeps a durably deleted cold image visible. Durable membership becomes
    // final only after the marker is absent.
    match deletion_buffer.get(row_id) {
        Some(DeleteMarker::Committed(ts)) => ts > reader_sts,
        Some(DeleteMarker::Ref(status)) => {
            let ts = status.ts();
            if trx_is_committed(ts) {
                ts > reader_sts
            } else {
                !addr_eq(status.as_ref(), reader_status)
            }
        }
        None => !durable_deleted,
    }
}

/// Checks whether a persisted row image is still the latest logical row.
///
/// Any committed marker means the immutable cold image has been consumed,
/// regardless of the reader's snapshot. This transaction's own active marker
/// also identifies an already consumed cold image; its replacement, if any, is
/// scanned in hot storage. A foreign active marker remains a write conflict.
#[inline]
fn read_latest_cold_row(
    deletion_buffer: &ColumnDeletionBuffer,
    reader_status: &SharedTrxStatus,
    row_id: RowID,
    durable_deleted: bool,
) -> ColdLatestRow {
    // Preserve the same CDB-over-durable ordering as snapshot reads. Here the
    // marker additionally owns foreground write-conflict/preparing state; the
    // durable bit is authoritative only when no marker remains.
    let Some(marker) = deletion_buffer.get(row_id) else {
        return if durable_deleted {
            ColdLatestRow::NotFound
        } else {
            ColdLatestRow::Readable
        };
    };
    match marker {
        DeleteMarker::Committed(_) => ColdLatestRow::NotFound,
        DeleteMarker::Ref(status) => {
            if trx_is_committed(status.ts()) || addr_eq(status.as_ref(), reader_status) {
                return ColdLatestRow::NotFound;
            }
            match status.prepare_listener() {
                PrepareListenerResult::Registered(listener) => {
                    return ColdLatestRow::Preparing(listener);
                }
                PrepareListenerResult::Completed(listener) => {
                    if trx_is_committed(status.ts()) {
                        return ColdLatestRow::NotFound;
                    }
                    return ColdLatestRow::Preparing(listener);
                }
                PrepareListenerResult::NotPreparing => (),
            }
            if trx_is_committed(status.ts()) {
                ColdLatestRow::NotFound
            } else {
                ColdLatestRow::WriteConflict
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ColdLatestRow, InsertedRow, ScanBoundaryTracker, cold_row_visible_mvcc,
        read_latest_cold_row,
    };
    use crate::buffer::BufferPool;
    use crate::buffer::frame::FrameKind;
    use crate::buffer::test_frame_kind;
    use crate::catalog::tests::table4;
    use crate::catalog::{
        ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
    };
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{
        DataIntegrityError, DiscloseError, DiscloseResultExt, Error, ErrorKind, FatalError,
        InternalError, IoError, OperationError, Result, RuntimeError,
    };
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::id::{PageID, RowID, TableID, TrxID};
    use crate::index::{LwcRowLocation, RowLocation};
    use crate::io::{StorageBackendFileIdentity, install_storage_backend_test_hook};
    use crate::latch::LatchFallbackMode;
    use crate::lock::tests::LockDebugEntryState;
    use crate::lock::{LockMode, LockResource};
    use crate::log::redo::RowRedoKind;
    use crate::poison::PoisonAwareListener;
    use crate::row::RowPage;
    use crate::row::ops::{
        DeleteMvcc, RowMutation, RowUpdateInput, ScanMvcc, SelectKey, SelectMvcc,
        TableMutationOutcome, UpdateCol, UpdateMvcc, UpsertMvcc,
    };
    use crate::session::Session;
    use crate::session::tests::{
        SessionTestExt, assert_checkpoint_published, remove_session_for_test,
        wait_for_checkpoint_purge,
    };
    use crate::table::hot::{
        DeleteInternal, HotRowMutator, InsertRowIntoPage, RowInserter, UpdateRowInplace,
    };
    use crate::table::lifecycle::TableCheckpointRootMutationScope;
    use crate::table::tests::*;
    use crate::table::{CheckpointOutcome, FreezeOutcome};
    use crate::table::{ColumnDeletionBuffer, DeleteMarker, Table};
    use crate::trx::row::LockRowForWrite;
    use crate::trx::stmt::tests as stmt_tests;
    use crate::trx::sys::tests::fatal_rollback_retention_count;
    use crate::trx::tests::{
        commit_preparing_shared_trx_status, prepare_event_is_installed, prepare_shared_trx_status,
        prepare_transaction, rollback_preparing_shared_trx_status,
        rollback_production_prepared_for_test, shared_trx_status, transaction_status_for_test,
    };
    use crate::trx::undo::RowUndoKind;
    use crate::trx::ver_map::RowPageState;
    use crate::trx::{MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID, Transaction};
    use crate::value::{Val, ValKind};
    use error_stack::Report;
    use smol::Timer;
    use smol::future::yield_now;
    use std::io::Error as StdIoError;
    use std::iter::repeat_n;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_foreground_root_layout_compatibility_uses_identity_and_slot_shape() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "foreground_root_layout").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let layout = table.layout_snapshot();
            let accessor = table.accessor_with_layout(&layout);
            let root = table.file().active_root_unchecked().clone();

            accessor.assert_foreground_root_layout_compatible(&root);

            let mut identity_mismatch = root.clone();
            identity_mismatch.metadata = Arc::new(identity_mismatch.metadata.as_ref().clone());
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    accessor.assert_foreground_root_layout_compatible(&identity_mismatch);
                }))
                .is_err()
            );

            let mut slot_mismatch = root;
            slot_mismatch.secondary_index_roots.push(SUPER_BLOCK_ID);
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    accessor.assert_foreground_root_layout_compatible(&slot_mismatch);
                }))
                .is_err()
            );
        });
    }

    async fn assert_secondary_mem_entries_absent(
        engine: &Engine,
        session: &mut Session,
        table_id: TableID,
        unique_key: &SelectKey,
        non_unique_key: &SelectKey,
        row_id: RowID,
    ) {
        let table = table_for_internal_assertion(engine, table_id);
        let guards = session.pool_guards();
        let layout = table.layout_snapshot();
        assert_eq!(
            layout
                .secondary_index(unique_key.index_no)
                .unwrap()
                .unique_mem()
                .unwrap()
                .bind(guards.index_guard())
                .lookup(&unique_key.vals, MAX_SNAPSHOT_TS)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            layout
                .secondary_index(non_unique_key.index_no)
                .unwrap()
                .non_unique_mem()
                .unwrap()
                .bind(guards.index_guard())
                .lookup_unique(&non_unique_key.vals, row_id, MAX_SNAPSHOT_TS)
                .await
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_mvcc_insert_normal() {
        smol::block_on(async {
            const SIZE: i32 = 10000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;

            let mut session = engine.new_session().unwrap();
            {
                let mut trx = session.begin_trx().unwrap();
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                trx.commit().await.unwrap();
            }
            {
                let mut trx = session.begin_trx().unwrap();
                for i in 16..SIZE {
                    let key = SelectKey::new(0, vec![Val::from(i)]);
                    trx = expect_trx_select(table_id, trx, &key, |vals| {
                        assert!(vals.len() == 2);
                        assert!(vals[0] == Val::from(i));
                        let s = format!("{}", i);
                        assert!(vals[1] == Val::from(&s[..]));
                    })
                    .await;
                }
                let _ = trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_mvcc_insert_dup_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            // dup key
            {
                // insert [1, "hello"]
                let insert = vec![Val::from(1i32), Val::from("hello")];
                let mut trx = session.begin_trx().unwrap();
                trx = expect_trx_insert(table_id, trx, insert).await;
                trx.commit().await.unwrap();

                // insert [1, "world"]
                let insert = vec![Val::from(1i32), Val::from("world")];
                let mut trx = session.begin_trx().unwrap();
                let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
                let err = res.unwrap_err();
                assert_eq!(
                    err.report().downcast_ref::<OperationError>().copied(),
                    Some(OperationError::DuplicateKey)
                );
                trx.rollback().await.unwrap();
            }
            // write conflict
            {
                // insert [2, "hello"], but not commit
                let insert1 = vec![Val::from(2i32), Val::from("hello")];
                let mut trx1 = session.begin_trx().unwrap();
                let res = trx_insert_row_by_id(&mut trx1, table_id, insert1).await;
                assert!(res.is_ok());

                // begin concurrent transaction and insert [2, "world"]
                let mut session2 = engine.new_session().unwrap();
                let insert2 = vec![Val::from(2i32), Val::from("world")];
                let mut trx2 = session2.begin_trx().unwrap();
                let res = trx_insert_row_by_id(&mut trx2, table_id, insert2).await;
                // still dup key because circuit breaker on index search.
                let err = res.unwrap_err();
                assert_eq!(
                    err.report().downcast_ref::<OperationError>().copied(),
                    Some(OperationError::DuplicateKey)
                );
                trx2.rollback().await.unwrap();
                drop(session2);

                trx1.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_mvcc_update_normal() {
        smol::block_on(async {
            const SIZE: i32 = 1000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1000 rows
                let mut trx = session.begin_trx().unwrap();
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                trx.commit().await.unwrap();

                // update 1 row with short value
                let mut trx = session.begin_trx().unwrap();
                let k1 = single_key(1i32);
                let s1 = "hello";
                let update1 = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(s1),
                }];
                trx = expect_trx_update(table_id, trx, &k1, update1).await;
                trx.commit().await.unwrap();

                // update 1 row with long value
                let mut trx = session.begin_trx().unwrap();
                let k2 = single_key(100i32);
                let s2: String = (0..50_000).map(|_| '1').collect();
                let update2 = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(&s2[..]),
                }];
                trx = expect_trx_update(table_id, trx, &k2, update2).await;

                // lookup this updated value inside same transaction
                trx = expect_trx_select(table_id, trx, &k2, |row| {
                    assert!(row.len() == 2);
                    assert!(row[0] == k2.vals[0]);
                    assert!(row[1] == Val::from(&s2[..]));
                })
                .await;

                trx.commit().await.unwrap();

                // lookup with a new transaction
                let mut trx = session.begin_trx().unwrap();
                trx = expect_trx_select(table_id, trx, &k2, |row| {
                    assert!(row.len() == 2);
                    assert!(row[0] == k2.vals[0]);
                    assert!(row[1] == Val::from(&s2[..]));
                })
                .await;

                let _ = trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_mvcc_upsert_unique_insert_and_update() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            let inserted = trx
                .exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        0,
                        vec![Val::from(1i32), Val::from("hello")],
                    )
                    .await
                })
                .await
                .unwrap();
            let inserted_row_id = match inserted {
                UpsertMvcc::Inserted(row_id) => row_id,
                UpsertMvcc::Updated(row_id) => panic!("unexpected update row_id={row_id}"),
            };
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let updated = trx
                .exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        0,
                        vec![Val::from(1i32), Val::from("world")],
                    )
                    .await
                })
                .await
                .unwrap();
            assert_eq!(updated, UpsertMvcc::Updated(inserted_row_id));
            trx.commit().await.unwrap();

            expect_select_committed(table_id, &mut session, &single_key(1i32), |row| {
                assert_eq!(row, vec![Val::from(1i32), Val::from("world")]);
            })
            .await;
        });
    }

    #[test]
    fn test_mvcc_upsert_unique_full_row_move_update_preserves_undo_and_indexes() {
        smol::block_on(async {
            const ROWS: i32 = 60;
            const BASE_PAYLOAD_SIZE: usize = 1000;
            const LARGE_PAYLOAD_SIZE: usize = 50_000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = {
                let mut ddl_session = engine.new_session().unwrap();
                ddl_session
                    .create_table(
                        TableSpec::new(vec![
                            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                            ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                            ColumnSpec::new("payload", ValKind::VarByte, ColumnAttributes::empty()),
                        ]),
                        vec![
                            IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                            IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::empty()),
                        ],
                    )
                    .await
                    .unwrap()
            };
            let mut session = engine.new_session().unwrap();
            let base_payload = vec![b'a'; BASE_PAYLOAD_SIZE];
            let mut row_ids = Vec::new();
            for id in 0..ROWS {
                let name = format!("name{id}");
                row_ids.push(
                    insert_one_row(
                        table_id,
                        &mut session,
                        vec![
                            Val::from(id),
                            Val::from(&name[..]),
                            Val::from(&base_payload[..]),
                        ],
                    )
                    .await,
                );
            }

            let key = single_key(0i32);
            let old_row_id = row_ids[0];
            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();
            assert_eq!(
                trx_select_row_mvcc_by_id(&mut old_reader, table_id, &key, &[0, 1, 2])
                    .await
                    .unwrap()
                    .unwrap_found(),
                vec![
                    Val::from(0i32),
                    Val::from("name0"),
                    Val::from(&base_payload[..]),
                ]
            );

            let large_payload = vec![b'b'; LARGE_PAYLOAD_SIZE];
            let mut writer = session.begin_trx().unwrap();
            let new_row_id = writer
                .exec(async |stmt| {
                    let updated = stmt
                        .table_upsert_unique_mvcc(
                            table_id,
                            0,
                            vec![
                                Val::from(0i32),
                                Val::from("name0"),
                                Val::from(&large_payload[..]),
                            ],
                        )
                        .await?;
                    let new_row_id = match updated {
                        UpsertMvcc::Updated(row_id) => row_id,
                        UpsertMvcc::Inserted(row_id) => {
                            panic!("expected full-row move update, inserted row_id={row_id}")
                        }
                    };
                    assert_ne!(new_row_id, old_row_id);
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    let table = table_for_internal_assertion(&engine, table_id);
                    let layout = table.layout_snapshot();
                    let guards = session.pool_guards();
                    let non_unique = layout
                        .secondary_index(1)
                        .unwrap()
                        .non_unique_mem()
                        .unwrap()
                        .bind(guards.index_guard());
                    let name_key = [Val::from("name0")];
                    assert_eq!(
                        non_unique
                            .lookup_unique(&name_key, old_row_id, stmt.runtime().sts())
                            .await
                            .unwrap(),
                        Some(false)
                    );
                    assert_eq!(
                        non_unique
                            .lookup_unique(&name_key, new_row_id, stmt.runtime().sts())
                            .await
                            .unwrap(),
                        Some(true)
                    );
                    Ok(new_row_id)
                })
                .await
                .unwrap();
            writer.commit().await.unwrap();

            let mut fresh_reader = session.begin_trx().unwrap();
            assert_eq!(
                trx_select_row_mvcc_by_id(&mut fresh_reader, table_id, &key, &[0, 1, 2])
                    .await
                    .unwrap()
                    .unwrap_found(),
                vec![
                    Val::from(0i32),
                    Val::from("name0"),
                    Val::from(&large_payload[..]),
                ]
            );
            fresh_reader.commit().await.unwrap();

            assert_eq!(
                trx_select_row_mvcc_by_id(&mut old_reader, table_id, &key, &[0, 1, 2])
                    .await
                    .unwrap()
                    .unwrap_found(),
                vec![
                    Val::from(0i32),
                    Val::from("name0"),
                    Val::from(&base_payload[..]),
                ]
            );
            old_reader.commit().await.unwrap();

            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                MAX_SNAPSHOT_TS,
                new_row_id,
                false,
            )
            .await;
        });
    }

    #[test]
    fn test_mvcc_upsert_unique_conflicts_on_existing_and_missing_key() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("base")],
            )
            .await;

            let mut trx1 = session.begin_trx().unwrap();
            assert!(matches!(
                trx1.exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        0,
                        vec![Val::from(1i32), Val::from("held")],
                    )
                    .await
                })
                .await
                .unwrap(),
                UpsertMvcc::Updated(_)
            ));

            let mut session2 = engine.new_session().unwrap();
            let mut trx2 = session2.begin_trx().unwrap();
            let err = trx2
                .exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        0,
                        vec![Val::from(1i32), Val::from("conflict")],
                    )
                    .await
                })
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::WriteConflict)
            );
            trx2.rollback().await.unwrap();
            trx1.rollback().await.unwrap();

            let mut trx1 = session.begin_trx().unwrap();
            assert!(matches!(
                trx1.exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        0,
                        vec![Val::from(2i32), Val::from("first")],
                    )
                    .await
                })
                .await
                .unwrap(),
                UpsertMvcc::Inserted(_)
            ));

            let mut trx2 = session2.begin_trx().unwrap();
            let err = trx2
                .exec(async |stmt| {
                    stmt.table_upsert_unique_mvcc(
                        table_id,
                        0,
                        vec![Val::from(2i32), Val::from("second")],
                    )
                    .await
                })
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::WriteConflict)
            );
            trx2.rollback().await.unwrap();
            trx1.commit().await.unwrap();
        });
    }

    #[test]
    fn test_mvcc_delete_normal() {
        smol::block_on(async {
            const SIZE: i32 = 1000;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1000 rows
                // let mut trx = session.begin_trx(trx_sys);
                let mut trx = session.begin_trx().unwrap();
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                trx.commit().await.unwrap();

                // delete 1 row
                let mut trx = session.begin_trx().unwrap();
                let k1 = single_key(1i32);
                trx = expect_trx_delete(table_id, trx, &k1).await;

                // lookup row in same transaction
                trx = expect_trx_select_not_found(table_id, trx, &k1).await;
                trx.commit().await.unwrap();

                // lookup row in new transaction
                let mut trx = session.begin_trx().unwrap();
                let k1 = single_key(1i32);
                trx = expect_trx_select_not_found(table_id, trx, &k1).await;
                let _ = trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_column_delete_basic() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let mut reader_session = engine.new_session().unwrap();
            let trx = reader_session.begin_trx().unwrap();
            let _ = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &reader_session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_select_not_found(table_id, trx, &key).await;
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_read_uses_readonly_buffer_pool() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let mut reader_session = engine.new_session().unwrap();
            let trx = reader_session.begin_trx().unwrap();
            let _ = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &reader_session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let allocated_after_route = engine.inner().pools.disk.allocated();
            assert!(allocated_after_route >= 1);

            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals[0], Val::from(1i32));
                assert_eq!(vals[1], Val::from("name"));
            })
            .await;
            let allocated_after_first = engine.inner().pools.disk.allocated();
            assert!(allocated_after_first >= allocated_after_route);

            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals[0], Val::from(1i32));
                assert_eq!(vals[1], Val::from("name"));
            })
            .await;
            assert_eq!(engine.inner().pools.disk.allocated(), allocated_after_first);
        });
    }

    #[test]
    fn test_find_row_returns_resolved_lwc_page_location() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let trx = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(&table, &pool_guards, key.index_no);
            let (row_id, _) = index.lookup(&key.vals, trx.sts()).await.unwrap().unwrap();

            let snapshot = column_block_index_snapshot(&engine, table_id);
            let column_index = snapshot.index(pool_guards.disk_guard());
            let resolved = column_index
                .locate_and_resolve_row(row_id)
                .await
                .unwrap()
                .unwrap();

            match table
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::LwcBlock(LwcRowLocation {
                    block_id,
                    row_idx,
                    row_shape_fingerprint,
                    durable_deleted,
                }) => {
                    assert_eq!(block_id, resolved.block_id());
                    assert_eq!(row_idx, resolved.row_idx());
                    assert_eq!(row_shape_fingerprint, resolved.row_shape_fingerprint());
                    assert_eq!(durable_deleted, resolved.durable_deleted());
                }
                RowLocation::RowPage(..) => panic!("row should be in lwc"),
                RowLocation::NotFound => panic!("row should exist"),
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_select_surfaces_persisted_corruption() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let trx = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
            trx.commit().await.unwrap();

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index.locate_block(row_id).await.unwrap().unwrap();
            let block_id = entry.block_id();

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_page_checksum(table_file_path, block_id);

            let mut trx = session.begin_trx().unwrap();
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            let err = match res {
                Err(err) => err,
                other => panic!("expected persisted LWC corruption, got {other:?}"),
            };
            assert_eq!(err.kind(), ErrorKind::Runtime);
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::TableAccess)
            );
            assert_table_data_integrity(
                err,
                "lwc_block",
                block_id,
                DataIntegrityError::ChecksumMismatch,
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_preserves_cold_data_integrity_context() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let pool_guards = session.pool_guards();
            let entry = snapshot
                .index(pool_guards.disk_guard())
                .collect_leaf_entries()
                .await
                .unwrap()
                .into_iter()
                .next()
                .expect("checkpointed table should have one cold entry");
            let block_id = entry.block_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_page_checksum(table_file_path, block_id);
            let _ = table.disk_pool().invalidate_block(
                session.pool_guards().disk_guard(),
                table.file().sparse_file().file_id(),
                block_id,
            );

            let mut trx = session.begin_trx().unwrap();
            let err = trx
                .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0, 1], |_| true).await)
                .await
                .unwrap_err();

            let rendered = format!("{err:?}");
            assert_eq!(rendered.matches("operation=table_scan_mvcc").count(), 1);
            assert_eq!(rendered.matches(&format!("table_id={table_id}")).count(), 1);
            assert_table_data_integrity(
                err,
                "lwc_block",
                block_id,
                DataIntegrityError::ChecksumMismatch,
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_select_surfaces_column_block_index_row_metadata_corruption() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let trx = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
            trx.commit().await.unwrap();

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index.locate_block(row_id).await.unwrap().unwrap();

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_leaf_row_codec(table_file_path, entry.leaf_block_id, 0);
            let _ = table.disk_pool().invalidate_block(
                session.pool_guards().disk_guard(),
                table.file().sparse_file().file_id(),
                entry.leaf_block_id,
            );

            let mut trx = session.begin_trx().unwrap();
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            let err = match res {
                Err(err) => err,
                other => panic!("expected persisted column-block-index corruption, got {other:?}"),
            };
            assert_table_data_integrity(
                err,
                "column_block_index",
                entry.leaf_block_id,
                DataIntegrityError::InvalidPayload,
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_select_surfaces_column_block_index_zero_block_id_corruption() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let trx = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
            trx.commit().await.unwrap();

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index.locate_block(row_id).await.unwrap().unwrap();

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_leaf_block_id(table_file_path, entry.leaf_block_id, 0);
            let _ = table.disk_pool().invalidate_block(
                session.pool_guards().disk_guard(),
                table.file().sparse_file().file_id(),
                entry.leaf_block_id,
            );

            let mut trx = session.begin_trx().unwrap();
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            let err = match res {
                Err(err) => err,
                other => panic!("expected persisted column-block-index corruption, got {other:?}"),
            };
            assert_table_data_integrity(
                err,
                "column_block_index",
                entry.leaf_block_id,
                DataIntegrityError::InvalidPayload,
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_select_surfaces_row_shape_fingerprint_mismatch_corruption() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let trx = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, trx.sts()).await;
            trx.commit().await.unwrap();

            let pool_guards = session.pool_guards();
            let snapshot = column_block_index_snapshot(&engine, table_id);
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index.locate_block(row_id).await.unwrap().unwrap();

            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            corrupt_lwc_row_shape_fingerprint(table_file_path, entry.block_id());
            let _ = table.disk_pool().invalidate_block(
                session.pool_guards().disk_guard(),
                table.file().sparse_file().file_id(),
                entry.block_id(),
            );

            let mut trx = session.begin_trx().unwrap();
            let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
            let err = match res {
                Err(err) => err,
                other => panic!("expected persisted LWC invalid-payload corruption, got {other:?}"),
            };
            assert_table_data_integrity(
                err,
                "lwc_block",
                entry.block_id(),
                DataIntegrityError::InvalidPayload,
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_column_delete_write_conflict() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(4i32);
            let trx = session.begin_trx().unwrap();
            let _ = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let mut trx1 = session.begin_trx().unwrap();
            let res1 = trx_delete_row_by_id(&mut trx1, table_id, &key).await;
            assert!(matches!(res1, Ok(DeleteMvcc::Deleted)));

            let mut session2 = engine.new_session().unwrap();
            let mut trx2 = session2.begin_trx().unwrap();
            let err = trx_delete_row_by_id(&mut trx2, table_id, &key)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::WriteConflict)
            );
            trx2.rollback().await.unwrap();
            drop(session2);

            trx1.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_column_delete_mvcc_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(5i32);
            let trx = session.begin_trx().unwrap();
            let _ = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let mut reader_session = engine.new_session().unwrap();
            let mut trx_reader = reader_session.begin_trx().unwrap();

            let mut delete_session = engine.new_session().unwrap();
            let mut trx_delete = delete_session.begin_trx().unwrap();
            let res = trx_delete_row_by_id(&mut trx_delete, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            trx_delete.commit().await.unwrap();

            trx_reader = expect_trx_select(table_id, trx_reader, &key, |row| {
                assert_eq!(row[0], Val::from(5i32));
            })
            .await;
            trx_reader.commit().await.unwrap();

            let mut trx_new = session.begin_trx().unwrap();
            trx_new = expect_trx_select_not_found(table_id, trx_new, &key).await;
            trx_new.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_delete_unique_conflicts_when_delete_committed_after_snapshot() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(5i32);
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let writer_sts = writer.sts();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &writer_session.pool_guards(),
                &key,
                writer_sts,
            )
            .await;

            expect_delete_committed(table_id, &mut session, &key).await;
            let delete_cts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap(),
            );
            assert!(delete_cts > writer_sts);

            writer = expect_trx_select(table_id, writer, &key, |row| {
                assert_eq!(row, vec![Val::from(5i32), Val::from("name")]);
            })
            .await;

            let err = trx_delete_row_by_id(&mut writer, table_id, &key)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::WriteConflict)
            );
            writer.rollback().await.unwrap();

            expect_select_not_found_committed(table_id, &mut session, &key).await;
        });
    }

    #[test]
    fn test_lwc_update_unique_same_key_reinserts_hot_and_preserves_old_snapshot() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();
            let old_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &old_reader_session.pool_guards(),
                &key,
                old_reader.sts(),
            )
            .await;

            let mut writer = session.begin_trx().unwrap();
            writer
                .exec(async |stmt| {
                    let res = stmt_update_row_by_id(
                        stmt,
                        table_id,
                        &key,
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from("updated"),
                        }],
                    )
                    .await;
                    let new_row_id = match res {
                        Ok(UpdateMvcc::Updated(row_id)) => row_id,
                        other => panic!("expected update success, got {other:?}"),
                    };
                    assert_ne!(old_row_id, new_row_id);
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    assert!(matches!(
                        table_for_internal_assertion(&engine, table_id)
                            .find_row(&session.pool_guards(), new_row_id)
                            .await
                            .unwrap(),
                        RowLocation::RowPage(_)
                    ));
                    match table_for_internal_assertion(&engine, table_id)
                        .deletion_buffer()
                        .get(old_row_id)
                        .unwrap()
                    {
                        DeleteMarker::Ref(status) => {
                            assert!(Arc::ptr_eq(&status, stmt.runtime().status()));
                        }
                        DeleteMarker::Committed(_) => {
                            panic!("update should hold an in-flight delete marker")
                        }
                    }

                    let res = stmt_select_row_mvcc_by_id(stmt, table_id, &key, &[0, 1]).await;
                    assert!(matches!(
                        res,
                        Ok(SelectMvcc::Found(vals))
                            if vals == vec![Val::from(1i32), Val::from("updated")]
                    ));
                    Ok(())
                })
                .await
                .unwrap();
            old_reader = expect_trx_select(table_id, old_reader, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;

            writer.commit().await.unwrap();

            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("updated")]);
            })
            .await;
            old_reader = expect_trx_select(table_id, old_reader, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;
            old_reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_update_unique_conflicts_when_delete_committed_after_snapshot() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(5i32);
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let writer_sts = writer.sts();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &writer_session.pool_guards(),
                &key,
                writer_sts,
            )
            .await;

            expect_delete_committed(table_id, &mut session, &key).await;
            let delete_cts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .unwrap(),
            );
            assert!(delete_cts > writer_sts);

            writer = expect_trx_select(table_id, writer, &key, |row| {
                assert_eq!(row, vec![Val::from(5i32), Val::from("name")]);
            })
            .await;

            let res = trx_update_row_by_id(
                &mut writer,
                table_id,
                &key,
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("updated"),
                }],
            )
            .await;
            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::WriteConflict)
            );
            writer.rollback().await.unwrap();

            expect_select_not_found_committed(table_id, &mut session, &key).await;
        });
    }

    #[test]
    fn test_lwc_update_unique_key_change_preserves_old_and_new_key_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let old_key = single_key(2i32);
            let new_key = single_key(20i32);
            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();
            let old_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &old_reader_session.pool_guards(),
                &old_key,
                old_reader.sts(),
            )
            .await;

            let mut writer = session.begin_trx().unwrap();
            writer
                .exec(async |stmt| {
                    let res = stmt_update_row_by_id(
                        stmt,
                        table_id,
                        &old_key,
                        vec![
                            UpdateCol {
                                idx: 0,
                                val: Val::from(20i32),
                            },
                            UpdateCol {
                                idx: 1,
                                val: Val::from("moved"),
                            },
                        ],
                    )
                    .await;
                    let new_row_id = match res {
                        Ok(UpdateMvcc::Updated(row_id)) => row_id,
                        other => panic!("expected update success, got {other:?}"),
                    };
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &old_key,
                        stmt.runtime().sts(),
                        old_row_id,
                        true,
                    )
                    .await;
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &new_key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    Ok(())
                })
                .await
                .unwrap();
            writer.commit().await.unwrap();

            expect_select_not_found_committed(table_id, &mut session, &old_key).await;
            expect_select_committed(table_id, &mut session, &new_key, |vals| {
                assert_eq!(vals, vec![Val::from(20i32), Val::from("moved")]);
            })
            .await;

            old_reader = expect_trx_select(table_id, old_reader, &old_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
            old_reader = expect_trx_select_not_found(table_id, old_reader, &new_key).await;
            old_reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_update_unique_duplicate_rolls_back_cold_marker_and_hot_insert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(1i32);
            let duplicate_key = single_key(2i32);
            let trx = session.begin_trx().unwrap();
            let old_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let res = trx_update_row_by_id(
                &mut trx,
                table_id,
                &key,
                vec![UpdateCol {
                    idx: 0,
                    val: Val::from(2i32),
                }],
            )
            .await;
            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::DuplicateKey)
            );
            trx.rollback().await.unwrap();

            assert!(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(old_row_id)
                    .is_none()
            );
            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;
            expect_select_committed(table_id, &mut session, &duplicate_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
        });
    }

    #[test]
    fn test_persisted_delete_without_mem_copies_does_not_install_overlays() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "persisted").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let unique_key = single_key(0i32);
            let non_unique_key = name_key("persisted");
            let reader = session.begin_trx().unwrap();
            let row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &unique_key,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let outcome = session
                .cleanup_secondary_mem_indexes(table_id, true)
                .await
                .unwrap();
            assert!(outcome.live_delay.is_none());
            assert_eq!(outcome.stats.indexes.len(), 2);
            assert!(outcome.stats.indexes.iter().all(|stats| stats.removed >= 1));

            assert_secondary_mem_entries_absent(
                &engine,
                &mut session,
                table_id,
                &unique_key,
                &non_unique_key,
                row_id,
            )
            .await;

            let mut deleting = session.begin_trx().unwrap();
            assert!(matches!(
                trx_delete_row_by_id(&mut deleting, table_id, &unique_key)
                    .await
                    .unwrap(),
                DeleteMvcc::Deleted
            ));
            assert_secondary_mem_entries_absent(
                &engine,
                &mut session,
                table_id,
                &unique_key,
                &non_unique_key,
                row_id,
            )
            .await;
            deleting.rollback().await.unwrap();
            assert!(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .is_none()
            );
            assert_secondary_mem_entries_absent(
                &engine,
                &mut session,
                table_id,
                &unique_key,
                &non_unique_key,
                row_id,
            )
            .await;
            expect_select_committed(table_id, &mut session, &unique_key, |vals| {
                assert_eq!(vals, vec![Val::from(0i32), Val::from("persisted")]);
            })
            .await;

            expect_delete_committed(table_id, &mut session, &unique_key).await;
            assert_secondary_mem_entries_absent(
                &engine,
                &mut session,
                table_id,
                &unique_key,
                &non_unique_key,
                row_id,
            )
            .await;
            assert!(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(row_id)
                    .is_some()
            );
            expect_select_not_found_committed(table_id, &mut session, &unique_key).await;
        });
    }

    #[test]
    fn test_lwc_update_unique_claims_committed_deleted_cold_owner_with_visibility_bridge() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 2, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let old_key = single_key(1i32);
            let claimed_key = single_key(2i32);
            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();
            let _ = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &old_reader_session.pool_guards(),
                &claimed_key,
                old_reader.sts(),
            )
            .await;

            expect_delete_committed(table_id, &mut session, &claimed_key).await;

            let mut gap_reader_session = engine.new_session().unwrap();
            let mut gap_reader = gap_reader_session.begin_trx().unwrap();

            expect_update_committed(
                table_id,
                &mut session,
                &old_key,
                vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("claimed"),
                    },
                ],
            )
            .await;

            expect_select_committed(table_id, &mut session, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("claimed")]);
            })
            .await;
            old_reader = expect_trx_select(table_id, old_reader, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
            gap_reader = expect_trx_select_not_found(table_id, gap_reader, &claimed_key).await;

            old_reader.commit().await.unwrap();
            gap_reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_update_unique_rejects_cold_owner_deleted_after_snapshot() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 2, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let old_key = single_key(1i32);
            let claimed_key = single_key(2i32);
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let writer_sts = writer.sts();
            let claimed_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &writer_session.pool_guards(),
                &claimed_key,
                writer_sts,
            )
            .await;

            expect_delete_committed(table_id, &mut session, &claimed_key).await;
            let delete_cts = delete_marker_ts(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .get(claimed_row_id)
                    .unwrap(),
            );
            assert!(delete_cts > writer_sts);
            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &writer_session.pool_guards(),
                &claimed_key,
                MAX_SNAPSHOT_TS,
                claimed_row_id,
                true,
            )
            .await;

            writer = expect_trx_select(table_id, writer, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;

            let res = trx_update_row_by_id(
                &mut writer,
                table_id,
                &old_key,
                vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("claimed"),
                    },
                ],
            )
            .await;
            let err = res.unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::DuplicateKey)
            );

            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &claimed_key,
                MAX_SNAPSHOT_TS,
                claimed_row_id,
                true,
            )
            .await;

            writer.rollback().await.unwrap();

            expect_select_committed(table_id, &mut session, &old_key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;
            expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;
        });
    }

    #[test]
    fn test_lwc_update_unique_claim_rollback_restores_deleted_cold_owner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 2, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let old_key = single_key(1i32);
            let claimed_key = single_key(2i32);
            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();
            let claimed_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &old_reader_session.pool_guards(),
                &claimed_key,
                old_reader.sts(),
            )
            .await;

            expect_delete_committed(table_id, &mut session, &claimed_key).await;
            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &claimed_key,
                MAX_SNAPSHOT_TS,
                claimed_row_id,
                true,
            )
            .await;

            let mut writer = session.begin_trx().unwrap();
            writer
                .exec(async |stmt| {
                    let res = stmt_update_row_by_id(
                        stmt,
                        table_id,
                        &old_key,
                        vec![
                            UpdateCol {
                                idx: 0,
                                val: Val::from(2i32),
                            },
                            UpdateCol {
                                idx: 1,
                                val: Val::from("claimed"),
                            },
                        ],
                    )
                    .await;
                    let new_row_id = match res {
                        Ok(UpdateMvcc::Updated(row_id)) => row_id,
                        other => panic!("expected update success, got {other:?}"),
                    };
                    assert_ne!(claimed_row_id, new_row_id);
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &claimed_key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    assert!(matches!(
                        table_for_internal_assertion(&engine, table_id)
                            .find_row(&session.pool_guards(), claimed_row_id)
                            .await
                            .unwrap(),
                        RowLocation::LwcBlock(..)
                    ));
                    Ok(())
                })
                .await
                .unwrap();

            // Keep the statement changes in the transaction so transaction rollback
            // exercises index undo before row undo. That is the path where the
            // claimed deleted owner must still resolve as RowLocation::LwcBlock.
            writer.rollback().await.unwrap();

            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &claimed_key,
                MAX_SNAPSHOT_TS,
                claimed_row_id,
                true,
            )
            .await;
            expect_select_committed(table_id, &mut session, &old_key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;
            expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;
            old_reader = expect_trx_select(table_id, old_reader, &claimed_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("name")]);
            })
            .await;
            old_reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_lwc_update_unique_claim_rollback_drops_purgeable_deleted_cold_owner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 2, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let old_key = single_key(1i32);
            let claimed_key = single_key(2i32);
            let reader = session.begin_trx().unwrap();
            let claimed_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &claimed_key,
                reader.sts(),
            )
            .await;
            reader.commit().await.unwrap();

            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                claimed_key.index_no,
            );
            assert!(
                index
                    .inject_mem_delete_mask(&claimed_key.vals, claimed_row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            let delete_cts = TrxID::new(1);
            table_for_internal_assertion(&engine, table_id)
                .deletion_buffer()
                .put_committed(claimed_row_id, delete_cts)
                .unwrap();
            expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;

            let mut writer = session.begin_trx().unwrap();
            assert!(delete_cts < writer.sts());
            assert_eq!(
                table_for_internal_assertion(&engine, table_id)
                    .deletion_buffer()
                    .delete_marker_purgeability(claimed_row_id, writer.sts()),
                Some(true)
            );
            writer
                .exec(async |stmt| {
                    let res = stmt_update_row_by_id(
                        stmt,
                        table_id,
                        &old_key,
                        vec![
                            UpdateCol {
                                idx: 0,
                                val: Val::from(2i32),
                            },
                            UpdateCol {
                                idx: 1,
                                val: Val::from("claimed"),
                            },
                        ],
                    )
                    .await;
                    let new_row_id = match res {
                        Ok(UpdateMvcc::Updated(row_id)) => row_id,
                        other => panic!("expected update success, got {other:?}"),
                    };
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &claimed_key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    assert!(matches!(
                        table_for_internal_assertion(&engine, table_id)
                            .find_row(&session.pool_guards(), claimed_row_id)
                            .await
                            .unwrap(),
                        RowLocation::LwcBlock(..)
                    ));
                    Ok(())
                })
                .await
                .unwrap();

            // This is the stale GC attempt from the original delete. While the
            // replacement claim owns the unique key, GC observes a row-id mismatch
            // and skips the entry, so rollback must not recreate that skipped
            // delete-masked owner.
            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();
            let deleted = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .delete_index(
                    &session.pool_guards(),
                    claimed_key.index_no,
                    &claimed_key.vals,
                    claimed_row_id,
                    true,
                    MAX_SNAPSHOT_TS,
                )
                .await
                .unwrap();
            assert!(!deleted);

            writer.rollback().await.unwrap();

            // The composite index may still fall through to the checkpointed cold
            // root; MVCC reads filter that stale cold owner through the committed
            // deletion marker.
            expect_select_committed(table_id, &mut session, &old_key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("name")]);
            })
            .await;
            expect_select_not_found_committed(table_id, &mut session, &claimed_key).await;
        });
    }

    #[test]
    fn test_row_page_transition_retries_update_delete() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            {
                let insert = vec![Val::from(1i32), Val::from("hello")];
                let mut trx = session.begin_trx().unwrap();
                trx = expect_trx_insert(table_id, trx, insert).await;
                trx.commit().await.unwrap();
            }
            let key = single_key(1i32);
            let mut trx = session.begin_trx().unwrap();
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            let (row_id, _) = index.lookup(&key.vals, trx.sts()).await.unwrap().unwrap();
            let page_id = match table_for_internal_assertion(&engine, table_id)
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::NotFound => panic!("row should exist"),
                RowLocation::LwcBlock(..) => unreachable!("lwc block"),
            };
            let page_guard = engine
                .inner()
                .pools
                .mem
                .get_page::<RowPage>(
                    session.pool_guards().mem_guard(),
                    page_id,
                    LatchFallbackMode::Shared,
                )
                .await
                .expect("buffer-pool read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            let row_ver = page_guard.unwrap_vmap();
            *row_ver.write_state() = RowPageState::Transition;

            let insert_page_guard = engine
                .inner()
                .pools
                .mem
                .get_page::<RowPage>(
                    session.pool_guards().mem_guard(),
                    page_id,
                    LatchFallbackMode::Shared,
                )
                .await
                .expect("buffer-pool read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            let insert = vec![Val::from(2i32), Val::from("insert")];
            let res: Result<()> = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(table_id)
                        .await
                        .disclose()?;
                    stmt.acquire_table_write_data_lock(table_id)
                        .await
                        .disclose()?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    let table = table_for_internal_assertion(&engine, table_id);
                    let layout = table.layout_snapshot();
                    let accessor = table.accessor_with_layout(&layout);
                    let insert_res = RowInserter::new(accessor.table_id(), accessor.metadata(), rt)
                        .insert_to_page(
                            effects,
                            insert_page_guard,
                            insert,
                            RowUndoKind::Insert,
                            vec![],
                        );
                    assert!(matches!(
                        insert_res,
                        InsertRowIntoPage::NoSpaceOrFrozen(_, _, _)
                    ));

                    let update = vec![UpdateCol {
                        idx: 1,
                        val: Val::from("world"),
                    }];
                    let table = table_for_internal_assertion(&engine, table_id);
                    let layout = table.layout_snapshot();
                    let accessor = table.accessor_with_layout(&layout);
                    let res = HotRowMutator::new(
                        accessor.table_id(),
                        accessor.metadata(),
                        rt,
                        &page_guard,
                        row_id,
                    )
                    .update_inplace(
                        effects,
                        key.index_no,
                        &key.vals,
                        RowUpdateInput::Sparse(update),
                        false,
                    )
                    .await
                    .disclose()?;
                    assert!(matches!(res, UpdateRowInplace::RetryInTransition(_)));
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::InvalidDmlInput)
            );
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let page_guard = engine
                .inner()
                .pools
                .mem
                .get_page::<RowPage>(
                    session.pool_guards().mem_guard(),
                    page_id,
                    LatchFallbackMode::Shared,
                )
                .await
                .expect("buffer-pool read failed in test")
                .lock_shared_async()
                .await
                .unwrap();
            let res: Result<()> = trx
                .exec(async |stmt| {
                    stmt.acquire_table_write_metadata_lock(table_id)
                        .await
                        .disclose()?;
                    stmt.acquire_table_write_data_lock(table_id)
                        .await
                        .disclose()?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    let table = table_for_internal_assertion(&engine, table_id);
                    let layout = table.layout_snapshot();
                    let accessor = table.accessor_with_layout(&layout);
                    let res = HotRowMutator::new(
                        accessor.table_id(),
                        accessor.metadata(),
                        rt,
                        &page_guard,
                        row_id,
                    )
                    .delete(effects, key.index_no, &key.vals, false)
                    .await
                    .disclose()?;
                    assert!(matches!(res, DeleteInternal::RetryInTransition));
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::InvalidDmlInput)
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_mvcc_insert_link_unique_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1 row
                let insert = vec![Val::from(1i32), Val::from("hello")];
                expect_insert_committed(table_id, &mut session, insert).await;

                // we must hold a transaction before the deletion,
                // to prevent index GC.
                let trx_to_prevent_gc = engine.new_session().unwrap().begin_trx().unwrap();
                // delete it
                let key = single_key(1i32);
                expect_delete_committed(table_id, &mut session, &key).await;

                // insert again, trigger insert+link
                let insert = vec![Val::from(1i32), Val::from("world")];
                expect_insert_committed(table_id, &mut session, insert).await;

                trx_to_prevent_gc.rollback().await.unwrap();

                // select 1 row
                let key = single_key(1i32);
                _ = expect_select_committed(table_id, &mut session, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
            }
        });
    }

    #[test]
    fn test_mvcc_insert_link_update() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1 row: v1=1, v2=hello
                let insert = vec![Val::from(1i32), Val::from("hello")];
                expect_insert_committed(table_id, &mut session, insert).await;

                // open one session and trnasaction to see this row
                let mut sess1 = engine.new_session().unwrap();
                let mut trx1 = sess1.begin_trx().unwrap();

                // update it: v1=2, v2=world
                let key = single_key(1i32);
                let update = vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("world"),
                    },
                ];
                expect_update_committed(table_id, &mut session, &key, update).await;

                // open session and transaction to see row 2
                let mut sess2 = engine.new_session().unwrap();
                let mut trx2 = sess2.begin_trx().unwrap();

                // insert again, trigger insert+link
                let insert = vec![Val::from(1i32), Val::from("rust")];
                expect_insert_committed(table_id, &mut session, insert).await;

                // use transaction 1 to see version 1.
                let key = single_key(1i32);
                trx1 = expect_trx_select(table_id, trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
                _ = trx1.commit().await.unwrap();

                // use transaction 2 to see version 2.
                let key = single_key(2i32);
                trx2 = expect_trx_select(table_id, trx2, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
                _ = trx2.commit().await.unwrap();

                // use new transaction to see version 3.
                let key = single_key(1i32);
                _ = expect_select_committed(table_id, &mut session, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("rust"));
                })
                .await;
            }
        });
    }

    #[test]
    fn test_mvcc_update_link_insert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1 row: v1=1, v2=hello
                let insert = vec![Val::from(1i32), Val::from("hello")];
                expect_insert_committed(table_id, &mut session, insert).await;
                println!("debug-only insert finish");

                // open one session and trnasaction to see this row
                let mut sess1 = engine.new_session().unwrap();
                let mut trx1 = sess1.begin_trx().unwrap();

                // update it: v1=2, v2=world
                let key = single_key(1i32);
                let update = vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("world"),
                    },
                ];
                expect_update_committed(table_id, &mut session, &key, update).await;
                println!("debug-only update finish");

                // open session and transaction to see row 2
                let mut sess2 = engine.new_session().unwrap();
                let mut trx2 = sess2.begin_trx().unwrap();

                // insert v1=5, v2=rust
                let insert = vec![Val::from(5i32), Val::from("rust")];
                expect_insert_committed(table_id, &mut session, insert).await;
                println!("debug-only insert2 finish");

                // update it: v1=1, v2=c++, trigger update+link
                let key = single_key(5i32);
                let update = vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(1i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("c++"),
                    },
                ];
                expect_update_committed(table_id, &mut session, &key, update).await;
                println!("debug-only update2 finish");

                // use transaction 1 to see version 1.
                let key = single_key(1i32);
                trx1 = expect_trx_select(table_id, trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
                _ = trx1.commit().await;

                // use transaction 2 to see version 2.
                let key = single_key(2i32);
                trx2 = expect_trx_select(table_id, trx2, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
                _ = trx2.commit().await;

                // use new transaction to see version 3.
                let key = single_key(1i32);
                _ = expect_select_committed(table_id, &mut session, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("c++"));
                })
            }
        });
    }

    #[test]
    fn test_mvcc_in_place_update_changes_only_affected_index_keys() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = {
                let mut ddl_session = engine.new_session().unwrap();
                ddl_session
                    .create_table(
                        TableSpec::new(vec![
                            ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                            ColumnSpec::new("code", ValKind::I32, ColumnAttributes::empty()),
                            ColumnSpec::new("region", ValKind::I32, ColumnAttributes::empty()),
                        ]),
                        vec![
                            IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                            IndexSpec::new(vec![IndexKey::new(1)], IndexAttributes::UK),
                            IndexSpec::new(vec![IndexKey::new(2)], IndexAttributes::UK),
                        ],
                    )
                    .await
                    .unwrap()
            };
            let mut session = engine.new_session().unwrap();
            let row_id = insert_one_row(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from(10i32), Val::from(100i32)],
            )
            .await;
            let id_key = SelectKey::new(0, vec![Val::from(1i32)]);
            let old_code_key = SelectKey::new(1, vec![Val::from(10i32)]);
            let new_code_key = SelectKey::new(1, vec![Val::from(20i32)]);
            let region_key = SelectKey::new(2, vec![Val::from(100i32)]);

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(
                trx_update_row_by_id(
                    &mut trx,
                    table_id,
                    &id_key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from(20i32),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::Updated(row_id)
            );
            assert!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &old_code_key, &[0, 1, 2])
                    .await
                    .unwrap()
                    .not_found()
            );
            for key in [&id_key, &new_code_key, &region_key] {
                assert_eq!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1, 2])
                        .await
                        .unwrap()
                        .unwrap_found(),
                    vec![Val::from(1i32), Val::from(20i32), Val::from(100i32)]
                );
            }
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            assert!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &new_code_key, &[0, 1, 2])
                    .await
                    .unwrap()
                    .not_found()
            );
            for key in [&id_key, &old_code_key, &region_key] {
                assert_eq!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, key, &[0, 1, 2])
                        .await
                        .unwrap()
                        .unwrap_found(),
                    vec![Val::from(1i32), Val::from(10i32), Val::from(100i32)]
                );
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_mvcc_multi_update() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert: v1
                let insert = vec![Val::from(1i32), Val::from("hello")];
                expect_insert_committed(table_id, &mut session, insert).await;

                // transaction to see version 1
                let mut sess1 = engine.new_session().unwrap();
                let mut trx1 = sess1.begin_trx().unwrap();

                let mut trx = session.begin_trx().unwrap();
                // update 1: v2
                let key = single_key(1i32);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from("rust"),
                }];
                trx = expect_trx_update(table_id, trx, &key, update).await;
                // update 2: v3
                let key = single_key(1i32);
                let update = vec![
                    UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from("world"),
                    },
                ];
                trx = expect_trx_update(table_id, trx, &key, update).await;
                // within transaction, query row
                // v2 not found
                let key = single_key(1i32);
                trx = expect_trx_select_not_found(table_id, trx, &key).await;
                // v3 found
                let key = single_key(2i32);
                trx = expect_trx_select(table_id, trx, &key, |vals| {
                    assert!(vals[0] == Val::from(2i32));
                    assert!(vals[1] == Val::from("world"));
                })
                .await;
                trx.commit().await.unwrap();

                //v1 found
                let key = single_key(1i32);
                trx1 = expect_trx_select(table_id, trx1, &key, |vals| {
                    assert!(vals[0] == Val::from(1i32));
                    assert!(vals[1] == Val::from("hello"));
                })
                .await;
                trx1.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_string_non_index_updates() {
        smol::block_on(async {
            const COUNT: usize = 100;
            const SIZE: usize = 500;
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                let value = vec![1u8; SIZE];
                let insert = vec![Val::from(1i32), Val::from(&[])];
                expect_insert_committed(table_id, &mut session, insert).await;
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                for i in SIZE - COUNT..SIZE {
                    expect_update_committed(
                        table_id,
                        &mut session,
                        &key,
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from(&value[..i]),
                        }],
                    )
                    .await;
                }
            }
        });
    }

    #[test]
    fn test_string_index_updates() {
        use crate::catalog::tests::table3;
        smol::block_on(async {
            const COUNT: usize = 100;
            const SIZE: usize = 500;
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let _ = create_table2_for_test(&engine).await;
            {
                let table_id = table3(&engine).await;
                let mut session = engine.new_session().unwrap();
                let s: String = repeat_n('0', SIZE).collect();
                // insert single row.
                {
                    let insert = vec![Val::from(&s[..0])];
                    let mut trx = session.begin_trx().unwrap();
                    let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
                    assert!(res.is_ok());
                    trx.commit().await.unwrap();
                }
                // perform updates.
                for i in 0..COUNT {
                    let key = SelectKey::new(0, vec![Val::from(&s[..i])]);
                    let update = vec![UpdateCol {
                        idx: 0,
                        val: Val::from(&s[..i + 1]),
                    }];
                    let mut trx = session.begin_trx().unwrap();
                    let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
                    assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                    trx.commit().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_mvcc_out_of_place_update() {
        use crate::catalog::tests::table3;
        smol::block_on(async {
            const COUNT: usize = 60;
            const DELTA: usize = 5;
            const BASE: usize = 1000;
            const SIZE: usize = 2000;
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let _ = create_table2_for_test(&engine).await;
            {
                let table_id = table3(&engine).await;
                let mut session = engine.new_session().unwrap();
                let s: String = repeat_n('0', SIZE).collect();
                // insert 60 rows
                for i in 0usize..COUNT {
                    let insert = vec![Val::from(&s[..BASE + i])];
                    let mut trx = session.begin_trx().unwrap();
                    let res = trx_insert_row_by_id(&mut trx, table_id, insert).await;
                    assert!(res.is_ok());
                    trx.commit().await.unwrap();
                }
                // perform updates to trigger out-of-place update.
                // try to update k=s[..BASE+DELTA] to s[..BASE+COUNT+DELTA]
                for i in 0..DELTA {
                    let key = SelectKey::new(0, vec![Val::from(&s[..BASE + i])]);
                    let update = vec![UpdateCol {
                        idx: 0,
                        val: Val::from(&s[..BASE + COUNT + i]),
                    }];
                    let mut trx = session.begin_trx().unwrap();
                    let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
                    assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                    trx.commit().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_table_scan_mvcc() {
        smol::block_on(async {
            const SIZE: i32 = 100;

            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;

            // insert 100 rows and commit
            let mut session1 = engine.new_session().unwrap();
            {
                let mut trx = session1.begin_trx().unwrap();
                for i in 0..SIZE {
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                _ = trx.commit().await.unwrap();
            }
            // we should see 100 committed rows.
            let mut session2 = engine.new_session().unwrap();
            {
                let mut trx = session2.begin_trx().unwrap();
                let mut res_len = 0usize;
                trx.exec(async |stmt| {
                    stmt.table_scan_mvcc(table_id, &[0], |_| {
                        res_len += 1;
                        true
                    })
                    .await?;
                    Ok(())
                })
                .await
                .unwrap();
                println!("res.len()={}", res_len);
                assert!(res_len == SIZE as usize);
                trx.commit().await.unwrap();
            }
            // insert 100 rows but not commit.
            let pending_trx = {
                let mut trx = session1.begin_trx().unwrap();
                for i in SIZE..SIZE * 2 {
                    let s = format!("{}", i);
                    let insert = vec![Val::from(i), Val::from(&s[..])];
                    trx = expect_trx_insert(table_id, trx, insert).await;
                }
                trx
            };
            // we should see only 100 rows
            {
                let mut trx = session2.begin_trx().unwrap();
                let mut res_len = 0usize;
                trx.exec(async |stmt| {
                    stmt.table_scan_mvcc(table_id, &[0], |_| {
                        res_len += 1;
                        true
                    })
                    .await?;
                    Ok(())
                })
                .await
                .unwrap();
                println!("res.len()={}", res_len);
                assert!(res_len == SIZE as usize);
                trx.commit().await.unwrap();
            }
            // commit the pending transaction.
            pending_trx.commit().await.unwrap();
            // now we should see 200 rows.
            {
                let mut trx = session2.begin_trx().unwrap();
                let mut res_len = 0usize;
                trx.exec(async |stmt| {
                    stmt.table_scan_mvcc(table_id, &[0], |_| {
                        res_len += 1;
                        true
                    })
                    .await?;
                    Ok(())
                })
                .await
                .unwrap();
                println!("res.len()={}", res_len);
                assert!(res_len == (SIZE * 2) as usize);
                trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_scan_boundary_tracker_uses_first_future_insert_boundary() {
        let pages = vec![
            crate::table::RowPageDescriptor {
                page_id: PageID::new(10),
                start_row_id: RowID::new(100),
                end_row_id: RowID::new(200),
            },
            crate::table::RowPageDescriptor {
                page_id: PageID::new(11),
                start_row_id: RowID::new(200),
                end_row_id: RowID::new(300),
            },
        ];
        let mut tracker = ScanBoundaryTracker::new(RowID::new(300), pages);

        tracker.observe_insert(InsertedRow::new(PageID::new(99), RowID::new(210)));
        tracker.observe_insert(InsertedRow::new(PageID::new(11), RowID::new(225)));
        tracker.observe_insert(InsertedRow::new(PageID::new(11), RowID::new(220)));
        tracker.observe_insert(InsertedRow::new(PageID::new(11), RowID::new(350)));
        assert_eq!(tracker.start_page(0, RowID::new(150)), RowID::new(150));
        tracker.observe_insert(InsertedRow::new(PageID::new(10), RowID::new(140)));
        assert_eq!(tracker.start_page(1, RowID::new(240)), RowID::new(220));
    }

    #[test]
    fn test_read_latest_cold_row_checks_delete_ownership() {
        let deletion_buffer = ColumnDeletionBuffer::new();
        let reader_status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 1));
        let assert_latest = |row_id, durable_deleted, expected| {
            let actual = read_latest_cold_row(
                &deletion_buffer,
                reader_status.as_ref(),
                RowID::new(row_id),
                durable_deleted,
            );
            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected)
            );
        };

        assert_latest(1, false, ColdLatestRow::Readable);
        assert_latest(1, true, ColdLatestRow::NotFound);

        deletion_buffer
            .put_committed(RowID::new(2), TrxID::new(20))
            .unwrap();
        assert_latest(2, false, ColdLatestRow::NotFound);

        deletion_buffer
            .put_ref(
                RowID::new(3),
                Arc::new(shared_trx_status(TrxID::new(30))),
                MAX_SNAPSHOT_TS,
            )
            .unwrap();
        assert_latest(3, false, ColdLatestRow::NotFound);

        deletion_buffer
            .put_ref(RowID::new(4), Arc::clone(&reader_status), MAX_SNAPSHOT_TS)
            .unwrap();
        assert_latest(4, false, ColdLatestRow::NotFound);

        deletion_buffer
            .put_ref(
                RowID::new(5),
                Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 2)),
                MAX_SNAPSHOT_TS,
            )
            .unwrap();
        assert_latest(5, false, ColdLatestRow::WriteConflict);
        assert_latest(5, true, ColdLatestRow::WriteConflict);

        let preparing = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 3));
        deletion_buffer
            .put_ref(RowID::new(6), Arc::clone(&preparing), MAX_SNAPSHOT_TS)
            .unwrap();
        prepare_shared_trx_status(&preparing);
        assert!(matches!(
            read_latest_cold_row(
                &deletion_buffer,
                reader_status.as_ref(),
                RowID::new(6),
                true,
            ),
            ColdLatestRow::Preparing(_)
        ));
        rollback_preparing_shared_trx_status(&preparing);
    }

    #[test]
    fn test_cold_row_visibility_prefers_cdb_marker_to_durable_delete() {
        let deletion_buffer = ColumnDeletionBuffer::new();
        let reader_status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 1));
        let reader_sts = TrxID::new(20);

        assert!(!cold_row_visible_mvcc(
            &deletion_buffer,
            reader_sts,
            reader_status.as_ref(),
            RowID::new(1),
            true,
        ));

        deletion_buffer
            .put_committed(RowID::new(2), TrxID::new(30))
            .unwrap();
        assert!(cold_row_visible_mvcc(
            &deletion_buffer,
            reader_sts,
            reader_status.as_ref(),
            RowID::new(2),
            true,
        ));

        deletion_buffer
            .put_ref(
                RowID::new(3),
                Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 2)),
                MAX_SNAPSHOT_TS,
            )
            .unwrap();
        assert!(cold_row_visible_mvcc(
            &deletion_buffer,
            reader_sts,
            reader_status.as_ref(),
            RowID::new(3),
            true,
        ));

        deletion_buffer
            .put_ref(RowID::new(4), Arc::clone(&reader_status), MAX_SNAPSHOT_TS)
            .unwrap();
        assert!(!cold_row_visible_mvcc(
            &deletion_buffer,
            reader_sts,
            reader_status.as_ref(),
            RowID::new(4),
            true,
        ));
    }

    async fn setup_single_cold_row(
        log_file_stem: &str,
    ) -> (
        TempDir,
        Engine,
        TableID,
        Session,
        Arc<Table>,
        SelectKey,
        RowID,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, log_file_stem).await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();
        insert_rows(table_id, &mut session, 1, 1, "cold").await;
        assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
        assert_checkpoint_published(&mut session, table_id).await;
        let table = table_for_internal_assertion(&engine, table_id);
        let key = single_key(1);
        let reader = session.begin_trx().unwrap();
        let row_id = assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
        reader.commit().await.unwrap();
        (temp_dir, engine, table_id, session, table, key, row_id)
    }

    async fn checkpoint_cold_delete(
        session: &mut Session,
        table_id: TableID,
        table: &Table,
        key: &SelectKey,
        row_id: RowID,
    ) {
        expect_delete_committed(table_id, session, key).await;
        let marker_ts = delete_marker_ts(table.deletion_buffer().get(row_id).unwrap());
        session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
        assert_checkpoint_published(session, table_id).await;
        match table
            .find_row(&session.pool_guards(), row_id)
            .await
            .unwrap()
        {
            RowLocation::LwcBlock(location) => assert!(location.durable_deleted),
            RowLocation::RowPage(_) | RowLocation::NotFound => {
                panic!("durably deleted row must retain its cold physical image")
            }
        }
    }

    #[test]
    fn test_unique_single_row_paths_reject_durable_deleted_candidate() {
        smol::block_on(async {
            let (_temp_dir, _engine, table_id, mut session, table, key, row_id) =
                setup_single_cold_row("unique_durable_deleted_candidate").await;
            checkpoint_cold_delete(&mut session, table_id, &table, &key, row_id).await;
            table.deletion_buffer().remove(row_id);

            let pool_guards = session.pool_guards();
            assert!(
                bound_unique_index(&table, &pool_guards, key.index_no)
                    .inject_mem_entry_if_absent(&key.vals, row_id, true, MAX_SNAPSHOT_TS)
                    .await
                    .unwrap()
                    .is_ok()
            );
            drop(pool_guards);

            let mut reader = session.begin_trx().unwrap();
            reader = expect_trx_select_not_found(table_id, reader, &key).await;
            reader.commit().await.unwrap();

            let mut updater = session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut updater,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("should-not-apply"),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::NotFound
            ));
            updater.commit().await.unwrap();

            let mut deleter = session.begin_trx().unwrap();
            assert!(matches!(
                trx_delete_row_by_id(&mut deleter, table_id, &key)
                    .await
                    .unwrap(),
                DeleteMvcc::NotFound
            ));
            deleter.commit().await.unwrap();
            assert!(table.deletion_buffer().get(row_id).is_none());

            // A stale unmasked unique owner with no CDB marker is reclaimable
            // because durable deletion is now the final authority.
            insert_rows(table_id, &mut session, 1, 1, "replacement").await;
            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("replacement")]);
            })
            .await;
        });
    }

    #[test]
    fn test_non_unique_lookup_rejects_durable_deleted_candidate() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "non_unique_durable_deleted_candidate",
            )
            .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "durable").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let unique_key = single_key(0i32);
            let non_unique_key = name_key("durable");
            let reader = session.begin_trx().unwrap();
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &unique_key, reader.sts()).await;
            reader.commit().await.unwrap();
            checkpoint_cold_delete(&mut session, table_id, &table, &unique_key, row_id).await;
            table.deletion_buffer().remove(row_id);

            let pool_guards = session.pool_guards();
            assert!(
                bound_non_unique_index_no(&table, &pool_guards, non_unique_key.index_no)
                    .inject_mem_entry_if_absent(
                        &non_unique_key.vals,
                        row_id,
                        true,
                        MAX_SNAPSHOT_TS,
                    )
                    .await
                    .unwrap()
                    .is_ok()
            );
            drop(pool_guards);

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(
                        table_id,
                        non_unique_key.index_no,
                        &non_unique_key.vals,
                        &[0, 1],
                    )
                    .await
                })
                .await
                .unwrap()
                .unwrap_rows();
            assert!(rows.is_empty());
            trx.commit().await.unwrap();
        });
    }

    fn assert_unrelated_poison_fatal(error: &Error) {
        assert_eq!(error.kind(), ErrorKind::Fatal);
        assert_eq!(
            error.report().downcast_ref::<FatalError>().copied(),
            Some(FatalError::StorageIo)
        );
        let report = format!("{error:?}");
        assert!(
            report.contains("unrelated foreground wait poison"),
            "fatal propagation lost the first poison report: {report}"
        );
    }

    #[test]
    fn test_hot_point_update_fast_path_skips_prepare_wait_helper() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "hot_update_prepare_fast_path").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 1, "hot").await;
            let key = single_key(1);
            let before = engine.inner().poisoner.test_observation_counts().2;

            let mut writer = session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut writer,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("updated"),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::Updated(_)
            ));
            assert_eq!(
                engine.inner().poisoner.test_observation_counts().2,
                before,
                "ordinary hot update must not enter the prepare slow path"
            );
            writer.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_prepare_completion_won_registration_rechecks_poison() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "prepare_completion_won_poison").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let result: Result<()> = trx
                .exec(async |stmt| {
                    stmt.runtime().engine().poisoner.poison(
                        Report::new(FatalError::StorageIo)
                            .attach("unrelated foreground wait poison: completion won"),
                    );
                    stmt.runtime()
                        .wait_prepare_or_poison(PoisonAwareListener::recheck_only())
                        .await
                        .disclose()
                })
                .await;

            assert_unrelated_poison_fatal(&result.unwrap_err());
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_cold_point_update_returns_fatal_on_unrelated_poison() {
        smol::block_on(async {
            let (_temp_dir, engine, table_id, _setup_session, table, key, row_id) =
                setup_single_cold_row("cold_update_unrelated_poison").await;

            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 104));
            table
                .deletion_buffer()
                .put_ref(row_id, Arc::clone(&owner), MAX_SNAPSHOT_TS)
                .unwrap();
            prepare_shared_trx_status(&owner);

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let update = async {
                let result = trx_update_row_by_id(
                    &mut writer,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("updated"),
                    }],
                )
                .await;
                writer.rollback().await.unwrap();
                result
            };
            let poison = async {
                while !prepare_event_is_installed(&owner) {
                    yield_now().await;
                }
                engine.inner().poisoner.poison(
                    Report::new(FatalError::StorageIo)
                        .attach("unrelated foreground wait poison: cold row"),
                );
            };
            let (result, ()) = futures::join!(update, poison);
            assert_unrelated_poison_fatal(&result.unwrap_err());

            table.deletion_buffer().remove(row_id);
            rollback_preparing_shared_trx_status(&owner);
        });
    }

    #[test]
    fn test_hot_point_update_returns_fatal_on_unrelated_poison() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "hot_update_unrelated_poison").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut setup_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut setup_session, 1, 1, "hot").await;
            let key = single_key(1);

            let mut owner_session = engine.new_session().unwrap();
            let mut owner = owner_session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut owner,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("owner"),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::Updated(_)
            ));
            let owner_status = transaction_status_for_test(&owner);
            let prepared = prepare_transaction(owner).unwrap();

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let update = async {
                let result = trx_update_row_by_id(
                    &mut writer,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("writer"),
                    }],
                )
                .await;
                writer.rollback().await.unwrap();
                result
            };
            let poison = async {
                while !prepare_event_is_installed(&owner_status) {
                    yield_now().await;
                }
                engine.inner().poisoner.poison(
                    Report::new(FatalError::StorageIo)
                        .attach("unrelated foreground wait poison: hot row"),
                );
            };
            let (result, ()) = futures::join!(update, poison);
            assert_unrelated_poison_fatal(&result.unwrap_err());
            rollback_production_prepared_for_test(prepared).await;
        });
    }

    #[test]
    fn test_cold_point_update_waits_for_preparing_owner_rollback() {
        smol::block_on(async {
            let (_temp_dir, engine, table_id, mut setup_session, table, key, row_id) =
                setup_single_cold_row("cold_update_prepare_rollback").await;

            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 100));
            table
                .deletion_buffer()
                .put_ref(row_id, Arc::clone(&owner), MAX_SNAPSHOT_TS)
                .unwrap();
            prepare_shared_trx_status(&owner);

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let update = async {
                let result = trx_update_row_by_id(
                    &mut writer,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("updated"),
                    }],
                )
                .await;
                if result.is_ok() {
                    writer.commit().await.unwrap();
                } else {
                    writer.rollback().await.unwrap();
                }
                result
            };
            let release = async {
                while !prepare_event_is_installed(&owner) {
                    yield_now().await;
                }
                table.deletion_buffer().remove(row_id);
                rollback_preparing_shared_trx_status(&owner);
            };
            let (result, ()) = futures::join!(update, release);
            assert!(matches!(result, Ok(UpdateMvcc::Updated(_))));
            expect_select_committed(table_id, &mut setup_session, &key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("updated")]);
            })
            .await;
        });
    }

    #[test]
    fn test_cold_point_delete_waits_for_preparing_owner_commit() {
        smol::block_on(async {
            let (_temp_dir, engine, table_id, _setup_session, table, key, row_id) =
                setup_single_cold_row("cold_delete_prepare_commit").await;

            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 101));
            table
                .deletion_buffer()
                .put_ref(row_id, Arc::clone(&owner), MAX_SNAPSHOT_TS)
                .unwrap();
            prepare_shared_trx_status(&owner);

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let owner_cts = writer.sts();
            let delete = async {
                let result = trx_delete_row_by_id(&mut writer, table_id, &key).await;
                if result.is_ok() {
                    writer.commit().await.unwrap();
                } else {
                    writer.rollback().await.unwrap();
                }
                result
            };
            let release = async {
                while !prepare_event_is_installed(&owner) {
                    yield_now().await;
                }
                commit_preparing_shared_trx_status(&owner, owner_cts);
            };
            let (result, ()) = futures::join!(delete, release);
            assert!(matches!(result, Ok(DeleteMvcc::NotFound)));
        });
    }

    #[test]
    fn test_full_table_cold_staged_wait_does_not_repeat_callback() {
        smol::block_on(async {
            let (_temp_dir, engine, table_id, _setup_session, table, _key, row_id) =
                setup_single_cold_row("full_table_cold_prepare_staged").await;

            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 102));
            prepare_shared_trx_status(&owner);
            let callbacks = AtomicUsize::new(0);
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let mutate = async {
                let result = writer
                    .exec(async |stmt| {
                        stmt.table_mutate_mvcc(table_id, |_| {
                            callbacks.fetch_add(1, Ordering::SeqCst);
                            table
                                .deletion_buffer()
                                .put_ref(row_id, Arc::clone(&owner), MAX_SNAPSHOT_TS)
                                .unwrap();
                            Ok(RowMutation::Delete)
                        })
                        .await
                    })
                    .await;
                if result.is_ok() {
                    writer.commit().await.unwrap();
                } else {
                    writer.rollback().await.unwrap();
                }
                result
            };
            let release = async {
                while !prepare_event_is_installed(&owner) {
                    yield_now().await;
                }
                table.deletion_buffer().remove(row_id);
                rollback_preparing_shared_trx_status(&owner);
            };
            let (result, ()) = futures::join!(mutate, release);
            assert_eq!(
                result.unwrap(),
                TableMutationOutcome {
                    delete_count: 1,
                    update_count: 0,
                }
            );
            assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn test_full_table_cold_wait_before_callback_reloads_row_once() {
        smol::block_on(async {
            let (_temp_dir, engine, table_id, _setup_session, table, _key, row_id) =
                setup_single_cold_row("full_table_cold_prepare_before_callback").await;
            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 103));
            table
                .deletion_buffer()
                .put_ref(row_id, Arc::clone(&owner), MAX_SNAPSHOT_TS)
                .unwrap();
            prepare_shared_trx_status(&owner);

            let callbacks = AtomicUsize::new(0);
            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            let mutate = async {
                let result = writer
                    .exec(async |stmt| {
                        stmt.table_mutate_mvcc(table_id, |_| {
                            callbacks.fetch_add(1, Ordering::SeqCst);
                            Ok(RowMutation::Delete)
                        })
                        .await
                    })
                    .await;
                if result.is_ok() {
                    writer.commit().await.unwrap();
                } else {
                    writer.rollback().await.unwrap();
                }
                result
            };
            let release = async {
                while !prepare_event_is_installed(&owner) {
                    yield_now().await;
                }
                table.deletion_buffer().remove(row_id);
                rollback_preparing_shared_trx_status(&owner);
            };
            let (result, ()) = futures::join!(mutate, release);
            assert_eq!(
                result.unwrap(),
                TableMutationOutcome {
                    delete_count: 1,
                    update_count: 0,
                }
            );
            assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn test_table_mutate_mvcc_hot_callback_and_statement_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_hot").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 5, "original").await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                let mut callbacks = 0usize;
                let outcome = stmt
                    .table_mutate_mvcc(table_id, |row| {
                        callbacks += 1;
                        assert_eq!(row.column_count(), 2);
                        let first = row.val(0)?.as_i32().unwrap();
                        assert_eq!(row.val(0)?.as_i32().unwrap(), first);
                        if first % 2 == 0 {
                            Ok(RowMutation::Update(vec![UpdateCol {
                                idx: 1,
                                val: Val::from(format!("updated-{first}").as_str()),
                            }]))
                        } else {
                            Ok(RowMutation::Skip)
                        }
                    })
                    .await?;
                assert_eq!(callbacks, 5);
                assert_eq!(
                    outcome,
                    TableMutationOutcome {
                        delete_count: 0,
                        update_count: 3,
                    }
                );
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut trx, table_id).await,
                vec![
                    (0, "updated-0".to_string()),
                    (1, "original".to_string()),
                    (2, "updated-2".to_string()),
                    (3, "original".to_string()),
                    (4, "updated-4".to_string()),
                ]
            );
            let before_error = scan_table_pairs(&mut trx, table_id).await;
            let result: Result<()> = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        if id == 2 {
                            _ = row.val(2)?;
                        }
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 1,
                            val: Val::from("should-rollback"),
                        }]))
                    })
                    .await?;
                    Ok(())
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::InvalidDmlInput)
            );
            assert_eq!(scan_table_pairs(&mut trx, table_id).await, before_error);
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_hot_point_mutation_rejects_stale_key_from_same_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "hot_point_same_trx_stale_key").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 1, "original").await;

            let old_key = single_key(1i32);
            let new_key = single_key(2i32);
            let mut trx = session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut trx,
                    table_id,
                    &old_key,
                    vec![UpdateCol {
                        idx: 0,
                        val: Val::from(2i32),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::Updated(_)
            ));
            assert_eq!(
                trx_update_row_by_id(
                    &mut trx,
                    table_id,
                    &old_key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("stale-update"),
                    }],
                )
                .await
                .unwrap(),
                UpdateMvcc::NotFound
            );
            assert_eq!(
                trx_delete_row_by_id(&mut trx, table_id, &old_key)
                    .await
                    .unwrap(),
                DeleteMvcc::NotFound
            );
            assert_eq!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &new_key, &[0, 1])
                    .await
                    .unwrap(),
                SelectMvcc::Found(vec![Val::from(2i32), Val::from("original")])
            );
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_empty_table_returns_default_outcome() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_mutate_empty").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |_| -> Result<RowMutation> {
                        panic!("empty table must not invoke mutation callback")
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome, TableMutationOutcome::default());
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_delete_unique_mvcc_emits_physical_hot_and_cold_redo() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "delete_unique_physical_redo",
            )
            .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 1, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 1, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                for id in [0, 10] {
                    let key = single_key(id);
                    assert_eq!(
                        stmt.table_delete_unique_mvcc(table_id, key.index_no, &key.vals)
                            .await?,
                        DeleteMvcc::Deleted
                    );
                }

                let rows = &stmt_tests::statement_redo(stmt)
                    .dml
                    .get(&table_id)
                    .unwrap()
                    .rows;
                let mut cold_deletes = 0;
                let mut hot_deletes = 0;
                for row in rows.values() {
                    match row.kind {
                        RowRedoKind::Delete(None) => cold_deletes += 1,
                        RowRedoKind::Delete(Some(_)) => hot_deletes += 1,
                        RowRedoKind::Insert(..)
                        | RowRedoKind::Update(..)
                        | RowRedoKind::DeleteByPrimaryKey(_)
                        | RowRedoKind::UpdateByPrimaryKey(..) => {
                            panic!("user-table delete must emit physical delete redo")
                        }
                    }
                }
                assert_eq!((cold_deletes, hot_deletes), (1, 1));
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_mixed_cold_hot_actions_and_outcome() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_mutate_mixed").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 3, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .exec(async |stmt| {
                    let outcome = stmt
                        .table_mutate_mvcc(table_id, |row| {
                            let id = row.val(0)?.as_i32().unwrap();
                            Ok(match id {
                                0 | 10 => RowMutation::Delete,
                                1 => RowMutation::Update(vec![UpdateCol {
                                    idx: 1,
                                    val: Val::from("cold-updated"),
                                }]),
                                11 => RowMutation::Update(vec![UpdateCol {
                                    idx: 1,
                                    val: Val::from("hot-updated"),
                                }]),
                                12 => RowMutation::Update(Vec::new()),
                                2 => RowMutation::Skip,
                                _ => unreachable!(),
                            })
                        })
                        .await?;
                    let rows = &stmt_tests::statement_redo(stmt)
                        .dml
                        .get(&table_id)
                        .unwrap()
                        .rows;
                    let mut cold_deletes = 0;
                    let mut hot_deletes = 0;
                    let mut inserts = 0;
                    let mut updates = 0;
                    for row in rows.values() {
                        match row.kind {
                            RowRedoKind::Delete(None) => cold_deletes += 1,
                            RowRedoKind::Delete(Some(_)) => hot_deletes += 1,
                            RowRedoKind::Insert(..) => inserts += 1,
                            RowRedoKind::Update(..) => updates += 1,
                            RowRedoKind::DeleteByPrimaryKey(_)
                            | RowRedoKind::UpdateByPrimaryKey(..) => {
                                panic!("user-table mutation must emit physical redo")
                            }
                        }
                    }
                    assert_eq!((cold_deletes, hot_deletes, inserts, updates), (2, 1, 1, 1));
                    Ok(outcome)
                })
                .await
                .unwrap();
            assert_eq!(
                outcome,
                TableMutationOutcome {
                    delete_count: 2,
                    update_count: 3,
                }
            );
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut trx, table_id).await,
                vec![
                    (1, "cold-updated".to_owned()),
                    (2, "cold".to_owned()),
                    (11, "hot-updated".to_owned()),
                    (12, "hot".to_owned()),
                ]
            );
            for id in [0, 10] {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0, 1]).await,
                    Ok(SelectMvcc::NotFound)
                ));
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_mixed_actions_callback_error_rolls_back() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "full_mutate_callback_rollback",
            )
            .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 2, "hot").await;

            let expected = vec![
                (0, "cold".to_owned()),
                (1, "cold".to_owned()),
                (10, "hot".to_owned()),
                (11, "hot".to_owned()),
            ];
            let mut trx = session.begin_trx().unwrap();
            let result: Result<()> = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        match id {
                            0 | 10 => Ok(RowMutation::Delete),
                            1 => Ok(RowMutation::Update(vec![UpdateCol {
                                idx: 1,
                                val: Val::from("changed"),
                            }])),
                            11 => Err(Report::new(OperationError::InvalidDmlInput).disclose()),
                            _ => unreachable!(),
                        }
                    })
                    .await?;
                    Ok(())
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::InvalidDmlInput)
            );
            assert_eq!(scan_table_pairs(&mut trx, table_id).await, expected);
            for id in [0, 1, 10, 11] {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0, 1]).await,
                    Ok(SelectMvcc::Found(_))
                ));
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_transaction_rollback_restores_mixed_actions() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "full_mutate_transaction_rollback",
            )
            .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 2, "hot").await;
            let expected = vec![
                (0, "cold".to_owned()),
                (1, "cold".to_owned()),
                (10, "hot".to_owned()),
                (11, "hot".to_owned()),
            ];

            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        Ok(match row.val(0)?.as_i32().unwrap() {
                            0 | 10 => RowMutation::Delete,
                            1 | 11 => RowMutation::Update(vec![UpdateCol {
                                idx: 1,
                                val: Val::from("changed"),
                            }]),
                            _ => unreachable!(),
                        })
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome.delete_count, 2);
            assert_eq!(outcome.update_count, 2);
            assert_eq!(
                scan_table_pairs(&mut trx, table_id).await,
                vec![(1, "changed".to_owned()), (11, "changed".to_owned())]
            );
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(scan_table_pairs(&mut trx, table_id).await, expected);
            for id in [0, 1, 10, 11] {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0, 1]).await,
                    Ok(SelectMvcc::Found(_))
                ));
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_cold_delete_decodes_unread_index_columns() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "full_mutate_cold_index_keys",
            )
            .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "shared").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        Ok(if id % 2 == 0 {
                            RowMutation::Delete
                        } else {
                            RowMutation::Skip
                        })
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome.delete_count, 2);
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &[Val::from("shared")], &[0])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                ScanMvcc::Rows(vec![vec![Val::from(1i32)], vec![Val::from(3i32)]])
            );
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_hot_delete_captures_unread_index_columns() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_mutate_hot_index_keys")
                    .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "shared").await;

            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        Ok(if id % 2 == 0 {
                            RowMutation::Delete
                        } else {
                            RowMutation::Skip
                        })
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome.delete_count, 2);
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &[Val::from("shared")], &[0])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                ScanMvcc::Rows(vec![vec![Val::from(1i32)], vec![Val::from(3i32)]])
            );
            for id in [0, 2] {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0]).await,
                    Ok(SelectMvcc::NotFound)
                ));
            }
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &[Val::from("shared")], &[0])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                ScanMvcc::Rows(vec![
                    vec![Val::from(0i32)],
                    vec![Val::from(1i32)],
                    vec![Val::from(2i32)],
                    vec![Val::from(3i32)],
                ])
            );
            for id in 0..4 {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0]).await,
                    Ok(SelectMvcc::Found(_))
                ));
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_hot_point_delete_masks_all_index_keys_and_rolls_back() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "hot_point_delete_index_keys",
            )
            .await;
            let table_id = create_non_unique_name_table_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "shared").await;
            let key = single_key(2i32);

            let mut trx = session.begin_trx().unwrap();
            let deleted = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            assert!(matches!(deleted, Ok(DeleteMvcc::Deleted)));
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &[Val::from("shared")], &[0])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                ScanMvcc::Rows(vec![
                    vec![Val::from(0i32)],
                    vec![Val::from(1i32)],
                    vec![Val::from(3i32)],
                ])
            );
            assert!(matches!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0]).await,
                Ok(SelectMvcc::NotFound)
            ));
            trx.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &[Val::from("shared")], &[0])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                ScanMvcc::Rows(vec![
                    vec![Val::from(0i32)],
                    vec![Val::from(1i32)],
                    vec![Val::from(2i32)],
                    vec![Val::from(3i32)],
                ])
            );
            assert!(matches!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0]).await,
                Ok(SelectMvcc::Found(_))
            ));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let deleted = trx_delete_row_by_id(&mut trx, table_id, &key).await;
            assert!(matches!(deleted, Ok(DeleteMvcc::Deleted)));
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &[Val::from("shared")], &[0])
                        .await
                })
                .await
                .unwrap();
            assert_eq!(
                rows,
                ScanMvcc::Rows(vec![
                    vec![Val::from(0i32)],
                    vec![Val::from(1i32)],
                    vec![Val::from(3i32)],
                ])
            );
            assert!(matches!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0]).await,
                Ok(SelectMvcc::NotFound)
            ));
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_enforces_uniqueness_in_action_order() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(
                &temp_dir,
                64u64 * 1024 * 1024,
                "full_mutate_uniqueness_order",
            )
            .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 2, "name").await;

            let mut trx = session.begin_trx().unwrap();
            let outcome = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        Ok(match row.val(0)?.as_i32().unwrap() {
                            1 => RowMutation::Delete,
                            2 => RowMutation::Update(vec![UpdateCol {
                                idx: 0,
                                val: Val::from(1i32),
                            }]),
                            _ => unreachable!(),
                        })
                    })
                    .await
                })
                .await
                .unwrap();
            assert_eq!(outcome.delete_count, 1);
            assert_eq!(outcome.update_count, 1);
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(scan_table_i32s(&mut trx, table_id).await, vec![1]);
            trx.commit().await.unwrap();

            let second_table_id = create_table2_for_test(&engine).await;
            insert_rows(second_table_id, &mut session, 1, 2, "name").await;
            let mut trx = session.begin_trx().unwrap();
            let result: Result<()> = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(second_table_id, |row| {
                        Ok(match row.val(0)?.as_i32().unwrap() {
                            1 => RowMutation::Update(vec![UpdateCol {
                                idx: 0,
                                val: Val::from(2i32),
                            }]),
                            2 => RowMutation::Delete,
                            _ => unreachable!(),
                        })
                    })
                    .await?;
                    Ok(())
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::DuplicateKey)
            );
            assert_eq!(scan_table_i32s(&mut trx, second_table_id).await, vec![1, 2]);
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_hot_callback_reads_latest_committed_value() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_latest_hot")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut older_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut older_session, 0, 1, "original").await;
            let mut older = older_session.begin_trx().unwrap();

            let mut writer_session = engine.new_session().unwrap();
            let mut writer = writer_session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut writer,
                    table_id,
                    &single_key(0),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("newer"),
                    }],
                )
                .await,
                Ok(UpdateMvcc::Updated(_))
            ));
            writer.commit().await.unwrap();

            older
                .exec(async |stmt| {
                    let outcome = stmt
                        .table_mutate_mvcc(table_id, |row| {
                            assert_eq!(row.val(1)?.as_str(), Some("newer"));
                            Ok(RowMutation::Update(Vec::new()))
                        })
                        .await?;
                    assert_eq!(outcome.update_count, 1);
                    Ok(())
                })
                .await
                .unwrap();
            older.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_cold_callback_reads_latest_committed_state() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_latest_cold")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut older_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut older_session, 0, 2, "original").await;
            assert_freeze_created(
                older_session
                    .freeze_table(table_id, usize::MAX)
                    .await
                    .unwrap(),
            );
            assert_checkpoint_published(&mut older_session, table_id).await;
            let mut older = older_session.begin_trx().unwrap();

            let mut writer_session = engine.new_session().unwrap();
            expect_update_committed(
                table_id,
                &mut writer_session,
                &single_key(0),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("newer"),
                }],
            )
            .await;
            expect_delete_committed(table_id, &mut writer_session, &single_key(1)).await;

            let mut seen = Vec::new();
            older
                .exec(async |stmt| {
                    let outcome = stmt
                        .table_mutate_mvcc(table_id, |row| {
                            seen.push((
                                row.val(0)?.as_i32().unwrap(),
                                row.val(1)?.as_str().unwrap().to_owned(),
                            ));
                            Ok(RowMutation::Update(Vec::new()))
                        })
                        .await?;
                    assert_eq!(outcome.update_count, 1);
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(seen, vec![(0, "newer".to_owned())]);
            older.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_mixed_storage_excludes_replacements() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_mixed_boundary")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 10, 2, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                let outcome = stmt
                    .table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 0,
                            val: Val::from(id + 100),
                        }]))
                    })
                    .await?;
                assert_eq!(
                    outcome.update_count, 5,
                    "replacement rows must not be revisited"
                );
                Ok(())
            })
            .await
            .unwrap();
            trx.commit().await.unwrap();

            let mut reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_i32s(&mut reader, table_id).await,
                vec![100, 101, 102, 110, 111]
            );
            reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_duplicate_rolls_back_all_rows_and_indexes() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_duplicate")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "name").await;

            let mut trx = session.begin_trx().unwrap();
            let result: Result<()> = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        let new_id = if id < 2 { 10 } else { id };
                        Ok(RowMutation::Update(vec![UpdateCol {
                            idx: 0,
                            val: Val::from(new_id),
                        }]))
                    })
                    .await?;
                    Ok(())
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::DuplicateKey)
            );
            assert_eq!(scan_table_i32s(&mut trx, table_id).await, vec![0, 1, 2]);
            for id in 0..3 {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0]).await,
                    Ok(SelectMvcc::Found(_))
                ));
            }
            assert!(matches!(
                trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(10), &[0]).await,
                Ok(SelectMvcc::NotFound)
            ));
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_cold_duplicate_marks_replacement_page_dirty() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_cold_dirty")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 2, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 100, 1, "hot").await;

            let replacement_page = session.load_active_insert_page(table_id).unwrap();
            session.save_active_insert_page(table_id, replacement_page);
            let table = table_for_internal_assertion(&engine, table_id);
            let page_guard = table
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), replacement_page)
                .await
                .unwrap()
                .unwrap();
            page_guard.bf().set_dirty(false);
            assert!(!page_guard.bf().is_dirty());
            drop(page_guard);

            let mut trx = session.begin_trx().unwrap();
            let result: Result<()> = trx
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |row| {
                        let id = row.val(0)?.as_i32().unwrap();
                        Ok(if id == 0 {
                            RowMutation::Update(vec![UpdateCol {
                                idx: 0,
                                val: Val::from(1i32),
                            }])
                        } else {
                            RowMutation::Skip
                        })
                    })
                    .await?;
                    Ok(())
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::DuplicateKey)
            );

            let page_guard = table
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), replacement_page)
                .await
                .unwrap()
                .unwrap();
            assert!(
                page_guard.bf().is_dirty(),
                "failed cold replacement must leave its row page dirty"
            );
            drop(page_guard);

            assert_eq!(scan_table_i32s(&mut trx, table_id).await, vec![0, 1, 100]);
            for id in [0, 1, 100] {
                assert!(matches!(
                    trx_select_row_mvcc_by_id(&mut trx, table_id, &single_key(id), &[0]).await,
                    Ok(SelectMvcc::Found(_))
                ));
            }
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_mutate_mvcc_x_blocks_freeze_until_transaction_end() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_freeze_lock")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut writer_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut writer_session, 0, 1, "name").await;

            let mut writer = writer_session.begin_trx().unwrap();
            writer
                .exec(async |stmt| {
                    assert_eq!(
                        stmt.table_mutate_mvcc(table_id, |_| {
                            Ok(RowMutation::Update(Vec::new()))
                        })
                        .await?
                        .update_count,
                        1,
                    );
                    Ok(())
                })
                .await
                .unwrap();

            let mut freeze_session = engine.new_session().unwrap();
            let freeze_session_id = freeze_session.id();
            let freeze = freeze_session.freeze_table(table_id, usize::MAX);
            let release_writer = async {
                let freeze_owner = wait_for_maintenance_lock_entry(
                    &engine,
                    freeze_session_id,
                    LockResource::TableData(table_id),
                    LockMode::IntentShared,
                    LockDebugEntryState::Waiting,
                )
                .await;
                writer.commit().await.unwrap();
                freeze_owner
            };
            let (freeze_result, freeze_owner) = futures::join!(freeze, release_writer);
            assert!(matches!(freeze_result, Ok(FreezeOutcome::Frozen { .. })));
            wait_for_no_lock_resource(&engine, freeze_owner, LockResource::TableData(table_id))
                .await;
        });
    }

    #[test]
    fn test_table_mutate_mvcc_ix_conversion_failure_has_no_callbacks() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "full_update_ix_conversion")
                    .await;
            let table_id = create_table2_for_test(&engine).await;
            let mut first_session = engine.new_session().unwrap();
            let mut second_session = engine.new_session().unwrap();
            insert_rows(table_id, &mut first_session, 0, 2, "name").await;

            let mut first = first_session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut first,
                    table_id,
                    &single_key(0),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("first"),
                    }],
                )
                .await,
                Ok(UpdateMvcc::Updated(_))
            ));
            let mut second = second_session.begin_trx().unwrap();
            assert!(matches!(
                trx_update_row_by_id(
                    &mut second,
                    table_id,
                    &single_key(1),
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("second"),
                    }],
                )
                .await,
                Ok(UpdateMvcc::Updated(_))
            ));

            let mut callbacks = 0usize;
            let result: Result<()> = first
                .exec(async |stmt| {
                    stmt.table_mutate_mvcc(table_id, |_| {
                        callbacks += 1;
                        Ok(RowMutation::Skip)
                    })
                    .await?;
                    Ok(())
                })
                .await;
            assert_eq!(
                result
                    .unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::LockUpgradeWouldBlock)
            );
            assert_eq!(callbacks, 0);
            first.rollback().await.unwrap();
            second.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_includes_cold_and_hot_rows() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 5, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_i32s(&mut trx, table_id).await,
                vec![0, 1, 2, 3, 4]
            );
            trx.commit().await.unwrap();

            insert_rows(table_id, &mut session, 100, 3, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_i32s(&mut trx, table_id).await,
                vec![0, 1, 2, 3, 4, 100, 101, 102]
            );
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_uses_locked_catalog_state_without_lifecycle_revalidation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            table.start_drop_lifecycle().unwrap().wait().await;

            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                .await
                .unwrap();
            trx.rollback().await.unwrap();
            table.mark_dropped_lifecycle();
        });
    }

    #[test]
    fn test_table_scan_mvcc_cold_delete_buffer_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 5, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();

            let key = single_key(2i32);
            expect_delete_committed(table_id, &mut session, &key).await;

            assert_eq!(
                scan_table_i32s(&mut old_reader, table_id).await,
                vec![0, 1, 2, 3, 4]
            );
            old_reader.commit().await.unwrap();

            let mut new_reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_i32s(&mut new_reader, table_id).await,
                vec![0, 1, 3, 4]
            );
            new_reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_cold_update_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 3, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let mut old_reader_session = engine.new_session().unwrap();
            let mut old_reader = old_reader_session.begin_trx().unwrap();

            expect_update_committed(
                table_id,
                &mut session,
                &single_key(1i32),
                vec![UpdateCol {
                    idx: 1,
                    val: Val::from("updated"),
                }],
            )
            .await;

            assert_eq!(
                scan_table_pairs(&mut old_reader, table_id).await,
                vec![
                    (0, "name".to_string()),
                    (1, "name".to_string()),
                    (2, "name".to_string()),
                ]
            );
            old_reader.commit().await.unwrap();

            let mut new_reader = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_pairs(&mut new_reader, table_id).await,
                vec![
                    (0, "name".to_string()),
                    (1, "updated".to_string()),
                    (2, "name".to_string()),
                ]
            );
            new_reader.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_uncommitted_cold_delete_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 4, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let mut writer = session.begin_trx().unwrap();
            let res = trx_delete_row_by_id(&mut writer, table_id, &single_key(1i32)).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

            assert_eq!(scan_table_i32s(&mut writer, table_id).await, vec![0, 2, 3]);

            let mut other_session = engine.new_session().unwrap();
            let mut other_reader = other_session.begin_trx().unwrap();
            assert_eq!(
                scan_table_i32s(&mut other_reader, table_id).await,
                vec![0, 1, 2, 3]
            );
            other_reader.commit().await.unwrap();
            writer.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_skips_persisted_delete_delta() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 6, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(4i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            expect_delete_committed(table_id, &mut session, &key).await;
            let marker_ts = delete_marker_ts(table.deletion_buffer().get(row_id).unwrap());
            session.wait_for_gc_horizon_after(marker_ts).await.unwrap();
            assert_checkpoint_published(&mut session, table_id).await;

            let snapshot = column_block_index_snapshot(&engine, table_id);
            let pool_guards = session.pool_guards();
            let index = snapshot.index(pool_guards.disk_guard());
            let entry = index.locate_block(row_id).await.unwrap().unwrap();
            let deltas = index.load_delete_deltas(&entry).await.unwrap();
            assert!(deltas.contains(&((row_id - entry.start_row_id) as u32)));

            let mut trx = session.begin_trx().unwrap();
            assert_eq!(
                scan_table_i32s(&mut trx, table_id).await,
                vec![0, 1, 2, 3, 5]
            );
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_scan_mvcc_early_stop_before_hot_phase() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 5, "cold").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;
            insert_rows(table_id, &mut session, 100, 2, "hot").await;

            let mut trx = session.begin_trx().unwrap();
            let mut rows = Vec::new();
            trx.exec(async |stmt| {
                stmt.table_scan_mvcc(table_id, &[0], |vals| {
                    rows.push(vals[0].as_i32().unwrap());
                    rows.len() < 3
                })
                .await?;
                Ok(())
            })
            .await
            .unwrap();
            assert_eq!(rows, vec![0, 1, 2]);
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_table_freeze() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;

            let mut session1 = engine.new_session().unwrap();
            {
                let trx = session1.begin_trx().unwrap();
                let insert = vec![Val::from(1), Val::from("1")];
                expect_trx_insert(table_id, trx, insert)
                    .await
                    .commit()
                    .await
                    .unwrap();
            }
            let row_pages = session1.total_row_pages(table_id).await.unwrap();
            assert!(row_pages == 1);
            assert_freeze_created(session1.freeze_table(table_id, 10).await.unwrap());
            // after freezing, new row should be inserted into second page.
            {
                let trx = session1.begin_trx().unwrap();
                let insert = vec![Val::from(2), Val::from("2")];
                expect_trx_insert(table_id, trx, insert)
                    .await
                    .commit()
                    .await
                    .unwrap();
            }
            let row_pages = session1.total_row_pages(table_id).await.unwrap();
            assert!(row_pages == 2);
            assert!(matches!(
                session1.freeze_table(table_id, 10).await.unwrap(),
                FreezeOutcome::AlreadyFrozen { .. }
            ));

            // Repeated freeze keeps the original prefix, so moving row 1 can
            // reuse the still-active second page.
            {
                let mut trx = session1.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(1)]);
                let res = trx_update_row_by_id(
                    &mut trx,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("3"),
                    }],
                )
                .await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.commit().await.unwrap();
            }
            let row_pages = session1.total_row_pages(table_id).await.unwrap();
            assert!(row_pages == 2);

            // update row 1 will just be in-place.
            {
                let mut trx = session1.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(1)]);
                let res = trx_update_row_by_id(
                    &mut trx,
                    table_id,
                    &key,
                    vec![UpdateCol {
                        idx: 1,
                        val: Val::from("4"),
                    }],
                )
                .await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.commit().await.unwrap();
            }
            let row_pages = session1.total_row_pages(table_id).await.unwrap();
            assert!(row_pages == 2);
        });
    }

    #[test]
    fn test_transition_captures_uncommitted_lock_into_deletion_buffer() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 1, 1, "lock").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());

            let key = single_key(1i32);
            let mut trx = session.begin_trx().unwrap();
            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            let (row_id, _) = index.lookup(&key.vals, trx.sts()).await.unwrap().unwrap();
            let page_id = match table_for_internal_assertion(&engine, table_id)
                .find_row(&session.pool_guards(), row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::NotFound => panic!("row should exist"),
                RowLocation::LwcBlock(..) => unreachable!("row page expected"),
            };

            let res: Result<()> = trx
                .exec(async |stmt| {
                    let page_guard = engine
                        .inner()
                        .pools
                        .mem
                        .get_page::<RowPage>(
                            session.pool_guards().mem_guard(),
                            page_id,
                            LatchFallbackMode::Shared,
                        )
                        .await
                        .expect("buffer-pool read failed in test")
                        .lock_shared_async()
                        .await
                        .unwrap();
                    stmt.acquire_table_write_metadata_lock(table_id)
                        .await
                        .disclose()?;
                    stmt.acquire_table_write_data_lock(table_id)
                        .await
                        .disclose()?;
                    let (rt, effects) = stmt_tests::runtime_and_effects_mut(stmt);
                    let table = table_for_internal_assertion(&engine, table_id);
                    let layout = table.layout_snapshot();
                    let accessor = table.accessor_with_layout(&layout);
                    // Hot-row writes acquire ownership by installing a `Lock`
                    // undo entry at the row's undo head. That entry is the
                    // row-level write lock and the rollback anchor that is
                    // later rewritten to Insert/Update/Delete.
                    let mut lock_row = HotRowMutator::new(
                        accessor.table_id(),
                        accessor.metadata(),
                        rt,
                        &page_guard,
                        row_id,
                    )
                    .lock_for_write(effects, Some((key.index_no, &key.vals)))
                    .await
                    .unwrap();
                    match &mut lock_row {
                        LockRowForWrite::Ok(access) => {
                            drop(access.take());
                        }
                        _ => panic!("lock should succeed"),
                    }

                    let table = table_for_internal_assertion(&engine, table_id);
                    let mut checkpoint_attempt = table
                        .checkpoint_workflow
                        .begin_checkpoint(&table.lifecycle)
                        .unwrap();
                    let root_lease =
                        TableCheckpointRootMutationScope::acquire(Arc::clone(&table)).unwrap();
                    let frozen_pages = checkpoint_attempt.batch().unwrap().pages.clone();
                    let transition_pages = table
                        .load_frozen_pages_for_transition(&session.pool_guards(), &frozen_pages)
                        .await
                        .unwrap();
                    let delay = table.prepare_page_transition(
                        &transition_pages,
                        checkpoint_attempt.batch_mut().unwrap(),
                        stmt.runtime().sts(),
                        &engine.inner().maintenance_test,
                    );
                    assert!(delay.is_none());
                    let transition_lease = table
                        .checkpoint_workflow
                        .try_begin_transition(&table.lifecycle)
                        .unwrap();
                    table.apply_page_transition(
                        &transition_pages,
                        checkpoint_attempt.batch_mut().unwrap(),
                        stmt.runtime().sts(),
                        &engine.inner().maintenance_test,
                    );

                    let marker = table_for_internal_assertion(&engine, table_id)
                        .deletion_buffer()
                        .get(row_id)
                        .unwrap();
                    match marker {
                        DeleteMarker::Ref(status) => {
                            assert!(std::sync::Arc::ptr_eq(&status, stmt.runtime().status()));
                        }
                        DeleteMarker::Committed(_) => {
                            panic!("uncommitted lock should remain as marker ref")
                        }
                    }
                    table.checkpoint_workflow.finish_publication();
                    drop(transition_lease);
                    drop(root_lease);
                    drop(lock_row);
                    drop(page_guard);
                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::InvalidDmlInput)
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_session_cached_insert_page_reuses_live_versioned_page() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            let _row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(1), Val::from("cached-row")],
                )
                .await,
            );
            trx.commit().await.unwrap();

            let cached_page = session.load_active_insert_page(table_id).unwrap();
            assert!(
                table_for_internal_assertion(&engine, table_id)
                    .mem
                    .get_row_page_versioned_shared(&session.pool_guards(), cached_page)
                    .await
                    .unwrap()
                    .is_some()
            );
            session.save_active_insert_page(table_id, cached_page);

            let mut trx = session.begin_trx().unwrap();
            let next_row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(2), Val::from("still-cached")],
                )
                .await,
            );
            trx.commit().await.unwrap();

            let next_page_id = match table_for_internal_assertion(&engine, table_id)
                .find_row(&session.pool_guards(), next_row_id)
                .await
                .unwrap()
            {
                RowLocation::RowPage(page_id) => page_id,
                RowLocation::LwcBlock(..) | RowLocation::NotFound => {
                    panic!("row should still be in the in-memory row store")
                }
            };
            assert_eq!(next_page_id, cached_page.page_id);

            let next_cached_page = session.load_active_insert_page(table_id).unwrap();
            assert_eq!(next_cached_page, cached_page);
        });
    }

    enum CachedInsertPageSessionEnd {
        Close,
        Drop,
    }

    async fn assert_session_end_returns_cached_insert_page(end: CachedInsertPageSessionEnd) {
        let temp_dir = TempDir::new().unwrap();
        let engine = evictable_test_engine(
            &temp_dir,
            64u64 * 1024 * 1024,
            "cached_insert_page_session_end",
        )
        .await;
        let table_id = create_table2_for_test(&engine).await;
        let mut session = engine.new_session().unwrap();

        let mut trx = session.begin_trx().unwrap();
        let first_row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1), Val::from("first-session")],
            )
            .await,
        );
        trx.commit().await.unwrap();
        let first_page_id = match table_for_internal_assertion(&engine, table_id)
            .find_row(&session.pool_guards(), first_row_id)
            .await
            .unwrap()
        {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::LwcBlock(..) | RowLocation::NotFound => {
                panic!("inserted row should remain in the row store")
            }
        };

        match end {
            CachedInsertPageSessionEnd::Close => session.close().await.unwrap(),
            CachedInsertPageSessionEnd::Drop => drop(session),
        }

        let mut next_session = engine.new_session().unwrap();
        let mut trx = next_session.begin_trx().unwrap();
        let next_row_id = unwrap_insert_result(
            trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(2), Val::from("next-session")],
            )
            .await,
        );
        trx.commit().await.unwrap();
        let next_page_id = match table_for_internal_assertion(&engine, table_id)
            .find_row(&next_session.pool_guards(), next_row_id)
            .await
            .unwrap()
        {
            RowLocation::RowPage(page_id) => page_id,
            RowLocation::LwcBlock(..) | RowLocation::NotFound => {
                panic!("inserted row should remain in the row store")
            }
        };
        assert_eq!(next_page_id, first_page_id);
    }

    #[test]
    fn test_session_close_returns_cached_insert_page() {
        smol::block_on(assert_session_end_returns_cached_insert_page(
            CachedInsertPageSessionEnd::Close,
        ));
    }

    #[test]
    fn test_idle_session_drop_returns_cached_insert_page() {
        smol::block_on(assert_session_end_returns_cached_insert_page(
            CachedInsertPageSessionEnd::Drop,
        ));
    }

    #[test]
    fn test_stale_session_cached_insert_page_falls_back_after_checkpoint_gc() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            let _row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(1), Val::from("cached-row")],
                )
                .await,
            );
            trx.commit().await.unwrap();

            let cached_page = session.load_active_insert_page(table_id).unwrap();
            session.save_active_insert_page(table_id, cached_page);

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut checkpoint_session = engine.new_session().unwrap();
            let outcome = checkpoint_session
                .checkpoint_table_with_wait(table_id)
                .await
                .unwrap();
            let CheckpointOutcome::Published { redo_cts, .. } = outcome else {
                panic!("checkpoint should publish, got {outcome:?}");
            };
            wait_for_checkpoint_purge(&session, redo_cts).await;
            let reclaimed = table_for_internal_assertion(&engine, table_id)
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), cached_page)
                .await
                .unwrap()
                .is_none();
            assert!(
                reclaimed,
                "row page should be reclaimed before repro insert"
            );

            let mut trx = session.begin_trx().unwrap();
            let _post_gc_row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(2), Val::from("post-gc-row")],
                )
                .await,
            );
            trx.commit().await.unwrap();

            let key = single_key(2i32);
            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals, vec![Val::from(2), Val::from("post-gc-row")]);
            })
            .await;

            let next_cached_page = session.load_active_insert_page(table_id).unwrap();
            assert_ne!(next_cached_page, cached_page);
        });
    }

    #[test]
    fn test_validated_row_page_shared_result_rejects_stale_reused_page_range() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut trx = session.begin_trx().unwrap();
            let stale_row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(1), Val::from("cached-row")],
                )
                .await,
            );
            trx.commit().await.unwrap();

            let stale_page = session.load_active_insert_page(table_id).unwrap();

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            let mut checkpoint_session = engine.new_session().unwrap();
            let outcome = checkpoint_session
                .checkpoint_table_with_wait(table_id)
                .await
                .unwrap();
            let CheckpointOutcome::Published { redo_cts, .. } = outcome else {
                panic!("checkpoint should publish, got {outcome:?}");
            };
            wait_for_checkpoint_purge(&session, redo_cts).await;
            let reclaimed = table_for_internal_assertion(&engine, table_id)
                .mem
                .get_row_page_versioned_shared(&session.pool_guards(), stale_page)
                .await
                .unwrap()
                .is_none();
            assert!(
                reclaimed,
                "row page should be reclaimed before stale-range validation"
            );

            let large = "r".repeat(48 * 1024);
            let mut reused_row_id = None;
            for key in 2..258 {
                let mut trx = session.begin_trx().unwrap();
                let row_id = unwrap_insert_result(
                    trx_insert_row_by_id(
                        &mut trx,
                        table_id,
                        vec![Val::from(key), Val::from(&large[..])],
                    )
                    .await,
                );
                trx.commit().await.unwrap();
                match table_for_internal_assertion(&engine, table_id)
                    .find_row(&session.pool_guards(), row_id)
                    .await
                    .unwrap()
                {
                    RowLocation::RowPage(page_id) if page_id == stale_page.page_id => {
                        reused_row_id = Some(row_id);
                        break;
                    }
                    RowLocation::RowPage(..) => (),
                    RowLocation::LwcBlock(..) | RowLocation::NotFound => {
                        panic!("newly inserted row should stay in a row page")
                    }
                }
            }
            let reused_row_id = reused_row_id.expect("stale row-page slot should be reused");

            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

            let stale_guard = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .mem()
                .try_get_validated_row_page_shared_result(
                    &session.pool_guards(),
                    stale_page.page_id,
                    stale_row_id,
                )
                .await
                .unwrap();
            assert!(
                stale_guard.is_none(),
                "stale row id should not validate against the reused page range"
            );

            let layout = table_for_internal_assertion(&engine, table_id).layout_snapshot();

            let reused_guard = table_for_internal_assertion(&engine, table_id)
                .accessor_with_layout(&layout)
                .mem()
                .try_get_validated_row_page_shared_result(
                    &session.pool_guards(),
                    stale_page.page_id,
                    reused_row_id,
                )
                .await
                .unwrap();
            assert!(
                reused_guard.is_some(),
                "reused row should validate on the reused page"
            );
        });
    }

    #[test]
    fn test_mvcc_insert_surfaces_cached_insert_page_reload_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(&temp_dir, 9u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let large = "r".repeat(48 * 1024);
            let mut trx = session.begin_trx().unwrap();
            let _row_id = unwrap_insert_result(
                trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(1), Val::from(&large[..])],
                )
                .await,
            );
            trx.commit().await.unwrap();

            let cached_page = session.load_active_insert_page(table_id).unwrap();
            session.save_active_insert_page(table_id, cached_page);

            let mut writer = engine.new_session().unwrap();
            for i in 2..258 {
                expect_insert_committed(
                    table_id,
                    &mut writer,
                    vec![Val::from(i), Val::from(&large[..])],
                )
                .await;
                if test_frame_kind(
                    &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                    cached_page.page_id,
                ) == FrameKind::Evicted
                {
                    break;
                }
            }
            // Timer audit: buffer-eviction/I/O test coordination.
            let mut evicted = false;
            for _ in 0..20 {
                if test_frame_kind(
                    &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                    cached_page.page_id,
                ) == FrameKind::Evicted
                {
                    evicted = true;
                    break;
                }
                Timer::after(Duration::from_millis(50)).await;
            }
            assert!(
                evicted,
                "cached insert page should be evicted before repro insert"
            );

            let mem_pool_file =
                StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp")).unwrap();
            let read_hook = Arc::new(FailingPageReadHook::for_page(
                mem_pool_file,
                cached_page.page_id,
                libc::EIO,
            ));
            let _hook = install_storage_backend_test_hook(read_hook.clone());
            let expected_error_kind = StdIoError::from_raw_os_error(libc::EIO).kind();

            let mut trx = session.begin_trx().unwrap();
            let res = trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(100), Val::from("reload-fails")],
            )
            .await;
            trx.rollback().await.unwrap();
            assert!(
                res.as_ref().is_err_and(|err| err
                    .report()
                    .downcast_ref::<IoError>()
                    .copied()
                    .map(IoError::kind)
                    == Some(expected_error_kind)),
                "expected insert-page reload failure, got {res:?}"
            );
            assert!(
                read_hook.call_count() > 0,
                "cached insert page should reload from disk"
            );
        });
    }

    #[test]
    fn test_mvcc_rollback_poisons_runtime_on_row_page_reload_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(&temp_dir, 9u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            let large = "r".repeat(48 * 1024);
            let mut trx = session.begin_trx().unwrap();
            let _row_id = match trx_insert_row_by_id(
                &mut trx,
                table_id,
                vec![Val::from(1), Val::from(&large[..])],
            )
            .await
            {
                Ok(row_id) => row_id,
                res => panic!("res={res:?}"),
            };

            let cached_page = session.load_active_insert_page(table_id).unwrap();

            let mut writer = engine.new_session().unwrap();
            for i in 2..258 {
                expect_insert_committed(
                    table_id,
                    &mut writer,
                    vec![Val::from(i), Val::from(&large[..])],
                )
                .await;
                if test_frame_kind(
                    &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                    cached_page.page_id,
                ) == FrameKind::Evicted
                {
                    break;
                }
            }
            // Timer audit: buffer-eviction/I/O test coordination.
            let mut evicted = false;
            for _ in 0..20 {
                if test_frame_kind(
                    &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                    cached_page.page_id,
                ) == FrameKind::Evicted
                {
                    evicted = true;
                    break;
                }
                Timer::after(Duration::from_millis(50)).await;
            }
            assert!(evicted, "rollback row page should be evicted before repro");

            let mem_pool_file =
                StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp")).unwrap();
            let read_hook = Arc::new(FailingPageReadHook::for_page(
                mem_pool_file,
                cached_page.page_id,
                libc::EIO,
            ));
            let _hook = install_storage_backend_test_hook(read_hook);
            let expected_io_kind = StdIoError::from_raw_os_error(libc::EIO).kind();

            let rollback_error = trx
                .rollback()
                .await
                .expect_err("row-page reload failure must fail terminal rollback");
            assert_eq!(rollback_error.kind(), ErrorKind::Fatal);
            assert_eq!(
                rollback_error
                    .report()
                    .downcast_ref::<FatalError>()
                    .copied(),
                Some(FatalError::RollbackAccess)
            );
            assert_eq!(
                rollback_error
                    .report()
                    .downcast_ref::<RuntimeError>()
                    .copied(),
                Some(RuntimeError::BufferPageAccess)
            );
            assert_eq!(
                rollback_error
                    .report()
                    .downcast_ref::<IoError>()
                    .copied()
                    .map(IoError::kind),
                Some(expected_io_kind)
            );
            assert_eq!(
                rollback_error
                    .report()
                    .frames()
                    .filter(|frame| frame.is::<ErrorKind>())
                    .count(),
                1
            );

            let poison_error = engine
                .inner()
                .poisoner
                .poison_error()
                .expect("terminal rollback failure must poison the runtime");
            assert_eq!(poison_error.current_context(), &FatalError::RollbackAccess);
            assert_eq!(
                poison_error.downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::BufferPageAccess)
            );
            assert_eq!(
                poison_error
                    .downcast_ref::<IoError>()
                    .copied()
                    .map(IoError::kind),
                Some(expected_io_kind)
            );
            assert!(poison_error.downcast_ref::<ErrorKind>().is_none());
            assert_eq!(fatal_rollback_retention_count(&engine.inner().trx_sys), 1);
            assert!(
                engine
                    .inner()
                    .poisoner
                    .ensure_healthy()
                    .as_ref()
                    .is_err_and(|err| *err.current_context() == FatalError::RollbackAccess)
            );
            assert!(
                session.in_trx().unwrap(),
                "failed-retained operation must remain attached to the session"
            );
            remove_session_for_test(&engine.inner().session_registry, session.id());
        });
    }

    #[test]
    fn test_statement_rollback_poisons_runtime_on_row_page_reload_error() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = evictable_test_engine(&temp_dir, 9u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let large = "r".repeat(48 * 1024);
            let mut writer = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let mut hook_guard = None;
            let mut read_hook = None;

            let res: Result<()> = trx
                .exec(async |stmt| {
                    let _row_id = match stmt_insert_row_by_id(
                        stmt,
                        table_id,
                        vec![Val::from(1), Val::from(&large[..])],
                    )
                    .await
                    {
                        Ok(row_id) => row_id,
                        res => panic!("res={res:?}"),
                    };

                    let cached_page = session.load_active_insert_page(table_id).unwrap();

                    for i in 2..258 {
                        expect_insert_committed(
                            table_id,
                            &mut writer,
                            vec![Val::from(i), Val::from(&large[..])],
                        )
                        .await;
                        if test_frame_kind(
                            &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                            cached_page.page_id,
                        ) == FrameKind::Evicted
                        {
                            break;
                        }
                    }
                    // Timer audit: buffer-eviction/I/O test coordination.
                    let mut evicted = false;
                    for _ in 0..20 {
                        if test_frame_kind(
                            &table_for_internal_assertion(&engine, table_id).mem.mem_pool,
                            cached_page.page_id,
                        ) == FrameKind::Evicted
                        {
                            evicted = true;
                            break;
                        }
                        Timer::after(Duration::from_millis(50)).await;
                    }
                    assert!(
                        evicted,
                        "statement rollback page should be evicted before repro"
                    );

                    let mem_pool_file =
                        StorageBackendFileIdentity::from_path(temp_dir.path().join("data.swp"))
                            .unwrap();
                    let hook = Arc::new(FailingPageReadHook::for_page(
                        mem_pool_file,
                        cached_page.page_id,
                        libc::EIO,
                    ));
                    hook_guard = Some(install_storage_backend_test_hook(hook.clone()));
                    read_hook = Some(hook);

                    Err(Report::new(OperationError::InvalidDmlInput).disclose())
                })
                .await;

            let rollback_error =
                res.expect_err("row-page reload failure must fail statement rollback");
            let expected_io_kind = StdIoError::from_raw_os_error(libc::EIO).kind();
            assert_eq!(rollback_error.kind(), ErrorKind::Fatal);
            assert_eq!(
                rollback_error
                    .report()
                    .downcast_ref::<FatalError>()
                    .copied(),
                Some(FatalError::RollbackAccess)
            );
            assert_eq!(
                rollback_error
                    .report()
                    .downcast_ref::<RuntimeError>()
                    .copied(),
                Some(RuntimeError::BufferPageAccess)
            );
            assert_eq!(
                rollback_error
                    .report()
                    .downcast_ref::<IoError>()
                    .copied()
                    .map(IoError::kind),
                Some(expected_io_kind)
            );
            assert_eq!(
                rollback_error
                    .report()
                    .frames()
                    .filter(|frame| frame.is::<ErrorKind>())
                    .count(),
                1
            );
            assert!(
                read_hook
                    .as_ref()
                    .is_some_and(|hook: &Arc<FailingPageReadHook>| hook.call_count() > 0),
                "statement rollback should reload the evicted page"
            );
            let poison_error = engine
                .inner()
                .poisoner
                .poison_error()
                .expect("statement rollback failure must poison the runtime");
            assert_eq!(poison_error.current_context(), &FatalError::RollbackAccess);
            assert_eq!(
                poison_error.downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::BufferPageAccess)
            );
            assert_eq!(
                poison_error
                    .downcast_ref::<IoError>()
                    .copied()
                    .map(IoError::kind),
                Some(expected_io_kind)
            );
            assert!(poison_error.downcast_ref::<ErrorKind>().is_none());
            assert_eq!(fatal_rollback_retention_count(&engine.inner().trx_sys), 1);
            assert!(
                session.in_trx().unwrap(),
                "failed-retained operation must keep the session unavailable"
            );

            let err = trx.rollback().await.unwrap_err();
            assert!(err.report().downcast_ref::<InternalError>().is_none());
            remove_session_for_test(&engine.inner().session_registry, session.id());
        });
    }

    #[test]
    fn test_user_secondary_indexes_evict_and_continue_serving_lookups() {
        smol::block_on(async {
            use crate::catalog::{
                ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec, TableSpec,
            };
            use crate::value::ValKind;

            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(temp_dir.path())
                    .index_buffer(
                        EvictableBufferPoolConfig::default()
                            .swap_file("index.swp")
                            .max_mem_size(16u64 * 1024 * 1024)
                            .max_file_size(32u64 * 1024 * 1024),
                    )
                    .data_buffer(
                        EvictableBufferPoolConfig::default()
                            .max_mem_size(64u64 * 1024 * 1024)
                            .max_file_size(128u64 * 1024 * 1024),
                    )
                    .trx(TrxSysConfig::default().log_file_stem("redo_index_evict")),
            )
            .await
            .unwrap();

            let mut ddl_session = engine.new_session().unwrap();
            let mut index_specs = vec![IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK)];
            for _ in 0..12 {
                index_specs.push(IndexSpec::new(
                    vec![IndexKey::new(1)],
                    IndexAttributes::empty(),
                ));
            }
            let table_id = ddl_session
                .create_table(
                    TableSpec::new(vec![
                        ColumnSpec::new("id", ValKind::I32, ColumnAttributes::empty()),
                        ColumnSpec::new("name", ValKind::VarByte, ColumnAttributes::empty()),
                    ]),
                    index_specs,
                )
                .await
                .unwrap();
            drop(ddl_session);

            let mut session = engine.new_session().unwrap();
            let mut inserted = Vec::new();

            for batch in 0..96usize {
                let mut trx = session.begin_trx().unwrap();
                for i in 0..64usize {
                    let row_id = (batch * 64 + i) as i32;
                    let seed = format!("{:08x}", row_id);
                    let key = seed.repeat(64);
                    let res = trx_insert_row_by_id(
                        &mut trx,
                        table_id,
                        vec![Val::from(row_id), Val::from(&key[..])],
                    )
                    .await;
                    assert!(res.is_ok(), "res={res:?}");
                    inserted.push((row_id, key));
                }
                trx.commit().await.unwrap();
                let stats = engine.inner().pools.index.stats();
                if stats.completed_writes > 0 && stats.write_errors == 0 {
                    break;
                }
            }

            // Timer audit: index-pool eviction/I/O test coordination.
            for _ in 0..20 {
                let stats = engine.inner().pools.index.stats();
                if stats.completed_writes > 0 && stats.write_errors == 0 {
                    break;
                }
                Timer::after(Duration::from_millis(50)).await;
            }

            let stats = engine.inner().pools.index.stats();
            assert!(
                stats.completed_writes > 0 && stats.write_errors == 0,
                "user secondary-index pool should evict with a small index buffer"
            );

            for key_idx in [0usize, inserted.len() / 2, inserted.len() - 1] {
                let key = SelectKey::new(0, vec![Val::from(inserted[key_idx].0)]);
                let mut trx = session.begin_trx().unwrap();
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, &[0, 1]).await;
                match res {
                    Ok(SelectMvcc::Found(vals)) => {
                        assert_eq!(
                            vals,
                            vec![
                                Val::from(inserted[key_idx].0),
                                Val::from(&inserted[key_idx].1[..]),
                            ]
                        );
                    }
                    other => panic!("unexpected lookup result: {other:?}"),
                }
                trx.commit().await.unwrap();
            }

            let mut trx = session.begin_trx().unwrap();
            let visible_rows = scan_table_i32s(&mut trx, table_id).await.len();
            trx.commit().await.unwrap();
            assert_eq!(visible_rows, inserted.len());
        });
    }

    async fn secondary_index_scan_rows(
        trx: &mut Transaction,
        table_id: TableID,
        key: i32,
    ) -> Vec<Vec<Val>> {
        let key_vals = [Val::from(key)];
        let read_set = [0usize, 1];
        trx.exec(async |stmt| {
            stmt.table_index_lookup_mvcc(table_id, 1, &key_vals, &read_set)
                .await
        })
        .await
        .unwrap()
        .unwrap_rows()
    }

    async fn secondary_index_stream_rows(
        trx: &mut Transaction,
        table_id: TableID,
        key: i32,
    ) -> Vec<Vec<Val>> {
        let key_vals = [Val::from(key)];
        let read_set = [0usize, 1];
        let mut stream = trx
            .stream_stmt()
            .table_index_scan_mvcc(table_id, 1, &key_vals[..]..=&key_vals[..], &read_set)
            .await
            .unwrap();
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await.unwrap() {
            rows.push(row);
        }
        rows
    }

    #[test]
    fn test_secondary_index_scan_mvcc_reads_lwc_projection_without_index_column() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(temp_dir.path())
                    .data_buffer(EvictableBufferPoolConfig::default())
                    .trx(TrxSysConfig::default().log_file_stem("redo_secidx_lwc_projection")),
            )
            .await
            .unwrap();
            let table_id = table4(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut insert = session.begin_trx().unwrap();
            let res =
                trx_insert_row_by_id(&mut insert, table_id, vec![Val::from(10), Val::from(7)])
                    .await;
            assert!(res.is_ok());
            insert.commit().await.unwrap();

            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let table = table_for_internal_assertion(&engine, table_id);
            let key = SelectKey::new(0, vec![Val::from(10i32)]);
            let reader = session.begin_trx().unwrap();
            assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            let key_vals = [Val::from(7i32)];
            let read_set = [0usize];
            let expected = vec![vec![Val::from(10i32)]];

            let mut trx = session.begin_trx().unwrap();
            let rows = trx
                .exec(async |stmt| {
                    stmt.table_index_lookup_mvcc(table_id, 1, &key_vals, &read_set)
                        .await
                })
                .await
                .unwrap()
                .unwrap_rows();
            trx.commit().await.unwrap();
            assert_eq!(rows, expected);

            let mut trx = session.begin_trx().unwrap();
            let mut stream = trx
                .stream_stmt()
                .table_index_scan_mvcc(table_id, 1, &key_vals[..]..=&key_vals[..], &read_set)
                .await
                .unwrap();
            let mut rows = Vec::new();
            while let Some(row) = stream.next().await.unwrap() {
                rows.push(row);
            }
            drop(stream);
            trx.commit().await.unwrap();
            assert_eq!(rows, expected);
        });
    }

    #[test]
    fn test_stream_stmt_validation_opt_out_is_stream_local() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(temp_dir.path())
                    .data_buffer(EvictableBufferPoolConfig::default())
                    .trx(TrxSysConfig::default().log_file_stem("redo_stream_validation_opt_out")),
            )
            .await
            .unwrap();
            let table_id = table4(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut insert = session.begin_trx().unwrap();
            let res =
                trx_insert_row_by_id(&mut insert, table_id, vec![Val::from(10), Val::from(7)])
                    .await;
            assert!(res.is_ok());
            insert.commit().await.unwrap();

            let key_vals = [Val::from(7i32)];
            let mut trx = session.begin_trx().unwrap();
            let err = match trx
                .stream_stmt()
                .table_index_scan_mvcc(table_id, 1, &key_vals[..]..=&key_vals[..], &[])
                .await
            {
                Ok(_) => panic!("empty read set should fail stream construction"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidDmlInput)
            );

            let mut stream = trx
                .stream_stmt()
                .disable_validation()
                .table_index_scan_mvcc(table_id, 1, &key_vals[..]..=&key_vals[..], &[])
                .await
                .unwrap();
            assert_eq!(stream.next().await.unwrap(), Some(Vec::new()));
            assert_eq!(stream.next().await.unwrap(), None);
            drop(stream);

            let err = match trx
                .stream_stmt()
                .table_index_scan_mvcc(table_id, 1, &key_vals[..]..=&key_vals[..], &[])
                .await
            {
                Ok(_) => panic!("empty read set should fail after opt-out stream"),
                Err(err) => err,
            };
            assert_eq!(
                err.report().downcast_ref::<OperationError>().copied(),
                Some(OperationError::InvalidDmlInput)
            );
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_secondary_index_common() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(main_dir)
                    .data_buffer(EvictableBufferPoolConfig::default())
                    .trx(TrxSysConfig::default().log_file_stem("redo_secidx1")),
            )
            .await
            .unwrap();
            let table_id = table4(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                let user_read_set = &[0usize, 1];
                let mut trx = session.begin_trx().unwrap();
                for i in 0i32..5i32 {
                    let res =
                        trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(i), Val::from(i)])
                            .await;
                    assert!(res.is_ok());
                }
                trx.commit().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(1, vec![Val::from(1i32)]);
                let res = trx
                    .exec(async |stmt| {
                        stmt.table_index_lookup_mvcc(
                            table_id,
                            key.index_no,
                            &key.vals,
                            user_read_set,
                        )
                        .await
                    })
                    .await;
                trx.commit().await.unwrap();
                assert!(res.unwrap().unwrap_rows().len() == 1);

                let mut trx = session.begin_trx().unwrap();
                let key_vals = [Val::from(1i32)];
                let rows = trx
                    .exec(async |stmt| {
                        stmt.table_index_scan_mvcc(
                            table_id,
                            1,
                            &key_vals[..]..=&key_vals[..],
                            user_read_set,
                        )
                        .await
                    })
                    .await
                    .unwrap()
                    .unwrap_rows();
                trx.commit().await.unwrap();
                assert_eq!(rows, vec![vec![Val::from(1i32), Val::from(1i32)]]);

                let mut trx = session.begin_trx().unwrap();
                let mut stream = trx
                    .stream_stmt()
                    .table_index_scan_mvcc(
                        table_id,
                        1,
                        &key_vals[..]..=&key_vals[..],
                        user_read_set,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    stream.next().await.unwrap(),
                    Some(vec![Val::from(1i32), Val::from(1i32)])
                );
                assert_eq!(stream.next().await.unwrap(), None);
                assert_eq!(stream.next().await.unwrap(), None);
                drop(stream);
                trx.commit().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let lower = [Val::from(1i32)];
                let upper = [Val::from(4i32)];
                let mut stream = trx
                    .stream_stmt()
                    .table_index_scan_mvcc(table_id, 0, &lower[..]..&upper[..], user_read_set)
                    .await
                    .unwrap();
                let mut rows = Vec::new();
                while let Some(vals) = stream.next().await.unwrap() {
                    rows.push(vals);
                }
                drop(stream);
                trx.commit().await.unwrap();
                let unique_range_expected = vec![
                    vec![Val::from(1i32), Val::from(1i32)],
                    vec![Val::from(2i32), Val::from(2i32)],
                    vec![Val::from(3i32), Val::from(3i32)],
                ];
                assert_eq!(rows, unique_range_expected);

                let mut trx = session.begin_trx().unwrap();
                let rows = trx
                    .exec(async |stmt| {
                        stmt.table_index_scan_mvcc(
                            table_id,
                            0,
                            &lower[..]..&upper[..],
                            user_read_set,
                        )
                        .await
                    })
                    .await
                    .unwrap()
                    .unwrap_rows();
                trx.commit().await.unwrap();
                assert_eq!(rows, unique_range_expected);

                let mut trx = session.begin_trx().unwrap();
                let mut stream = trx
                    .stream_stmt()
                    .table_index_scan_mvcc(table_id, 1, .., user_read_set)
                    .await
                    .unwrap();
                assert!(stream.next().await.unwrap().is_some());
                drop(stream);
                let key = SelectKey::new(0, vec![Val::from(2i32)]);
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));

                let mut trx = session.begin_trx().unwrap();
                let err = match trx
                    .stream_stmt()
                    .table_index_scan_mvcc(table_id, 1, .., &[])
                    .await
                {
                    Ok(_) => panic!("empty read set should fail stream construction"),
                    Err(err) => err,
                };
                assert_eq!(
                    err.report().downcast_ref::<OperationError>().copied(),
                    Some(OperationError::InvalidDmlInput)
                );
                trx.commit().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let invalid_key_vals = [Val::from(1i32), Val::from(2i32)];
                let err = match trx
                    .stream_stmt()
                    .table_index_scan_mvcc(
                        table_id,
                        1,
                        &invalid_key_vals[..]..=&invalid_key_vals[..],
                        user_read_set,
                    )
                    .await
                {
                    Ok(_) => panic!("invalid stream key shape should fail stream construction"),
                    Err(err) => err,
                };
                assert_eq!(
                    err.report().downcast_ref::<OperationError>().copied(),
                    Some(OperationError::InvalidDmlInput)
                );
                trx.commit().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(0i32),
                }];
                let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(1, vec![Val::from(0i32)]);
                let res = trx
                    .exec(async |stmt| {
                        stmt.table_index_lookup_mvcc(
                            table_id,
                            key.index_no,
                            &key.vals,
                            user_read_set,
                        )
                        .await
                    })
                    .await;
                trx.commit().await.unwrap();
                assert!(res.unwrap().unwrap_rows().len() == 2);

                let mut trx = session.begin_trx().unwrap();
                let lower = [Val::from(0i32)];
                let upper = [Val::from(5i32)];
                let mut stream = trx
                    .stream_stmt()
                    .table_index_scan_mvcc(table_id, 1, &lower[..]..&upper[..], user_read_set)
                    .await
                    .unwrap();
                let mut rows = Vec::new();
                while let Some(vals) = stream.next().await.unwrap() {
                    rows.push(vals);
                }
                drop(stream);
                trx.commit().await.unwrap();
                assert_eq!(rows.len(), 5);
                assert_eq!(
                    rows.iter()
                        .filter(|vals| vals[0] == Val::from(1i32))
                        .count(),
                    1
                );

                let mut trx = session.begin_trx().unwrap();
                let rows = trx
                    .exec(async |stmt| {
                        stmt.table_index_scan_mvcc(
                            table_id,
                            1,
                            &lower[..]..&upper[..],
                            user_read_set,
                        )
                        .await
                    })
                    .await
                    .unwrap()
                    .unwrap_rows();
                trx.commit().await.unwrap();
                assert_eq!(rows.len(), 5);
                assert_eq!(
                    rows.iter()
                        .filter(|vals| vals[0] == Val::from(1i32))
                        .count(),
                    1
                );

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(1, vec![Val::from(0i32)]);
                let res = trx
                    .exec(async |stmt| {
                        stmt.table_index_lookup_mvcc(
                            table_id,
                            key.index_no,
                            &key.vals,
                            user_read_set,
                        )
                        .await
                    })
                    .await;
                _ = trx.commit().await.unwrap();
                assert!(res.unwrap().unwrap_rows().len() == 1);
            }
        })
    }

    #[test]
    fn test_secondary_index_scan_mvcc_uncommitted_delete_candidate_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(temp_dir.path())
                    .data_buffer(EvictableBufferPoolConfig::default())
                    .trx(TrxSysConfig::default().log_file_stem("redo_secidx_uncommitted_delete")),
            )
            .await
            .unwrap();
            let table_id = table4(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut insert = session.begin_trx().unwrap();
            let res =
                trx_insert_row_by_id(&mut insert, table_id, vec![Val::from(10), Val::from(7)])
                    .await;
            assert!(res.is_ok());
            insert.commit().await.unwrap();

            let mut writer = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(10i32)]);
            let res = trx_delete_row_by_id(&mut writer, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

            assert_eq!(
                secondary_index_scan_rows(&mut writer, table_id, 7).await,
                Vec::<Vec<Val>>::new()
            );
            assert_eq!(
                secondary_index_stream_rows(&mut writer, table_id, 7).await,
                Vec::<Vec<Val>>::new()
            );

            let mut reader_session = engine.new_session().unwrap();
            let mut reader = reader_session.begin_trx().unwrap();
            let expected = vec![vec![Val::from(10i32), Val::from(7i32)]];
            assert_eq!(
                secondary_index_scan_rows(&mut reader, table_id, 7).await,
                expected
            );
            assert_eq!(
                secondary_index_stream_rows(&mut reader, table_id, 7).await,
                expected
            );
            reader.commit().await.unwrap();
            writer.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_secondary_index_scan_mvcc_delete_committed_after_snapshot() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(temp_dir.path())
                    .data_buffer(EvictableBufferPoolConfig::default())
                    .trx(TrxSysConfig::default().log_file_stem("redo_secidx_late_delete")),
            )
            .await
            .unwrap();
            let table_id = table4(&engine).await;
            let mut session = engine.new_session().unwrap();

            let mut insert = session.begin_trx().unwrap();
            let res =
                trx_insert_row_by_id(&mut insert, table_id, vec![Val::from(10), Val::from(7)])
                    .await;
            assert!(res.is_ok());
            insert.commit().await.unwrap();

            let mut reader_session = engine.new_session().unwrap();
            let mut old_reader = reader_session.begin_trx().unwrap();

            let mut writer = session.begin_trx().unwrap();
            let key = SelectKey::new(0, vec![Val::from(10i32)]);
            let res = trx_delete_row_by_id(&mut writer, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
            writer.commit().await.unwrap();

            let expected = vec![vec![Val::from(10i32), Val::from(7i32)]];
            assert_eq!(
                secondary_index_scan_rows(&mut old_reader, table_id, 7).await,
                expected
            );
            assert_eq!(
                secondary_index_stream_rows(&mut old_reader, table_id, 7).await,
                expected
            );
            old_reader.commit().await.unwrap();

            let mut fresh_reader = session.begin_trx().unwrap();
            assert_eq!(
                secondary_index_scan_rows(&mut fresh_reader, table_id, 7).await,
                Vec::<Vec<Val>>::new()
            );
            assert_eq!(
                secondary_index_stream_rows(&mut fresh_reader, table_id, 7).await,
                Vec::<Vec<Val>>::new()
            );
            fresh_reader.commit().await.unwrap();
        });
    }
}
