//! Composite MemIndex/DiskTree secondary-index core.
//!
//! The composite types group the current in-memory BTree-backed secondary
//! index with the current published secondary DiskTree root. Catalog tables use
//! the catalog-only in-memory enum in this module because they do not have a
//! cold DiskTree layer.

use super::disk_tree::{
    NonUniqueDiskTree, NonUniqueDiskTreeRuntime, UniqueDiskTree, UniqueDiskTreeRuntime,
};
use super::index_stream::{
    NonUniqueDiskTreeCandidateStream, NonUniqueMemIndexCandidateStream,
    UniqueDiskTreeCandidateStream, UniqueMemIndexCandidateStream,
};
use super::non_unique_index::{GuardedNonUniqueMemIndex, IndexMask, NonUniqueMemIndex};
use super::unique_index::{GuardedUniqueMemIndex, UniqueMemIndex};
use crate::buffer::{BufferPool, PoolGuard, PoolGuards, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{InternalError, RuntimeError, RuntimeResult, SecondaryIndexBinding};
use crate::file::table_file::TableFile;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::util::Maskable;
use crate::index::{BTreeKeyEncoder, IndexBatchStream, IndexLookupCandidate, KeyRange};
use crate::quiescent::QuiescentGuard;
use crate::value::{Val, ValType};
use error_stack::Report;
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

/// Result of attempting to insert a secondary-index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum IndexInsert {
    /// Insert succeeded. `true` means a delete-marked entry was merged.
    Ok(bool),
    /// Insert found an existing owner, carrying row id and delete flag.
    DuplicateKey(RowID, bool),
}

impl IndexInsert {
    /// Returns whether the insert attempt succeeded.
    #[inline]
    pub(crate) fn is_ok(&self) -> bool {
        matches!(self, IndexInsert::Ok(_))
    }
}

/// Result of a secondary-index compare-exchange operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum IndexCompareExchange {
    /// The expected owner matched and the value was updated.
    Ok,
    /// The key existed but did not carry the expected owner.
    Mismatch,
    /// The key did not exist in the index.
    NotExists,
}

impl IndexCompareExchange {
    /// Returns whether the compare-exchange updated the entry.
    #[inline]
    pub(crate) fn is_ok(self) -> bool {
        matches!(self, IndexCompareExchange::Ok)
    }
}

/// Catalog-table secondary-index variant over the mutable in-memory layer only.
pub(crate) enum InMemorySecondaryIndex<P: 'static> {
    /// Unique MemIndex backend.
    Unique(UniqueMemIndex<P>),
    /// Non-unique MemIndex backend.
    NonUnique(NonUniqueMemIndex<P>),
}

impl<P: BufferPool> InMemorySecondaryIndex<P> {
    /// Build an in-memory secondary index from catalog index metadata.
    #[inline]
    pub(crate) async fn new<F: Fn(usize) -> ValType>(
        index_pool: QuiescentGuard<P>,
        index_pool_guard: &PoolGuard,
        index_spec: &IndexSpec,
        ty_infer: F,
        ts: TrxID,
    ) -> RuntimeResult<Self> {
        if index_spec.unique() {
            let index =
                UniqueMemIndex::new(index_pool, index_pool_guard, index_spec, ty_infer, ts).await?;
            Ok(Self::Unique(index))
        } else {
            let index =
                NonUniqueMemIndex::new(index_pool, index_pool_guard, index_spec, ty_infer, ts)
                    .await?;
            Ok(Self::NonUnique(index))
        }
    }

    /// Destroy this in-memory secondary index and reclaim all pages it owns.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> RuntimeResult<()> {
        match self {
            Self::Unique(index) => index.destroy(pool_guard).await,
            Self::NonUnique(index) => index.destroy(pool_guard).await,
        }
    }

    /// Return whether this in-memory index enforces uniqueness.
    #[inline]
    pub(crate) fn is_unique(&self) -> bool {
        matches!(self, Self::Unique(_))
    }

    /// Returns the unique-index view when this slot is unique.
    #[inline]
    pub(crate) fn unique(&self) -> Option<&UniqueMemIndex<P>> {
        match self {
            Self::Unique(index) => Some(index),
            Self::NonUnique(_) => None,
        }
    }

    /// Returns the non-unique-index view when this slot is non-unique.
    #[inline]
    pub(crate) fn non_unique(&self) -> Option<&NonUniqueMemIndex<P>> {
        match self {
            Self::Unique(_) => None,
            Self::NonUnique(index) => Some(index),
        }
    }
}

/// Runtime cold-layer opener for one user-table secondary DiskTree.
///
/// The runtime is table-specific by construction. Foreground/runtime callers
/// must pass a root id captured from a proof-gated table snapshot. Already
/// opened readers keep their root snapshot.
pub(crate) struct SecondaryDiskTreeRuntime {
    index_no: usize,
    kind: SecondaryDiskTreeRuntimeKind,
}

impl SecondaryDiskTreeRuntime {
    /// Create a cold-layer runtime for one table secondary index.
    #[inline]
    pub(crate) fn new(
        index_no: usize,
        metadata: Arc<TableMetadata>,
        table_file: Arc<TableFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> RuntimeResult<Self> {
        let index_spec = metadata.idx.index_spec(index_no).ok_or_else(|| {
            Report::new(InternalError::SecondaryIndexOutOfBounds)
                .attach(format!(
                    "index_no={index_no}, index_slot_count={}",
                    metadata.idx.index_slot_count()
                ))
                .change_context(RuntimeError::IndexAccess)
                .attach("operation=build_secondary_index_runtime")
        })?;
        let file_kind = table_file.file_kind();
        let file = Arc::clone(table_file.sparse_file());
        let kind = if index_spec.unique() {
            SecondaryDiskTreeRuntimeKind::Unique(UniqueDiskTreeRuntime::new(
                index_spec,
                metadata.as_ref(),
                file_kind,
                file,
                disk_pool,
            ))
        } else {
            SecondaryDiskTreeRuntimeKind::NonUnique(NonUniqueDiskTreeRuntime::new(
                index_spec,
                metadata.as_ref(),
                file_kind,
                file,
                disk_pool,
            ))
        };
        let runtime = Self { index_no, kind };
        Ok(runtime)
    }

    /// Return the table index number represented by this context.
    #[inline]
    pub(crate) fn index_no(&self) -> usize {
        self.index_no
    }

    /// Borrow a guard for opening one or more DiskTree readers on this runtime.
    #[inline]
    pub(crate) fn disk_pool_guard(&self) -> PoolGuard {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => runtime.disk_pool_guard(),
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => runtime.disk_pool_guard(),
        }
    }

    /// Returns the shared key encoder for this secondary index.
    #[inline]
    pub(crate) fn key_encoder(&self) -> Arc<BTreeKeyEncoder> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => runtime.encoder(),
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => runtime.encoder(),
        }
    }

    #[inline]
    pub(super) fn unique_runtime(&self) -> &UniqueDiskTreeRuntime {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => runtime,
            SecondaryDiskTreeRuntimeKind::NonUnique(_) => {
                unreachable!("unique secondary index has non-unique DiskTree runtime")
            }
        }
    }

    #[inline]
    pub(super) fn non_unique_runtime(&self) -> &NonUniqueDiskTreeRuntime {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => runtime,
            SecondaryDiskTreeRuntimeKind::Unique(_) => {
                unreachable!("non-unique secondary index has unique DiskTree runtime")
            }
        }
    }

    /// Open the unique secondary DiskTree at one captured root block id.
    #[inline]
    pub(crate) fn open_unique_at<'a>(
        &'a self,
        root_block_id: BlockID,
        disk_pool_guard: &'a PoolGuard,
    ) -> RuntimeResult<UniqueDiskTree<'a>> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => {
                Ok(runtime.open(root_block_id, disk_pool_guard))
            }
            SecondaryDiskTreeRuntimeKind::NonUnique(_) => {
                Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                    .attach(SecondaryIndexBinding {
                        expected: "unique",
                        actual: "non-unique",
                    })
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=bind_secondary_index_runtime"))
            }
        }
    }

    /// Open the non-unique secondary DiskTree at one captured root block id.
    #[inline]
    pub(crate) fn open_non_unique_at<'a>(
        &'a self,
        root_block_id: BlockID,
        disk_pool_guard: &'a PoolGuard,
    ) -> RuntimeResult<NonUniqueDiskTree<'a>> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(_) => {
                Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                    .attach(SecondaryIndexBinding {
                        expected: "non-unique",
                        actual: "unique",
                    })
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=bind_secondary_index_runtime"))
            }
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => {
                Ok(runtime.open(root_block_id, disk_pool_guard))
            }
        }
    }

    /// Collect all DiskTree node blocks reachable from one captured root.
    #[inline]
    pub(crate) async fn collect_reachable_blocks(
        &self,
        root_block_id: BlockID,
        disk_pool_guard: &PoolGuard,
        out: &mut BTreeSet<BlockID>,
    ) -> RuntimeResult<()> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => {
                runtime
                    .open(root_block_id, disk_pool_guard)
                    .collect_reachable_blocks(out)
                    .await
            }
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => {
                runtime
                    .open(root_block_id, disk_pool_guard)
                    .collect_reachable_blocks(out)
                    .await
            }
        }
    }
}

enum SecondaryDiskTreeRuntimeKind {
    Unique(UniqueDiskTreeRuntime),
    NonUnique(NonUniqueDiskTreeRuntime),
}

/// Owned user-table secondary-index storage.
pub(crate) enum SecondaryIndex<P: 'static> {
    /// Unique MemIndex plus cold DiskTree runtime.
    Unique {
        mem: UniqueMemIndex<P>,
        disk: SecondaryDiskTreeRuntime,
    },
    /// Non-unique MemIndex plus cold DiskTree runtime.
    NonUnique {
        mem: NonUniqueMemIndex<P>,
        disk: SecondaryDiskTreeRuntime,
    },
}

impl<P: BufferPool> SecondaryIndex<P> {
    /// Destroy the mutable MemIndex owned by this composite index.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> RuntimeResult<()> {
        match self {
            Self::Unique { mem, .. } => mem.destroy(pool_guard).await,
            Self::NonUnique { mem, .. } => mem.destroy(pool_guard).await,
        }
    }

    /// Return whether this composite index enforces uniqueness.
    #[inline]
    pub(crate) fn is_unique(&self) -> bool {
        matches!(self, Self::Unique { .. })
    }

    /// Return the table index number from the cold runtime.
    #[inline]
    pub(crate) fn index_no(&self) -> usize {
        self.disk_runtime().index_no()
    }

    /// Return the cold DiskTree runtime used by this composite.
    #[inline]
    pub(crate) fn disk_runtime(&self) -> &SecondaryDiskTreeRuntime {
        match self {
            Self::Unique { disk, .. } | Self::NonUnique { disk, .. } => disk,
        }
    }

    /// Returns the shared key encoder for this index.
    #[inline]
    pub(crate) fn key_encoder(&self) -> Arc<BTreeKeyEncoder> {
        self.disk_runtime().key_encoder()
    }

    /// Return the unique MemIndex when this slot is unique.
    #[inline]
    pub(crate) fn unique_mem(&self) -> RuntimeResult<&UniqueMemIndex<P>> {
        match self {
            Self::Unique { mem, .. } => Ok(mem),
            Self::NonUnique { .. } => {
                Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                    .attach(SecondaryIndexBinding {
                        expected: "unique",
                        actual: "non-unique",
                    })
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=bind_secondary_index_runtime"))
            }
        }
    }

    /// Return the non-unique MemIndex when this slot is non-unique.
    #[inline]
    pub(crate) fn non_unique_mem(&self) -> RuntimeResult<&NonUniqueMemIndex<P>> {
        match self {
            Self::Unique { .. } => Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                .attach(SecondaryIndexBinding {
                    expected: "non-unique",
                    actual: "unique",
                })
                .change_context(RuntimeError::IndexAccess)
                .attach("operation=bind_secondary_index_runtime")),
            Self::NonUnique { mem, .. } => Ok(mem),
        }
    }

    /// Bind a unique user-table secondary index to one captured DiskTree root.
    #[inline]
    pub(crate) fn bind_unique<'a, 'g>(
        &'a self,
        guards: &'g PoolGuards,
        root: BlockID,
    ) -> RuntimeResult<UniqueSecondaryIndex<'a, 'g, P>> {
        match self {
            Self::Unique { mem, disk } => Ok(UniqueSecondaryIndex::new(
                mem,
                disk,
                root,
                guards.index_guard(),
                guards.disk_guard(),
            )),
            Self::NonUnique { .. } => {
                Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                    .attach(SecondaryIndexBinding {
                        expected: "unique",
                        actual: "non-unique",
                    })
                    .change_context(RuntimeError::IndexAccess)
                    .attach("operation=bind_secondary_index_runtime"))
            }
        }
    }

    /// Bind a non-unique user-table secondary index to one captured DiskTree root.
    #[inline]
    pub(crate) fn bind_non_unique<'a, 'g>(
        &'a self,
        guards: &'g PoolGuards,
        root: BlockID,
    ) -> RuntimeResult<NonUniqueSecondaryIndex<'a, 'g, P>> {
        match self {
            Self::Unique { .. } => Err(Report::new(InternalError::SecondaryIndexBindingMismatch)
                .attach(SecondaryIndexBinding {
                    expected: "non-unique",
                    actual: "unique",
                })
                .change_context(RuntimeError::IndexAccess)
                .attach("operation=bind_secondary_index_runtime")),
            Self::NonUnique { mem, disk } => Ok(NonUniqueSecondaryIndex::new(
                mem,
                disk,
                root,
                guards.index_guard(),
                guards.disk_guard(),
            )),
        }
    }
}

/// Root-bound unique secondary index view over one MemIndex plus DiskTree root.
pub(crate) struct UniqueSecondaryIndex<'a, 'g, P: 'static> {
    mem: GuardedUniqueMemIndex<'a, 'g, P>,
    disk: &'a SecondaryDiskTreeRuntime,
    root: BlockID,
    disk_pool_guard: &'g PoolGuard,
}

impl<P: 'static> Clone for UniqueSecondaryIndex<'_, '_, P> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: 'static> Copy for UniqueSecondaryIndex<'_, '_, P> {}

impl<'a, 'g, P: BufferPool> UniqueSecondaryIndex<'a, 'g, P> {
    #[inline]
    fn new(
        mem: &'a UniqueMemIndex<P>,
        disk: &'a SecondaryDiskTreeRuntime,
        root: BlockID,
        index_pool_guard: &'g PoolGuard,
        disk_pool_guard: &'g PoolGuard,
    ) -> Self {
        Self {
            mem: mem.bind(index_pool_guard),
            disk,
            root,
            disk_pool_guard,
        }
    }

    #[inline]
    fn open(&self) -> RuntimeResult<UniqueDiskTree<'_>> {
        self.disk.open_unique_at(self.root, self.disk_pool_guard)
    }

    /// Atomically update only MemIndex, without lookup or DiskTree fallback.
    #[inline]
    pub(crate) async fn compare_exchange_mem(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        self.mem
            .compare_exchange(key, old_row_id, new_row_id, ts)
            .await
    }

    /// Replace an exact MemIndex owner or insert on true MemIndex absence.
    #[inline]
    pub(crate) async fn replace_or_insert_mem(
        &self,
        key: &[Val],
        expected_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        self.mem
            .replace_or_insert(key, expected_row_id, new_row_id, ts)
            .await
    }

    /// Attempt insertion while retaining the exact selected owner on conflict.
    ///
    /// MemIndex is checked first because it may shadow the captured DiskTree
    /// owner. If both layers are absent, the final MemIndex insertion closes the
    /// race with a concurrent claim. Every occupied result records the layer and
    /// exact owner representation that must still match when the caller later
    /// consumes the observation.
    #[inline]
    pub(crate) async fn insert_if_not_exists_observed<'k>(
        &self,
        key: &'k [Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> RuntimeResult<UniqueInsertAttempt<'a, 'g, 'k, P>> {
        debug_assert!(!row_id.is_deleted());
        if let Some((owner_row_id, deleted)) = self.mem.lookup(key, ts).await? {
            if merge_if_match_deleted && deleted && owner_row_id == row_id {
                return Ok(
                    match self.mem.insert_if_not_exists(key, row_id, true, ts).await? {
                        IndexInsert::Ok(merged) => UniqueInsertAttempt::Inserted { merged },
                        IndexInsert::DuplicateKey(owner_row_id, deleted) => {
                            UniqueInsertAttempt::Occupied(UniqueOwnerObservation {
                                index: *self,
                                key,
                                owner_row_id,
                                deleted,
                                source: UniqueOwnerSource::Mem,
                            })
                        }
                    },
                );
            }
            return Ok(UniqueInsertAttempt::Occupied(UniqueOwnerObservation {
                index: *self,
                key,
                owner_row_id,
                deleted,
                source: UniqueOwnerSource::Mem,
            }));
        }

        let disk = self.open()?;
        if let Some(owner_row_id) = disk.lookup(key).await? {
            return Ok(UniqueInsertAttempt::Occupied(UniqueOwnerObservation {
                index: *self,
                key,
                owner_row_id,
                deleted: false,
                source: UniqueOwnerSource::Disk,
            }));
        }

        Ok(
            match self
                .mem
                .insert_if_not_exists(key, row_id, false, ts)
                .await?
            {
                IndexInsert::Ok(merged) => UniqueInsertAttempt::Inserted { merged },
                IndexInsert::DuplicateKey(owner_row_id, deleted) => {
                    UniqueInsertAttempt::Occupied(UniqueOwnerObservation {
                        index: *self,
                        key,
                        owner_row_id,
                        deleted,
                        source: UniqueOwnerSource::Mem,
                    })
                }
            },
        )
    }
}

impl<P: BufferPool> UniqueSecondaryIndex<'_, '_, P> {
    /// Lookup one unique owner across MemIndex and the captured DiskTree root.
    #[inline]
    pub(crate) async fn lookup(
        &self,
        key: &[Val],
        ts: TrxID,
    ) -> RuntimeResult<Option<(RowID, bool)>> {
        if let Some(hit) = self.mem.lookup(key, ts).await? {
            return Ok(Some(hit));
        }
        let disk = self.open()?;
        Ok(disk.lookup(key).await?.map(|row_id| (row_id, false)))
    }

    /// Physically delete only a matching MemIndex owner.
    #[inline]
    pub(crate) async fn compare_delete_mem(
        &self,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!old_row_id.is_deleted());
        self.mem
            .compare_delete(key, old_row_id, ignore_del_mask, ts)
            .await
    }

    /// Scan lookup candidates across MemIndex and the captured DiskTree root.
    #[inline]
    pub(crate) fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        ts: TrxID,
    ) -> RuntimeResult<
        SecondaryIndexCandidateStream<
            UniqueMemIndexCandidateStream<'a, P>,
            UniqueDiskTreeCandidateStream<'a, 'a>,
        >,
    > {
        let mem = self.mem.index_scan_candidates(range, ts)?;
        let disk = self.open()?.scan_candidate_stream(range);
        Ok(SecondaryIndexCandidateStream::new(mem, disk))
    }
}

/// Layer that supplied one exact unique-owner observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum UniqueOwnerSource {
    /// Mutable owner selected from MemIndex, possibly in deleted form.
    Mem,
    /// Active owner selected from the immutable captured DiskTree root.
    Disk,
}

/// Exact owner selected while attempting to claim one unique logical key.
///
/// This is root-and-key-bound selection evidence, not permission to replace the
/// owner. The caller must still validate row visibility, the current key,
/// deletion state, and any required unique-version link. The private fields
/// bind that validation to the exact composite index, borrowed key, owner, and
/// source that were observed.
///
/// The token is deliberately non-`Clone` and consumed by [`Self::replace`].
/// A Mem-sourced token expects the same active or delete-marked MemIndex value.
/// A Disk-sourced token expects an immutable active owner and remembers that an
/// equivalent MemIndex entry was absent when the DiskTree was consulted.
pub(crate) struct UniqueOwnerObservation<'a, 'g, 'k, P: 'static> {
    index: UniqueSecondaryIndex<'a, 'g, P>,
    key: &'k [Val],
    owner_row_id: RowID,
    deleted: bool,
    source: UniqueOwnerSource,
}

impl<'a, 'g, 'k, P: BufferPool> UniqueOwnerObservation<'a, 'g, 'k, P> {
    /// Return the logical owner RowID without its MemIndex delete encoding.
    #[inline]
    pub(crate) fn owner_row_id(&self) -> RowID {
        self.owner_row_id
    }

    /// Return whether the observed MemIndex owner was delete-marked.
    ///
    /// DiskTree observations are always active, so this is always `false` for
    /// [`UniqueOwnerSource::Disk`].
    #[inline]
    pub(crate) fn deleted(&self) -> bool {
        self.deleted
    }

    /// Consume this exact observation to claim the key for `new_row_id`.
    ///
    /// Mem observations compare-exchange the representation that was actually
    /// selected. DiskTree is immutable, so a Disk observation instead performs
    /// one atomic MemIndex replace-or-insert traversal: it replaces a matching
    /// copy installed after observation, inserts when MemIndex is still absent,
    /// and reports a mismatch if another owner won the race.
    #[inline]
    pub(crate) async fn replace(
        self,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        debug_assert!(!new_row_id.is_deleted());
        let expected_row_id = if self.deleted {
            self.owner_row_id.deleted()
        } else {
            self.owner_row_id
        };
        match self.source {
            UniqueOwnerSource::Mem => {
                // The observed MemIndex entry itself remains the mutation
                // target, including its delete bit.
                self.index
                    .compare_exchange_mem(self.key, expected_row_id, new_row_id, ts)
                    .await
            }
            UniqueOwnerSource::Disk => {
                debug_assert!(!self.deleted);
                // The immutable DiskTree owner cannot be updated. Install the
                // latest owner in MemIndex while accepting an equivalent Mem
                // copy that may have appeared since the observation.
                self.index
                    .replace_or_insert_mem(self.key, expected_row_id, new_row_id, ts)
                    .await
            }
        }
    }
}

/// Result of an observation-preserving unique-key insertion attempt.
pub(crate) enum UniqueInsertAttempt<'a, 'g, 'k, P: 'static> {
    /// The new owner was inserted. `merged` preserves insert-undo semantics.
    Inserted { merged: bool },
    /// Another owner was selected from MemIndex or the captured DiskTree.
    Occupied(UniqueOwnerObservation<'a, 'g, 'k, P>),
}

/// Root-bound non-unique secondary index view over one MemIndex plus DiskTree root.
#[derive(Clone, Copy)]
pub(crate) struct NonUniqueSecondaryIndex<'a, 'g, P: 'static> {
    mem: GuardedNonUniqueMemIndex<'a, 'g, P>,
    disk: &'a SecondaryDiskTreeRuntime,
    root: BlockID,
    disk_pool_guard: &'g PoolGuard,
}

impl<'a, 'g, P: BufferPool> NonUniqueSecondaryIndex<'a, 'g, P> {
    #[inline]
    fn new(
        mem: &'a NonUniqueMemIndex<P>,
        disk: &'a SecondaryDiskTreeRuntime,
        root: BlockID,
        index_pool_guard: &'g PoolGuard,
        disk_pool_guard: &'g PoolGuard,
    ) -> Self {
        Self {
            mem: mem.bind(index_pool_guard),
            disk,
            root,
            disk_pool_guard,
        }
    }

    #[inline]
    fn open(&self) -> RuntimeResult<NonUniqueDiskTree<'_>> {
        self.disk
            .open_non_unique_at(self.root, self.disk_pool_guard)
    }

    /// Atomically mask one exact MemIndex entry without lookup or DiskTree.
    #[inline]
    pub(crate) async fn mask_mem_if_present(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexMask> {
        self.mem.mask_if_present(key, row_id, ts).await
    }

    /// Insert one exact entry into MemIndex without consulting DiskTree.
    #[inline]
    pub(crate) async fn insert_mem_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> RuntimeResult<IndexInsert> {
        self.mem
            .insert_if_not_exists(key, row_id, merge_if_match_deleted, ts)
            .await
    }
}

impl<P: BufferPool> NonUniqueSecondaryIndex<'_, '_, P> {
    /// Lookup one exact entry across MemIndex and the captured DiskTree root.
    #[inline]
    pub(crate) async fn lookup_unique(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<Option<bool>> {
        if let Some(mem_hit) = self.mem.lookup_unique(key, row_id, ts).await? {
            return Ok(Some(mem_hit));
        }
        let disk = self.open()?;
        if disk.contains_exact(key, row_id).await? {
            Ok(Some(true))
        } else {
            Ok(None)
        }
    }

    /// Physically delete only a matching MemIndex exact entry.
    #[inline]
    pub(crate) async fn compare_delete_mem(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        self.mem
            .compare_delete(key, row_id, ignore_del_mask, ts)
            .await
    }

    /// Scan lookup candidates across MemIndex and the captured DiskTree root.
    #[inline]
    pub(crate) fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        ts: TrxID,
    ) -> RuntimeResult<
        SecondaryIndexCandidateStream<
            NonUniqueMemIndexCandidateStream<'a, P>,
            NonUniqueDiskTreeCandidateStream<'a, 'a>,
        >,
    > {
        let mem = self.mem.index_scan_candidates(range, ts)?;
        let disk = self.open()?.scan_candidate_stream(range);
        Ok(SecondaryIndexCandidateStream::new(mem, disk))
    }

    /// Scan candidates equal to one encoded logical-key range.
    #[inline]
    pub(crate) fn equal_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        ts: TrxID,
    ) -> RuntimeResult<
        SecondaryIndexCandidateStream<
            NonUniqueMemIndexCandidateStream<'a, P>,
            NonUniqueDiskTreeCandidateStream<'a, 'a>,
        >,
    > {
        let mem = self.mem.equal_scan_candidates(range, ts)?;
        let disk = self.open()?.scan_candidate_stream(range);
        Ok(SecondaryIndexCandidateStream::new(mem, disk))
    }
}

/// Incremental lookup-candidate stream over a MemIndex/DiskTree pair.
pub(crate) struct SecondaryIndexCandidateStream<M, D> {
    mem: M,
    disk: D,
    mem_buf: VecDeque<IndexLookupCandidate>,
    mem_done: bool,
    disk_buf: VecDeque<IndexLookupCandidate>,
    disk_done: bool,
}

impl<M, D> SecondaryIndexCandidateStream<M, D>
where
    M: IndexBatchStream<IndexLookupCandidate>,
    D: IndexBatchStream<IndexLookupCandidate>,
{
    #[inline]
    pub(super) fn new(mem: M, disk: D) -> Self {
        Self {
            mem,
            disk,
            mem_buf: VecDeque::new(),
            mem_done: false,
            disk_buf: VecDeque::new(),
            disk_done: false,
        }
    }

    async fn ensure_mem(&mut self) -> RuntimeResult<bool> {
        loop {
            if !self.mem_buf.is_empty() {
                return Ok(true);
            }
            if self.mem_done {
                return Ok(false);
            }
            match self.mem.next_batch().await? {
                Some(entries) => {
                    if entries.is_empty() {
                        continue;
                    }
                    self.mem_buf = VecDeque::from(entries);
                    return Ok(true);
                }
                None => {
                    self.mem_done = true;
                    return Ok(false);
                }
            }
        }
    }

    async fn ensure_disk(&mut self) -> RuntimeResult<bool> {
        loop {
            if !self.disk_buf.is_empty() {
                return Ok(true);
            }
            if self.disk_done {
                return Ok(false);
            }
            match self.disk.next_batch().await? {
                Some(entries) => {
                    if entries.is_empty() {
                        continue;
                    }
                    self.disk_buf = VecDeque::from(entries);
                    return Ok(true);
                }
                None => {
                    self.disk_done = true;
                    return Ok(false);
                }
            }
        }
    }

    #[inline]
    fn push_mem(&mut self, out: &mut Vec<IndexLookupCandidate>) {
        if let Some(mem) = self.mem_buf.pop_front() {
            out.push(mem);
        }
    }

    #[inline]
    fn push_disk(&mut self, out: &mut Vec<IndexLookupCandidate>) {
        if let Some(disk) = self.disk_buf.pop_front() {
            out.push(disk);
        }
    }

    #[inline]
    fn push_equal(&mut self, out: &mut Vec<IndexLookupCandidate>) {
        self.push_mem(out);
        let _ = self.disk_buf.pop_front();
    }
}

impl<M, D> IndexBatchStream<IndexLookupCandidate> for SecondaryIndexCandidateStream<M, D>
where
    M: IndexBatchStream<IndexLookupCandidate>,
    D: IndexBatchStream<IndexLookupCandidate>,
{
    async fn next_batch(&mut self) -> RuntimeResult<Option<Vec<IndexLookupCandidate>>> {
        let mut out = Vec::new();
        loop {
            let mem_has = self.ensure_mem().await?;
            let disk_has = self.ensure_disk().await?;
            match (mem_has, disk_has) {
                (false, false) => {
                    return Ok((!out.is_empty()).then_some(out));
                }
                (true, false) => {
                    self.push_mem(&mut out);
                }
                (false, true) => {
                    self.push_disk(&mut out);
                }
                (true, true) => {
                    let (Some(mem), Some(disk)) = (self.mem_buf.front(), self.disk_buf.front())
                    else {
                        continue;
                    };
                    match mem.encoded_key.as_bytes().cmp(disk.encoded_key.as_bytes()) {
                        Ordering::Less => {
                            self.push_mem(&mut out);
                        }
                        Ordering::Equal => {
                            self.push_equal(&mut out);
                        }
                        Ordering::Greater => {
                            self.push_disk(&mut out);
                        }
                    }
                }
            }
            if !out.is_empty()
                && ((self.mem_buf.is_empty() && !self.mem_done)
                    || (self.disk_buf.is_empty() && !self.disk_done))
            {
                return Ok(Some(out));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{
        FixedBufferPool, PoolGuard, PoolGuards, PoolRole, global_readonly_pool_scope,
        table_readonly_pool,
    };
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey, IndexSpec};
    use crate::file::build_test_fs;
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::file::table_file::{MutableTableFile, TableFile};
    use crate::index::btree::BTreeKeyEncoder;
    use crate::index::disk_tree::{
        NonUniqueDiskTree, NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedPut,
    };
    use crate::index::util::tests::drain_row_ids;
    use crate::quiescent::QuiescentBox;
    use crate::table::test_user_table_id;
    use crate::value::{ValKind, ValType};
    use std::ops::Bound::Unbounded;

    fn test_row_ids<const N: usize>(values: [u64; N]) -> Vec<RowID> {
        values.into_iter().map(RowID::new).collect()
    }

    fn metadata_with_indexes() -> Arc<TableMetadata> {
        Arc::new(
            TableMetadata::try_new(
                vec![ColumnSpec::new(
                    "c0",
                    ValKind::U32,
                    ColumnAttributes::empty(),
                )],
                vec![
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK),
                    IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty()),
                ],
            )
            .expect("valid table metadata"),
        )
    }

    async fn unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> UniqueMemIndex<FixedBufferPool> {
        let index_spec = IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::UK);
        UniqueMemIndex::new(
            pool.guard(),
            pool_guard,
            &index_spec,
            |_| ValType::new(ValKind::U32, false),
            TrxID::new(100),
        )
        .await
        .expect("unique MemIndex should be created")
    }

    async fn non_unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> NonUniqueMemIndex<FixedBufferPool> {
        let index_spec = IndexSpec::new(vec![IndexKey::new(0)], IndexAttributes::empty());
        NonUniqueMemIndex::new(
            pool.guard(),
            pool_guard,
            &index_spec,
            |_| ValType::new(ValKind::U32, false),
            TrxID::new(100),
        )
        .await
        .expect("non-unique MemIndex should be created")
    }

    struct UniqueDiskTreePut<'a> {
        key: &'a [Val],
        row_id: RowID,
    }

    struct NonUniqueDiskTreeExact<'a> {
        key: &'a [Val],
        row_id: RowID,
    }

    async fn non_unique_disk_tree_scan_rows(
        tree: &NonUniqueDiskTree<'_>,
        range: &KeyRange,
    ) -> RuntimeResult<Vec<RowID>> {
        let mut stream = tree.scan_candidate_stream(range);
        let mut rows = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            rows.extend(batch.into_iter().map(|candidate| candidate.row_id));
        }
        Ok(rows)
    }

    fn encode_unique_puts(entries: &[UniqueDiskTreePut<'_>]) -> Vec<(Vec<u8>, RowID)> {
        let encoder = BTreeKeyEncoder::new(vec![ValType::new(ValKind::U32, false)]);
        entries
            .iter()
            .map(|entry| (encoder.encode(entry.key).as_bytes().to_vec(), entry.row_id))
            .collect()
    }

    fn unique_put_refs(encoded: &[(Vec<u8>, RowID)]) -> Vec<UniqueDiskTreeEncodedPut<'_>> {
        encoded
            .iter()
            .map(|(key, row_id)| UniqueDiskTreeEncodedPut {
                key,
                row_id: *row_id,
            })
            .collect()
    }

    fn encode_non_unique_exact(entries: &[NonUniqueDiskTreeExact<'_>]) -> Vec<Vec<u8>> {
        let encoder = BTreeKeyEncoder::new(vec![
            ValType::new(ValKind::U32, false),
            ValType::new(ValKind::U64, false),
        ]);
        entries
            .iter()
            .map(|entry| {
                encoder
                    .encode_pair(entry.key, Val::from(entry.row_id))
                    .as_bytes()
                    .to_vec()
            })
            .collect()
    }

    fn non_unique_exact_refs(encoded: &[Vec<u8>]) -> Vec<NonUniqueDiskTreeEncodedExact<'_>> {
        encoded
            .iter()
            .map(|key| NonUniqueDiskTreeEncodedExact { key })
            .collect()
    }

    async fn publish_secondary_root(
        mut mutable: MutableTableFile,
        index_no: usize,
        root: BlockID,
        ts: TrxID,
    ) -> Arc<TableFile> {
        mutable
            .set_secondary_index_root(index_no, root)
            .expect("test secondary root publication should accept index number");
        let (table, old_root) = mutable
            .commit(ts, false)
            .await
            .expect("test secondary root publication should commit");
        drop(old_root);
        table
    }

    macro_rules! unique_runtime {
        ($metadata:ident, $disk_pool:ident) => {
            UniqueDiskTreeRuntime::new(
                &$metadata.idx.index_specs()[0],
                $metadata.as_ref(),
                $disk_pool.file_kind(),
                Arc::clone($disk_pool.sparse_file()),
                $disk_pool.global_pool().clone(),
            )
        };
    }

    macro_rules! non_unique_runtime {
        ($metadata:ident, $disk_pool:ident) => {
            NonUniqueDiskTreeRuntime::new(
                &$metadata.idx.index_specs()[1],
                $metadata.as_ref(),
                $disk_pool.file_kind(),
                Arc::clone($disk_pool.sparse_file()),
                $disk_pool.global_pool().clone(),
            )
        };
    }

    #[test]
    fn test_unique_dual_tree_method_semantics() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(test_user_table_id(611), Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(611), &table);
            let disk_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                disk_pool.global_pool().clone(),
            );
            let disk_runtime = unique_runtime!(metadata, disk_pool);
            let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let key4 = [Val::from(4u32)];
            let key5 = [Val::from(5u32)];
            let mut writer = disk.batch_writer(&mut mutable, TrxID::new(2));
            let puts = [
                UniqueDiskTreePut {
                    key: &key1,
                    row_id: RowID::new(10),
                },
                UniqueDiskTreePut {
                    key: &key2,
                    row_id: RowID::new(20),
                },
                UniqueDiskTreePut {
                    key: &key3,
                    row_id: RowID::new(30),
                },
                UniqueDiskTreePut {
                    key: &key4,
                    row_id: RowID::new(40),
                },
            ];
            let encoded_puts = encode_unique_puts(&puts);
            writer.batch_put_encoded(&unique_put_refs(&encoded_puts));
            let root = writer.finish().await.unwrap();
            let table = publish_secondary_root(mutable, 0, root, TrxID::new(2)).await;

            let index_pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let index_guard = (*index_pool).pool_guard();
            let mem = unique_mem_index(&index_pool, &index_guard).await;
            assert!(
                mem.bind(&index_guard)
                    .insert_if_not_exists(&key1, RowID::new(100), false, TrxID::new(3))
                    .await
                    .unwrap()
                    .is_ok()
            );
            let runtime = SecondaryDiskTreeRuntime::new(
                0,
                Arc::clone(&metadata),
                Arc::clone(&table),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let index = SecondaryIndex::Unique { mem, disk: runtime };
            let pool_guards = PoolGuards::builder()
                .push(PoolRole::Index, (*index_pool).pool_guard())
                .push(PoolRole::Disk, disk_pool.pool_guard())
                .build();
            let bound = index.bind_unique(&pool_guards, root).unwrap();

            assert_eq!(table.active_root_unchecked().secondary_index_roots[0], root);
            assert_eq!(index.disk_runtime().index_no(), 0);
            assert!(
                index
                    .unique_mem()
                    .unwrap()
                    .bind(&index_guard)
                    .lookup(&key1, TrxID::new(3))
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                bound.lookup(&key1, TrxID::new(3)).await.unwrap(),
                Some((RowID::new(100), false))
            );
            assert_eq!(
                bound.lookup(&key2, TrxID::new(3)).await.unwrap(),
                Some((RowID::new(20), false))
            );

            let mem_bound = index.unique_mem().unwrap().bind(&index_guard);
            assert!(
                mem_bound
                    .insert_if_not_exists(&key3, RowID::new(30), false, TrxID::new(4))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                mem_bound
                    .mask_as_deleted(&key3, RowID::new(30), TrxID::new(4))
                    .await
                    .unwrap()
            );
            assert_eq!(
                bound.lookup(&key3, TrxID::new(4)).await.unwrap(),
                Some((RowID::new(30), true))
            );
            assert!(matches!(
                bound
                    .insert_if_not_exists_observed(&key4, RowID::new(400), false, TrxID::new(5))
                    .await
                    .unwrap(),
                UniqueInsertAttempt::Occupied(_)
            ));
            assert!(matches!(
                bound
                    .insert_if_not_exists_observed(&key5, RowID::new(50), false, TrxID::new(5))
                    .await
                    .unwrap(),
                UniqueInsertAttempt::Inserted { merged: false }
            ));
            let observation = match bound
                .insert_if_not_exists_observed(&key2, RowID::new(200), false, TrxID::new(6))
                .await
                .unwrap()
            {
                UniqueInsertAttempt::Inserted { .. } => {
                    panic!("captured DiskTree owner must prevent direct insertion")
                }
                UniqueInsertAttempt::Occupied(observation) => observation,
            };
            assert_eq!(observation.owner_row_id(), RowID::new(20));
            assert!(!observation.deleted());
            let mem_claim_start = (*index_pool).stats();
            let disk_claim_start = disk_pool.global_stats();
            assert_eq!(
                observation
                    .replace(RowID::new(200), TrxID::new(6))
                    .await
                    .unwrap(),
                IndexCompareExchange::Ok
            );
            let mem_claim_delta = (*index_pool).stats().delta_since(mem_claim_start);
            assert_eq!(mem_claim_delta.cache_hits, 1);
            assert_eq!(mem_claim_delta.cache_misses, 0);
            let disk_claim_delta = disk_pool.global_stats().delta_since(disk_claim_start);
            assert_eq!(disk_claim_delta.cache_hits, 0);
            assert_eq!(disk_claim_delta.cache_misses, 0);

            assert!(
                mem_bound
                    .compare_delete(&key2, RowID::new(200), true, TrxID::new(7))
                    .await
                    .unwrap()
            );
            let observation = match bound
                .insert_if_not_exists_observed(&key2, RowID::new(201), false, TrxID::new(7))
                .await
                .unwrap()
            {
                UniqueInsertAttempt::Inserted { .. } => {
                    panic!("captured DiskTree owner must prevent direct insertion")
                }
                UniqueInsertAttempt::Occupied(observation) => observation,
            };
            assert!(
                mem_bound
                    .insert_if_not_exists(&key2, RowID::new(20), false, TrxID::new(7))
                    .await
                    .unwrap()
                    .is_ok()
            );
            let mem_claim_start = (*index_pool).stats();
            assert_eq!(
                observation
                    .replace(RowID::new(201), TrxID::new(7))
                    .await
                    .unwrap(),
                IndexCompareExchange::Ok
            );
            let mem_claim_delta = (*index_pool).stats().delta_since(mem_claim_start);
            assert_eq!(mem_claim_delta.cache_hits, 1);
            assert_eq!(mem_claim_delta.cache_misses, 0);

            assert!(
                mem_bound
                    .compare_delete(&key2, RowID::new(201), true, TrxID::new(8))
                    .await
                    .unwrap()
            );
            let observation = match bound
                .insert_if_not_exists_observed(&key2, RowID::new(202), false, TrxID::new(8))
                .await
                .unwrap()
            {
                UniqueInsertAttempt::Inserted { .. } => {
                    panic!("captured DiskTree owner must prevent direct insertion")
                }
                UniqueInsertAttempt::Occupied(observation) => observation,
            };
            assert!(
                mem_bound
                    .insert_if_not_exists(&key2, RowID::new(999), false, TrxID::new(8))
                    .await
                    .unwrap()
                    .is_ok()
            );
            let mem_claim_start = (*index_pool).stats();
            assert_eq!(
                observation
                    .replace(RowID::new(202), TrxID::new(8))
                    .await
                    .unwrap(),
                IndexCompareExchange::Mismatch
            );
            let mem_claim_delta = (*index_pool).stats().delta_since(mem_claim_start);
            assert_eq!(mem_claim_delta.cache_hits, 1);
            assert_eq!(mem_claim_delta.cache_misses, 0);
            assert_eq!(
                mem_bound.lookup(&key2, TrxID::new(8)).await.unwrap(),
                Some((RowID::new(999), false))
            );

            assert!(
                mem_bound
                    .compare_delete(&key2, RowID::new(999), true, TrxID::new(9))
                    .await
                    .unwrap()
            );
            let observation = match bound
                .insert_if_not_exists_observed(&key2, RowID::new(203), false, TrxID::new(9))
                .await
                .unwrap()
            {
                UniqueInsertAttempt::Inserted { .. } => {
                    panic!("captured DiskTree owner must prevent direct insertion")
                }
                UniqueInsertAttempt::Occupied(observation) => observation,
            };
            assert!(
                mem_bound
                    .insert_if_not_exists(&key2, RowID::new(20), false, TrxID::new(9))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                mem_bound
                    .mask_as_deleted(&key2, RowID::new(20), TrxID::new(9))
                    .await
                    .unwrap()
            );
            let mem_claim_start = (*index_pool).stats();
            assert_eq!(
                observation
                    .replace(RowID::new(203), TrxID::new(9))
                    .await
                    .unwrap(),
                IndexCompareExchange::Mismatch
            );
            let mem_claim_delta = (*index_pool).stats().delta_since(mem_claim_start);
            assert_eq!(mem_claim_delta.cache_hits, 1);
            assert_eq!(mem_claim_delta.cache_misses, 0);
            assert_eq!(
                mem_bound.lookup(&key2, TrxID::new(9)).await.unwrap(),
                Some((RowID::new(20), true))
            );
            assert_eq!(
                mem_bound
                    .compare_exchange(
                        &key2,
                        RowID::new(20).deleted(),
                        RowID::new(200),
                        TrxID::new(10),
                    )
                    .await
                    .unwrap(),
                IndexCompareExchange::Ok
            );
            assert!(
                bound
                    .compare_delete_mem(&key4, RowID::new(40), false, TrxID::new(7))
                    .await
                    .unwrap()
            );
            assert!(
                bound
                    .compare_delete_mem(&key4, RowID::new(41), false, TrxID::new(7))
                    .await
                    .unwrap()
            );

            let full_range = KeyRange::new(Unbounded, Unbounded);
            let mut full_stream = bound
                .index_scan_candidates(&full_range, TrxID::new(8))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut full_stream).await,
                test_row_ids([100, 200, 30, 40, 50])
            );
            let range = index.key_encoder().encode_range(&key2[..]..=&key5[..]);
            let mut stream = bound.index_scan_candidates(&range, TrxID::new(8)).unwrap();
            assert_eq!(
                drain_row_ids(&mut stream).await,
                test_row_ids([200, 30, 40, 50])
            );

            let unchanged_disk = disk_runtime.open(root, &disk_guard);
            assert_eq!(
                unchanged_disk.lookup(&key2).await.unwrap(),
                Some(RowID::new(20))
            );
            assert_eq!(
                unchanged_disk.lookup(&key3).await.unwrap(),
                Some(RowID::new(30))
            );
            assert_eq!(
                unchanged_disk.lookup(&key4).await.unwrap(),
                Some(RowID::new(40))
            );
        });
    }

    #[test]
    fn test_disk_runtime_resolves_published_root_per_open() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(test_user_table_id(614), Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(614), &table);
            let disk_guard = disk_pool.pool_guard();
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];

            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                disk_pool.global_pool().clone(),
            );
            let disk_runtime = unique_runtime!(metadata, disk_pool);
            let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
            let root_a = {
                let mut writer = disk.batch_writer(&mut mutable, TrxID::new(2));
                let puts = [UniqueDiskTreePut {
                    key: &key1,
                    row_id: RowID::new(10),
                }];
                let encoded_puts = encode_unique_puts(&puts);
                writer.batch_put_encoded(&unique_put_refs(&encoded_puts));
                writer.finish().await.unwrap()
            };
            let table = publish_secondary_root(mutable, 0, root_a, TrxID::new(2)).await;
            let runtime = SecondaryDiskTreeRuntime::new(
                0,
                Arc::clone(&metadata),
                Arc::clone(&table),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let opened_a_guard = runtime.disk_pool_guard();
            let opened_a = runtime.open_unique_at(root_a, &opened_a_guard).unwrap();
            assert_eq!(opened_a.lookup(&key1).await.unwrap(), Some(RowID::new(10)));
            assert_eq!(opened_a.lookup(&key2).await.unwrap(), None);

            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                disk_pool.global_pool().clone(),
            );
            let disk = disk_runtime.open(root_a, &disk_guard);
            let root_b = {
                let mut writer = disk.batch_writer(&mut mutable, TrxID::new(3));
                let puts = [UniqueDiskTreePut {
                    key: &key2,
                    row_id: RowID::new(20),
                }];
                let encoded_puts = encode_unique_puts(&puts);
                writer.batch_put_encoded(&unique_put_refs(&encoded_puts));
                writer.finish().await.unwrap()
            };
            assert_ne!(root_a, root_b);
            let table_after_b = publish_secondary_root(mutable, 0, root_b, TrxID::new(3)).await;
            assert_eq!(
                table_after_b.active_root_unchecked().secondary_index_roots[0],
                root_b
            );

            assert_eq!(opened_a.lookup(&key1).await.unwrap(), Some(RowID::new(10)));
            assert_eq!(opened_a.lookup(&key2).await.unwrap(), None);

            let opened_b_guard = runtime.disk_pool_guard();
            let opened_b = runtime.open_unique_at(root_b, &opened_b_guard).unwrap();
            assert_eq!(opened_b.lookup(&key1).await.unwrap(), Some(RowID::new(10)));
            assert_eq!(opened_b.lookup(&key2).await.unwrap(), Some(RowID::new(20)));
        });
    }

    #[test]
    fn test_non_unique_dual_tree_merge_and_overlay_semantics() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(test_user_table_id(612), Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(612), &table);
            let disk_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(
                &table,
                fs.background_writes(),
                disk_pool.global_pool().clone(),
            );
            let disk_runtime = non_unique_runtime!(metadata, disk_pool);
            let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let mut writer = disk.batch_writer(&mut mutable, TrxID::new(2));
            let entries = [
                NonUniqueDiskTreeExact {
                    key: &key1,
                    row_id: RowID::new(10),
                },
                NonUniqueDiskTreeExact {
                    key: &key1,
                    row_id: RowID::new(11),
                },
                NonUniqueDiskTreeExact {
                    key: &key2,
                    row_id: RowID::new(20),
                },
                NonUniqueDiskTreeExact {
                    key: &key3,
                    row_id: RowID::new(30),
                },
            ];
            let encoded_entries = encode_non_unique_exact(&entries);
            writer
                .batch_insert_encoded(&non_unique_exact_refs(&encoded_entries))
                .unwrap();
            let root = writer.finish().await.unwrap();
            let table = publish_secondary_root(mutable, 1, root, TrxID::new(2)).await;

            let index_pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let index_guard = (*index_pool).pool_guard();
            let mem = non_unique_mem_index(&index_pool, &index_guard).await;
            assert!(
                mem.bind(&index_guard)
                    .insert_if_not_exists(&key1, RowID::new(12), false, TrxID::new(3))
                    .await
                    .unwrap()
                    .is_ok()
            );
            let runtime = SecondaryDiskTreeRuntime::new(
                1,
                Arc::clone(&metadata),
                Arc::clone(&table),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let index = SecondaryIndex::NonUnique { mem, disk: runtime };
            let pool_guards = PoolGuards::builder()
                .push(PoolRole::Index, (*index_pool).pool_guard())
                .push(PoolRole::Disk, disk_pool.pool_guard())
                .build();
            let bound = index.bind_non_unique(&pool_guards, root).unwrap();

            assert_eq!(table.active_root_unchecked().secondary_index_roots[1], root);
            assert_eq!(index.disk_runtime().index_no(), 1);
            assert!(
                index
                    .non_unique_mem()
                    .unwrap()
                    .bind(&index_guard)
                    .lookup_unique(&key1, RowID::new(12), TrxID::new(3))
                    .await
                    .unwrap()
                    .is_some()
            );
            let hot_insert_start = disk_pool.global_stats();
            assert_eq!(
                bound
                    .insert_mem_if_not_exists(&key1, RowID::new(100), false, TrxID::new(4))
                    .await
                    .unwrap(),
                IndexInsert::Ok(false)
            );
            let hot_insert_delta = disk_pool.global_stats().delta_since(hot_insert_start);
            assert_eq!(hot_insert_delta.cache_hits, 0);
            assert_eq!(hot_insert_delta.cache_misses, 0);
            assert!(
                bound
                    .compare_delete_mem(&key1, RowID::new(100), true, TrxID::new(4))
                    .await
                    .unwrap()
            );
            assert_eq!(
                bound
                    .insert_mem_if_not_exists(&key1, RowID::new(10), false, TrxID::new(4))
                    .await
                    .unwrap(),
                IndexInsert::Ok(false)
            );
            assert_eq!(
                bound
                    .mask_mem_if_present(&key1, RowID::new(10), TrxID::new(4))
                    .await
                    .unwrap(),
                IndexMask::Masked
            );
            let key1_range = index.key_encoder().encode_non_unique_equal_range(&key1);
            let mut key1_stream = bound
                .equal_scan_candidates(&key1_range, TrxID::new(4))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut key1_stream).await,
                test_row_ids([10, 11, 12])
            );
            assert_eq!(
                bound
                    .lookup_unique(&key1, RowID::new(10), TrxID::new(4))
                    .await
                    .unwrap(),
                Some(false)
            );
            assert_eq!(
                bound
                    .lookup_unique(&key1, RowID::new(11), TrxID::new(4))
                    .await
                    .unwrap(),
                Some(true)
            );
            assert_eq!(
                bound
                    .lookup_unique(&key1, RowID::new(99), TrxID::new(4))
                    .await
                    .unwrap(),
                None
            );
            let non_unique_mem_bound = index.non_unique_mem().unwrap().bind(&index_guard);
            assert!(
                non_unique_mem_bound
                    .mask_as_active(&key1, RowID::new(10), TrxID::new(5))
                    .await
                    .unwrap()
            );
            let key1_range = index.key_encoder().encode_non_unique_equal_range(&key1);
            let mut key1_stream = bound
                .equal_scan_candidates(&key1_range, TrxID::new(5))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut key1_stream).await,
                test_row_ids([10, 11, 12])
            );
            assert!(
                bound
                    .insert_mem_if_not_exists(&key1, RowID::new(13), false, TrxID::new(6))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert_eq!(
                bound
                    .insert_mem_if_not_exists(&key2, RowID::new(20), false, TrxID::new(7))
                    .await
                    .unwrap(),
                IndexInsert::Ok(false)
            );
            assert_eq!(
                bound
                    .mask_mem_if_present(&key2, RowID::new(20), TrxID::new(7))
                    .await
                    .unwrap(),
                IndexMask::Masked
            );
            assert!(
                bound
                    .compare_delete_mem(&key3, RowID::new(30), false, TrxID::new(7))
                    .await
                    .unwrap()
            );

            let key1_range = index.key_encoder().encode_non_unique_equal_range(&key1);
            let mut key1_stream = bound
                .equal_scan_candidates(&key1_range, TrxID::new(8))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut key1_stream).await,
                test_row_ids([10, 11, 12, 13])
            );
            let key2_range = index.key_encoder().encode_non_unique_equal_range(&key2);
            let mut key2_stream = bound
                .equal_scan_candidates(&key2_range, TrxID::new(8))
                .unwrap();
            assert_eq!(drain_row_ids(&mut key2_stream).await, test_row_ids([20]));
            let range = index
                .key_encoder()
                .encode_non_unique_range(&key1[..]..=&key3[..]);
            let mut range_stream = bound.index_scan_candidates(&range, TrxID::new(8)).unwrap();
            assert_eq!(
                drain_row_ids(&mut range_stream).await,
                test_row_ids([10, 11, 12, 13, 20, 30])
            );

            let unchanged_disk = disk_runtime.open(root, &disk_guard);
            assert_eq!(
                non_unique_disk_tree_scan_rows(&unchanged_disk, &key1_range)
                    .await
                    .unwrap(),
                test_row_ids([10, 11])
            );
            assert_eq!(
                non_unique_disk_tree_scan_rows(&unchanged_disk, &key2_range)
                    .await
                    .unwrap(),
                test_row_ids([20])
            );
        });
    }

    #[test]
    fn test_secondary_index_batch_stream_early_drop_releases_sources() {
        smol::block_on(async {
            {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = metadata_with_indexes();
                let table = fs
                    .create_table_file(test_user_table_id(615), Arc::clone(&metadata), false)
                    .unwrap();
                let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, test_user_table_id(615), &table);
                let disk_guard = disk_pool.pool_guard();
                let mut mutable = MutableTableFile::fork(
                    &table,
                    fs.background_writes(),
                    disk_pool.global_pool().clone(),
                );
                let disk_runtime = unique_runtime!(metadata, disk_pool);
                let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
                let key1 = [Val::from(1u32)];
                let key2 = [Val::from(2u32)];
                let key3 = [Val::from(3u32)];
                let key4 = [Val::from(4u32)];
                let key5 = [Val::from(5u32)];
                let mut writer = disk.batch_writer(&mut mutable, TrxID::new(2));
                let puts = [
                    UniqueDiskTreePut {
                        key: &key1,
                        row_id: RowID::new(10),
                    },
                    UniqueDiskTreePut {
                        key: &key2,
                        row_id: RowID::new(20),
                    },
                    UniqueDiskTreePut {
                        key: &key3,
                        row_id: RowID::new(30),
                    },
                    UniqueDiskTreePut {
                        key: &key4,
                        row_id: RowID::new(40),
                    },
                ];
                let encoded_puts = encode_unique_puts(&puts);
                writer.batch_put_encoded(&unique_put_refs(&encoded_puts));
                let root = writer.finish().await.unwrap();
                let table = publish_secondary_root(mutable, 0, root, TrxID::new(2)).await;

                let index_pool = QuiescentBox::new(
                    FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
                );
                let index_guard = (*index_pool).pool_guard();
                let mem = unique_mem_index(&index_pool, &index_guard).await;
                assert!(
                    mem.bind(&index_guard)
                        .insert_if_not_exists(&key1, RowID::new(100), false, TrxID::new(3))
                        .await
                        .unwrap()
                        .is_ok()
                );
                let runtime = SecondaryDiskTreeRuntime::new(
                    0,
                    Arc::clone(&metadata),
                    Arc::clone(&table),
                    disk_pool.global_pool().clone(),
                )
                .unwrap();
                let index = SecondaryIndex::Unique { mem, disk: runtime };
                let pool_guards = PoolGuards::builder()
                    .push(PoolRole::Index, (*index_pool).pool_guard())
                    .push(PoolRole::Disk, disk_pool.pool_guard())
                    .build();
                let bound = index.bind_unique(&pool_guards, root).unwrap();

                let range = index.key_encoder().encode_range(&key1[..]..=&key4[..]);
                let mut stream = bound.index_scan_candidates(&range, TrxID::new(4)).unwrap();
                assert_eq!(
                    stream.next_batch().await.unwrap().map(|batch| {
                        batch
                            .into_iter()
                            .map(|candidate| candidate.row_id)
                            .collect::<Vec<_>>()
                    }),
                    Some(test_row_ids([100]))
                );
                drop(stream);

                assert_eq!(
                    bound.lookup(&key2, TrxID::new(5)).await.unwrap(),
                    Some((RowID::new(20), false))
                );
                assert!(matches!(
                    bound
                        .insert_if_not_exists_observed(&key5, RowID::new(50), false, TrxID::new(6))
                        .await
                        .unwrap(),
                    UniqueInsertAttempt::Inserted { merged: false }
                ));
                let fresh_range = index.key_encoder().encode_range(&key1[..]..=&key5[..]);
                let mut fresh = bound
                    .index_scan_candidates(&fresh_range, TrxID::new(7))
                    .unwrap();
                assert_eq!(
                    drain_row_ids(&mut fresh).await,
                    test_row_ids([100, 20, 30, 40, 50])
                );
            }

            {
                let (_temp_dir, fs) = build_test_fs();
                let metadata = metadata_with_indexes();
                let table = fs
                    .create_table_file(test_user_table_id(616), Arc::clone(&metadata), false)
                    .unwrap();
                let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
                drop(old_root);
                let global = global_readonly_pool_scope(64 * 1024 * 1024);
                let disk_pool = table_readonly_pool(&global, test_user_table_id(616), &table);
                let disk_guard = disk_pool.pool_guard();
                let mut mutable = MutableTableFile::fork(
                    &table,
                    fs.background_writes(),
                    disk_pool.global_pool().clone(),
                );
                let disk_runtime = non_unique_runtime!(metadata, disk_pool);
                let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
                let key1 = [Val::from(1u32)];
                let key2 = [Val::from(2u32)];
                let mut writer = disk.batch_writer(&mut mutable, TrxID::new(2));
                let entries = [
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: RowID::new(10),
                    },
                    NonUniqueDiskTreeExact {
                        key: &key1,
                        row_id: RowID::new(11),
                    },
                    NonUniqueDiskTreeExact {
                        key: &key2,
                        row_id: RowID::new(20),
                    },
                ];
                let encoded_entries = encode_non_unique_exact(&entries);
                writer
                    .batch_insert_encoded(&non_unique_exact_refs(&encoded_entries))
                    .unwrap();
                let root = writer.finish().await.unwrap();
                let table = publish_secondary_root(mutable, 1, root, TrxID::new(2)).await;

                let index_pool = QuiescentBox::new(
                    FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
                );
                let index_guard = (*index_pool).pool_guard();
                let mem = non_unique_mem_index(&index_pool, &index_guard).await;
                assert!(
                    mem.bind(&index_guard)
                        .insert_if_not_exists(&key1, RowID::new(12), false, TrxID::new(3))
                        .await
                        .unwrap()
                        .is_ok()
                );
                let runtime = SecondaryDiskTreeRuntime::new(
                    1,
                    Arc::clone(&metadata),
                    Arc::clone(&table),
                    disk_pool.global_pool().clone(),
                )
                .unwrap();
                let index = SecondaryIndex::NonUnique { mem, disk: runtime };
                let pool_guards = PoolGuards::builder()
                    .push(PoolRole::Index, (*index_pool).pool_guard())
                    .push(PoolRole::Disk, disk_pool.pool_guard())
                    .build();
                let bound = index.bind_non_unique(&pool_guards, root).unwrap();

                let range = index.key_encoder().encode_non_unique_equal_range(&key1);
                let mut stream = bound.equal_scan_candidates(&range, TrxID::new(4)).unwrap();
                assert_eq!(
                    stream.next_batch().await.unwrap().map(|batch| {
                        batch
                            .into_iter()
                            .map(|candidate| candidate.row_id)
                            .collect::<Vec<_>>()
                    }),
                    Some(test_row_ids([10, 11]))
                );
                drop(stream);

                assert!(
                    bound
                        .insert_mem_if_not_exists(&key1, RowID::new(13), false, TrxID::new(5))
                        .await
                        .unwrap()
                        .is_ok()
                );
                let fresh_range = index.key_encoder().encode_non_unique_equal_range(&key1);
                let mut fresh = bound
                    .equal_scan_candidates(&fresh_range, TrxID::new(7))
                    .unwrap();
                assert_eq!(
                    drain_row_ids(&mut fresh).await,
                    test_row_ids([10, 11, 12, 13])
                );
            }
        });
    }

    #[test]
    fn test_dual_tree_secondary_index_wrapper() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(test_user_table_id(613), Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, test_user_table_id(613), &table);
            let index_pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let index_guard = (*index_pool).pool_guard();
            let mem = unique_mem_index(&index_pool, &index_guard).await;
            let shadow_key = [Val::from(9u32)];
            let guarded = mem.bind(&index_guard);
            assert!(
                guarded
                    .insert_if_not_exists(&shadow_key, RowID::new(90), false, TrxID::new(2))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                guarded
                    .mask_as_deleted(&shadow_key, RowID::new(90), TrxID::new(2))
                    .await
                    .unwrap()
            );
            let runtime = SecondaryDiskTreeRuntime::new(
                0,
                Arc::clone(&metadata),
                Arc::clone(&table),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let composite = SecondaryIndex::Unique { mem, disk: runtime };
            assert!(composite.is_unique());
            assert_eq!(composite.index_no(), 0);

            let mem = non_unique_mem_index(&index_pool, &index_guard).await;
            let runtime = SecondaryDiskTreeRuntime::new(
                1,
                Arc::clone(&metadata),
                Arc::clone(&table),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let composite = SecondaryIndex::NonUnique { mem, disk: runtime };
            assert!(!composite.is_unique());
            assert_eq!(composite.index_no(), 1);
        });
    }
}
