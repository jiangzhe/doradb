//! Composite MemIndex/DiskTree secondary-index core.
//!
//! The composite types group the current in-memory BTree-backed secondary
//! index with the current published secondary DiskTree root. Catalog tables use
//! the catalog-only in-memory enum in this module because they do not have a
//! cold DiskTree layer.

use super::disk_tree::{
    DiskTreeEntry, NonUniqueDiskTree, NonUniqueDiskTreeRuntime, UniqueDiskTree,
    UniqueDiskTreeRuntime,
};
use super::index_stream::{
    NonUniqueDiskTreeEntryStream, NonUniqueMemIndexEntryStream, UniqueDiskTreeEntryStream,
    UniqueMemIndexEntryStream,
};
use super::mem_index::MemIndexEntry;
use super::non_unique_index::{GuardedNonUniqueMemIndex, NonUniqueIndex, NonUniqueMemIndex};
use super::unique_index::{GuardedUniqueMemIndex, UniqueIndex, UniqueMemIndex};
use crate::buffer::{BufferPool, PoolGuard, PoolGuards, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{Error, InternalError, Result};
use crate::file::table_file::TableFile;
use crate::id::{BlockID, RowID, TrxID};
use crate::index::IndexBatchStream;
use crate::index::util::Maskable;
use crate::quiescent::QuiescentGuard;
use crate::value::{Val, ValType};
use error_stack::Report;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::RangeBounds;
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
    ) -> Result<Self> {
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
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
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
    ) -> Result<Self> {
        let index_spec = metadata.idx.index_spec(index_no).ok_or_else(|| {
            Error::from(
                Report::new(InternalError::SecondaryIndexOutOfBounds).attach(format!(
                    "index_no={index_no}, index_slot_count={}",
                    metadata.idx.index_slot_count()
                )),
            )
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
            )?)
        } else {
            SecondaryDiskTreeRuntimeKind::NonUnique(NonUniqueDiskTreeRuntime::new(
                index_spec,
                metadata.as_ref(),
                file_kind,
                file,
                disk_pool,
            )?)
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

    /// Open the unique secondary DiskTree at one captured root block id.
    #[inline]
    pub(crate) fn open_unique_at<'a>(
        &'a self,
        root_block_id: BlockID,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<UniqueDiskTree<'a>> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => {
                Ok(runtime.open(root_block_id, disk_pool_guard))
            }
            SecondaryDiskTreeRuntimeKind::NonUnique(_) => {
                Err(Error::wrong_secondary_index_binding("unique", "non-unique"))
            }
        }
    }

    /// Open the non-unique secondary DiskTree at one captured root block id.
    #[inline]
    pub(crate) fn open_non_unique_at<'a>(
        &'a self,
        root_block_id: BlockID,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<NonUniqueDiskTree<'a>> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(_) => {
                Err(Error::wrong_secondary_index_binding("non-unique", "unique"))
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
    ) -> Result<()> {
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
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
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

    /// Return the unique MemIndex when this slot is unique.
    #[inline]
    pub(crate) fn unique_mem(&self) -> Result<&UniqueMemIndex<P>> {
        match self {
            Self::Unique { mem, .. } => Ok(mem),
            Self::NonUnique { .. } => {
                Err(Error::wrong_secondary_index_binding("unique", "non-unique"))
            }
        }
    }

    /// Return the non-unique MemIndex when this slot is non-unique.
    #[inline]
    pub(crate) fn non_unique_mem(&self) -> Result<&NonUniqueMemIndex<P>> {
        match self {
            Self::Unique { .. } => {
                Err(Error::wrong_secondary_index_binding("non-unique", "unique"))
            }
            Self::NonUnique { mem, .. } => Ok(mem),
        }
    }

    /// Bind a unique user-table secondary index to one captured DiskTree root.
    #[inline]
    pub(crate) fn bind_unique<'a, 'g>(
        &'a self,
        guards: &'g PoolGuards,
        root: BlockID,
    ) -> Result<UniqueSecondaryIndex<'a, 'g, P>> {
        match self {
            Self::Unique { mem, disk } => Ok(UniqueSecondaryIndex::new(
                mem,
                disk,
                root,
                guards.index_guard(),
                guards.disk_guard(),
            )),
            Self::NonUnique { .. } => {
                Err(Error::wrong_secondary_index_binding("unique", "non-unique"))
            }
        }
    }

    /// Bind a non-unique user-table secondary index to one captured DiskTree root.
    #[inline]
    pub(crate) fn bind_non_unique<'a, 'g>(
        &'a self,
        guards: &'g PoolGuards,
        root: BlockID,
    ) -> Result<NonUniqueSecondaryIndex<'a, 'g, P>> {
        match self {
            Self::Unique { .. } => {
                Err(Error::wrong_secondary_index_binding("non-unique", "unique"))
            }
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
#[derive(Clone, Copy)]
pub(crate) struct UniqueSecondaryIndex<'a, 'g, P: 'static> {
    mem: GuardedUniqueMemIndex<'a, 'g, P>,
    disk: &'a SecondaryDiskTreeRuntime,
    root: BlockID,
    disk_pool_guard: &'g PoolGuard,
}

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
    fn open(&self) -> Result<UniqueDiskTree<'_>> {
        self.disk.open_unique_at(self.root, self.disk_pool_guard)
    }
}

impl<P: BufferPool> UniqueIndex for UniqueSecondaryIndex<'_, '_, P> {
    type RowIdStream<'a>
        = SecondaryIndexBatchStream<UniqueMemIndexEntryStream<'a, P>, UniqueDiskTreeEntryStream<'a>>
    where
        Self: 'a;

    #[inline]
    async fn lookup(&self, key: &[Val], ts: TrxID) -> Result<Option<(RowID, bool)>> {
        if let Some(hit) = self.mem.lookup(key, ts).await? {
            return Ok(Some(hit));
        }
        let disk = self.open()?;
        Ok(disk.lookup(key).await?.map(|row_id| (row_id, false)))
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        debug_assert!(!row_id.is_deleted());
        if let Some((old_row_id, deleted)) = self.mem.lookup(key, ts).await? {
            if merge_if_match_deleted && deleted && old_row_id == row_id {
                return self.mem.insert_if_not_exists(key, row_id, true, ts).await;
            }
            return Ok(IndexInsert::DuplicateKey(old_row_id, deleted));
        }
        let disk = self.open()?;
        if let Some(cold_row_id) = disk.lookup(key).await? {
            return Ok(IndexInsert::DuplicateKey(cold_row_id, false));
        }
        self.mem.insert_if_not_exists(key, row_id, false, ts).await
    }

    #[inline]
    async fn compare_delete(
        &self,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!old_row_id.is_deleted());
        self.mem
            .compare_delete(key, old_row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        if self.mem.lookup(key, ts).await?.is_some() {
            return self
                .mem
                .compare_exchange(key, old_row_id, new_row_id, ts)
                .await;
        }
        if old_row_id.is_deleted() {
            return Ok(IndexCompareExchange::NotExists);
        }
        let disk = self.open()?;
        match disk.lookup(key).await? {
            Some(cold_row_id) if cold_row_id == old_row_id => {
                if self
                    .mem
                    .insert_overlay_if_absent(key, new_row_id, ts)
                    .await?
                {
                    Ok(IndexCompareExchange::Ok)
                } else {
                    Ok(IndexCompareExchange::Mismatch)
                }
            }
            Some(_) => Ok(IndexCompareExchange::Mismatch),
            None => Ok(IndexCompareExchange::NotExists),
        }
    }

    #[inline]
    fn scan_row_ids<'a, 'r, R>(&'a self, range: R, _ts: TrxID) -> Result<Self::RowIdStream<'a>>
    where
        R: RangeBounds<&'r [Val]> + Clone,
    {
        let mem = self.mem.scan_encoded_entry_stream(range.clone())?;
        let disk = self.open()?.scan_entry_stream(range);
        Ok(SecondaryIndexBatchStream::unique(mem, disk))
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) -> Result<()> {
        let mem_entries = self.mem.scan_encoded_entries().await?;
        let disk = self.open()?;
        let disk_entries = disk.scan_entries().await?;
        merge_unique_entries(&mem_entries, &disk_entries, values);
        Ok(())
    }
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
    fn open(&self) -> Result<NonUniqueDiskTree<'_>> {
        self.disk
            .open_non_unique_at(self.root, self.disk_pool_guard)
    }
}

impl<P: BufferPool> NonUniqueIndex for NonUniqueSecondaryIndex<'_, '_, P> {
    type RowIdStream<'a>
        = SecondaryIndexBatchStream<
        NonUniqueMemIndexEntryStream<'a, P>,
        NonUniqueDiskTreeEntryStream<'a>,
    >
    where
        Self: 'a;

    #[inline]
    async fn lookup(&self, key: &[Val], res: &mut Vec<RowID>, _ts: TrxID) -> Result<()> {
        let mem_entries = self.mem.lookup_encoded_entries(key).await?;
        let disk = self.open()?;
        let disk_entries = disk.prefix_scan_entries(key).await?;
        merge_non_unique_entries(&mem_entries, &disk_entries, res);
        Ok(())
    }

    #[inline]
    async fn lookup_unique(&self, key: &[Val], row_id: RowID, ts: TrxID) -> Result<Option<bool>> {
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

    #[inline]
    async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        debug_assert!(!row_id.is_deleted());
        if let Some(active) = self.mem.lookup_unique(key, row_id, ts).await? {
            if merge_if_match_deleted && !active {
                return self.mem.insert_if_not_exists(key, row_id, true, ts).await;
            }
            return Ok(IndexInsert::DuplicateKey(row_id, !active));
        }
        let disk = self.open()?;
        if disk.contains_exact(key, row_id).await? {
            return Ok(IndexInsert::DuplicateKey(row_id, false));
        }
        self.mem.insert_if_not_exists(key, row_id, false, ts).await
    }

    #[inline]
    async fn mask_as_deleted(&self, key: &[Val], row_id: RowID, ts: TrxID) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        match self.mem.lookup_unique(key, row_id, ts).await? {
            Some(true) => self.mem.mask_as_deleted(key, row_id, ts).await,
            Some(false) => Ok(false),
            None => {
                let disk = self.open()?;
                if disk.contains_exact(key, row_id).await? {
                    self.mem
                        .insert_delete_overlay_if_absent(key, row_id, ts)
                        .await
                } else {
                    Ok(false)
                }
            }
        }
    }

    #[inline]
    async fn mask_as_active(&self, key: &[Val], row_id: RowID, ts: TrxID) -> Result<bool> {
        self.mem.mask_as_active(key, row_id, ts).await
    }

    #[inline]
    async fn compare_delete(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        self.mem
            .compare_delete(key, row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    fn scan_row_ids<'a, 'r, R>(&'a self, range: R, _ts: TrxID) -> Result<Self::RowIdStream<'a>>
    where
        R: RangeBounds<&'r [Val]> + Clone,
    {
        let mem = self.mem.scan_encoded_entry_stream(range.clone())?;
        let disk = self.open()?.scan_entry_stream(range);
        Ok(SecondaryIndexBatchStream::non_unique(mem, disk))
    }

    #[inline]
    fn equal_scan_row_ids<'a>(&'a self, key: &[Val], _ts: TrxID) -> Result<Self::RowIdStream<'a>> {
        let mem = self.mem.equal_scan_encoded_entry_stream(key)?;
        let disk = self.open()?.equal_scan_entry_stream(key);
        Ok(SecondaryIndexBatchStream::non_unique(mem, disk))
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) -> Result<()> {
        let mem_entries = self.mem.scan_encoded_entries().await?;
        let disk = self.open()?;
        let disk_entries = disk.scan_entries().await?;
        merge_non_unique_entries(&mem_entries, &disk_entries, values);
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum SecondaryIndexBatchMode {
    Unique,
    NonUnique,
}

/// Incremental row-id stream over a MemIndex/DiskTree pair.
pub(crate) struct SecondaryIndexBatchStream<M, D> {
    mem: M,
    disk: D,
    mode: SecondaryIndexBatchMode,
    mem_buf: Vec<MemIndexEntry>,
    mem_idx: usize,
    mem_done: bool,
    disk_buf: Vec<DiskTreeEntry>,
    disk_idx: usize,
    disk_done: bool,
}

impl<M, D> SecondaryIndexBatchStream<M, D>
where
    M: IndexBatchStream<MemIndexEntry>,
    D: IndexBatchStream<DiskTreeEntry>,
{
    #[inline]
    fn unique(mem: M, disk: D) -> Self {
        Self::new(mem, disk, SecondaryIndexBatchMode::Unique)
    }

    #[inline]
    fn non_unique(mem: M, disk: D) -> Self {
        Self::new(mem, disk, SecondaryIndexBatchMode::NonUnique)
    }

    #[inline]
    fn new(mem: M, disk: D, mode: SecondaryIndexBatchMode) -> Self {
        Self {
            mem,
            disk,
            mode,
            mem_buf: Vec::new(),
            mem_idx: 0,
            mem_done: false,
            disk_buf: Vec::new(),
            disk_idx: 0,
            disk_done: false,
        }
    }

    #[inline]
    fn mem_needs_refill(&self) -> bool {
        self.mem_idx == self.mem_buf.len() && !self.mem_done
    }

    #[inline]
    fn disk_needs_refill(&self) -> bool {
        self.disk_idx == self.disk_buf.len() && !self.disk_done
    }

    async fn ensure_mem(&mut self) -> Result<bool> {
        if self.mem_idx < self.mem_buf.len() {
            return Ok(true);
        }
        if self.mem_done {
            return Ok(false);
        }
        match self.mem.next_batch().await? {
            Some(entries) => {
                self.mem_buf = entries;
                self.mem_idx = 0;
                Ok(true)
            }
            None => {
                self.mem_done = true;
                Ok(false)
            }
        }
    }

    async fn ensure_disk(&mut self) -> Result<bool> {
        if self.disk_idx < self.disk_buf.len() {
            return Ok(true);
        }
        if self.disk_done {
            return Ok(false);
        }
        match self.disk.next_batch().await? {
            Some(entries) => {
                self.disk_buf = entries;
                self.disk_idx = 0;
                Ok(true)
            }
            None => {
                self.disk_done = true;
                Ok(false)
            }
        }
    }

    #[inline]
    fn push_mem(&mut self, out: &mut Vec<RowID>) {
        match self.mode {
            SecondaryIndexBatchMode::Unique => {
                out.push(self.mem_buf[self.mem_idx].row_id);
            }
            SecondaryIndexBatchMode::NonUnique => {
                let mem = &self.mem_buf[self.mem_idx];
                if !mem.deleted {
                    out.push(mem.row_id);
                }
            }
        }
        self.mem_idx += 1;
    }

    #[inline]
    fn push_disk(&mut self, out: &mut Vec<RowID>) {
        out.push(self.disk_buf[self.disk_idx].row_id);
        self.disk_idx += 1;
    }

    #[inline]
    fn push_equal(&mut self, out: &mut Vec<RowID>) {
        self.push_mem(out);
        self.disk_idx += 1;
    }
}

impl<M, D> IndexBatchStream<RowID> for SecondaryIndexBatchStream<M, D>
where
    M: IndexBatchStream<MemIndexEntry>,
    D: IndexBatchStream<DiskTreeEntry>,
{
    async fn next_batch(&mut self) -> Result<Option<Vec<RowID>>> {
        let mut out = Vec::new();
        loop {
            if !out.is_empty() && (self.mem_needs_refill() || self.disk_needs_refill()) {
                return Ok(Some(out));
            }
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
                    let mem = &self.mem_buf[self.mem_idx];
                    let disk = &self.disk_buf[self.disk_idx];
                    match mem.encoded_key.as_slice().cmp(disk.encoded_key.as_slice()) {
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
        }
    }
}

#[inline]
fn merge_unique_entries(
    mem_entries: &[MemIndexEntry],
    disk_entries: &[(Vec<u8>, RowID)],
    values: &mut Vec<RowID>,
) {
    let mut mem_idx = 0;
    let mut disk_idx = 0;
    while mem_idx < mem_entries.len() && disk_idx < disk_entries.len() {
        let mem = &mem_entries[mem_idx];
        let (disk_key, disk_row_id) = &disk_entries[disk_idx];
        match mem.encoded_key.as_slice().cmp(disk_key.as_slice()) {
            Ordering::Less => {
                values.push(if mem.deleted {
                    mem.row_id.deleted()
                } else {
                    mem.row_id
                });
                mem_idx += 1;
            }
            Ordering::Equal => {
                values.push(if mem.deleted {
                    mem.row_id.deleted()
                } else {
                    mem.row_id
                });
                mem_idx += 1;
                disk_idx += 1;
            }
            Ordering::Greater => {
                values.push(*disk_row_id);
                disk_idx += 1;
            }
        }
    }
    for mem in &mem_entries[mem_idx..] {
        values.push(if mem.deleted {
            mem.row_id.deleted()
        } else {
            mem.row_id
        });
    }
    for (_, row_id) in &disk_entries[disk_idx..] {
        values.push(*row_id);
    }
}

#[inline]
fn merge_non_unique_entries(
    mem_entries: &[MemIndexEntry],
    disk_entries: &[(Vec<u8>, RowID)],
    values: &mut Vec<RowID>,
) {
    let mut mem_idx = 0;
    let mut disk_idx = 0;
    // Both inputs are sorted and exact-key unique, so a hot/cold duplicate can
    // only appear when the two current heads compare equal.
    while mem_idx < mem_entries.len() && disk_idx < disk_entries.len() {
        let mem = &mem_entries[mem_idx];
        let (disk_key, disk_row_id) = &disk_entries[disk_idx];
        match mem.encoded_key.as_slice().cmp(disk_key.as_slice()) {
            Ordering::Less => {
                if !mem.deleted {
                    values.push(mem.row_id);
                }
                mem_idx += 1;
            }
            Ordering::Equal => {
                if !mem.deleted {
                    values.push(mem.row_id);
                }
                mem_idx += 1;
                disk_idx += 1;
            }
            Ordering::Greater => {
                values.push(*disk_row_id);
                disk_idx += 1;
            }
        }
    }
    for mem in &mem_entries[mem_idx..] {
        if !mem.deleted {
            values.push(mem.row_id);
        }
    }
    for (_, row_id) in &disk_entries[disk_idx..] {
        values.push(*row_id);
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
    use crate::index::btree::BTree;
    use crate::index::btree::BTreeKeyEncoder;
    use crate::index::disk_tree::{
        NonUniqueDiskTree, NonUniqueDiskTreeEncodedExact, UniqueDiskTreeEncodedPut,
    };
    use crate::quiescent::QuiescentBox;
    use crate::table::test_user_table_id;
    use crate::value::{ValKind, ValType};

    fn test_row_ids<const N: usize>(values: [u64; N]) -> Vec<RowID> {
        values.into_iter().map(RowID::new).collect()
    }

    async fn drain_row_ids<S: IndexBatchStream<RowID>>(stream: &mut S) -> Vec<RowID> {
        let mut row_ids = Vec::new();
        while let Some(batch) = stream.next_batch().await.unwrap() {
            row_ids.extend(batch);
        }
        row_ids
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
        let tree = BTree::new(pool.guard(), pool_guard, true, TrxID::new(100))
            .await
            .expect("unique MemIndex should be created");
        UniqueMemIndex::with_encoder(
            tree,
            BTreeKeyEncoder::new(vec![ValType::new(ValKind::U32, false)]),
        )
    }

    async fn non_unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> NonUniqueMemIndex<FixedBufferPool> {
        let tree = BTree::new(pool.guard(), pool_guard, true, TrxID::new(100))
            .await
            .expect("non-unique MemIndex should be created");
        NonUniqueMemIndex::with_encoder(
            tree,
            BTreeKeyEncoder::new(vec![
                ValType::new(ValKind::U32, false),
                ValType::new(ValKind::U64, false),
            ]),
        )
    }

    struct UniqueDiskTreePut<'a> {
        key: &'a [Val],
        row_id: RowID,
    }

    struct NonUniqueDiskTreeExact<'a> {
        key: &'a [Val],
        row_id: RowID,
    }

    async fn non_unique_disk_tree_prefix_scan_rows(
        tree: &NonUniqueDiskTree<'_>,
        key: &[Val],
    ) -> Result<Vec<RowID>> {
        Ok(tree
            .prefix_scan_entries(key)
            .await?
            .into_iter()
            .map(|(_, row_id)| row_id)
            .collect())
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
            .unwrap()
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
            .unwrap()
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
            let key6 = [Val::from(6u32)];
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
            writer
                .batch_put_encoded(&unique_put_refs(&encoded_puts))
                .unwrap();
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

            assert!(
                bound
                    .mask_as_deleted(&key3, RowID::new(30), TrxID::new(4))
                    .await
                    .unwrap()
            );
            assert_eq!(
                bound.lookup(&key3, TrxID::new(4)).await.unwrap(),
                Some((RowID::new(30), true))
            );
            assert_eq!(
                bound
                    .insert_if_not_exists(&key4, RowID::new(400), false, TrxID::new(5))
                    .await
                    .unwrap(),
                IndexInsert::DuplicateKey(RowID::new(40), false)
            );
            assert!(
                bound
                    .insert_if_not_exists(&key5, RowID::new(50), false, TrxID::new(5))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert_eq!(
                bound
                    .compare_exchange(&key2, RowID::new(20), RowID::new(200), TrxID::new(6))
                    .await
                    .unwrap(),
                IndexCompareExchange::Ok
            );
            assert_eq!(
                bound
                    .compare_exchange(&key4, RowID::new(999), RowID::new(9999), TrxID::new(6))
                    .await
                    .unwrap(),
                IndexCompareExchange::Mismatch
            );
            assert_eq!(
                bound
                    .compare_exchange(
                        &key4,
                        RowID::new(40).deleted(),
                        RowID::new(400),
                        TrxID::new(6)
                    )
                    .await
                    .unwrap(),
                IndexCompareExchange::NotExists
            );
            assert_eq!(
                bound
                    .compare_exchange(&key6, RowID::new(60), RowID::new(600), TrxID::new(6))
                    .await
                    .unwrap(),
                IndexCompareExchange::NotExists
            );
            assert!(
                bound
                    .compare_delete(&key4, RowID::new(40), false, TrxID::new(7))
                    .await
                    .unwrap()
            );
            assert!(
                bound
                    .compare_delete(&key4, RowID::new(41), false, TrxID::new(7))
                    .await
                    .unwrap()
            );

            let mut values = Vec::new();
            bound.scan_values(&mut values, TrxID::new(8)).await.unwrap();
            assert_eq!(
                values,
                vec![
                    RowID::new(100),
                    RowID::new(200),
                    RowID::new(30).deleted(),
                    RowID::new(40),
                    RowID::new(50)
                ]
            );
            let mut stream = bound
                .scan_row_ids(&key2[..]..=&key5[..], TrxID::new(8))
                .unwrap();
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
                writer
                    .batch_put_encoded(&unique_put_refs(&encoded_puts))
                    .unwrap();
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
                writer
                    .batch_put_encoded(&unique_put_refs(&encoded_puts))
                    .unwrap();
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
            assert!(
                bound
                    .mask_as_deleted(&key1, RowID::new(10), TrxID::new(4))
                    .await
                    .unwrap()
            );
            let mut rows = Vec::new();
            bound.lookup(&key1, &mut rows, TrxID::new(4)).await.unwrap();
            assert_eq!(rows, test_row_ids([11, 12]));
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
            assert_eq!(
                bound
                    .insert_if_not_exists(&key1, RowID::new(11), false, TrxID::new(5))
                    .await
                    .unwrap(),
                IndexInsert::DuplicateKey(RowID::new(11), false)
            );
            assert!(
                bound
                    .mask_as_active(&key1, RowID::new(10), TrxID::new(5))
                    .await
                    .unwrap()
            );
            rows.clear();
            bound.lookup(&key1, &mut rows, TrxID::new(5)).await.unwrap();
            assert_eq!(rows, test_row_ids([10, 11, 12]));
            assert!(
                bound
                    .insert_if_not_exists(&key1, RowID::new(13), false, TrxID::new(6))
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                bound
                    .mask_as_deleted(&key2, RowID::new(20), TrxID::new(7))
                    .await
                    .unwrap()
            );
            assert!(
                bound
                    .compare_delete(&key3, RowID::new(30), false, TrxID::new(7))
                    .await
                    .unwrap()
            );

            let mut values = Vec::new();
            bound.scan_values(&mut values, TrxID::new(8)).await.unwrap();
            assert_eq!(values, test_row_ids([10, 11, 12, 13, 30]));
            let mut key1_stream = bound.equal_scan_row_ids(&key1, TrxID::new(8)).unwrap();
            assert_eq!(
                drain_row_ids(&mut key1_stream).await,
                test_row_ids([10, 11, 12, 13])
            );
            let mut key2_stream = bound.equal_scan_row_ids(&key2, TrxID::new(8)).unwrap();
            assert!(drain_row_ids(&mut key2_stream).await.is_empty());
            let mut range_stream = bound
                .scan_row_ids(&key1[..]..=&key3[..], TrxID::new(8))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut range_stream).await,
                test_row_ids([10, 11, 12, 13, 30])
            );

            let unchanged_disk = disk_runtime.open(root, &disk_guard);
            assert_eq!(
                non_unique_disk_tree_prefix_scan_rows(&unchanged_disk, &key1)
                    .await
                    .unwrap(),
                test_row_ids([10, 11])
            );
            assert_eq!(
                non_unique_disk_tree_prefix_scan_rows(&unchanged_disk, &key2)
                    .await
                    .unwrap(),
                test_row_ids([20])
            );
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
            assert!(
                mem.insert_overlay_if_absent(
                    &index_guard,
                    &shadow_key,
                    RowID::new(90).deleted(),
                    TrxID::new(2)
                )
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
