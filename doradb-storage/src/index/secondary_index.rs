//! Composite MemIndex/DiskTree secondary-index core.
//!
//! The composite types group the current in-memory BTree-backed secondary
//! index with the current published secondary DiskTree root. Catalog tables use
//! the catalog-only in-memory enum in this module because they do not have a
//! cold DiskTree layer.

use super::disk_tree::{
    NonUniqueDiskTree, NonUniqueDiskTreeRuntime, UniqueDiskTree, UniqueDiskTreeRuntime,
};
use super::non_unique_index::{
    NonUniqueIndex, NonUniqueMemIndex, NonUniqueMemIndexCleanupScan, NonUniqueMemIndexEntry,
};
use super::unique_index::{
    UniqueIndex, UniqueMemIndex, UniqueMemIndexCleanupScan, UniqueMemIndexEntry,
};
use crate::buffer::{BufferPool, PoolGuard, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{Error, Result};
use crate::file::cow_file::BlockID;
use crate::file::table_file::TableFile;
use crate::index::util::Maskable;
use crate::quiescent::QuiescentGuard;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::{Val, ValType};
use std::cmp::Ordering;
use std::sync::Arc;

/// Result of attempting to insert a secondary-index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexInsert {
    /// Insert succeeded. `true` means a delete-marked entry was merged.
    Ok(bool),
    /// Insert found an existing owner, carrying row id and delete flag.
    DuplicateKey(RowID, bool),
}

impl IndexInsert {
    /// Returns whether the insert attempt succeeded.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, IndexInsert::Ok(_))
    }

    /// Returns whether the insert succeeded by merging a delete-marked entry.
    #[inline]
    pub fn is_merged(&self) -> bool {
        matches!(self, IndexInsert::Ok(true))
    }
}

/// Result of a secondary-index compare-exchange operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexCompareExchange {
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
    pub fn is_ok(self) -> bool {
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
/// The runtime is table-specific by construction. Each open reads the currently
/// published secondary root from the table file, then hands that copied root to
/// a typed DiskTree reader. Already opened readers keep their root snapshot.
///
/// This is a `runtime_checked_future` boundary. RFC-0015 should migrate normal
/// runtime opens to root ids captured from `TableRootSnapshot`.
pub(crate) struct SecondaryDiskTreeRuntime {
    index_no: usize,
    table_file: Arc<TableFile>,
    kind: SecondaryDiskTreeRuntimeKind,
}

enum SecondaryDiskTreeRuntimeKind {
    Unique(UniqueDiskTreeRuntime),
    NonUnique(NonUniqueDiskTreeRuntime),
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
        let index_spec = metadata
            .index_specs
            .get(index_no)
            .ok_or(Error::InvalidArgument)?;
        // Load-time validation only checks that the checkpoint root contains
        // this index slot. Foreground opens should later use snapshot roots.
        table_file
            .active_root()
            .secondary_index_roots
            .get(index_no)
            .ok_or(Error::InvalidArgument)?;
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
        let runtime = Self {
            index_no,
            table_file,
            kind,
        };
        Ok(runtime)
    }

    /// Return the current published DiskTree root for this secondary index.
    ///
    /// Runtime callers should treat this as the transitional unchecked root
    /// source until proof-gated table snapshots provide the root id directly.
    #[inline]
    pub(crate) fn published_root(&self) -> Result<BlockID> {
        self.table_file
            .active_root()
            .secondary_index_roots
            .get(self.index_no)
            .copied()
            .ok_or(Error::InvalidArgument)
    }

    /// Return the table index number represented by this context.
    #[inline]
    pub(crate) fn index_no(&self) -> usize {
        self.index_no
    }

    #[inline]
    pub(crate) fn is_unique(&self) -> bool {
        matches!(self.kind, SecondaryDiskTreeRuntimeKind::Unique(_))
    }

    #[inline]
    pub(crate) fn disk_pool_guard(&self) -> PoolGuard {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(runtime) => runtime.disk_pool_guard(),
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => runtime.disk_pool_guard(),
        }
    }

    #[inline]
    pub(crate) fn open_unique<'a>(
        &'a self,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<UniqueDiskTree<'a>> {
        // Future runtime callers should pass a snapshot-derived root id into
        // `open_unique_at` instead of reading the moving current root here.
        self.open_unique_at(self.published_root()?, disk_pool_guard)
    }

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
            SecondaryDiskTreeRuntimeKind::NonUnique(_) => Err(Error::InvalidArgument),
        }
    }

    #[inline]
    pub(crate) fn open_non_unique<'a>(
        &'a self,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<NonUniqueDiskTree<'a>> {
        // Future runtime callers should pass a snapshot-derived root id into
        // `open_non_unique_at` instead of reading the moving current root here.
        self.open_non_unique_at(self.published_root()?, disk_pool_guard)
    }

    #[inline]
    pub(crate) fn open_non_unique_at<'a>(
        &'a self,
        root_block_id: BlockID,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<NonUniqueDiskTree<'a>> {
        match &self.kind {
            SecondaryDiskTreeRuntimeKind::Unique(_) => Err(Error::InvalidArgument),
            SecondaryDiskTreeRuntimeKind::NonUnique(runtime) => {
                Ok(runtime.open(root_block_id, disk_pool_guard))
            }
        }
    }
}

/// Composite unique secondary index over a mutable MemIndex and cold DiskTree.
pub(crate) struct UniqueSecondaryIndex<P: 'static> {
    mem: UniqueMemIndex<P>,
    disk: SecondaryDiskTreeRuntime,
}

impl<P: BufferPool> UniqueSecondaryIndex<P> {
    /// Return the cold DiskTree runtime used by this composite.
    #[inline]
    pub(crate) fn disk(&self) -> &SecondaryDiskTreeRuntime {
        &self.disk
    }

    /// Destroy the mutable MemIndex owned by this composite index.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
        self.mem.destroy(pool_guard).await
    }

    /// Scan the mutable MemIndex entries without reading DiskTree.
    #[cfg_attr(not(test), allow(dead_code))]
    #[inline]
    pub(crate) async fn scan_mem_entries(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<Vec<UniqueMemIndexEntry>> {
        self.mem.scan_encoded_entries(pool_guard).await
    }

    /// Create a cleanup-only MemIndex scan without reading DiskTree.
    #[inline]
    pub(crate) fn cleanup_mem_scan<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
        pivot_row_id: RowID,
        clean_live_entries: bool,
    ) -> UniqueMemIndexCleanupScan<'a, P> {
        self.mem
            .cleanup_scan(pool_guard, pivot_row_id, clean_live_entries)
    }

    /// Return whether `key` encodes to the same MemIndex key bytes captured by
    /// a cleanup scan.
    #[inline]
    pub(crate) fn mem_encoded_key_matches(&self, key: &[Val], encoded_key: &[u8]) -> bool {
        self.mem.encoded_key_matches(key, encoded_key)
    }

    /// Remove one scanned MemIndex entry only if its current state still
    /// matches the scan result.
    #[inline]
    pub(crate) async fn compare_delete_mem_encoded_entry(
        &self,
        pool_guard: &PoolGuard,
        encoded_key: &[u8],
        row_id: RowID,
        deleted: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem
            .compare_delete_encoded_entry(pool_guard, encoded_key, row_id, deleted, ts)
            .await
    }

    /// Insert into the MemIndex backend during recovery without probing DiskTree.
    ///
    /// Recovery uses this for hot/post-checkpoint row-page state. Checkpointed
    /// cold state is already represented by the DiskTree root and must not be
    /// treated as a duplicate while rebuilding MemIndex.
    #[inline]
    pub(crate) async fn insert_recovery_mem_only(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        self.mem
            .insert_if_not_exists(pool_guard, key, row_id, merge_if_match_deleted, ts)
            .await
    }

    /// Compare-exchange the MemIndex backend during recovery without DiskTree access.
    #[inline]
    pub(crate) async fn compare_exchange_recovery_mem_only(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        self.mem
            .compare_exchange(pool_guard, key, old_row_id, new_row_id, ts)
            .await
    }
}

impl<P: BufferPool + 'static> UniqueIndex for UniqueSecondaryIndex<P> {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        ts: TrxID,
    ) -> Result<Option<(RowID, bool)>> {
        if let Some(hit) = self.mem.lookup(pool_guard, key, ts).await? {
            return Ok(Some(hit));
        }
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_unique(&disk_pool_guard)?;
        Ok(disk.lookup(key).await?.map(|row_id| (row_id, false)))
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        debug_assert!(!row_id.is_deleted());
        if let Some((old_row_id, deleted)) = self.mem.lookup(pool_guard, key, ts).await? {
            if merge_if_match_deleted && deleted && old_row_id == row_id {
                return self
                    .mem
                    .insert_if_not_exists(pool_guard, key, row_id, true, ts)
                    .await;
            }
            return Ok(IndexInsert::DuplicateKey(old_row_id, deleted));
        }
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_unique(&disk_pool_guard)?;
        if let Some(cold_row_id) = disk.lookup(key).await? {
            return Ok(IndexInsert::DuplicateKey(cold_row_id, false));
        }
        self.mem
            .insert_if_not_exists(pool_guard, key, row_id, false, ts)
            .await
    }

    #[inline]
    async fn compare_delete(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!old_row_id.is_deleted());
        if self.mem.lookup(pool_guard, key, ts).await?.is_some() {
            return self
                .mem
                .compare_delete(pool_guard, key, old_row_id, ignore_del_mask, ts)
                .await;
        }
        Ok(true)
    }

    #[inline]
    async fn compare_exchange(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        if self.mem.lookup(pool_guard, key, ts).await?.is_some() {
            return self
                .mem
                .compare_exchange(pool_guard, key, old_row_id, new_row_id, ts)
                .await;
        }
        // Delete-shadows are MemIndex overlays. If purge removed the shadow
        // between the caller's lookup and this compare-exchange, retry instead
        // of comparing the masked RowID against the unmasked DiskTree owner.
        if old_row_id.is_deleted() {
            return Ok(IndexCompareExchange::NotExists);
        }
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_unique(&disk_pool_guard)?;
        match disk.lookup(key).await? {
            Some(cold_row_id) if cold_row_id == old_row_id => {
                if self
                    .mem
                    .insert_overlay_if_absent(pool_guard, key, new_row_id, ts)
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
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let mem_entries = self.mem.scan_encoded_entries(pool_guard).await?;
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_unique(&disk_pool_guard)?;
        let disk_entries = disk.scan_entries().await?;
        merge_unique_entries(&mem_entries, &disk_entries, values);
        Ok(())
    }
}

/// Composite non-unique secondary index over a mutable MemIndex and cold DiskTree.
pub(crate) struct NonUniqueSecondaryIndex<P: 'static> {
    mem: NonUniqueMemIndex<P>,
    disk: SecondaryDiskTreeRuntime,
}

impl<P: BufferPool> NonUniqueSecondaryIndex<P> {
    /// Return the cold DiskTree runtime used by this composite.
    #[inline]
    pub(crate) fn disk(&self) -> &SecondaryDiskTreeRuntime {
        &self.disk
    }

    /// Destroy the mutable MemIndex owned by this composite index.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
        self.mem.destroy(pool_guard).await
    }

    /// Scan the mutable MemIndex entries without reading DiskTree.
    #[cfg_attr(not(test), allow(dead_code))]
    #[inline]
    pub(crate) async fn scan_mem_entries(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<Vec<NonUniqueMemIndexEntry>> {
        self.mem.scan_encoded_entries(pool_guard).await
    }

    /// Create a cleanup-only MemIndex scan without reading DiskTree.
    #[inline]
    pub(crate) fn cleanup_mem_scan<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
        pivot_row_id: RowID,
        clean_live_entries: bool,
    ) -> NonUniqueMemIndexCleanupScan<'a, P> {
        self.mem
            .cleanup_scan(pool_guard, pivot_row_id, clean_live_entries)
    }

    /// Return whether `key` plus `row_id` encodes to the same exact MemIndex key
    /// bytes captured by a cleanup scan.
    #[inline]
    pub(crate) fn mem_encoded_exact_key_matches(
        &self,
        key: &[Val],
        row_id: RowID,
        encoded_key: &[u8],
    ) -> bool {
        self.mem.encoded_exact_key_matches(key, row_id, encoded_key)
    }

    /// Remove one scanned MemIndex exact entry only if its current state still
    /// matches the scan result.
    #[inline]
    pub(crate) async fn compare_delete_mem_encoded_entry(
        &self,
        pool_guard: &PoolGuard,
        encoded_key: &[u8],
        deleted: bool,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem
            .compare_delete_encoded_entry(pool_guard, encoded_key, deleted, ts)
            .await
    }

    /// Insert into the MemIndex backend during recovery without probing DiskTree.
    ///
    /// Non-unique DiskTree exact entries are checkpointed cold state. Hot redo
    /// rebuilds only exact MemIndex entries and must not reject a row because a
    /// checkpointed cold exact entry exists.
    #[inline]
    pub(crate) async fn insert_recovery_mem_only(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        self.mem
            .insert_if_not_exists(pool_guard, key, row_id, merge_if_match_deleted, ts)
            .await
    }
}

impl<P: BufferPool + 'static> NonUniqueIndex for NonUniqueSecondaryIndex<P> {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        res: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let mem_entries = self.mem.lookup_encoded_entries(pool_guard, key).await?;
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_non_unique(&disk_pool_guard)?;
        let disk_entries = disk.prefix_scan_entries(key).await?;
        merge_non_unique_entries(&mem_entries, &disk_entries, res);
        Ok(())
    }

    #[inline]
    async fn lookup_unique(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<Option<bool>> {
        if let Some(mem_hit) = self.mem.lookup_unique(pool_guard, key, row_id, ts).await? {
            return Ok(Some(mem_hit));
        }
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_non_unique(&disk_pool_guard)?;
        if disk.contains_exact(key, row_id).await? {
            Ok(Some(true))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> Result<IndexInsert> {
        debug_assert!(!row_id.is_deleted());
        if let Some(active) = self.mem.lookup_unique(pool_guard, key, row_id, ts).await? {
            if merge_if_match_deleted && !active {
                return self
                    .mem
                    .insert_if_not_exists(pool_guard, key, row_id, true, ts)
                    .await;
            }
            return Ok(IndexInsert::DuplicateKey(row_id, !active));
        }
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_non_unique(&disk_pool_guard)?;
        if disk.contains_exact(key, row_id).await? {
            return Ok(IndexInsert::DuplicateKey(row_id, false));
        }
        self.mem
            .insert_if_not_exists(pool_guard, key, row_id, false, ts)
            .await
    }

    #[inline]
    async fn mask_as_deleted(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        match self.mem.lookup_unique(pool_guard, key, row_id, ts).await? {
            Some(true) => self.mem.mask_as_deleted(pool_guard, key, row_id, ts).await,
            Some(false) => Ok(false),
            None => {
                let disk_pool_guard = self.disk.disk_pool_guard();
                let disk = self.disk.open_non_unique(&disk_pool_guard)?;
                if disk.contains_exact(key, row_id).await? {
                    self.mem
                        .insert_delete_overlay_if_absent(pool_guard, key, row_id, ts)
                        .await
                } else {
                    Ok(false)
                }
            }
        }
    }

    #[inline]
    async fn mask_as_active(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        self.mem.mask_as_active(pool_guard, key, row_id, ts).await
    }

    #[inline]
    async fn compare_delete(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        if self
            .mem
            .lookup_unique(pool_guard, key, row_id, ts)
            .await?
            .is_some()
        {
            return self
                .mem
                .compare_delete(pool_guard, key, row_id, ignore_del_mask, ts)
                .await;
        }
        Ok(true)
    }

    #[inline]
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let mem_entries = self.mem.scan_encoded_entries(pool_guard).await?;
        let disk_pool_guard = self.disk.disk_pool_guard();
        let disk = self.disk.open_non_unique(&disk_pool_guard)?;
        let disk_entries = disk.scan_entries().await?;
        merge_non_unique_entries(&mem_entries, &disk_entries, values);
        Ok(())
    }
}

/// Composite secondary-index variant for one table secondary index.
pub(crate) enum SecondaryIndex<P: 'static> {
    /// Composite unique secondary index.
    Unique(UniqueSecondaryIndex<P>),
    /// Composite non-unique secondary index.
    NonUnique(NonUniqueSecondaryIndex<P>),
}

impl<P: BufferPool> SecondaryIndex<P> {
    /// Pair a freshly built unique MemIndex with its cold DiskTree runtime.
    #[inline]
    pub(crate) async fn new_unique(
        index_no: usize,
        mem: UniqueMemIndex<P>,
        disk: SecondaryDiskTreeRuntime,
        pool_guard: &PoolGuard,
    ) -> Result<Self> {
        if disk.index_no() != index_no || !disk.is_unique() {
            let _ = mem.destroy(pool_guard).await;
            return Err(Error::InvalidState);
        }
        Ok(Self::Unique(UniqueSecondaryIndex { mem, disk }))
    }

    /// Pair a freshly built non-unique MemIndex with its cold DiskTree runtime.
    #[inline]
    pub(crate) async fn new_non_unique(
        index_no: usize,
        mem: NonUniqueMemIndex<P>,
        disk: SecondaryDiskTreeRuntime,
        pool_guard: &PoolGuard,
    ) -> Result<Self> {
        if disk.index_no() != index_no || disk.is_unique() {
            let _ = mem.destroy(pool_guard).await;
            return Err(Error::InvalidState);
        }
        Ok(Self::NonUnique(NonUniqueSecondaryIndex { mem, disk }))
    }

    /// Destroy the mutable MemIndex owned by this composite index.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
        match self {
            Self::Unique(index) => index.destroy(pool_guard).await,
            Self::NonUnique(index) => index.destroy(pool_guard).await,
        }
    }

    /// Return whether this composite index enforces uniqueness.
    #[inline]
    pub(crate) fn is_unique(&self) -> bool {
        matches!(self, Self::Unique(_))
    }

    /// Return the index number from the cold context.
    #[inline]
    pub(crate) fn index_no(&self) -> usize {
        match self {
            Self::Unique(index) => index.disk().index_no(),
            Self::NonUnique(index) => index.disk().index_no(),
        }
    }

    /// Returns the unique-index view when this slot is unique.
    #[inline]
    pub(crate) fn unique(&self) -> Option<&UniqueSecondaryIndex<P>> {
        match self {
            Self::Unique(index) => Some(index),
            Self::NonUnique(_) => None,
        }
    }

    /// Returns the non-unique-index view when this slot is non-unique.
    #[inline]
    pub(crate) fn non_unique(&self) -> Option<&NonUniqueSecondaryIndex<P>> {
        match self {
            Self::Unique(_) => None,
            Self::NonUnique(index) => Some(index),
        }
    }
}

#[inline]
fn merge_unique_entries(
    mem_entries: &[UniqueMemIndexEntry],
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
                values.push(mem.scan_row_id());
                mem_idx += 1;
            }
            Ordering::Equal => {
                values.push(mem.scan_row_id());
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
        values.push(mem.scan_row_id());
    }
    for (_, row_id) in &disk_entries[disk_idx..] {
        values.push(*row_id);
    }
}

#[inline]
fn merge_non_unique_entries(
    mem_entries: &[NonUniqueMemIndexEntry],
    disk_entries: &[(Vec<u8>, RowID)],
    values: &mut Vec<RowID>,
) {
    let mut mem_idx = 0;
    let mut disk_idx = 0;
    let mut last_emitted_key: Option<Vec<u8>> = None;
    while mem_idx < mem_entries.len() && disk_idx < disk_entries.len() {
        let mem = &mem_entries[mem_idx];
        let (disk_key, disk_row_id) = &disk_entries[disk_idx];
        match mem.encoded_key.as_slice().cmp(disk_key.as_slice()) {
            Ordering::Less => {
                push_active_mem_entry(mem, values, &mut last_emitted_key);
                mem_idx += 1;
            }
            Ordering::Equal => {
                if !mem.deleted {
                    push_row_once(&mem.encoded_key, mem.row_id, values, &mut last_emitted_key);
                }
                mem_idx += 1;
                disk_idx += 1;
            }
            Ordering::Greater => {
                push_row_once(disk_key, *disk_row_id, values, &mut last_emitted_key);
                disk_idx += 1;
            }
        }
    }
    for mem in &mem_entries[mem_idx..] {
        push_active_mem_entry(mem, values, &mut last_emitted_key);
    }
    for (disk_key, row_id) in &disk_entries[disk_idx..] {
        push_row_once(disk_key, *row_id, values, &mut last_emitted_key);
    }
}

#[inline]
fn push_active_mem_entry(
    entry: &NonUniqueMemIndexEntry,
    values: &mut Vec<RowID>,
    last_emitted_key: &mut Option<Vec<u8>>,
) {
    if !entry.deleted {
        push_row_once(&entry.encoded_key, entry.row_id, values, last_emitted_key);
    }
}

#[inline]
fn push_row_once(
    encoded_key: &[u8],
    row_id: RowID,
    values: &mut Vec<RowID>,
    last_emitted_key: &mut Option<Vec<u8>>,
) {
    if last_emitted_key.as_deref() != Some(encoded_key) {
        values.push(row_id);
        *last_emitted_key = Some(encoded_key.to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{
        FixedBufferPool, PoolGuard, PoolRole, global_readonly_pool_scope, table_readonly_pool,
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
    use crate::value::{ValKind, ValType};

    fn metadata_with_indexes() -> Arc<TableMetadata> {
        Arc::new(TableMetadata::new(
            vec![ColumnSpec::new(
                "c0",
                ValKind::U32,
                ColumnAttributes::empty(),
            )],
            vec![
                IndexSpec::new("idx_unique", vec![IndexKey::new(0)], IndexAttributes::UK),
                IndexSpec::new(
                    "idx_non_unique",
                    vec![IndexKey::new(0)],
                    IndexAttributes::empty(),
                ),
            ],
        ))
    }

    async fn unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> UniqueMemIndex<FixedBufferPool> {
        let tree = BTree::new(pool.guard(), pool_guard, true, 100)
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
        let tree = BTree::new(pool.guard(), pool_guard, true, 100)
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
                &$metadata.index_specs[0],
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
                &$metadata.index_specs[1],
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
                .create_table_file(611, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 611, &table);
            let disk_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table, fs.background_writes());
            let disk_runtime = unique_runtime!(metadata, disk_pool);
            let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let key4 = [Val::from(4u32)];
            let key5 = [Val::from(5u32)];
            let key6 = [Val::from(6u32)];
            let mut writer = disk.batch_writer(&mut mutable, 2);
            let puts = [
                UniqueDiskTreePut {
                    key: &key1,
                    row_id: 10,
                },
                UniqueDiskTreePut {
                    key: &key2,
                    row_id: 20,
                },
                UniqueDiskTreePut {
                    key: &key3,
                    row_id: 30,
                },
                UniqueDiskTreePut {
                    key: &key4,
                    row_id: 40,
                },
            ];
            let encoded_puts = encode_unique_puts(&puts);
            writer
                .batch_put_encoded(&unique_put_refs(&encoded_puts))
                .unwrap();
            let root = writer.finish().await.unwrap();
            let table = publish_secondary_root(mutable, 0, root, 2).await;

            let index_pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let index_guard = (*index_pool).pool_guard();
            let mem = unique_mem_index(&index_pool, &index_guard).await;
            assert!(
                mem.insert_if_not_exists(&index_guard, &key1, 100, false, 3)
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
            let index = UniqueSecondaryIndex { mem, disk: runtime };

            assert_eq!(index.disk().published_root().unwrap(), root);
            assert_eq!(index.disk().index_no(), 0);
            assert!(
                index
                    .mem
                    .lookup(&index_guard, &key1, 3)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                index.lookup(&index_guard, &key1, 3).await.unwrap(),
                Some((100, false))
            );
            assert_eq!(
                index.lookup(&index_guard, &key2, 3).await.unwrap(),
                Some((20, false))
            );

            assert!(
                index
                    .mask_as_deleted(&index_guard, &key3, 30, 4)
                    .await
                    .unwrap()
            );
            assert_eq!(
                index.lookup(&index_guard, &key3, 4).await.unwrap(),
                Some((30, true))
            );
            assert_eq!(
                index
                    .insert_if_not_exists(&index_guard, &key4, 400, false, 5)
                    .await
                    .unwrap(),
                IndexInsert::DuplicateKey(40, false)
            );
            assert!(
                index
                    .insert_if_not_exists(&index_guard, &key5, 50, false, 5)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert_eq!(
                index
                    .compare_exchange(&index_guard, &key2, 20, 200, 6)
                    .await
                    .unwrap(),
                IndexCompareExchange::Ok
            );
            assert_eq!(
                index
                    .compare_exchange(&index_guard, &key4, 999, 9999, 6)
                    .await
                    .unwrap(),
                IndexCompareExchange::Mismatch
            );
            assert_eq!(
                index
                    .compare_exchange(&index_guard, &key4, 40.deleted(), 400, 6)
                    .await
                    .unwrap(),
                IndexCompareExchange::NotExists
            );
            assert_eq!(
                index
                    .compare_exchange(&index_guard, &key6, 60, 600, 6)
                    .await
                    .unwrap(),
                IndexCompareExchange::NotExists
            );
            assert!(
                index
                    .compare_delete(&index_guard, &key4, 40, false, 7)
                    .await
                    .unwrap()
            );
            assert!(
                index
                    .compare_delete(&index_guard, &key4, 41, false, 7)
                    .await
                    .unwrap()
            );

            let mut values = Vec::new();
            index
                .scan_values(&index_guard, &mut values, 8)
                .await
                .unwrap();
            assert_eq!(values, vec![100, 200, 30u64.deleted(), 40, 50]);

            let unchanged_disk = disk_runtime.open(root, &disk_guard);
            assert_eq!(unchanged_disk.lookup(&key2).await.unwrap(), Some(20));
            assert_eq!(unchanged_disk.lookup(&key3).await.unwrap(), Some(30));
            assert_eq!(unchanged_disk.lookup(&key4).await.unwrap(), Some(40));
        });
    }

    #[test]
    fn test_disk_runtime_resolves_published_root_per_open() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(614, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 614, &table);
            let disk_guard = disk_pool.pool_guard();
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];

            let mut mutable = MutableTableFile::fork(&table, fs.background_writes());
            let disk_runtime = unique_runtime!(metadata, disk_pool);
            let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
            let root_a = {
                let mut writer = disk.batch_writer(&mut mutable, 2);
                let puts = [UniqueDiskTreePut {
                    key: &key1,
                    row_id: 10,
                }];
                let encoded_puts = encode_unique_puts(&puts);
                writer
                    .batch_put_encoded(&unique_put_refs(&encoded_puts))
                    .unwrap();
                writer.finish().await.unwrap()
            };
            let table = publish_secondary_root(mutable, 0, root_a, 2).await;
            let runtime = SecondaryDiskTreeRuntime::new(
                0,
                Arc::clone(&metadata),
                Arc::clone(&table),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            assert_eq!(runtime.published_root().unwrap(), root_a);

            let opened_a_guard = runtime.disk_pool_guard();
            let opened_a = runtime.open_unique(&opened_a_guard).unwrap();
            assert_eq!(opened_a.lookup(&key1).await.unwrap(), Some(10));
            assert_eq!(opened_a.lookup(&key2).await.unwrap(), None);

            let mut mutable = MutableTableFile::fork(&table, fs.background_writes());
            let disk = disk_runtime.open(root_a, &disk_guard);
            let root_b = {
                let mut writer = disk.batch_writer(&mut mutable, 3);
                let puts = [UniqueDiskTreePut {
                    key: &key2,
                    row_id: 20,
                }];
                let encoded_puts = encode_unique_puts(&puts);
                writer
                    .batch_put_encoded(&unique_put_refs(&encoded_puts))
                    .unwrap();
                writer.finish().await.unwrap()
            };
            assert_ne!(root_a, root_b);
            let table_after_b = publish_secondary_root(mutable, 0, root_b, 3).await;
            assert_eq!(table_after_b.active_root().secondary_index_roots[0], root_b);
            assert_eq!(runtime.published_root().unwrap(), root_b);

            assert_eq!(opened_a.lookup(&key1).await.unwrap(), Some(10));
            assert_eq!(opened_a.lookup(&key2).await.unwrap(), None);

            let opened_b_guard = runtime.disk_pool_guard();
            let opened_b = runtime.open_unique(&opened_b_guard).unwrap();
            assert_eq!(opened_b.lookup(&key1).await.unwrap(), Some(10));
            assert_eq!(opened_b.lookup(&key2).await.unwrap(), Some(20));
        });
    }

    #[test]
    fn test_non_unique_dual_tree_merge_and_overlay_semantics() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(612, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 612, &table);
            let disk_guard = disk_pool.pool_guard();
            let mut mutable = MutableTableFile::fork(&table, fs.background_writes());
            let disk_runtime = non_unique_runtime!(metadata, disk_pool);
            let disk = disk_runtime.open(SUPER_BLOCK_ID, &disk_guard);
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let mut writer = disk.batch_writer(&mut mutable, 2);
            let entries = [
                NonUniqueDiskTreeExact {
                    key: &key1,
                    row_id: 10,
                },
                NonUniqueDiskTreeExact {
                    key: &key1,
                    row_id: 11,
                },
                NonUniqueDiskTreeExact {
                    key: &key2,
                    row_id: 20,
                },
                NonUniqueDiskTreeExact {
                    key: &key3,
                    row_id: 30,
                },
            ];
            let encoded_entries = encode_non_unique_exact(&entries);
            writer
                .batch_insert_encoded(&non_unique_exact_refs(&encoded_entries))
                .unwrap();
            let root = writer.finish().await.unwrap();
            let table = publish_secondary_root(mutable, 1, root, 2).await;

            let index_pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let index_guard = (*index_pool).pool_guard();
            let mem = non_unique_mem_index(&index_pool, &index_guard).await;
            assert!(
                mem.insert_if_not_exists(&index_guard, &key1, 12, false, 3)
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
            let index = NonUniqueSecondaryIndex { mem, disk: runtime };

            assert_eq!(index.disk().published_root().unwrap(), root);
            assert_eq!(index.disk().index_no(), 1);
            assert!(
                index
                    .mem
                    .lookup_unique(&index_guard, &key1, 12, 3)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                index
                    .mask_as_deleted(&index_guard, &key1, 10, 4)
                    .await
                    .unwrap()
            );
            let mut rows = Vec::new();
            index
                .lookup(&index_guard, &key1, &mut rows, 4)
                .await
                .unwrap();
            assert_eq!(rows, vec![11, 12]);
            assert_eq!(
                index
                    .lookup_unique(&index_guard, &key1, 10, 4)
                    .await
                    .unwrap(),
                Some(false)
            );
            assert_eq!(
                index
                    .lookup_unique(&index_guard, &key1, 11, 4)
                    .await
                    .unwrap(),
                Some(true)
            );
            assert_eq!(
                index
                    .lookup_unique(&index_guard, &key1, 99, 4)
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                index
                    .insert_if_not_exists(&index_guard, &key1, 11, false, 5)
                    .await
                    .unwrap(),
                IndexInsert::DuplicateKey(11, false)
            );
            assert!(
                index
                    .mask_as_active(&index_guard, &key1, 10, 5)
                    .await
                    .unwrap()
            );
            rows.clear();
            index
                .lookup(&index_guard, &key1, &mut rows, 5)
                .await
                .unwrap();
            assert_eq!(rows, vec![10, 11, 12]);
            assert!(
                index
                    .insert_if_not_exists(&index_guard, &key1, 13, false, 6)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&index_guard, &key2, 20, 7)
                    .await
                    .unwrap()
            );
            assert!(
                index
                    .compare_delete(&index_guard, &key3, 30, false, 7)
                    .await
                    .unwrap()
            );

            let mut values = Vec::new();
            index
                .scan_values(&index_guard, &mut values, 8)
                .await
                .unwrap();
            assert_eq!(values, vec![10, 11, 12, 13, 30]);

            let unchanged_disk = disk_runtime.open(root, &disk_guard);
            assert_eq!(
                non_unique_disk_tree_prefix_scan_rows(&unchanged_disk, &key1)
                    .await
                    .unwrap(),
                vec![10, 11]
            );
            assert_eq!(
                non_unique_disk_tree_prefix_scan_rows(&unchanged_disk, &key2)
                    .await
                    .unwrap(),
                vec![20]
            );
        });
    }

    #[test]
    fn test_dual_tree_secondary_index_wrapper() {
        smol::block_on(async {
            let (_temp_dir, fs) = build_test_fs();
            let metadata = metadata_with_indexes();
            let table = fs
                .create_table_file(613, Arc::clone(&metadata), false)
                .unwrap();
            let (table, old_root) = table.commit(1, false).await.unwrap();
            drop(old_root);
            let global = global_readonly_pool_scope(64 * 1024 * 1024);
            let disk_pool = table_readonly_pool(&global, 613, &table);
            let index_pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let index_guard = (*index_pool).pool_guard();
            let mem = unique_mem_index(&index_pool, &index_guard).await;
            let shadow_key = [Val::from(9u32)];
            assert!(
                mem.insert_overlay_if_absent(&index_guard, &shadow_key, 90u64.deleted(), 2)
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
            let composite = SecondaryIndex::Unique(UniqueSecondaryIndex { mem, disk: runtime });
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
            let composite =
                SecondaryIndex::NonUnique(NonUniqueSecondaryIndex { mem, disk: runtime });
            assert!(!composite.is_unique());
            assert_eq!(composite.index_no(), 1);
        });
    }
}
