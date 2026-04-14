//! Composite MemTree/DiskTree secondary-index core.
//!
//! The types in this module are the RFC 0014 phase-3 user-table core only.
//! They group the current in-memory BTree-backed secondary index with one
//! checkpointed secondary DiskTree root and preserve the existing unique and
//! non-unique trait contracts without wiring the composite into table runtime.

#![cfg_attr(not(test), allow(dead_code))]

use super::disk_tree::{NonUniqueDiskTree, UniqueDiskTree};
use super::non_unique_index::{GenericNonUniqueBTreeIndex, NonUniqueIndex, NonUniqueMemTreeEntry};
use super::secondary_index::{IndexCompareExchange, IndexInsert};
use super::unique_index::{GenericUniqueBTreeIndex, UniqueIndex, UniqueMemTreeEntry};
use crate::buffer::{BufferPool, PoolGuard, ReadonlyBufferPool};
use crate::catalog::{IndexSpec, TableMetadata};
use crate::error::{Error, FileKind, Result};
use crate::file::SparseFile;
use crate::file::cow_file::BlockID;
use crate::index::util::Maskable;
use crate::quiescent::QuiescentGuard;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::Val;
use std::cmp::Ordering;
use std::sync::Arc;

/// Immutable cold-root context for one user-table secondary DiskTree.
///
/// The context is table-specific by construction. DiskTree readers opened from
/// it always use `FileKind::TableFile`, and key encoders remain owned by the
/// concrete DiskTree readers derived from table metadata.
#[derive(Clone)]
pub(crate) struct SecondaryDiskTreeContext {
    root: BlockID,
    index_no: usize,
    metadata: Arc<TableMetadata>,
    file: Arc<SparseFile>,
    disk_pool: QuiescentGuard<ReadonlyBufferPool>,
}

impl SecondaryDiskTreeContext {
    /// Create a cold-root context for one table secondary-index root.
    #[inline]
    pub(crate) fn new(
        root: BlockID,
        index_no: usize,
        metadata: Arc<TableMetadata>,
        file: Arc<SparseFile>,
        disk_pool: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Result<Self> {
        if metadata.index_specs.get(index_no).is_none() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            root,
            index_no,
            metadata,
            file,
            disk_pool,
        })
    }

    /// Return the immutable DiskTree root snapshot held by this context.
    #[inline]
    pub(crate) fn root(&self) -> BlockID {
        self.root
    }

    /// Return the table index number represented by this context.
    #[inline]
    pub(crate) fn index_no(&self) -> usize {
        self.index_no
    }

    #[inline]
    fn index_spec(&self) -> Result<&IndexSpec> {
        self.metadata
            .index_specs
            .get(self.index_no)
            .ok_or(Error::InvalidArgument)
    }

    #[inline]
    fn open_unique<'a>(&'a self, disk_pool_guard: &'a PoolGuard) -> Result<UniqueDiskTree<'a>> {
        UniqueDiskTree::new(
            self.root,
            self.index_spec()?,
            self.metadata.as_ref(),
            FileKind::TableFile,
            &self.file,
            &self.disk_pool,
            disk_pool_guard,
        )
    }

    #[inline]
    fn open_non_unique<'a>(
        &'a self,
        disk_pool_guard: &'a PoolGuard,
    ) -> Result<NonUniqueDiskTree<'a>> {
        NonUniqueDiskTree::new(
            self.root,
            self.index_spec()?,
            self.metadata.as_ref(),
            FileKind::TableFile,
            &self.file,
            &self.disk_pool,
            disk_pool_guard,
        )
    }

    #[inline]
    async fn unique_lookup(&self, key: &[Val]) -> Result<Option<RowID>> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        let tree = self.open_unique(&disk_pool_guard)?;
        tree.lookup(key).await
    }

    #[inline]
    async fn unique_scan_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        let tree = self.open_unique(&disk_pool_guard)?;
        tree.scan_entries().await
    }

    #[inline]
    async fn non_unique_contains_exact(&self, key: &[Val], row_id: RowID) -> Result<bool> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        let tree = self.open_non_unique(&disk_pool_guard)?;
        tree.contains_exact(key, row_id).await
    }

    #[inline]
    async fn non_unique_prefix_entries(&self, key: &[Val]) -> Result<Vec<(Vec<u8>, RowID)>> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        let tree = self.open_non_unique(&disk_pool_guard)?;
        tree.prefix_scan_entries(key).await
    }

    #[inline]
    async fn non_unique_scan_entries(&self) -> Result<Vec<(Vec<u8>, RowID)>> {
        let disk_pool_guard = self.disk_pool.pool_guard();
        let tree = self.open_non_unique(&disk_pool_guard)?;
        tree.scan_entries().await
    }
}

/// Composite unique secondary index over a mutable MemTree and cold DiskTree.
pub(crate) struct DualTreeUniqueIndex<P: 'static> {
    mem: GenericUniqueBTreeIndex<P>,
    disk: SecondaryDiskTreeContext,
}

impl<P: BufferPool> DualTreeUniqueIndex<P> {
    /// Create a composite unique index from an existing MemTree and cold root.
    #[inline]
    pub(crate) fn new(
        mem: GenericUniqueBTreeIndex<P>,
        disk: SecondaryDiskTreeContext,
    ) -> Result<Self> {
        if !disk.index_spec()?.unique() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self { mem, disk })
    }

    /// Return the immutable cold-root context used by this composite.
    #[inline]
    pub(crate) fn disk(&self) -> &SecondaryDiskTreeContext {
        &self.disk
    }

    /// Return the MemTree backend used by this composite.
    #[inline]
    pub(crate) fn mem(&self) -> &GenericUniqueBTreeIndex<P> {
        &self.mem
    }
}

impl<P: BufferPool + 'static> UniqueIndex for DualTreeUniqueIndex<P> {
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
        Ok(self
            .disk
            .unique_lookup(key)
            .await?
            .map(|row_id| (row_id, false)))
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
        if let Some(cold_row_id) = self.disk.unique_lookup(key).await? {
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
        match self.disk.unique_lookup(key).await? {
            Some(cold_row_id) => Ok(cold_row_id == old_row_id),
            None => Ok(true),
        }
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
        match self.disk.unique_lookup(key).await? {
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
        let disk_entries = self.disk.unique_scan_entries().await?;
        merge_unique_entries(&mem_entries, &disk_entries, values);
        Ok(())
    }
}

/// Composite non-unique secondary index over a mutable MemTree and cold DiskTree.
pub(crate) struct DualTreeNonUniqueIndex<P: 'static> {
    mem: GenericNonUniqueBTreeIndex<P>,
    disk: SecondaryDiskTreeContext,
}

impl<P: BufferPool> DualTreeNonUniqueIndex<P> {
    /// Create a composite non-unique index from an existing MemTree and cold root.
    #[inline]
    pub(crate) fn new(
        mem: GenericNonUniqueBTreeIndex<P>,
        disk: SecondaryDiskTreeContext,
    ) -> Result<Self> {
        if disk.index_spec()?.unique() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self { mem, disk })
    }

    /// Return the immutable cold-root context used by this composite.
    #[inline]
    pub(crate) fn disk(&self) -> &SecondaryDiskTreeContext {
        &self.disk
    }

    /// Return the MemTree backend used by this composite.
    #[inline]
    pub(crate) fn mem(&self) -> &GenericNonUniqueBTreeIndex<P> {
        &self.mem
    }
}

impl<P: BufferPool + 'static> NonUniqueIndex for DualTreeNonUniqueIndex<P> {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        res: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let mem_entries = self.mem.lookup_encoded_entries(pool_guard, key).await?;
        let disk_entries = self.disk.non_unique_prefix_entries(key).await?;
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
        if self.disk.non_unique_contains_exact(key, row_id).await? {
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
        if self.disk.non_unique_contains_exact(key, row_id).await? {
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
                if self.disk.non_unique_contains_exact(key, row_id).await? {
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
        let disk_entries = self.disk.non_unique_scan_entries().await?;
        merge_non_unique_entries(&mem_entries, &disk_entries, values);
        Ok(())
    }
}

/// Composite secondary-index variant for one table secondary index.
pub(crate) enum DualTreeSecondaryIndex<P: 'static> {
    /// Composite unique secondary index.
    Unique(DualTreeUniqueIndex<P>),
    /// Composite non-unique secondary index.
    NonUnique(DualTreeNonUniqueIndex<P>),
}

impl<P: BufferPool> DualTreeSecondaryIndex<P> {
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
}

#[inline]
fn merge_unique_entries(
    mem_entries: &[UniqueMemTreeEntry],
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
    mem_entries: &[NonUniqueMemTreeEntry],
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
    entry: &NonUniqueMemTreeEntry,
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
    use crate::catalog::{ColumnAttributes, ColumnSpec, IndexAttributes, IndexKey};
    use crate::file::build_test_fs;
    use crate::file::cow_file::SUPER_BLOCK_ID;
    use crate::file::table_file::MutableTableFile;
    use crate::index::btree::BTree;
    use crate::index::btree_key::BTreeKeyEncoder;
    use crate::index::disk_tree::{NonUniqueDiskTreeExact, UniqueDiskTreePut};
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
    ) -> GenericUniqueBTreeIndex<FixedBufferPool> {
        let tree = BTree::new(pool.guard(), pool_guard, true, 100)
            .await
            .expect("unique MemTree should be created");
        GenericUniqueBTreeIndex::new(
            tree,
            BTreeKeyEncoder::new(vec![ValType::new(ValKind::U32, false)]),
        )
    }

    async fn non_unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
    ) -> GenericNonUniqueBTreeIndex<FixedBufferPool> {
        let tree = BTree::new(pool.guard(), pool_guard, true, 100)
            .await
            .expect("non-unique MemTree should be created");
        GenericNonUniqueBTreeIndex::new(
            tree,
            BTreeKeyEncoder::new(vec![
                ValType::new(ValKind::U32, false),
                ValType::new(ValKind::U64, false),
            ]),
        )
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
            let disk = UniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[0],
                &metadata,
                FileKind::TableFile,
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_guard,
            )
            .unwrap();
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let key4 = [Val::from(4u32)];
            let key5 = [Val::from(5u32)];
            let key6 = [Val::from(6u32)];
            let mut writer = disk.batch_writer(&mut mutable, 2);
            writer
                .batch_put(&[
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
                ])
                .unwrap();
            let root = writer.finish().await.unwrap();

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
            let context = SecondaryDiskTreeContext::new(
                root,
                0,
                Arc::clone(&metadata),
                Arc::clone(disk_pool.sparse_file()),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let index = DualTreeUniqueIndex::new(mem, context).unwrap();

            assert_eq!(index.disk().root(), root);
            assert_eq!(index.disk().index_no(), 0);
            assert!(
                index
                    .mem()
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
                !index
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

            let unchanged_disk = UniqueDiskTree::new(
                root,
                &metadata.index_specs[0],
                &metadata,
                FileKind::TableFile,
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_guard,
            )
            .unwrap();
            assert_eq!(unchanged_disk.lookup(&key2).await.unwrap(), Some(20));
            assert_eq!(unchanged_disk.lookup(&key3).await.unwrap(), Some(30));
            assert_eq!(unchanged_disk.lookup(&key4).await.unwrap(), Some(40));
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
            let disk = NonUniqueDiskTree::new(
                SUPER_BLOCK_ID,
                &metadata.index_specs[1],
                &metadata,
                FileKind::TableFile,
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_guard,
            )
            .unwrap();
            let key1 = [Val::from(1u32)];
            let key2 = [Val::from(2u32)];
            let key3 = [Val::from(3u32)];
            let mut writer = disk.batch_writer(&mut mutable, 2);
            writer
                .batch_insert(&[
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
                ])
                .unwrap();
            let root = writer.finish().await.unwrap();

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
            let context = SecondaryDiskTreeContext::new(
                root,
                1,
                Arc::clone(&metadata),
                Arc::clone(disk_pool.sparse_file()),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let index = DualTreeNonUniqueIndex::new(mem, context).unwrap();

            assert_eq!(index.disk().root(), root);
            assert_eq!(index.disk().index_no(), 1);
            assert!(
                index
                    .mem()
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

            let unchanged_disk = NonUniqueDiskTree::new(
                root,
                &metadata.index_specs[1],
                &metadata,
                FileKind::TableFile,
                disk_pool.sparse_file(),
                disk_pool.global_pool(),
                &disk_guard,
            )
            .unwrap();
            assert_eq!(
                unchanged_disk.prefix_scan(&key1).await.unwrap(),
                vec![10, 11]
            );
            assert_eq!(unchanged_disk.prefix_scan(&key2).await.unwrap(), vec![20]);
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
                mem.insert_delete_shadow_if_absent(&index_guard, &shadow_key, 90, 2)
                    .await
                    .unwrap()
            );
            let context = SecondaryDiskTreeContext::new(
                SUPER_BLOCK_ID,
                0,
                Arc::clone(&metadata),
                Arc::clone(disk_pool.sparse_file()),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let composite =
                DualTreeSecondaryIndex::Unique(DualTreeUniqueIndex::new(mem, context).unwrap());
            assert!(composite.is_unique());
            assert_eq!(composite.index_no(), 0);

            let mem = non_unique_mem_index(&index_pool, &index_guard).await;
            let context = SecondaryDiskTreeContext::new(
                SUPER_BLOCK_ID,
                1,
                Arc::clone(&metadata),
                Arc::clone(disk_pool.sparse_file()),
                disk_pool.global_pool().clone(),
            )
            .unwrap();
            let composite = DualTreeSecondaryIndex::NonUnique(
                DualTreeNonUniqueIndex::new(mem, context).unwrap(),
            );
            assert!(!composite.is_unique());
            assert_eq!(composite.index_no(), 1);
        });
    }
}
