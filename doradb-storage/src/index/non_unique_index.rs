use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, PoolGuard};
use crate::catalog::IndexSpec;
use crate::error::RuntimeResult;
use crate::id::{RowID, TrxID};
#[cfg(test)]
use crate::index::btree::BTreeKeyEncoder;
use crate::index::btree::BTreeSlotCallback;
#[cfg(test)]
use crate::index::btree::GenericBTree;
use crate::index::btree::{BTREE_BYTE_ZERO, BTreeByte, BTreeU64};
use crate::index::btree::{BTreeDelete, BTreeInsert, BTreeUpdate};
use crate::index::btree::{BTreeNode, BTreeSlot};
use crate::index::index_stream::NonUniqueMemIndexCandidateStream;
use crate::index::mem_index::{
    MemIndex, MemIndexCleanupScan, MemIndexEntry, NonUniqueMemIndexCleanupSpec,
    NonUniqueMemIndexEntryScanSpec, push_non_unique_encoded_entry,
};
use crate::index::util::Maskable;
use crate::index::{IndexBatchStream, IndexInsert, IndexLookupCandidate, KeyRange};
use crate::quiescent::QuiescentGuard;
use crate::value::{Val, ValKind, ValType};
use std::future::Future;
use std::mem;
use std::ops::Deref;

/// Abstraction of non-unique index.
pub(crate) trait NonUniqueIndex: Send + Sync {
    /// Candidate stream returned by bounded non-unique-index scans.
    type LookupCandidateStream<'a>: IndexBatchStream<IndexLookupCandidate> + 'a
    where
        Self: 'a;

    /// Lookup key and put associated values into given collection.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "legacy eager non-unique lookup")
    )]
    fn lookup(
        &self,
        key: &[Val],
        res: &mut Vec<RowID>,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<()>>;

    /// Lookup key and value(row_id) uniquely.
    /// Return None if not found.
    /// Return Some(true) if found and exist(not masked as deleted).
    fn lookup_unique(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<Option<bool>>>;

    /// Insert key value pair into the index.
    /// Value is always RowID.
    fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<IndexInsert>>;

    /// Mask a given key value as deleted.
    fn mask_as_deleted(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<bool>>;

    /// Mask a given key value as not deleted.
    fn mask_as_active(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<bool>>;

    /// Delete key value pair from the index.
    /// Value is always RowID.
    fn compare_delete(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<bool>>;

    /// Scan values into given collection.
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved scan_values"))]
    fn scan_values(
        &self,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<()>>;

    /// Scan lookup candidates over a bounded encoded-key range.
    ///
    /// Returned entries are index-derived candidates for MVCC lookup, not
    /// visibility-confirmed rows. Non-unique candidates carry encoded exact
    /// `(logical_key, row_id)` keys. Delete-marked in-memory entries are still
    /// candidates; row-version and column-deletion-buffer checks decide whether
    /// a candidate is visible.
    fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        ts: TrxID,
    ) -> RuntimeResult<Self::LookupCandidateStream<'a>>;

    /// Scan lookup candidates equal to one encoded secondary-key range.
    fn equal_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        ts: TrxID,
    ) -> RuntimeResult<Self::LookupCandidateStream<'a>>;
}

/// Generic non-unique-index implementation backed by a generic B-Tree.
pub(crate) struct NonUniqueMemIndex<P: 'static>(MemIndex<P>);

impl<P: 'static> Deref for NonUniqueMemIndex<P> {
    type Target = MemIndex<P>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: BufferPool> NonUniqueMemIndex<P> {
    /// Build a non-unique MemIndex from catalog index metadata.
    #[inline]
    pub(crate) async fn new<F: Fn(usize) -> ValType>(
        index_pool: QuiescentGuard<P>,
        index_pool_guard: &PoolGuard,
        index_spec: &IndexSpec,
        ty_infer: F,
        ts: TrxID,
    ) -> RuntimeResult<Self> {
        debug_assert!(!index_spec.unique());
        debug_assert!(!index_spec.cols.is_empty());
        let mut types: Vec<_> = index_spec
            .cols
            .iter()
            .map(|key| ty_infer(key.col_no as usize))
            .collect();
        // Non-unique MemIndex keys include RowID so every BTree key is unique.
        types.push(ValType::new(ValKind::U64, false));
        Ok(Self(
            MemIndex::new_with_types(index_pool, index_pool_guard, types, ts).await?,
        ))
    }

    /// Wrap an already-created BTree with a key encoder.
    #[inline]
    #[cfg(test)]
    pub(crate) fn with_encoder(tree: GenericBTree<P>, encoder: BTreeKeyEncoder) -> Self {
        Self(MemIndex::with_encoder(tree, encoder))
    }

    /// Bind this index to one pool guard for trait-based operations.
    #[inline]
    pub(crate) fn bind<'g>(
        &self,
        pool_guard: &'g PoolGuard,
    ) -> GuardedNonUniqueMemIndex<'_, 'g, P> {
        GuardedNonUniqueMemIndex {
            index: self,
            pool_guard,
        }
    }

    /// Destroy this non-unique index and reclaim all backing tree pages.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> RuntimeResult<()> {
        self.0.destroy(pool_guard).await
    }

    /// Insert a delete-marked exact overlay when the exact key is absent.
    ///
    /// This helper is intentionally concrete to the BTree-backed MemIndex so the
    /// dual-tree composite can shadow cold DiskTree exact entries without
    /// widening the public `NonUniqueIndex` trait.
    #[inline]
    pub(crate) async fn insert_delete_overlay_if_absent(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        let key = self.encoder().encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .tree()
                .insert::<BTreeByte>(
                    pool_guard,
                    key.as_bytes(),
                    BTREE_BYTE_ZERO.deleted(),
                    false,
                    ts,
                )
                .await?
            {
                BTreeInsert::Ok(_) => true,
                BTreeInsert::DuplicateKey(_) => false,
            },
        )
    }

    /// Scan exact MemIndex entries for one logical key with encoded keys.
    ///
    /// The returned entries are ordered by encoded exact key and include
    /// delete-marked overlays so the composite can suppress matching DiskTree
    /// exact entries.
    #[inline]
    pub(crate) async fn lookup_encoded_entries(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
    ) -> RuntimeResult<Vec<MemIndexEntry>> {
        let key = self
            .encoder()
            .encode_prefix(key, Some(mem::size_of::<RowID>()));
        let mut entries = Vec::new();
        let mut scanner = self
            .tree()
            .prefix_scanner(pool_guard, CollectEncodedExactEntries(&mut entries));
        scanner.scan_prefix(key.as_bytes()).await?;
        Ok(entries)
    }

    /// Scan all exact MemIndex entries with encoded keys and delete state.
    ///
    /// The returned entries are ordered by encoded exact key because they are
    /// produced by the underlying BTree leaf cursor.
    #[inline]
    pub(crate) async fn scan_encoded_entries(
        &self,
        pool_guard: &PoolGuard,
    ) -> RuntimeResult<Vec<MemIndexEntry>> {
        self.0
            .scan_encoded_entries::<NonUniqueMemIndexEntryScanSpec>(pool_guard)
            .await
    }

    /// Create a leaf-bounded scanner for cleanup candidates.
    #[inline]
    pub(crate) fn cleanup_scan<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
        pivot_row_id: RowID,
        clean_live_entries: bool,
    ) -> MemIndexCleanupScan<'a, P, NonUniqueMemIndexCleanupSpec> {
        self.0.cleanup_scan::<NonUniqueMemIndexCleanupSpec>(
            pool_guard,
            pivot_row_id,
            clean_live_entries,
        )
    }

    /// Return whether `key` plus `row_id` encodes to the exact MemIndex key
    /// bytes captured by a cleanup scan.
    #[inline]
    pub(crate) fn encoded_exact_key_matches(
        &self,
        key: &[Val],
        row_id: RowID,
        encoded_key: &[u8],
    ) -> bool {
        self.encoder()
            .encode_pair(key, Val::from(row_id))
            .as_bytes()
            == encoded_key
    }

    /// Remove an encoded exact MemIndex entry only when its delete state still
    /// matches the previously scanned entry.
    ///
    /// Full-scan cleanup works on encoded exact keys so it can avoid reversing
    /// physical BTree keys back into logical `SelectKey` values.
    #[inline]
    pub(crate) async fn compare_delete_encoded_entry(
        &self,
        pool_guard: &PoolGuard,
        encoded_key: &[u8],
        deleted: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        Ok(matches!(
            self.tree()
                .delete_exact(pool_guard, encoded_key, BTREE_BYTE_ZERO, deleted, ts)
                .await?,
            BTreeDelete::Ok
        ))
    }
}

/// Non-unique MemIndex view bound to one index-pool guard.
#[derive(Clone, Copy)]
pub(crate) struct GuardedNonUniqueMemIndex<'a, 'g, P: 'static> {
    index: &'a NonUniqueMemIndex<P>,
    pool_guard: &'g PoolGuard,
}

impl<P: BufferPool> GuardedNonUniqueMemIndex<'_, '_, P> {
    /// Scan exact MemIndex entries for one logical key with encoded keys.
    #[inline]
    pub(crate) async fn lookup_encoded_entries(
        &self,
        key: &[Val],
    ) -> RuntimeResult<Vec<MemIndexEntry>> {
        self.index
            .lookup_encoded_entries(self.pool_guard, key)
            .await
    }

    /// Insert a delete-marked exact overlay when the exact key is absent.
    #[inline]
    pub(crate) async fn insert_delete_overlay_if_absent(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.index
            .insert_delete_overlay_if_absent(self.pool_guard, key, row_id, ts)
            .await
    }

    /// Scan all exact MemIndex entries with encoded keys and delete state.
    #[inline]
    pub(crate) async fn scan_encoded_entries(&self) -> RuntimeResult<Vec<MemIndexEntry>> {
        self.index.scan_encoded_entries(self.pool_guard).await
    }
}

impl<P: BufferPool> NonUniqueIndex for GuardedNonUniqueMemIndex<'_, '_, P> {
    type LookupCandidateStream<'a>
        = NonUniqueMemIndexCandidateStream<'a, P>
    where
        Self: 'a;

    #[inline]
    async fn lookup(&self, key: &[Val], res: &mut Vec<RowID>, _ts: TrxID) -> RuntimeResult<()> {
        let k = self
            .index
            .encoder()
            .encode_prefix(key, Some(mem::size_of::<RowID>()));
        let mut scanner = self
            .index
            .tree()
            .prefix_scanner(self.pool_guard, CollectRowID(res));
        scanner.scan_prefix(k.as_bytes()).await
    }

    #[inline]
    async fn lookup_unique(
        &self,
        key: &[Val],
        row_id: RowID,
        _ts: TrxID,
    ) -> RuntimeResult<Option<bool>> {
        let k = self.index.encoder().encode_pair(key, Val::from(row_id));
        Ok(self
            .index
            .tree()
            .lookup_optimistic::<BTreeByte>(self.pool_guard, k.as_bytes())
            .await?
            .map(|v| !v.is_deleted()))
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> RuntimeResult<IndexInsert> {
        debug_assert!(!row_id.is_deleted());
        let k = self.index.encoder().encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .index
                .tree()
                .insert::<BTreeByte>(
                    self.pool_guard,
                    k.as_bytes(),
                    BTREE_BYTE_ZERO,
                    merge_if_match_deleted,
                    ts,
                )
                .await?
            {
                BTreeInsert::Ok(merged) => IndexInsert::Ok(merged),
                BTreeInsert::DuplicateKey(v) => IndexInsert::DuplicateKey(row_id, v.is_deleted()),
            },
        )
    }

    #[inline]
    async fn mask_as_deleted(&self, key: &[Val], row_id: RowID, ts: TrxID) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.index.encoder().encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .index
                .tree()
                .update(
                    self.pool_guard,
                    k.as_bytes(),
                    BTREE_BYTE_ZERO,
                    BTREE_BYTE_ZERO.deleted(),
                    ts,
                )
                .await?
            {
                BTreeUpdate::Ok(_) => true,
                BTreeUpdate::NotFound | BTreeUpdate::ValueMismatch(_) => false,
            },
        )
    }

    #[inline]
    async fn mask_as_active(&self, key: &[Val], row_id: RowID, ts: TrxID) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.index.encoder().encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .index
                .tree()
                .update(
                    self.pool_guard,
                    k.as_bytes(),
                    BTREE_BYTE_ZERO.deleted(),
                    BTREE_BYTE_ZERO,
                    ts,
                )
                .await?
            {
                BTreeUpdate::Ok(_) => true,
                BTreeUpdate::NotFound | BTreeUpdate::ValueMismatch(_) => false,
            },
        )
    }

    #[inline]
    async fn compare_delete(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.index.encoder().encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .index
                .tree()
                .delete(
                    self.pool_guard,
                    k.as_bytes(),
                    BTREE_BYTE_ZERO,
                    ignore_del_mask,
                    ts,
                )
                .await?
            {
                BTreeDelete::Ok | BTreeDelete::NotFound => true,
                BTreeDelete::ValueMismatch => false,
            },
        )
    }

    #[inline]
    fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        _ts: TrxID,
    ) -> RuntimeResult<Self::LookupCandidateStream<'a>> {
        Ok(NonUniqueMemIndexCandidateStream::new(
            self.index.tree().cursor(self.pool_guard, 0),
            range,
        ))
    }

    #[inline]
    fn equal_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        _ts: TrxID,
    ) -> RuntimeResult<Self::LookupCandidateStream<'a>> {
        Ok(NonUniqueMemIndexCandidateStream::new(
            self.index.tree().cursor(self.pool_guard, 0),
            range,
        ))
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) -> RuntimeResult<()> {
        let mut cursor = self.index.tree().cursor(self.pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(g) = cursor.next().await? {
            let node = g.page();
            for slot in node.slots() {
                let value = node.unpack_value::<BTreeU64>(slot);
                values.push(value.to_row_id());
            }
        }
        Ok(())
    }
}

struct CollectRowID<'a>(&'a mut Vec<RowID>);

impl BTreeSlotCallback for CollectRowID<'_> {
    #[inline]
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> RuntimeResult<bool> {
        // todo: with covering index support, we may skip deleted entries.
        let value = node.unpack_value::<BTreeU64>(slot);
        // The result collection may contain row ids from deleted entries.
        self.0.push(value.to_row_id());
        Ok(true)
    }
}

struct CollectEncodedExactEntries<'a>(&'a mut Vec<MemIndexEntry>);

impl BTreeSlotCallback for CollectEncodedExactEntries<'_> {
    #[inline]
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> RuntimeResult<bool> {
        // In-memory BTree slot data has already been validated by the scanner.
        push_non_unique_encoded_entry(node, slot, self.0)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{FixedBufferPool, PoolRole};
    use crate::error::InternalError;
    use crate::index::btree::BTree;
    use crate::index::util::tests::drain_row_ids;
    use crate::quiescent::QuiescentBox;
    use crate::value::{ValKind, ValType};

    #[test]
    fn test_non_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                // i32 column and row id
                let index = NonUniqueMemIndex::with_encoder(
                    BTree::new(pool.guard(), &pool_guard, true, TrxID::new(100))
                        .await
                        .expect("test btree construction should succeed"),
                    BTreeKeyEncoder::new(vec![
                        ValType {
                            kind: ValKind::I32,
                            nullable: false,
                        },
                        ValType {
                            kind: ValKind::U64,
                            nullable: false,
                        },
                    ]),
                );
                run_test_suit_for_non_unique_index(index.bind(&pool_guard)).await;
            }
        })
    }

    #[test]
    fn test_non_unique_mem_index_row_id_stream_prefix_and_range() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = NonUniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, true, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![
                    ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    },
                    ValType {
                        kind: ValKind::U64,
                        nullable: false,
                    },
                ]),
            );
            let guarded = index.bind(&pool_guard);
            let key1 = [Val::from(1i32)];
            let key2 = [Val::from(2i32)];
            let key3 = [Val::from(3i32)];
            for (key, row_id) in [
                (&key1[..], RowID::new(10)),
                (&key1[..], RowID::new(11)),
                (&key2[..], RowID::new(20)),
                (&key3[..], RowID::new(30)),
            ] {
                assert!(
                    guarded
                        .insert_if_not_exists(key, row_id, false, TrxID::new(100))
                        .await
                        .unwrap()
                        .is_ok()
                );
            }
            assert!(
                guarded
                    .mask_as_deleted(&key1, RowID::new(11), TrxID::new(101))
                    .await
                    .unwrap()
            );

            let equal_range = index.encoder().encode_non_unique_equal_range(&key1);
            let mut equal = guarded
                .equal_scan_candidates(&equal_range, TrxID::new(102))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut equal).await,
                vec![RowID::new(10), RowID::new(11)]
            );

            let scan_range = index
                .encoder()
                .encode_non_unique_range(&key1[..]..&key3[..]);
            let mut range = guarded
                .index_scan_candidates(&scan_range, TrxID::new(103))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut range).await,
                vec![RowID::new(10), RowID::new(11), RowID::new(20)]
            );
        })
    }

    #[test]
    fn test_lookup_encoded_entries_propagates_malformed_exact_key() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = NonUniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, true, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![
                    ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    },
                    ValType {
                        kind: ValKind::U64,
                        nullable: false,
                    },
                ]),
            );

            let key = vec![Val::from(42i32)];
            let malformed_exact_key = index
                .encoder()
                .encode_prefix(&key, Some(mem::size_of::<RowID>()));
            assert!(malformed_exact_key.as_bytes().len() < mem::size_of::<RowID>());
            index
                .tree()
                .insert::<BTreeByte>(
                    &pool_guard,
                    malformed_exact_key.as_bytes(),
                    BTREE_BYTE_ZERO,
                    false,
                    TrxID::new(100),
                )
                .await
                .expect("test btree insert should succeed");

            let err = index
                .lookup_encoded_entries(&pool_guard, &key)
                .await
                .expect_err("malformed exact key should fail encoded-entry lookup");
            assert_eq!(
                err.downcast_ref::<InternalError>().copied(),
                Some(InternalError::MemIndexKeyMalformed)
            );
        })
    }

    #[test]
    fn test_non_unique_mem_index_compare_delete_encoded_entry_checks_snapshot() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = NonUniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, true, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![
                    ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    },
                    ValType {
                        kind: ValKind::U64,
                        nullable: false,
                    },
                ]),
            );
            let key = vec![Val::from(42i32)];
            let row_id = 100u64;
            let guarded = index.bind(&pool_guard);
            assert!(
                guarded
                    .insert_if_not_exists(&key, RowID::new(row_id), false, TrxID::new(100))
                    .await
                    .unwrap()
                    .is_ok()
            );

            let active_entry = index
                .scan_encoded_entries(&pool_guard)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert!(!active_entry.deleted);
            assert!(
                !index
                    .compare_delete_encoded_entry(
                        &pool_guard,
                        &active_entry.encoded_key,
                        true,
                        TrxID::new(101)
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                guarded
                    .lookup_unique(&key, RowID::new(row_id), TrxID::new(101))
                    .await
                    .unwrap(),
                Some(true)
            );

            assert!(
                guarded
                    .mask_as_deleted(&key, RowID::new(row_id), TrxID::new(102))
                    .await
                    .unwrap()
            );
            let deleted_entry = index
                .scan_encoded_entries(&pool_guard)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert!(deleted_entry.deleted);
            assert!(
                !index
                    .compare_delete_encoded_entry(
                        &pool_guard,
                        &deleted_entry.encoded_key,
                        false,
                        TrxID::new(103)
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                guarded
                    .lookup_unique(&key, RowID::new(row_id), TrxID::new(103))
                    .await
                    .unwrap(),
                Some(false)
            );
            assert!(
                index
                    .compare_delete_encoded_entry(
                        &pool_guard,
                        &deleted_entry.encoded_key,
                        true,
                        TrxID::new(104)
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                guarded
                    .lookup_unique(&key, RowID::new(row_id), TrxID::new(104))
                    .await
                    .unwrap(),
                None
            );
        })
    }

    #[test]
    fn test_non_unique_mem_index_cleanup_scan_filters_live_entries_by_policy() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = NonUniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, true, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![
                    ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    },
                    ValType {
                        kind: ValKind::U64,
                        nullable: false,
                    },
                ]),
            );
            let guarded = index.bind(&pool_guard);

            let cold_live_key = vec![Val::from(1i32)];
            let hot_live_key = vec![Val::from(2i32)];
            let cold_deleted_key = vec![Val::from(3i32)];
            let hot_deleted_key = vec![Val::from(4i32)];
            guarded
                .insert_if_not_exists(&cold_live_key, RowID::new(10), false, TrxID::new(100))
                .await
                .unwrap();
            guarded
                .insert_if_not_exists(&hot_live_key, RowID::new(200), false, TrxID::new(100))
                .await
                .unwrap();
            guarded
                .insert_if_not_exists(&cold_deleted_key, RowID::new(30), false, TrxID::new(100))
                .await
                .unwrap();
            assert!(
                guarded
                    .mask_as_deleted(&cold_deleted_key, RowID::new(30), TrxID::new(101))
                    .await
                    .unwrap()
            );
            guarded
                .insert_if_not_exists(&hot_deleted_key, RowID::new(300), false, TrxID::new(100))
                .await
                .unwrap();
            assert!(
                guarded
                    .mask_as_deleted(&hot_deleted_key, RowID::new(300), TrxID::new(101))
                    .await
                    .unwrap()
            );

            let mut scan = index.cleanup_scan(&pool_guard, RowID::new(100), true);
            let batch = scan.next_batch().await.unwrap().unwrap();
            assert_eq!(batch.skipped_live, 1);
            assert_eq!(batch.skipped_hot_deleted, 1);
            assert_eq!(batch.entries.len(), 2);
            assert!(batch.entries.iter().any(|entry| !entry.deleted));
            assert!(batch.entries.iter().any(|entry| entry.deleted));
            assert!(scan.next_batch().await.unwrap().is_none());

            let mut scan = index.cleanup_scan(&pool_guard, RowID::new(100), false);
            let batch = scan.next_batch().await.unwrap().unwrap();
            assert_eq!(batch.skipped_live, 2);
            assert_eq!(batch.skipped_hot_deleted, 1);
            assert_eq!(batch.entries.len(), 1);
            assert!(batch.entries[0].deleted);
            assert!(scan.next_batch().await.unwrap().is_none());
        })
    }

    async fn run_test_suit_for_non_unique_index<T: NonUniqueIndex>(index: T) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        index
            .insert_if_not_exists(&key, RowID::new(row_id), false, TrxID::new(100))
            .await
            .unwrap();

        // 测试查找
        let mut res = vec![];
        index.lookup(&key, &mut res, TrxID::new(100)).await.unwrap();
        assert_eq!(res.len(), 1);

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        res.clear();
        index
            .lookup(&non_existent_key, &mut res, TrxID::new(100))
            .await
            .unwrap();
        assert!(res.is_empty());

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        index
            .insert_if_not_exists(&key, RowID::new(new_row_id), false, TrxID::new(100))
            .await
            .unwrap();
        // non-unique index allow duplicate key, but row id must be different.
        res.clear();
        index.lookup(&key, &mut res, TrxID::new(100)).await.unwrap();
        assert_eq!(res.len(), 2);

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(&key, RowID::new(row_id), true, TrxID::new(100))
                .await
                .unwrap()
        );
        res.clear();
        index.lookup(&key, &mut res, TrxID::new(100)).await.unwrap();
        assert_eq!(res.len(), 1);

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(&key, RowID::new(1000), false, TrxID::new(100))
                .await
                .unwrap()
        );

        // 测试用例4：scan_values 操作
        res.clear();
        let mut values = vec![];
        index
            .scan_values(&mut values, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RowID::new(new_row_id));

        // 测试用例5：多分区操作
        let key1 = vec![Val::from(1i32)];
        let key2 = vec![Val::from(2i32)];
        let key3 = vec![Val::from(3i32)];

        let row_id1 = 500u64;
        let row_id2 = 600u64;
        let row_id3 = 700u64;

        // 插入多个键值对
        index
            .insert_if_not_exists(&key1, RowID::new(row_id1), false, TrxID::new(100))
            .await
            .unwrap();
        index
            .insert_if_not_exists(&key2, RowID::new(row_id2), false, TrxID::new(100))
            .await
            .unwrap();
        index
            .insert_if_not_exists(&key3, RowID::new(row_id3), false, TrxID::new(100))
            .await
            .unwrap();

        // 验证所有键都能正确查找
        res.clear();
        index
            .lookup(&key1, &mut res, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        res.clear();
        index
            .lookup(&key2, &mut res, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        res.clear();
        index
            .lookup(&key3, &mut res, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(res.len(), 1);

        // 验证 scan_values 包含所有值
        res.clear();
        let mut values = vec![];
        index
            .scan_values(&mut values, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2

        // 验证insert覆盖
        let key4 = vec![Val::from(97i32)];
        let row_id4 = 800u64;
        let inserted = index
            .insert_if_not_exists(&key4, RowID::new(row_id4), false, TrxID::new(100))
            .await
            .unwrap();
        assert!(inserted.is_ok());
        let masked = index
            .mask_as_deleted(&key4, RowID::new(row_id4), TrxID::new(100))
            .await
            .unwrap();
        assert!(masked);
        let inserted = index
            .insert_if_not_exists(&key4, RowID::new(row_id4), true, TrxID::new(100))
            .await
            .unwrap();
        assert!(inserted.is_ok());
        assert!(matches!(inserted, IndexInsert::Ok(true)));
        res.clear();
        index
            .lookup(&key4, &mut res, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(res[0], RowID::new(row_id4));
    }
}
