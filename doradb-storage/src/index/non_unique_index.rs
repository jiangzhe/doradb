use crate::buffer::{BufferPool, PoolGuard};
use crate::catalog::IndexSpec;
use crate::error::RuntimeResult;
use crate::id::{RowID, TrxID};
use crate::index::btree::{BTREE_BYTE_ZERO, BTreeByte};
use crate::index::btree::{BTreeDelete, BTreeInsert, BTreeUpdate};
use crate::index::index_stream::NonUniqueMemIndexCandidateStream;
use crate::index::mem_index::{MemIndex, MemIndexCleanupScan, NonUniqueMemIndexCleanupSpec};
use crate::index::util::Maskable;
use crate::index::{IndexInsert, KeyRange};
use crate::quiescent::QuiescentGuard;
use crate::value::{Val, ValKind, ValType};
use std::ops::Deref;

/// Result of atomically masking one exact non-unique MemIndex entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum IndexMask {
    /// The active exact entry was changed to delete-marked.
    Masked,
    /// The exact entry exists and was already delete-marked.
    AlreadyMasked,
    /// The exact entry does not exist in MemIndex.
    NotFound,
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

    /// Bind this index to one pool guard for MemIndex operations.
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
    /// Atomically mask one exact entry without a preliminary lookup.
    #[inline]
    pub(crate) async fn mask_if_present(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexMask> {
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
                BTreeUpdate::Ok(_) => IndexMask::Masked,
                BTreeUpdate::NotFound => IndexMask::NotFound,
                BTreeUpdate::ValueMismatch(_) => IndexMask::AlreadyMasked,
            },
        )
    }

    /// Lookup one exact `(logical_key, row_id)` entry and its active state.
    #[inline]
    pub(crate) async fn lookup_unique(
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

    /// Insert one exact entry unless it already exists.
    #[inline]
    pub(crate) async fn insert_if_not_exists(
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

    /// Mask one matching exact entry as deleted.
    #[inline]
    pub(crate) async fn mask_as_deleted(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        Ok(matches!(
            self.mask_if_present(key, row_id, ts).await?,
            IndexMask::Masked
        ))
    }

    /// Restore one matching delete-marked exact entry to active state.
    #[inline]
    pub(crate) async fn mask_as_active(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
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

    /// Delete one exact entry when its active value matches.
    #[inline]
    pub(crate) async fn compare_delete(
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

    /// Scan lookup candidates over a bounded encoded-key range.
    #[inline]
    pub(crate) fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        _ts: TrxID,
    ) -> RuntimeResult<NonUniqueMemIndexCandidateStream<'a, P>> {
        Ok(NonUniqueMemIndexCandidateStream::new(
            self.index.tree().cursor(self.pool_guard, 0),
            range,
        ))
    }

    /// Scan lookup candidates equal to one encoded secondary-key range.
    #[inline]
    pub(crate) fn equal_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        _ts: TrxID,
    ) -> RuntimeResult<NonUniqueMemIndexCandidateStream<'a, P>> {
        Ok(NonUniqueMemIndexCandidateStream::new(
            self.index.tree().cursor(self.pool_guard, 0),
            range,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{FixedBufferPool, PoolRole};
    use crate::catalog::{IndexAttributes, IndexKey};
    use crate::index::btree::BTreeKeyEncoder;
    use crate::index::mem_index::MemIndexEntry;
    use crate::index::util::tests::drain_row_ids;
    use crate::quiescent::QuiescentBox;
    use crate::value::{ValKind, ValType};
    use std::mem;
    use std::ops::Bound::Unbounded;

    async fn test_non_unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
        types: Vec<ValType>,
    ) -> NonUniqueMemIndex<FixedBufferPool> {
        let index_spec = IndexSpec::new(
            (0..types.len())
                .map(|col_no| IndexKey::new(col_no as u16))
                .collect(),
            IndexAttributes::empty(),
        );
        NonUniqueMemIndex::new(
            pool.guard(),
            pool_guard,
            &index_spec,
            |col_no| types[col_no],
            TrxID::new(100),
        )
        .await
        .expect("test non-unique MemIndex construction should succeed")
    }

    async fn cleanup_entry<P: BufferPool>(
        index: &NonUniqueMemIndex<P>,
        pool_guard: &PoolGuard,
        pivot_row_id: RowID,
    ) -> MemIndexEntry {
        let mut scan = index.cleanup_scan(pool_guard, pivot_row_id, true);
        let mut entries = Vec::new();
        while let Some(batch) = scan.next_batch().await.unwrap() {
            entries.extend(batch.entries);
        }
        assert_eq!(entries.len(), 1);
        entries.pop().unwrap()
    }

    #[test]
    fn test_non_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let index = test_non_unique_mem_index(
                    &pool,
                    &pool_guard,
                    vec![ValType::new(ValKind::I32, false)],
                )
                .await;
                run_test_suit_for_non_unique_index(index.bind(&pool_guard)).await;
            }
        })
    }

    #[test]
    fn test_non_unique_mem_index_mask_outcome() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 64 * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_non_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType::new(ValKind::I32, false)],
            )
            .await;
            let guarded = index.bind(&pool_guard);
            let key = [Val::from(42i32)];
            let row_id = RowID::new(7);

            assert_eq!(
                guarded
                    .mask_if_present(&key, row_id, TrxID::new(101))
                    .await
                    .unwrap(),
                IndexMask::NotFound
            );
            assert_eq!(
                guarded
                    .insert_if_not_exists(&key, row_id, false, TrxID::new(102))
                    .await
                    .unwrap(),
                IndexInsert::Ok(false)
            );
            assert_eq!(
                guarded
                    .mask_if_present(&key, row_id, TrxID::new(103))
                    .await
                    .unwrap(),
                IndexMask::Masked
            );
            assert_eq!(
                guarded
                    .mask_if_present(&key, row_id, TrxID::new(104))
                    .await
                    .unwrap(),
                IndexMask::AlreadyMasked
            );
        })
    }

    #[test]
    fn test_non_unique_mem_index_row_id_stream_prefix_and_range() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_non_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType::new(ValKind::I32, false)],
            )
            .await;
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
    #[should_panic(expected = "non-unique MemIndex key is missing its row-id suffix")]
    fn test_cleanup_scan_asserts_malformed_exact_key() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_non_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType::new(ValKind::I32, false)],
            )
            .await;

            let key = vec![Val::from(42i32)];
            let malformed_exact_key =
                BTreeKeyEncoder::new(vec![ValType::new(ValKind::I32, false)]).encode(&key);
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

            let mut scan = index.cleanup_scan(&pool_guard, RowID::new(1), true);
            let _ = scan.next_batch().await.unwrap();
        })
    }

    #[test]
    fn test_non_unique_mem_index_compare_delete_encoded_entry_checks_snapshot() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_non_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType::new(ValKind::I32, false)],
            )
            .await;
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

            let active_entry = cleanup_entry(&index, &pool_guard, RowID::new(row_id + 1)).await;
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
            let deleted_entry = cleanup_entry(&index, &pool_guard, RowID::new(row_id + 1)).await;
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
            let index = test_non_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType::new(ValKind::I32, false)],
            )
            .await;
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

    async fn run_test_suit_for_non_unique_index<P: BufferPool>(
        index: GuardedNonUniqueMemIndex<'_, '_, P>,
    ) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        index
            .insert_if_not_exists(&key, RowID::new(row_id), false, TrxID::new(100))
            .await
            .unwrap();

        // 测试查找
        assert_eq!(lookup_rows(&index, &key).await, vec![RowID::new(row_id)]);

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        assert!(lookup_rows(&index, &non_existent_key).await.is_empty());

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        index
            .insert_if_not_exists(&key, RowID::new(new_row_id), false, TrxID::new(100))
            .await
            .unwrap();
        // non-unique index allow duplicate key, but row id must be different.
        assert_eq!(
            lookup_rows(&index, &key).await,
            vec![RowID::new(row_id), RowID::new(new_row_id)]
        );

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(&key, RowID::new(row_id), true, TrxID::new(100))
                .await
                .unwrap()
        );
        assert_eq!(
            lookup_rows(&index, &key).await,
            vec![RowID::new(new_row_id)]
        );

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(&key, RowID::new(1000), false, TrxID::new(100))
                .await
                .unwrap()
        );

        // 测试用例4：scan_values 操作
        let values = scan_rows(&index).await;
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
        assert_eq!(lookup_rows(&index, &key1).await.len(), 1);
        assert_eq!(lookup_rows(&index, &key2).await.len(), 1);
        assert_eq!(lookup_rows(&index, &key3).await.len(), 1);

        // 验证 scan_values 包含所有值
        let values = scan_rows(&index).await;
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
        assert_eq!(lookup_rows(&index, &key4).await, vec![RowID::new(row_id4)]);
    }

    async fn lookup_rows<P: BufferPool>(
        index: &GuardedNonUniqueMemIndex<'_, '_, P>,
        key: &[Val],
    ) -> Vec<RowID> {
        let range = index.index.encoder().encode_non_unique_equal_range(key);
        let mut stream = index
            .equal_scan_candidates(&range, TrxID::new(100))
            .unwrap();
        drain_row_ids(&mut stream).await
    }

    async fn scan_rows<P: BufferPool>(index: &GuardedNonUniqueMemIndex<'_, '_, P>) -> Vec<RowID> {
        let range = KeyRange::new(Unbounded, Unbounded);
        let mut stream = index
            .index_scan_candidates(&range, TrxID::new(100))
            .unwrap();
        drain_row_ids(&mut stream).await
    }
}
