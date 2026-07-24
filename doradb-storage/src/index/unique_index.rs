use crate::buffer::{BufferPool, PoolGuard};
use crate::catalog::IndexSpec;
use crate::error::RuntimeResult;
use crate::id::{RowID, TrxID};
use crate::index::btree::BTreeU64;
use crate::index::btree::{BTreeDelete, BTreeInsert, BTreeReplaceOrInsert, BTreeUpdate};
use crate::index::index_stream::UniqueMemIndexCandidateStream;
use crate::index::mem_index::{MemIndex, MemIndexCleanupScan, UniqueMemIndexCleanupSpec};
use crate::index::util::Maskable;
use crate::index::{IndexCompareExchange, IndexInsert, KeyRange};
use crate::quiescent::QuiescentGuard;
use crate::value::{Val, ValType};
use std::ops::Deref;

/// Generic unique-index implementation backed by a generic B-Tree.
pub(crate) struct UniqueMemIndex<P: 'static>(MemIndex<P>);

impl<P: 'static> Deref for UniqueMemIndex<P> {
    type Target = MemIndex<P>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<P: BufferPool> UniqueMemIndex<P> {
    /// Build a unique MemIndex from catalog index metadata.
    #[inline]
    pub(crate) async fn new<F: Fn(usize) -> ValType>(
        index_pool: QuiescentGuard<P>,
        index_pool_guard: &PoolGuard,
        index_spec: &IndexSpec,
        ty_infer: F,
        ts: TrxID,
    ) -> RuntimeResult<Self> {
        debug_assert!(index_spec.unique());
        debug_assert!(!index_spec.cols.is_empty());
        let types = index_spec
            .cols
            .iter()
            .map(|key| ty_infer(key.col_no as usize))
            .collect();
        Ok(Self(
            MemIndex::new_with_types(index_pool, index_pool_guard, types, ts).await?,
        ))
    }

    /// Bind this index to one pool guard for MemIndex operations.
    #[inline]
    pub(crate) fn bind<'g>(&self, pool_guard: &'g PoolGuard) -> GuardedUniqueMemIndex<'_, 'g, P> {
        GuardedUniqueMemIndex {
            index: self,
            pool_guard,
        }
    }

    /// Destroy this unique index and reclaim all backing tree pages.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> RuntimeResult<()> {
        self.0.destroy(pool_guard).await
    }

    /// Replace an exact current owner or insert the new owner on true absence.
    ///
    /// This performs one BTree traversal and compares the complete expected
    /// RowID value, including its delete bit.
    #[inline]
    pub(crate) async fn replace_or_insert(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        expected_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        debug_assert!(!new_row_id.is_deleted());
        let key = self.encoder().encode(key);
        Ok(
            match self
                .tree()
                .replace_or_insert(
                    pool_guard,
                    key.as_bytes(),
                    BTreeU64::from(expected_row_id),
                    BTreeU64::from(new_row_id),
                    ts,
                )
                .await?
            {
                BTreeReplaceOrInsert::Inserted | BTreeReplaceOrInsert::Replaced => {
                    IndexCompareExchange::Ok
                }
                BTreeReplaceOrInsert::ValueMismatch(_) => IndexCompareExchange::Mismatch,
            },
        )
    }

    /// Create a leaf-bounded scanner for cleanup candidates.
    #[inline]
    pub(crate) fn cleanup_scan<'a>(
        &'a self,
        pool_guard: &'a PoolGuard,
        pivot_row_id: RowID,
        clean_live_entries: bool,
    ) -> MemIndexCleanupScan<'a, P, UniqueMemIndexCleanupSpec> {
        self.0.cleanup_scan::<UniqueMemIndexCleanupSpec>(
            pool_guard,
            pivot_row_id,
            clean_live_entries,
        )
    }

    /// Return whether `key` encodes to the same MemIndex key bytes captured by
    /// a cleanup scan.
    #[inline]
    pub(crate) fn encoded_key_matches(&self, key: &[Val], encoded_key: &[u8]) -> bool {
        self.encoder().encode(key).as_bytes() == encoded_key
    }

    /// Remove an encoded MemIndex entry only when row id and delete state still
    /// match the previously scanned entry.
    ///
    /// This is intentionally encoded-key based for full-scan cleanup, which
    /// should not decode physical BTree keys back into logical `SelectKey`
    /// values.
    #[inline]
    pub(crate) async fn compare_delete_encoded_entry(
        &self,
        pool_guard: &PoolGuard,
        encoded_key: &[u8],
        row_id: RowID,
        deleted: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        Ok(matches!(
            self.tree()
                .delete_exact(pool_guard, encoded_key, BTreeU64::from(row_id), deleted, ts,)
                .await?,
            BTreeDelete::Ok
        ))
    }
}

/// Unique MemIndex view bound to one index-pool guard.
pub(crate) struct GuardedUniqueMemIndex<'a, 'g, P: 'static> {
    index: &'a UniqueMemIndex<P>,
    pool_guard: &'g PoolGuard,
}

impl<P: 'static> Clone for GuardedUniqueMemIndex<'_, '_, P> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<P: 'static> Copy for GuardedUniqueMemIndex<'_, '_, P> {}

impl<P: BufferPool> GuardedUniqueMemIndex<'_, '_, P> {
    /// Replace an exact current owner or insert the new owner on true absence.
    #[inline]
    pub(crate) async fn replace_or_insert(
        &self,
        key: &[Val],
        expected_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        self.index
            .replace_or_insert(self.pool_guard, key, expected_row_id, new_row_id, ts)
            .await
    }

    /// Lookup a unique key and return its owner and delete state.
    #[inline]
    pub(crate) async fn lookup(
        &self,
        key: &[Val],
        _ts: TrxID,
    ) -> RuntimeResult<Option<(RowID, bool)>> {
        let k = self.index.encoder().encode(key);
        Ok(self
            .index
            .tree()
            .lookup_optimistic::<BTreeU64>(self.pool_guard, k.as_bytes())
            .await?
            .map(|res| (res.value().to_row_id(), res.is_deleted())))
    }

    /// Insert a unique key unless another owner already exists.
    #[inline]
    pub(crate) async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> RuntimeResult<IndexInsert> {
        debug_assert!(!row_id.is_deleted());
        let k = self.index.encoder().encode(key);
        Ok(
            match self
                .index
                .tree()
                .insert::<BTreeU64>(
                    self.pool_guard,
                    k.as_bytes(),
                    BTreeU64::from(row_id),
                    merge_if_match_deleted,
                    ts,
                )
                .await?
            {
                BTreeInsert::Ok(merged) => IndexInsert::Ok(merged),
                BTreeInsert::DuplicateKey(res) => {
                    IndexInsert::DuplicateKey(res.value().to_row_id(), res.is_deleted())
                }
            },
        )
    }

    /// Delete a unique key when its owner matches `row_id`.
    #[inline]
    pub(crate) async fn compare_delete(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.index.encoder().encode(key);
        Ok(
            match self
                .index
                .tree()
                .delete(
                    self.pool_guard,
                    k.as_bytes(),
                    BTreeU64::from(row_id),
                    ignore_del_mask,
                    ts,
                )
                .await?
            {
                // Treat not found as success.
                BTreeDelete::Ok | BTreeDelete::NotFound => true,
                BTreeDelete::ValueMismatch => false,
            },
        )
    }

    /// Mask a matching unique owner as deleted.
    #[inline]
    pub(crate) async fn mask_as_deleted(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        debug_assert!(!row_id.is_deleted());
        Ok(matches!(
            self.compare_exchange(key, row_id, row_id.deleted(), ts)
                .await?,
            IndexCompareExchange::Ok
        ))
    }

    /// Atomically replace a matching unique owner.
    #[inline]
    pub(crate) async fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        let k = self.index.encoder().encode(key);
        Ok(
            match self
                .index
                .tree()
                .update(
                    self.pool_guard,
                    k.as_bytes(),
                    BTreeU64::from(old_row_id),
                    BTreeU64::from(new_row_id),
                    ts,
                )
                .await?
            {
                BTreeUpdate::Ok(row_id) => {
                    debug_assert!(BTreeU64::from(old_row_id) == row_id);
                    IndexCompareExchange::Ok
                }
                BTreeUpdate::NotFound => IndexCompareExchange::NotExists,
                BTreeUpdate::ValueMismatch(_) => IndexCompareExchange::Mismatch,
            },
        )
    }

    /// Scan lookup candidates over a bounded encoded-key range.
    #[inline]
    pub(crate) fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        _ts: TrxID,
    ) -> RuntimeResult<UniqueMemIndexCandidateStream<'a, P>> {
        Ok(UniqueMemIndexCandidateStream::new(
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
    use crate::index::mem_index::MemIndexEntry;
    use crate::index::util::tests::drain_row_ids;
    use crate::quiescent::QuiescentBox;
    use crate::value::{ValKind, ValType};
    use std::ops::Bound::Unbounded;

    async fn test_unique_mem_index(
        pool: &QuiescentBox<FixedBufferPool>,
        pool_guard: &PoolGuard,
        types: Vec<ValType>,
    ) -> UniqueMemIndex<FixedBufferPool> {
        let index_spec = IndexSpec::new(
            (0..types.len())
                .map(|col_no| IndexKey::new(col_no as u16))
                .collect(),
            IndexAttributes::UK,
        );
        UniqueMemIndex::new(
            pool.guard(),
            pool_guard,
            &index_spec,
            |col_no| types[col_no],
            TrxID::new(100),
        )
        .await
        .expect("test unique MemIndex construction should succeed")
    }

    async fn cleanup_entry<P: BufferPool>(
        index: &UniqueMemIndex<P>,
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
    fn test_single_key_btree_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let index = test_unique_mem_index(
                    &pool,
                    &pool_guard,
                    vec![ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    }],
                )
                .await;
                run_test_suit_for_single_key_unique_index(index.bind(&pool_guard)).await;
            }
        });
    }

    #[test]
    fn test_multi_key_btree_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let index = test_unique_mem_index(
                    &pool,
                    &pool_guard,
                    vec![
                        ValType {
                            kind: ValKind::VarByte,
                            nullable: false,
                        },
                        ValType {
                            kind: ValKind::I32,
                            nullable: false,
                        },
                    ],
                )
                .await;
                run_test_suit_for_multi_key_unique_index(index.bind(&pool_guard)).await;
            }
        });
    }

    #[test]
    fn test_unique_mem_index_row_id_stream_bounds_and_drop() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType {
                    kind: ValKind::I32,
                    nullable: false,
                }],
            )
            .await;
            let guarded = index.bind(&pool_guard);
            for key in 1..=5 {
                assert!(
                    guarded
                        .insert_if_not_exists(
                            &[Val::from(key)],
                            RowID::new(key as u64 * 10),
                            false,
                            TrxID::new(100),
                        )
                        .await
                        .unwrap()
                        .is_ok()
                );
            }

            let range = index
                .encoder()
                .encode_range(&[Val::from(2i32)][..]..&[Val::from(4i32)][..]);
            let mut stream = guarded
                .index_scan_candidates(&range, TrxID::new(101))
                .unwrap();
            assert_eq!(
                drain_row_ids(&mut stream).await,
                vec![RowID::new(20), RowID::new(30)]
            );

            let range = index.encoder().encode_range(..);
            let mut stream = guarded
                .index_scan_candidates(&range, TrxID::new(102))
                .unwrap();
            assert!(stream.next_batch().await.unwrap().is_some());
            drop(stream);
            assert_eq!(
                guarded
                    .lookup(&[Val::from(1i32)], TrxID::new(103))
                    .await
                    .unwrap(),
                Some((RowID::new(10), false))
            );
        });
    }

    #[test]
    fn test_unique_mem_index_compare_delete_encoded_entry_checks_snapshot() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType {
                    kind: ValKind::I32,
                    nullable: false,
                }],
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
                        active_entry.row_id,
                        true,
                        TrxID::new(101),
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                guarded.lookup(&key, TrxID::new(101)).await.unwrap(),
                Some((RowID::new(row_id), false))
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
                        deleted_entry.row_id,
                        false,
                        TrxID::new(103),
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                guarded.lookup(&key, TrxID::new(103)).await.unwrap(),
                Some((RowID::new(row_id), true))
            );
            assert!(
                index
                    .compare_delete_encoded_entry(
                        &pool_guard,
                        &deleted_entry.encoded_key,
                        deleted_entry.row_id,
                        true,
                        TrxID::new(104),
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(guarded.lookup(&key, TrxID::new(104)).await.unwrap(), None);
        });
    }

    #[test]
    fn test_unique_mem_index_cleanup_scan_filters_live_entries_by_policy() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            let pool_guard = (*pool).pool_guard();
            let index = test_unique_mem_index(
                &pool,
                &pool_guard,
                vec![ValType {
                    kind: ValKind::I32,
                    nullable: false,
                }],
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
        });
    }

    async fn run_test_suit_for_single_key_unique_index<P: BufferPool>(
        index: GuardedUniqueMemIndex<'_, '_, P>,
    ) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert!(
            index
                .insert_if_not_exists(&key, RowID::new(row_id), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );

        // 测试查找
        assert_eq!(
            index.lookup(&key, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id), false))
        );

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        assert_eq!(
            index
                .lookup(&non_existent_key, TrxID::new(100))
                .await
                .unwrap(),
            None
        );

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index
            .insert_if_not_exists(&key, RowID::new(new_row_id), false, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(
            old_row_id,
            IndexInsert::DuplicateKey(RowID::new(row_id), false)
        );
        assert_eq!(
            index.lookup(&key, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id), false))
        );

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(&key, RowID::new(row_id), true, TrxID::new(100))
                .await
                .unwrap()
        );
        assert_eq!(index.lookup(&key, TrxID::new(100)).await.unwrap(), None);

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(&key, RowID::new(new_row_id), false, TrxID::new(100))
                .await
                .unwrap()
        );

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert!(
            index
                .insert_if_not_exists(&key, RowID::new(row_id1), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok(),
        );

        // 测试成功的 compare_exchange
        assert!(
            index
                .compare_exchange(
                    &key,
                    RowID::new(row_id1),
                    RowID::new(row_id2),
                    TrxID::new(100)
                )
                .await
                .unwrap()
                == IndexCompareExchange::Ok
        );
        assert_eq!(
            index.lookup(&key, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id2), false))
        );

        // 测试失败的 compare_exchange
        assert!(
            index
                .compare_exchange(
                    &key,
                    RowID::new(row_id1),
                    RowID::new(row_id2),
                    TrxID::new(100)
                )
                .await
                .unwrap()
                == IndexCompareExchange::Mismatch
        );

        // 测试用例5：scan_values 操作
        let values = scan_rows(&index).await;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RowID::new(row_id2));

        // 测试用例6：多分区操作
        let key1 = vec![Val::from(1i32)];
        let key2 = vec![Val::from(2i32)];
        let key3 = vec![Val::from(3i32)];

        let row_id1 = 500u64;
        let row_id2 = 600u64;
        let row_id3 = 700u64;

        // 插入多个键值对
        assert!(
            index
                .insert_if_not_exists(&key1, RowID::new(row_id1), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key2, RowID::new(row_id2), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key3, RowID::new(row_id3), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );

        // 验证所有键都能正确查找
        assert_eq!(
            index.lookup(&key1, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id1), false))
        );
        assert_eq!(
            index.lookup(&key2, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id2), false))
        );
        assert_eq!(
            index.lookup(&key3, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id3), false))
        );

        // 验证 scan_values 包含所有值
        let values = scan_rows(&index).await;
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2
    }

    async fn run_test_suit_for_multi_key_unique_index<P: BufferPool>(
        index: GuardedUniqueMemIndex<'_, '_, P>,
    ) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from("hello"), Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert!(
            index
                .insert_if_not_exists(&key, RowID::new(row_id), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );

        // 测试查找
        assert_eq!(
            index.lookup(&key, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id), false))
        );

        // 测试不存在的键
        let non_existent_key = vec![Val::from("hello"), Val::from(43i32)];
        assert_eq!(
            index
                .lookup(&non_existent_key, TrxID::new(100))
                .await
                .unwrap(),
            None
        );

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index
            .insert_if_not_exists(&key, RowID::new(new_row_id), false, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(
            old_row_id,
            IndexInsert::DuplicateKey(RowID::new(row_id), false)
        );
        assert_eq!(
            index.lookup(&key, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id), false))
        );

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(&key, RowID::new(row_id), true, TrxID::new(100))
                .await
                .unwrap()
        );
        assert_eq!(index.lookup(&key, TrxID::new(100)).await.unwrap(), None);

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(&key, RowID::new(new_row_id), false, TrxID::new(100))
                .await
                .unwrap()
        );

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from("hello"), Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert!(
            index
                .insert_if_not_exists(&key, RowID::new(row_id1), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );

        // 测试成功的 compare_exchange
        assert!(
            index
                .compare_exchange(
                    &key,
                    RowID::new(row_id1),
                    RowID::new(row_id2),
                    TrxID::new(100)
                )
                .await
                .unwrap()
                == IndexCompareExchange::Ok
        );
        assert_eq!(
            index.lookup(&key, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id2), false))
        );

        // 测试失败的 compare_exchange
        assert!(
            index
                .compare_exchange(
                    &key,
                    RowID::new(row_id1),
                    RowID::new(row_id2),
                    TrxID::new(100)
                )
                .await
                .unwrap()
                == IndexCompareExchange::Mismatch
        );

        // 测试用例5：scan_values 操作
        let values = scan_rows(&index).await;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RowID::new(row_id2));

        // 测试用例6：多分区操作
        let key1 = vec![Val::from("world"), Val::from(1i32)];
        let key2 = vec![Val::from("world"), Val::from(2i32)];
        let key3 = vec![Val::from("world"), Val::from(3i32)];

        let row_id1 = 500u64;
        let row_id2 = 600u64;
        let row_id3 = 700u64;

        // 插入多个键值对
        assert!(
            index
                .insert_if_not_exists(&key1, RowID::new(row_id1), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key2, RowID::new(row_id2), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key3, RowID::new(row_id3), false, TrxID::new(100))
                .await
                .unwrap()
                .is_ok()
        );

        // 验证所有键都能正确查找
        assert_eq!(
            index.lookup(&key1, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id1), false))
        );
        assert_eq!(
            index.lookup(&key2, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id2), false))
        );
        assert_eq!(
            index.lookup(&key3, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id3), false))
        );

        // 验证 scan_values 包含所有值
        let values = scan_rows(&index).await;
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2

        // 验证insert覆盖
        let key4 = vec![Val::from("rust"), Val::from(97i32)];
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
        assert_eq!(
            index.lookup(&key4, TrxID::new(100)).await.unwrap(),
            Some((RowID::new(row_id4), false))
        );
    }

    async fn scan_rows<P: BufferPool>(index: &GuardedUniqueMemIndex<'_, '_, P>) -> Vec<RowID> {
        let range = KeyRange::new(Unbounded, Unbounded);
        let mut stream = index
            .index_scan_candidates(&range, TrxID::new(100))
            .unwrap();
        drain_row_ids(&mut stream).await
    }
}
