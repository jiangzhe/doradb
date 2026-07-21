use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, PoolGuard};
use crate::catalog::IndexSpec;
use crate::error::RuntimeResult;
use crate::id::{RowID, TrxID};
#[cfg(test)]
use crate::index::btree::BTreeKeyEncoder;
use crate::index::btree::BTreeU64;
#[cfg(test)]
use crate::index::btree::GenericBTree;
use crate::index::btree::{BTreeDelete, BTreeInsert, BTreeUpdate};
use crate::index::index_stream::UniqueMemIndexCandidateStream;
use crate::index::mem_index::{
    MemIndex, MemIndexCleanupScan, MemIndexEntry, UniqueMemIndexCleanupSpec,
    UniqueMemIndexEntryScanSpec,
};
use crate::index::util::Maskable;
use crate::index::{
    IndexBatchStream, IndexCompareExchange, IndexInsert, IndexLookupCandidate, KeyRange,
};
use crate::quiescent::QuiescentGuard;
use crate::value::{Val, ValType};
use futures::FutureExt;
use std::future::Future;
use std::ops::Deref;

/// Abstraction of unique index.
pub(crate) trait UniqueIndex: Send + Sync {
    /// Candidate stream returned by bounded unique-index scans.
    type LookupCandidateStream<'a>: IndexBatchStream<IndexLookupCandidate> + 'a
    where
        Self: 'a;

    /// Lookup unique key in this index.
    /// Return associated value and delete flag.
    fn lookup(
        &self,
        key: &[Val],
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<Option<(RowID, bool)>>>;

    /// Scan lookup candidates over a bounded encoded-key range.
    ///
    /// Returned entries are index-derived candidates for MVCC lookup, not
    /// visibility-confirmed rows. Unique candidates carry encoded logical keys.
    /// The stream must return unmasked row ids even when an in-memory
    /// delete-shadow entry is encountered; row-version and
    /// column-deletion-buffer checks decide whether a candidate is visible. Hot
    /// in-memory entries shadow equal cold DiskTree entries.
    fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        ts: TrxID,
    ) -> RuntimeResult<Self::LookupCandidateStream<'a>>;

    /// Insert new key value pair into this index.
    /// If same key exists, return old key and its delete flag.
    /// merge_if_match_deleted flag is an optimization for key change on same row.
    /// In this case, we can directly unset the delete flag to finish the insert.
    fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<IndexInsert>>;

    /// Delete a given key if value matches input value.
    /// For normal delete index operation, we always mark the entry as deleted
    /// before actually delete it.
    /// But in some scenarios, e.g. recovery or rollback, we would not mask the entry
    /// as deleted, so we set ignore_del_mask to true to force the deletion.
    ///
    /// todo: return more information about ts comparison with page sts,
    /// to support minimal cost of index GC.
    fn compare_delete(
        &self,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<bool>>;

    /// Mask a given key value as deleted.
    #[inline]
    fn mask_as_deleted(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<bool>> {
        debug_assert!(!row_id.is_deleted());
        let new_row_id = row_id.deleted();
        self.compare_exchange(key, row_id, new_row_id, ts)
            .map(|res| {
                res.map(|res| match res {
                    IndexCompareExchange::Ok => true,
                    IndexCompareExchange::Mismatch | IndexCompareExchange::NotExists => false,
                })
            })
    }

    /// atomically update an existing value associated to given key to another value.
    /// if not exists, returns specified bool value.
    fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<IndexCompareExchange>>;

    /// Scan values into given collection.
    #[cfg_attr(not(test), expect(dead_code, reason = "reserved scan_values"))]
    fn scan_values(
        &self,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> impl Future<Output = RuntimeResult<()>>;
}

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

    /// Wrap an already-created BTree with a key encoder.
    #[inline]
    #[cfg(test)]
    pub(crate) fn with_encoder(tree: GenericBTree<P>, encoder: BTreeKeyEncoder) -> Self {
        Self(MemIndex::with_encoder(tree, encoder))
    }

    /// Bind this index to one pool guard for trait-based operations.
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

    /// Insert a live or delete-shadow overlay when the logical key is absent.
    ///
    /// This helper is intentionally concrete to the BTree-backed MemIndex so the
    /// dual-tree composite can claim or shadow cold DiskTree owners without
    /// widening the public `UniqueIndex` trait.
    #[inline]
    pub(crate) async fn insert_overlay_if_absent(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        let key = self.encoder().encode(key);
        Ok(
            match self
                .tree()
                .insert::<BTreeU64>(
                    pool_guard,
                    key.as_bytes(),
                    BTreeU64::from(row_id),
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

    /// Scan MemIndex entries with encoded logical keys and delete state.
    ///
    /// The returned entries are ordered by encoded key because they are produced
    /// by the underlying BTree leaf cursor.
    #[inline]
    pub(crate) async fn scan_encoded_entries(
        &self,
        pool_guard: &PoolGuard,
    ) -> RuntimeResult<Vec<MemIndexEntry>> {
        self.0
            .scan_encoded_entries::<UniqueMemIndexEntryScanSpec>(pool_guard)
            .await
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
#[derive(Clone, Copy)]
pub(crate) struct GuardedUniqueMemIndex<'a, 'g, P: 'static> {
    index: &'a UniqueMemIndex<P>,
    pool_guard: &'g PoolGuard,
}

impl<P: BufferPool> GuardedUniqueMemIndex<'_, '_, P> {
    /// Insert a live or delete-shadow overlay when the logical key is absent.
    #[inline]
    pub(crate) async fn insert_overlay_if_absent(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.index
            .insert_overlay_if_absent(self.pool_guard, key, row_id, ts)
            .await
    }

    /// Scan MemIndex entries with encoded logical keys and delete state.
    #[inline]
    pub(crate) async fn scan_encoded_entries(&self) -> RuntimeResult<Vec<MemIndexEntry>> {
        self.index.scan_encoded_entries(self.pool_guard).await
    }
}

impl<P: BufferPool> UniqueIndex for GuardedUniqueMemIndex<'_, '_, P> {
    type LookupCandidateStream<'a>
        = UniqueMemIndexCandidateStream<'a, P>
    where
        Self: 'a;

    #[inline]
    async fn lookup(&self, key: &[Val], _ts: TrxID) -> RuntimeResult<Option<(RowID, bool)>> {
        let k = self.index.encoder().encode(key);
        Ok(self
            .index
            .tree()
            .lookup_optimistic::<BTreeU64>(self.pool_guard, k.as_bytes())
            .await?
            .map(|res| (res.value().to_row_id(), res.is_deleted())))
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

    #[inline]
    async fn compare_delete(
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

    #[inline]
    async fn compare_exchange(
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

    #[inline]
    fn index_scan_candidates<'a>(
        &'a self,
        range: &'a KeyRange,
        _ts: TrxID,
    ) -> RuntimeResult<Self::LookupCandidateStream<'a>> {
        Ok(UniqueMemIndexCandidateStream::new(
            self.index.tree().cursor(self.pool_guard, 0),
            range,
        ))
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) -> RuntimeResult<()> {
        let mut cursor = self.index.tree().cursor(self.pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(g) = cursor.next().await? {
            g.page().values(values, BTreeU64::to_row_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{FixedBufferPool, PoolRole};
    use crate::index::btree::BTree;
    use crate::index::util::tests::drain_row_ids;
    use crate::quiescent::QuiescentBox;
    use crate::value::{ValKind, ValType};

    #[test]
    fn test_single_key_btree_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(PoolRole::Index, 1024usize * 1024 * 1024).unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let index = UniqueMemIndex::with_encoder(
                    BTree::new(pool.guard(), &pool_guard, false, TrxID::new(100))
                        .await
                        .expect("test btree construction should succeed"),
                    BTreeKeyEncoder::new(vec![ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    }]),
                );
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
                let index = UniqueMemIndex::with_encoder(
                    BTree::new(pool.guard(), &pool_guard, false, TrxID::new(100))
                        .await
                        .expect("test btree construction should succeed"),
                    BTreeKeyEncoder::new(vec![
                        ValType {
                            kind: ValKind::VarByte,
                            nullable: false,
                        },
                        ValType {
                            kind: ValKind::I32,
                            nullable: false,
                        },
                    ]),
                );
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
            let index = UniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, false, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![ValType {
                    kind: ValKind::I32,
                    nullable: false,
                }]),
            );
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
            let index = UniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, false, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![ValType {
                    kind: ValKind::I32,
                    nullable: false,
                }]),
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
            let index = UniqueMemIndex::with_encoder(
                BTree::new(pool.guard(), &pool_guard, false, TrxID::new(100))
                    .await
                    .expect("test btree construction should succeed"),
                BTreeKeyEncoder::new(vec![ValType {
                    kind: ValKind::I32,
                    nullable: false,
                }]),
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
        });
    }

    async fn run_test_suit_for_single_key_unique_index<T: UniqueIndex>(index: T) {
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
        let mut values = Vec::new();
        index
            .scan_values(&mut values, TrxID::new(100))
            .await
            .unwrap();
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
        let mut values = Vec::new();
        index
            .scan_values(&mut values, TrxID::new(100))
            .await
            .unwrap();
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2
    }

    async fn run_test_suit_for_multi_key_unique_index<T: UniqueIndex>(index: T) {
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
        let mut values = Vec::new();
        index
            .scan_values(&mut values, TrxID::new(100))
            .await
            .unwrap();
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
        let mut values = Vec::new();
        index
            .scan_values(&mut values, TrxID::new(100))
            .await
            .unwrap();
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
}
