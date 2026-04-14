use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
use crate::error::{Error, Result};
use crate::index::IndexCompareExchange;
use crate::index::btree::{BTreeDelete, BTreeInsert, BTreeUpdate, GenericBTree};
use crate::index::btree_key::BTreeKeyEncoder;
use crate::index::btree_value::BTreeU64;
use crate::index::secondary_index::{
    EncodeKeySelf, IndexInsert, PartitionMultiKeyIndex, PartitionSingleKeyIndex,
};
use crate::index::util::Maskable;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::Val;
use futures::FutureExt;
use std::collections::btree_map::Entry;
use std::future::Future;
use std::hash::Hash;

/// Abstraction of unique index.
pub trait UniqueIndex: Send + Sync + 'static {
    /// Lookup unique key in this index.
    /// Return associated value and delete flag.
    fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        ts: TrxID,
    ) -> impl Future<Output = Result<Option<(RowID, bool)>>>;

    /// Insert new key value pair into this index.
    /// If same key exists, return old key and its delete flag.
    /// merge_if_match_deleted flag is an optimization for key change on same row.
    /// In this case, we can directly unset the delete flag to finish the insert.
    fn insert_if_not_exists(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> impl Future<Output = Result<IndexInsert>>;

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
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> impl Future<Output = Result<bool>>;

    /// Mask a given key value as deleted.
    #[inline]
    fn mask_as_deleted(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Result<bool>> {
        debug_assert!(!row_id.is_deleted());
        let new_row_id = row_id.deleted();
        self.compare_exchange(pool_guard, key, row_id, new_row_id, ts)
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
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Result<IndexCompareExchange>>;

    /// Scan values into given collection.
    fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> impl Future<Output = Result<()>>;
}

/// Generic unique-index implementation backed by a generic B-Tree.
pub struct GenericUniqueBTreeIndex<P: 'static> {
    tree: GenericBTree<P>,
    encoder: BTreeKeyEncoder,
}

/// Encoded MemTree state for one unique secondary-index entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct UniqueMemTreeEntry {
    /// Encoded logical secondary key in BTree order.
    pub(crate) encoded_key: Vec<u8>,
    /// Row id stored in the MemTree entry with the delete bit stripped.
    pub(crate) row_id: RowID,
    /// Whether the MemTree entry is a delete-shadow.
    pub(crate) deleted: bool,
}

impl UniqueMemTreeEntry {
    /// Return the row id in the same shape as current MemTree scan results.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn scan_row_id(&self) -> RowID {
        if self.deleted {
            self.row_id.deleted()
        } else {
            self.row_id
        }
    }
}

/// Compatibility alias for runtime unique index backed by `FixedBufferPool`.
pub type UniqueBTreeIndex = GenericUniqueBTreeIndex<FixedBufferPool>;

impl<P: BufferPool> GenericUniqueBTreeIndex<P> {
    /// Create a unique B-Tree index with key encoder.
    #[inline]
    pub fn new(tree: GenericBTree<P>, encoder: BTreeKeyEncoder) -> Self {
        GenericUniqueBTreeIndex { tree, encoder }
    }

    /// Destroy this unique index and reclaim all backing tree pages.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
        self.tree.destory(pool_guard).await
    }

    /// Insert a live or delete-shadow overlay when the logical key is absent.
    ///
    /// This helper is intentionally concrete to the BTree-backed MemTree so the
    /// dual-tree composite can claim or shadow cold DiskTree owners without
    /// widening the public `UniqueIndex` trait.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn insert_overlay_if_absent(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        let key = self.encoder.encode(key);
        Ok(
            match self
                .tree
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

    /// Insert a cold-owner delete-shadow when the logical key is absent.
    ///
    /// The retained row id remains available for MVCC/key-recheck routing while
    /// preventing a later composite lookup from falling through to stale
    /// DiskTree state.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn insert_delete_shadow_if_absent(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        self.insert_overlay_if_absent(pool_guard, key, row_id.deleted(), ts)
            .await
    }

    /// Scan MemTree entries with encoded logical keys and delete state.
    ///
    /// The returned entries are ordered by encoded key because they are produced
    /// by the underlying BTree leaf cursor.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn scan_encoded_entries(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<Vec<UniqueMemTreeEntry>> {
        let mut entries = Vec::new();
        let mut cursor = self.tree.cursor(pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(guard) = cursor.next().await? {
            let node = guard.page();
            for idx in 0..node.count() {
                let encoded_key = node.key_checked(idx).ok_or(Error::InvalidState)?;
                let value = node.value::<BTreeU64>(idx);
                entries.push(UniqueMemTreeEntry {
                    encoded_key,
                    row_id: value.value().to_u64(),
                    deleted: value.is_deleted(),
                });
            }
        }
        Ok(entries)
    }
}

impl<P: BufferPool> UniqueIndex for GenericUniqueBTreeIndex<P> {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        _ts: TrxID,
    ) -> Result<Option<(RowID, bool)>> {
        let k = self.encoder.encode(key);
        Ok(self
            .tree
            .lookup_optimistic::<BTreeU64>(pool_guard, k.as_bytes())
            .await?
            .map(|res| (res.value().to_u64(), res.is_deleted())))
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
        let k = self.encoder.encode(key);
        Ok(
            match self
                .tree
                .insert::<BTreeU64>(
                    pool_guard,
                    k.as_bytes(),
                    BTreeU64::from(row_id),
                    merge_if_match_deleted,
                    ts,
                )
                .await?
            {
                BTreeInsert::Ok(merged) => IndexInsert::Ok(merged),
                BTreeInsert::DuplicateKey(res) => {
                    IndexInsert::DuplicateKey(res.value().to_u64(), res.is_deleted())
                }
            },
        )
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
        let k = self.encoder.encode(key);
        Ok(
            match self
                .tree
                .delete(
                    pool_guard,
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
        pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        let k = self.encoder.encode(key);
        Ok(
            match self
                .tree
                .update(
                    pool_guard,
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
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let mut cursor = self.tree.cursor(pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(g) = cursor.next().await? {
            g.page().values(values, BTreeU64::to_u64);
        }
        Ok(())
    }
}

impl<T: Hash + Ord + EncodeKeySelf + Send + Sync + 'static> UniqueIndex
    for PartitionSingleKeyIndex<T, false>
{
    #[inline]
    async fn lookup(
        &self,
        _pool_guard: &PoolGuard,
        key: &[Val],
        _ts: TrxID,
    ) -> Result<Option<(RowID, bool)>> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let g = tree.read();
        Ok(g.get(&key).map(|res| {
            (
                BTreeU64::from(*res).value().to_u64(),
                BTreeU64::from(*res).is_deleted(),
            )
        }))
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        _pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        _ts: TrxID,
    ) -> Result<IndexInsert> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        Ok(match g.entry(key) {
            Entry::Occupied(mut occ) => {
                let v = *occ.get();
                if merge_if_match_deleted && v.is_deleted() && v.value() == row_id {
                    *occ.get_mut() = row_id;
                    return Ok(IndexInsert::Ok(true));
                }
                IndexInsert::DuplicateKey(
                    BTreeU64::from(v).value().to_u64(),
                    BTreeU64::from(v).is_deleted(),
                )
            }
            Entry::Vacant(vac) => {
                vac.insert(row_id);
                IndexInsert::Ok(false)
            }
        })
    }

    #[inline]
    async fn compare_delete(
        &self,
        _pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        _ts: TrxID,
    ) -> Result<bool> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        Ok(match g.entry(key) {
            Entry::Occupied(occ) => {
                let index_row_id = *occ.get();
                if index_row_id == old_row_id && (ignore_del_mask || index_row_id.is_deleted()) {
                    occ.remove();
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(_) => true,
        })
    }

    #[inline]
    async fn compare_exchange(
        &self,
        _pool_guard: &PoolGuard,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        _ts: TrxID,
    ) -> Result<IndexCompareExchange> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        Ok(match g.get_mut(&key) {
            Some(row_id) => {
                if *row_id == old_row_id {
                    *row_id = new_row_id;
                    IndexCompareExchange::Ok
                } else {
                    IndexCompareExchange::Mismatch
                }
            }
            None => IndexCompareExchange::NotExists,
        })
    }

    #[inline]
    async fn scan_values(
        &self,
        _pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        for tree in &self.0 {
            let g = tree.read();
            values.extend(g.values());
        }
        Ok(())
    }
}

impl UniqueIndex for PartitionMultiKeyIndex {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        keys: &[Val],
        ts: TrxID,
    ) -> Result<Option<(RowID, bool)>> {
        let encoded = self.encode(keys);
        let key = std::slice::from_ref(&encoded);
        self.index.lookup(pool_guard, key, ts).await
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
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index
            .insert_if_not_exists(pool_guard, key, row_id, merge_if_match_deleted, ts)
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
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index
            .compare_delete(pool_guard, key, old_row_id, ignore_del_mask, ts)
            .await
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
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index
            .compare_exchange(pool_guard, key, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> Result<()> {
        self.index.scan_values(pool_guard, values, ts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::index::btree::BTree;
    use crate::index::secondary_index::multi_key_encoder;
    use crate::quiescent::QuiescentBox;
    use crate::value::{ValKind, ValType};

    #[test]
    fn test_single_key_partition_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 1024usize * 1024)
                    .unwrap(),
            );
            let guard = (*pool).pool_guard();
            let index = PartitionSingleKeyIndex::<i32, false>::empty();
            run_test_suit_for_single_key_unique_index(&index, &guard).await;
        });
    }

    #[test]
    fn test_multi_key_partition_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(crate::buffer::PoolRole::Index, 1024usize * 1024)
                    .unwrap(),
            );
            let guard = (*pool).pool_guard();
            let encoder = multi_key_encoder(vec![
                ValType {
                    kind: ValKind::VarByte,
                    nullable: false,
                },
                ValType {
                    kind: ValKind::I32,
                    nullable: false,
                },
            ]);
            let index = PartitionMultiKeyIndex::empty(encoder);
            run_test_suit_for_multi_key_unique_index(&index, &guard).await;
        })
    }

    #[test]
    fn test_single_key_btree_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(
                    crate::buffer::PoolRole::Index,
                    1024usize * 1024 * 1024,
                )
                .unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let index = UniqueBTreeIndex {
                    tree: BTree::new(pool.guard(), &pool_guard, false, 100)
                        .await
                        .expect("test btree construction should succeed"),
                    encoder: BTreeKeyEncoder::new(vec![ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    }]),
                };
                run_test_suit_for_single_key_unique_index(&index, &pool_guard).await;
            }
        });
    }

    #[test]
    fn test_multi_key_btree_unique_index() {
        smol::block_on(async {
            let pool = QuiescentBox::new(
                FixedBufferPool::with_capacity(
                    crate::buffer::PoolRole::Index,
                    1024usize * 1024 * 1024,
                )
                .unwrap(),
            );
            {
                let pool_guard = (*pool).pool_guard();
                let index = UniqueBTreeIndex {
                    tree: BTree::new(pool.guard(), &pool_guard, false, 100)
                        .await
                        .expect("test btree construction should succeed"),
                    encoder: BTreeKeyEncoder::new(vec![
                        ValType {
                            kind: ValKind::VarByte,
                            nullable: false,
                        },
                        ValType {
                            kind: ValKind::I32,
                            nullable: false,
                        },
                    ]),
                };
                run_test_suit_for_multi_key_unique_index(&index, &pool_guard).await;
            }
        });
    }

    async fn run_test_suit_for_single_key_unique_index<T: UniqueIndex>(
        index: &T,
        pool_guard: &PoolGuard,
    ) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert!(
            index
                .insert_if_not_exists(pool_guard, &key, row_id, false, 100)
                .await
                .unwrap()
                .is_ok()
        );

        // 测试查找
        assert_eq!(
            index.lookup(pool_guard, &key, 100).await.unwrap(),
            Some((row_id, false))
        );

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        assert_eq!(
            index
                .lookup(pool_guard, &non_existent_key, 100)
                .await
                .unwrap(),
            None
        );

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index
            .insert_if_not_exists(pool_guard, &key, new_row_id, false, 100)
            .await
            .unwrap();
        assert_eq!(old_row_id, IndexInsert::DuplicateKey(row_id, false));
        assert_eq!(
            index.lookup(pool_guard, &key, 100).await.unwrap(),
            Some((row_id, false))
        );

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(pool_guard, &key, row_id, true, 100)
                .await
                .unwrap()
        );
        assert_eq!(index.lookup(pool_guard, &key, 100).await.unwrap(), None);

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(pool_guard, &key, new_row_id, false, 100)
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
                .insert_if_not_exists(pool_guard, &key, row_id1, false, 100)
                .await
                .unwrap()
                .is_ok(),
        );

        // 测试成功的 compare_exchange
        assert!(
            index
                .compare_exchange(pool_guard, &key, row_id1, row_id2, 100)
                .await
                .unwrap()
                == IndexCompareExchange::Ok
        );
        assert_eq!(
            index.lookup(pool_guard, &key, 100).await.unwrap(),
            Some((row_id2, false))
        );

        // 测试失败的 compare_exchange
        assert!(
            index
                .compare_exchange(pool_guard, &key, row_id1, row_id2, 100)
                .await
                .unwrap()
                == IndexCompareExchange::Mismatch
        );

        // 测试用例5：scan_values 操作
        let mut values = Vec::new();
        index
            .scan_values(pool_guard, &mut values, 100)
            .await
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], row_id2);

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
                .insert_if_not_exists(pool_guard, &key1, row_id1, false, 100)
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(pool_guard, &key2, row_id2, false, 100)
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(pool_guard, &key3, row_id3, false, 100)
                .await
                .unwrap()
                .is_ok()
        );

        // 验证所有键都能正确查找
        assert_eq!(
            index.lookup(pool_guard, &key1, 100).await.unwrap(),
            Some((row_id1, false))
        );
        assert_eq!(
            index.lookup(pool_guard, &key2, 100).await.unwrap(),
            Some((row_id2, false))
        );
        assert_eq!(
            index.lookup(pool_guard, &key3, 100).await.unwrap(),
            Some((row_id3, false))
        );

        // 验证 scan_values 包含所有值
        let mut values = Vec::new();
        index
            .scan_values(pool_guard, &mut values, 100)
            .await
            .unwrap();
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2
    }

    async fn run_test_suit_for_multi_key_unique_index<T: UniqueIndex>(
        index: &T,
        pool_guard: &PoolGuard,
    ) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from("hello"), Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert!(
            index
                .insert_if_not_exists(pool_guard, &key, row_id, false, 100)
                .await
                .unwrap()
                .is_ok()
        );

        // 测试查找
        assert_eq!(
            index.lookup(pool_guard, &key, 100).await.unwrap(),
            Some((row_id, false))
        );

        // 测试不存在的键
        let non_existent_key = vec![Val::from("hello"), Val::from(43i32)];
        assert_eq!(
            index
                .lookup(pool_guard, &non_existent_key, 100)
                .await
                .unwrap(),
            None
        );

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index
            .insert_if_not_exists(pool_guard, &key, new_row_id, false, 100)
            .await
            .unwrap();
        assert_eq!(old_row_id, IndexInsert::DuplicateKey(row_id, false));
        assert_eq!(
            index.lookup(pool_guard, &key, 100).await.unwrap(),
            Some((row_id, false))
        );

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(pool_guard, &key, row_id, true, 100)
                .await
                .unwrap()
        );
        assert_eq!(index.lookup(pool_guard, &key, 100).await.unwrap(), None);

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(pool_guard, &key, new_row_id, false, 100)
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
                .insert_if_not_exists(pool_guard, &key, row_id1, false, 100)
                .await
                .unwrap()
                .is_ok()
        );

        // 测试成功的 compare_exchange
        assert!(
            index
                .compare_exchange(pool_guard, &key, row_id1, row_id2, 100)
                .await
                .unwrap()
                == IndexCompareExchange::Ok
        );
        assert_eq!(
            index.lookup(pool_guard, &key, 100).await.unwrap(),
            Some((row_id2, false))
        );

        // 测试失败的 compare_exchange
        assert!(
            index
                .compare_exchange(pool_guard, &key, row_id1, row_id2, 100)
                .await
                .unwrap()
                == IndexCompareExchange::Mismatch
        );

        // 测试用例5：scan_values 操作
        let mut values = Vec::new();
        index
            .scan_values(pool_guard, &mut values, 100)
            .await
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], row_id2);

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
                .insert_if_not_exists(pool_guard, &key1, row_id1, false, 100)
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(pool_guard, &key2, row_id2, false, 100)
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(pool_guard, &key3, row_id3, false, 100)
                .await
                .unwrap()
                .is_ok()
        );

        // 验证所有键都能正确查找
        assert_eq!(
            index.lookup(pool_guard, &key1, 100).await.unwrap(),
            Some((row_id1, false))
        );
        assert_eq!(
            index.lookup(pool_guard, &key2, 100).await.unwrap(),
            Some((row_id2, false))
        );
        assert_eq!(
            index.lookup(pool_guard, &key3, 100).await.unwrap(),
            Some((row_id3, false))
        );

        // 验证 scan_values 包含所有值
        let mut values = Vec::new();
        index
            .scan_values(pool_guard, &mut values, 100)
            .await
            .unwrap();
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2

        // 验证insert覆盖
        let key4 = vec![Val::from("rust"), Val::from(97i32)];
        let row_id4 = 800u64;
        let inserted = index
            .insert_if_not_exists(pool_guard, &key4, row_id4, false, 100)
            .await
            .unwrap();
        assert!(inserted.is_ok());
        let masked = index
            .mask_as_deleted(pool_guard, &key4, row_id4, 100)
            .await
            .unwrap();
        assert!(masked);
        let inserted = index
            .insert_if_not_exists(pool_guard, &key4, row_id4, true, 100)
            .await
            .unwrap();
        assert!(inserted.is_merged());
        assert_eq!(
            index.lookup(pool_guard, &key4, 100).await.unwrap(),
            Some((row_id4, false))
        );
    }
}
