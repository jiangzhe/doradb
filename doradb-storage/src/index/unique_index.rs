use crate::buffer::guard::PageGuard;
use crate::index::IndexCompareExchange;
use crate::index::btree::{BTree, BTreeDelete, BTreeInsert, BTreeUpdate};
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
    fn lookup(&self, key: &[Val], ts: TrxID) -> impl Future<Output = Option<(RowID, bool)>>;

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
    ) -> impl Future<Output = IndexInsert>;

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
    ) -> impl Future<Output = bool>;

    /// Mask a given key value as deleted.
    #[inline]
    fn mask_as_deleted(&self, key: &[Val], row_id: RowID, ts: TrxID) -> impl Future<Output = bool> {
        debug_assert!(!row_id.is_deleted());
        let new_row_id = row_id.deleted();
        self.compare_exchange(key, row_id, new_row_id, ts)
            .map(|res| match res {
                IndexCompareExchange::Ok => true,
                IndexCompareExchange::Mismatch | IndexCompareExchange::NotExists => false,
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
    ) -> impl Future<Output = IndexCompareExchange>;

    /// Scan values into given collection.
    fn scan_values(&self, values: &mut Vec<RowID>, ts: TrxID) -> impl Future<Output = ()>;
}

pub struct UniqueBTreeIndex {
    tree: BTree,
    encoder: BTreeKeyEncoder,
}

impl UniqueBTreeIndex {
    #[inline]
    pub fn new(tree: BTree, encoder: BTreeKeyEncoder) -> Self {
        UniqueBTreeIndex { tree, encoder }
    }
}

impl UniqueIndex for UniqueBTreeIndex {
    #[inline]
    async fn lookup(&self, key: &[Val], _ts: TrxID) -> Option<(RowID, bool)> {
        let k = self.encoder.encode(key);
        self.tree
            .lookup_optimistic::<BTreeU64>(k.as_bytes())
            .await
            .map(|res| (res.value().to_u64(), res.is_deleted()))
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> IndexInsert {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode(key);
        match self
            .tree
            .insert::<BTreeU64>(
                k.as_bytes(),
                BTreeU64::from(row_id),
                merge_if_match_deleted,
                ts,
            )
            .await
        {
            BTreeInsert::Ok(merged) => IndexInsert::Ok(merged),
            BTreeInsert::DuplicateKey(res) => {
                IndexInsert::DuplicateKey(res.value().to_u64(), res.is_deleted())
            }
        }
    }

    #[inline]
    async fn compare_delete(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> bool {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode(key);
        match self
            .tree
            .delete(k.as_bytes(), BTreeU64::from(row_id), ignore_del_mask, ts)
            .await
        {
            // Treat not found as success.
            BTreeDelete::Ok | BTreeDelete::NotFound => true,
            BTreeDelete::ValueMismatch => false,
        }
    }

    #[inline]
    async fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> IndexCompareExchange {
        let k = self.encoder.encode(key);
        match self
            .tree
            .update(
                k.as_bytes(),
                BTreeU64::from(old_row_id),
                BTreeU64::from(new_row_id),
                ts,
            )
            .await
        {
            BTreeUpdate::Ok(row_id) => {
                debug_assert!(BTreeU64::from(old_row_id) == row_id);
                IndexCompareExchange::Ok
            }
            BTreeUpdate::NotFound => IndexCompareExchange::NotExists,
            BTreeUpdate::ValueMismatch(_) => IndexCompareExchange::Mismatch,
        }
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) {
        let mut cursor = self.tree.cursor(0);
        cursor.seek(&[]).await;
        while let Some(g) = cursor.next().await {
            g.page().values(values, BTreeU64::to_u64);
        }
    }
}

impl<T: Hash + Ord + EncodeKeySelf + Send + Sync + 'static> UniqueIndex
    for PartitionSingleKeyIndex<T, false>
{
    #[inline]
    async fn lookup(&self, key: &[Val], _ts: TrxID) -> Option<(RowID, bool)> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let g = tree.read();
        g.get(&key).map(|res| {
            (
                BTreeU64::from(*res).value().to_u64(),
                BTreeU64::from(*res).is_deleted(),
            )
        })
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        _ts: TrxID,
    ) -> IndexInsert {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.entry(key) {
            Entry::Occupied(mut occ) => {
                let v = *occ.get();
                if merge_if_match_deleted && v.is_deleted() && v.value() == row_id {
                    *occ.get_mut() = row_id;
                    return IndexInsert::Ok(true);
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
        }
    }

    #[inline]
    async fn compare_delete(
        &self,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        _ts: TrxID,
    ) -> bool {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.entry(key) {
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
        }
    }

    #[inline]
    async fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
        _ts: TrxID,
    ) -> IndexCompareExchange {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.get_mut(&key) {
            Some(row_id) => {
                if *row_id == old_row_id {
                    *row_id = new_row_id;
                    IndexCompareExchange::Ok
                } else {
                    IndexCompareExchange::Mismatch
                }
            }
            None => IndexCompareExchange::NotExists,
        }
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) {
        for tree in &self.0 {
            let g = tree.read();
            values.extend(g.values());
        }
    }
}

impl UniqueIndex for PartitionMultiKeyIndex {
    #[inline]
    async fn lookup(&self, keys: &[Val], ts: TrxID) -> Option<(RowID, bool)> {
        let encoded = self.encode(keys);
        let key = std::slice::from_ref(&encoded);
        self.index.lookup(key, ts).await
    }

    #[inline]
    async fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> IndexInsert {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index
            .insert_if_not_exists(key, row_id, merge_if_match_deleted, ts)
            .await
    }

    #[inline]
    async fn compare_delete(
        &self,
        key: &[Val],
        old_row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> bool {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index
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
    ) -> IndexCompareExchange {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index
            .compare_exchange(key, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, ts: TrxID) {
        self.index.scan_values(values, ts).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::index::secondary_index::multi_key_encoder;
    use crate::lifetime::StaticLifetimeScope;
    use crate::value::{ValKind, ValType};

    #[test]
    fn test_single_key_partition_unique_index() {
        smol::block_on(async {
            let index = PartitionSingleKeyIndex::<i32, false>::empty();
            run_test_suit_for_single_key_unique_index(&index).await;
        });
    }

    #[test]
    fn test_multi_key_partition_unique_index() {
        smol::block_on(async {
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
            run_test_suit_for_multi_key_unique_index(&index).await;
        })
    }

    #[test]
    fn test_single_key_btree_unique_index() {
        smol::block_on(async {
            let scope = StaticLifetimeScope::new();
            let pool = scope
                .adopt(FixedBufferPool::with_capacity_static(1024usize * 1024 * 1024).unwrap());
            let pool = pool.as_static();
            {
                let index = UniqueBTreeIndex {
                    tree: BTree::new(pool, false, 100).await,
                    encoder: BTreeKeyEncoder::new(vec![ValType {
                        kind: ValKind::I32,
                        nullable: false,
                    }]),
                };
                run_test_suit_for_single_key_unique_index(&index).await;
            }
        });
    }

    #[test]
    fn test_multi_key_btree_unique_index() {
        smol::block_on(async {
            let scope = StaticLifetimeScope::new();
            let pool = scope
                .adopt(FixedBufferPool::with_capacity_static(1024usize * 1024 * 1024).unwrap());
            let pool = pool.as_static();
            {
                let index = UniqueBTreeIndex {
                    tree: BTree::new(pool, false, 100).await,
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
                run_test_suit_for_multi_key_unique_index(&index).await;
            }
        });
    }

    async fn run_test_suit_for_single_key_unique_index<T: UniqueIndex>(index: &T) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert!(
            index
                .insert_if_not_exists(&key, row_id, false, 100)
                .await
                .is_ok()
        );

        // 测试查找
        assert_eq!(index.lookup(&key, 100).await, Some((row_id, false)));

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        assert_eq!(index.lookup(&non_existent_key, 100).await, None);

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index
            .insert_if_not_exists(&key, new_row_id, false, 100)
            .await;
        assert_eq!(old_row_id, IndexInsert::DuplicateKey(row_id, false));
        assert_eq!(index.lookup(&key, 100).await.unwrap(), (row_id, false));

        // 测试用例3：删除操作
        assert!(index.compare_delete(&key, row_id, true, 100).await);
        assert_eq!(index.lookup(&key, 100).await, None);

        // 测试删除不存在的键 still ok
        assert!(index.compare_delete(&key, new_row_id, false, 100).await);

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert!(
            index
                .insert_if_not_exists(&key, row_id1, false, 100)
                .await
                .is_ok(),
        );

        // 测试成功的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await == IndexCompareExchange::Ok
        );
        assert_eq!(index.lookup(&key, 100).await, Some((row_id2, false)));

        // 测试失败的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await
                == IndexCompareExchange::Mismatch
        );

        // 测试用例5：scan_values 操作
        let mut values = Vec::new();
        index.scan_values(&mut values, 100).await;
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
                .insert_if_not_exists(&key1, row_id1, false, 100)
                .await
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key2, row_id2, false, 100)
                .await
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key3, row_id3, false, 100)
                .await
                .is_ok()
        );

        // 验证所有键都能正确查找
        assert_eq!(index.lookup(&key1, 100).await, Some((row_id1, false)));
        assert_eq!(index.lookup(&key2, 100).await, Some((row_id2, false)));
        assert_eq!(index.lookup(&key3, 100).await, Some((row_id3, false)));

        // 验证 scan_values 包含所有值
        let mut values = Vec::new();
        index.scan_values(&mut values, 100).await;
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2
    }

    async fn run_test_suit_for_multi_key_unique_index<T: UniqueIndex>(index: &T) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from("hello"), Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert!(
            index
                .insert_if_not_exists(&key, row_id, false, 100)
                .await
                .is_ok()
        );

        // 测试查找
        assert_eq!(index.lookup(&key, 100).await, Some((row_id, false)));

        // 测试不存在的键
        let non_existent_key = vec![Val::from("hello"), Val::from(43i32)];
        assert_eq!(index.lookup(&non_existent_key, 100).await, None);

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index
            .insert_if_not_exists(&key, new_row_id, false, 100)
            .await;
        assert_eq!(old_row_id, IndexInsert::DuplicateKey(row_id, false));
        assert_eq!(index.lookup(&key, 100).await.unwrap(), (row_id, false));

        // 测试用例3：删除操作
        assert!(index.compare_delete(&key, row_id, true, 100).await);
        assert_eq!(index.lookup(&key, 100).await, None);

        // 测试删除不存在的键 still ok
        assert!(index.compare_delete(&key, new_row_id, false, 100).await);

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from("hello"), Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert!(
            index
                .insert_if_not_exists(&key, row_id1, false, 100)
                .await
                .is_ok()
        );

        // 测试成功的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await == IndexCompareExchange::Ok
        );
        assert_eq!(index.lookup(&key, 100).await, Some((row_id2, false)));

        // 测试失败的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await
                == IndexCompareExchange::Mismatch
        );

        // 测试用例5：scan_values 操作
        let mut values = Vec::new();
        index.scan_values(&mut values, 100).await;
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
                .insert_if_not_exists(&key1, row_id1, false, 100)
                .await
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key2, row_id2, false, 100)
                .await
                .is_ok()
        );
        assert!(
            index
                .insert_if_not_exists(&key3, row_id3, false, 100)
                .await
                .is_ok()
        );

        // 验证所有键都能正确查找
        assert_eq!(index.lookup(&key1, 100).await, Some((row_id1, false)));
        assert_eq!(index.lookup(&key2, 100).await, Some((row_id2, false)));
        assert_eq!(index.lookup(&key3, 100).await, Some((row_id3, false)));

        // 验证 scan_values 包含所有值
        let mut values = Vec::new();
        index.scan_values(&mut values, 100).await;
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2

        // 验证insert覆盖
        let key4 = vec![Val::from("rust"), Val::from(97i32)];
        let row_id4 = 800u64;
        let inserted = index.insert_if_not_exists(&key4, row_id4, false, 100).await;
        assert!(inserted.is_ok());
        let masked = index.mask_as_deleted(&key4, row_id4, 100).await;
        assert!(masked);
        let inserted = index.insert_if_not_exists(&key4, row_id4, true, 100).await;
        assert!(inserted.is_merged());
        assert_eq!(index.lookup(&key4, 100).await, Some((row_id4, false)));
    }
}
