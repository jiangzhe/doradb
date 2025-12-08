use crate::buffer::guard::PageGuard;
use crate::index::btree::{BTree, BTreeDelete, BTreeInsert, BTreeUpdate};
use crate::index::btree_key::BTreeKeyEncoder;
use crate::index::btree_node::{BTreeNode, BTreeSlot};
use crate::index::btree_scan::BTreeSlotCallback;
use crate::index::btree_value::{BTREE_BYTE_ZERO, BTreeByte, BTreeU64};
use crate::index::secondary_index::IndexInsert;
use crate::index::util::Maskable;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::Val;
use std::future::Future;
use std::mem;

/// Abstraction of non-unique index.
pub trait NonUniqueIndex: Send + Sync + 'static {
    /// Lookup key and put associated values into given collection.
    fn lookup(&self, key: &[Val], res: &mut Vec<RowID>, ts: TrxID) -> impl Future<Output = ()>;

    /// Lookup key and value(row_id) uniquely.
    /// Return None if not found.
    /// Return Some(true) if found and exist(not masked as deleted).
    fn lookup_unique(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Option<bool>>;

    /// Insert key value pair into the index.
    /// Value is always RowID.
    fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> impl Future<Output = IndexInsert>;

    /// Mask a given key value as deleted.
    fn mask_as_deleted(&self, key: &[Val], row_id: RowID, ts: TrxID) -> impl Future<Output = bool>;

    /// Mask a given key value as not deleted.
    fn mask_as_active(&self, key: &[Val], row_id: RowID, ts: TrxID) -> impl Future<Output = bool>;

    /// Delete key value pair from the index.
    /// Value is always RowID.
    fn compare_delete(
        &self,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> impl Future<Output = bool>;

    /// Scan values into given collection.
    fn scan_values(&self, values: &mut Vec<RowID>, ts: TrxID) -> impl Future<Output = ()>;
}

pub struct NonUniqueBTreeIndex {
    tree: BTree,
    encoder: BTreeKeyEncoder,
}

impl NonUniqueBTreeIndex {
    #[inline]
    pub fn new(tree: BTree, encoder: BTreeKeyEncoder) -> Self {
        NonUniqueBTreeIndex { tree, encoder }
    }
}

impl NonUniqueIndex for NonUniqueBTreeIndex {
    #[inline]
    async fn lookup(&self, key: &[Val], res: &mut Vec<RowID>, _ts: TrxID) {
        let k = self
            .encoder
            .encode_prefix(key, Some(mem::size_of::<RowID>()));
        let mut scanner = self.tree.prefix_scanner(CollectRowID(res));
        scanner.scan_prefix(k.as_bytes()).await;
    }

    #[inline]
    async fn lookup_unique(&self, key: &[Val], row_id: RowID, _ts: TrxID) -> Option<bool> {
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        self.tree
            .lookup_optimistic::<BTreeByte>(k.as_bytes())
            .await
            .map(|v| !v.is_deleted())
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
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        match self
            .tree
            .insert::<BTreeByte>(k.as_bytes(), BTREE_BYTE_ZERO, merge_if_match_deleted, ts)
            .await
        {
            BTreeInsert::Ok(merged) => IndexInsert::Ok(merged),
            BTreeInsert::DuplicateKey(v) => IndexInsert::DuplicateKey(row_id, v.is_deleted()),
        }
    }

    #[inline]
    async fn mask_as_deleted(&self, key: &[Val], row_id: RowID, ts: TrxID) -> bool {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        match self
            .tree
            .update(k.as_bytes(), BTREE_BYTE_ZERO, BTREE_BYTE_ZERO.deleted(), ts)
            .await
        {
            BTreeUpdate::Ok(_) => true,
            BTreeUpdate::NotFound | BTreeUpdate::ValueMismatch(_) => false,
        }
    }

    #[inline]
    async fn mask_as_active(&self, key: &[Val], row_id: RowID, ts: TrxID) -> bool {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        match self
            .tree
            .update(k.as_bytes(), BTREE_BYTE_ZERO.deleted(), BTREE_BYTE_ZERO, ts)
            .await
        {
            BTreeUpdate::Ok(_) => true,
            BTreeUpdate::NotFound | BTreeUpdate::ValueMismatch(_) => false,
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
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        match self
            .tree
            .delete(k.as_bytes(), BTREE_BYTE_ZERO, ignore_del_mask, ts)
            .await
        {
            BTreeDelete::Ok | BTreeDelete::NotFound => true,
            BTreeDelete::ValueMismatch => false,
        }
    }

    #[inline]
    async fn scan_values(&self, values: &mut Vec<RowID>, _ts: TrxID) -> () {
        let mut cursor = self.tree.cursor(0);
        cursor.seek(&[]).await;
        while let Some(g) = cursor.next().await {
            let node = g.page();
            for slot in node.slots() {
                let value = node.unpack_value::<BTreeU64>(slot);
                values.push(value.to_u64());
            }
        }
    }
}

struct CollectRowID<'a>(&'a mut Vec<RowID>);

impl BTreeSlotCallback for CollectRowID<'_> {
    #[inline]
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> bool {
        // todo: with covering index support, we may further skip deleted row ids.
        let value = node.unpack_value::<BTreeU64>(slot);
        // The result collection may contains deleted row id.
        self.0.push(value.to_u64());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::lifetime::StaticLifetime;
    use crate::value::{ValKind, ValType};

    #[test]
    fn test_non_unique_index() {
        smol::block_on(async {
            let pool = FixedBufferPool::with_capacity_static(1024usize * 1024 * 1024).unwrap();
            {
                // i32 column and row id
                let index = NonUniqueBTreeIndex {
                    tree: BTree::new(pool, true, 100).await,
                    encoder: BTreeKeyEncoder::new(vec![
                        ValType {
                            kind: ValKind::I32,
                            nullable: false,
                        },
                        ValType {
                            kind: ValKind::U64,
                            nullable: false,
                        },
                    ]),
                };
                run_test_suit_for_non_unique_index(&index).await;
            }
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        })
    }

    async fn run_test_suit_for_non_unique_index<T: NonUniqueIndex>(index: &T) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        index.insert_if_not_exists(&key, row_id, false, 100).await;

        // 测试查找
        let mut res = vec![];
        index.lookup(&key, &mut res, 100).await;
        assert_eq!(res.len(), 1);

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        res.clear();
        index.lookup(&non_existent_key, &mut res, 100).await;
        assert!(res.is_empty());

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        index
            .insert_if_not_exists(&key, new_row_id, false, 100)
            .await;
        // non-unique index allow duplicate key, but row id must be different.
        res.clear();
        index.lookup(&key, &mut res, 100).await;
        assert_eq!(res.len(), 2);

        // 测试用例3：删除操作
        assert!(index.compare_delete(&key, row_id, true, 100).await);
        res.clear();
        index.lookup(&key, &mut res, 100).await;
        assert_eq!(res.len(), 1);

        // 测试删除不存在的键 still ok
        assert!(index.compare_delete(&key, 1000, false, 100).await);

        // 测试用例4：scan_values 操作
        res.clear();
        index.scan_values(&mut res, 100).await;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], new_row_id);

        // 测试用例5：多分区操作
        let key1 = vec![Val::from(1i32)];
        let key2 = vec![Val::from(2i32)];
        let key3 = vec![Val::from(3i32)];

        let row_id1 = 500u64;
        let row_id2 = 600u64;
        let row_id3 = 700u64;

        // 插入多个键值对
        index.insert_if_not_exists(&key1, row_id1, false, 100).await;
        index.insert_if_not_exists(&key2, row_id2, false, 100).await;
        index.insert_if_not_exists(&key3, row_id3, false, 100).await;

        // 验证所有键都能正确查找
        res.clear();
        index.lookup(&key1, &mut res, 100).await;
        assert_eq!(res.len(), 1);
        res.clear();
        index.lookup(&key2, &mut res, 100).await;
        assert_eq!(res.len(), 1);
        res.clear();
        index.lookup(&key3, &mut res, 100).await;
        assert_eq!(res.len(), 1);

        // 验证 scan_values 包含所有值
        res.clear();
        index.scan_values(&mut res, 100).await;
        assert_eq!(res.len(), 4); // 包含之前插入的 row_id2

        // 验证insert覆盖
        let key4 = vec![Val::from(97i32)];
        let row_id4 = 800u64;
        let inserted = index.insert_if_not_exists(&key4, row_id4, false, 100).await;
        assert!(inserted.is_ok());
        let masked = index.mask_as_deleted(&key4, row_id4, 100).await;
        assert!(masked);
        let inserted = index.insert_if_not_exists(&key4, row_id4, true, 100).await;
        assert!(inserted.is_merged());
        res.clear();
        index.lookup(&key4, &mut res, 100).await;
        assert_eq!(res[0], row_id4);
    }
}
