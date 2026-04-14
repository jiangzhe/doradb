use crate::buffer::guard::PageGuard;
use crate::buffer::{BufferPool, FixedBufferPool, PoolGuard};
use crate::error::{Error, Result};
use crate::index::btree::{BTreeDelete, BTreeInsert, BTreeUpdate, GenericBTree};
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
    fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        res: &mut Vec<RowID>,
        ts: TrxID,
    ) -> impl Future<Output = Result<()>>;

    /// Lookup key and value(row_id) uniquely.
    /// Return None if not found.
    /// Return Some(true) if found and exist(not masked as deleted).
    fn lookup_unique(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Result<Option<bool>>>;

    /// Insert key value pair into the index.
    /// Value is always RowID.
    fn insert_if_not_exists(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        merge_if_match_deleted: bool,
        ts: TrxID,
    ) -> impl Future<Output = Result<IndexInsert>>;

    /// Mask a given key value as deleted.
    fn mask_as_deleted(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Result<bool>>;

    /// Mask a given key value as not deleted.
    fn mask_as_active(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Result<bool>>;

    /// Delete key value pair from the index.
    /// Value is always RowID.
    fn compare_delete(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> impl Future<Output = Result<bool>>;

    /// Scan values into given collection.
    fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        ts: TrxID,
    ) -> impl Future<Output = Result<()>>;
}

/// Generic non-unique-index implementation backed by a generic B-Tree.
pub struct GenericNonUniqueBTreeIndex<P: 'static> {
    tree: GenericBTree<P>,
    encoder: BTreeKeyEncoder,
}

/// Encoded MemTree state for one non-unique exact secondary-index entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct NonUniqueMemTreeEntry {
    /// Encoded exact key in BTree order, including the row-id suffix.
    pub(crate) encoded_key: Vec<u8>,
    /// Row id decoded from the exact key suffix.
    pub(crate) row_id: RowID,
    /// Whether the exact entry is delete-marked in MemTree.
    pub(crate) deleted: bool,
}

/// Compatibility alias for runtime non-unique index backed by `FixedBufferPool`.
pub type NonUniqueBTreeIndex = GenericNonUniqueBTreeIndex<FixedBufferPool>;

impl<P: BufferPool> GenericNonUniqueBTreeIndex<P> {
    /// Create a non-unique B-Tree index with key encoder.
    #[inline]
    pub fn new(tree: GenericBTree<P>, encoder: BTreeKeyEncoder) -> Self {
        GenericNonUniqueBTreeIndex { tree, encoder }
    }

    /// Destroy this non-unique index and reclaim all backing tree pages.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> Result<()> {
        self.tree.destory(pool_guard).await
    }

    /// Insert a delete-marked exact overlay when the exact key is absent.
    ///
    /// This helper is intentionally concrete to the BTree-backed MemTree so the
    /// dual-tree composite can shadow cold DiskTree exact entries without
    /// widening the public `NonUniqueIndex` trait.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn insert_delete_overlay_if_absent(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        let key = self.encoder.encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .tree
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

    /// Scan exact MemTree entries for one logical key with encoded keys.
    ///
    /// The returned entries are ordered by encoded exact key and include
    /// delete-marked overlays so the composite can suppress matching DiskTree
    /// exact entries.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn lookup_encoded_entries(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
    ) -> Result<Vec<NonUniqueMemTreeEntry>> {
        let key = self
            .encoder
            .encode_prefix(key, Some(mem::size_of::<RowID>()));
        let mut entries = Vec::new();
        let mut scanner = self
            .tree
            .prefix_scanner(pool_guard, CollectEncodedExactEntries(&mut entries));
        scanner.scan_prefix(key.as_bytes()).await?;
        Ok(entries)
    }

    /// Scan all exact MemTree entries with encoded keys and delete state.
    ///
    /// The returned entries are ordered by encoded exact key because they are
    /// produced by the underlying BTree leaf cursor.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn scan_encoded_entries(
        &self,
        pool_guard: &PoolGuard,
    ) -> Result<Vec<NonUniqueMemTreeEntry>> {
        let mut entries = Vec::new();
        let mut cursor = self.tree.cursor(pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(guard) = cursor.next().await? {
            let node = guard.page();
            for idx in 0..node.count() {
                let slot = node.slot(idx);
                push_encoded_exact_entry(node, slot, &mut entries)?;
            }
        }
        Ok(entries)
    }
}

impl<P: BufferPool> NonUniqueIndex for GenericNonUniqueBTreeIndex<P> {
    #[inline]
    async fn lookup(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        res: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let k = self
            .encoder
            .encode_prefix(key, Some(mem::size_of::<RowID>()));
        let mut scanner = self.tree.prefix_scanner(pool_guard, CollectRowID(res));
        scanner.scan_prefix(k.as_bytes()).await
    }

    #[inline]
    async fn lookup_unique(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        _ts: TrxID,
    ) -> Result<Option<bool>> {
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        Ok(self
            .tree
            .lookup_optimistic::<BTreeByte>(pool_guard, k.as_bytes())
            .await?
            .map(|v| !v.is_deleted()))
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
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .tree
                .insert::<BTreeByte>(
                    pool_guard,
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
    async fn mask_as_deleted(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .tree
                .update(
                    pool_guard,
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
    async fn mask_as_active(
        &self,
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .tree
                .update(
                    pool_guard,
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
        pool_guard: &PoolGuard,
        key: &[Val],
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> Result<bool> {
        debug_assert!(!row_id.is_deleted());
        let k = self.encoder.encode_pair(key, Val::from(row_id));
        Ok(
            match self
                .tree
                .delete(
                    pool_guard,
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
    async fn scan_values(
        &self,
        pool_guard: &PoolGuard,
        values: &mut Vec<RowID>,
        _ts: TrxID,
    ) -> Result<()> {
        let mut cursor = self.tree.cursor(pool_guard, 0);
        cursor.seek(&[]).await?;
        while let Some(g) = cursor.next().await? {
            let node = g.page();
            for slot in node.slots() {
                let value = node.unpack_value::<BTreeU64>(slot);
                values.push(value.to_u64());
            }
        }
        Ok(())
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

#[cfg_attr(not(test), allow(dead_code))]
struct CollectEncodedExactEntries<'a>(&'a mut Vec<NonUniqueMemTreeEntry>);

impl BTreeSlotCallback for CollectEncodedExactEntries<'_> {
    #[inline]
    fn apply(&mut self, node: &BTreeNode, slot: &BTreeSlot) -> bool {
        // In-memory BTree slot data has already been validated by the scanner.
        push_encoded_exact_entry(node, slot, self.0).is_ok()
    }
}

#[inline]
#[cfg_attr(not(test), allow(dead_code))]
fn push_encoded_exact_entry(
    node: &BTreeNode,
    slot: &BTreeSlot,
    entries: &mut Vec<NonUniqueMemTreeEntry>,
) -> Result<()> {
    let mut encoded_key = Vec::new();
    node.extend_slot_key(slot, &mut encoded_key);
    if encoded_key.len() < mem::size_of::<RowID>() {
        return Err(Error::InvalidState);
    }
    let row_id = node.unpack_value::<BTreeU64>(slot).to_u64();
    let value = node.value_for_slot::<BTreeByte>(slot);
    entries.push(NonUniqueMemTreeEntry {
        encoded_key,
        row_id,
        deleted: value.is_deleted(),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::FixedBufferPool;
    use crate::index::btree::BTree;
    use crate::quiescent::QuiescentBox;
    use crate::value::{ValKind, ValType};

    #[test]
    fn test_non_unique_index() {
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
                // i32 column and row id
                let index = NonUniqueBTreeIndex {
                    tree: BTree::new(pool.guard(), &pool_guard, true, 100)
                        .await
                        .expect("test btree construction should succeed"),
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
                run_test_suit_for_non_unique_index(&index, &pool_guard).await;
            }
        })
    }

    async fn run_test_suit_for_non_unique_index<T: NonUniqueIndex>(
        index: &T,
        pool_guard: &PoolGuard,
    ) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        index
            .insert_if_not_exists(pool_guard, &key, row_id, false, 100)
            .await
            .unwrap();

        // 测试查找
        let mut res = vec![];
        index.lookup(pool_guard, &key, &mut res, 100).await.unwrap();
        assert_eq!(res.len(), 1);

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        res.clear();
        index
            .lookup(pool_guard, &non_existent_key, &mut res, 100)
            .await
            .unwrap();
        assert!(res.is_empty());

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        index
            .insert_if_not_exists(pool_guard, &key, new_row_id, false, 100)
            .await
            .unwrap();
        // non-unique index allow duplicate key, but row id must be different.
        res.clear();
        index.lookup(pool_guard, &key, &mut res, 100).await.unwrap();
        assert_eq!(res.len(), 2);

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(pool_guard, &key, row_id, true, 100)
                .await
                .unwrap()
        );
        res.clear();
        index.lookup(pool_guard, &key, &mut res, 100).await.unwrap();
        assert_eq!(res.len(), 1);

        // 测试删除不存在的键 still ok
        assert!(
            index
                .compare_delete(pool_guard, &key, 1000, false, 100)
                .await
                .unwrap()
        );

        // 测试用例4：scan_values 操作
        res.clear();
        let mut values = vec![];
        index
            .scan_values(pool_guard, &mut values, 100)
            .await
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], new_row_id);

        // 测试用例5：多分区操作
        let key1 = vec![Val::from(1i32)];
        let key2 = vec![Val::from(2i32)];
        let key3 = vec![Val::from(3i32)];

        let row_id1 = 500u64;
        let row_id2 = 600u64;
        let row_id3 = 700u64;

        // 插入多个键值对
        index
            .insert_if_not_exists(pool_guard, &key1, row_id1, false, 100)
            .await
            .unwrap();
        index
            .insert_if_not_exists(pool_guard, &key2, row_id2, false, 100)
            .await
            .unwrap();
        index
            .insert_if_not_exists(pool_guard, &key3, row_id3, false, 100)
            .await
            .unwrap();

        // 验证所有键都能正确查找
        res.clear();
        index
            .lookup(pool_guard, &key1, &mut res, 100)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        res.clear();
        index
            .lookup(pool_guard, &key2, &mut res, 100)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        res.clear();
        index
            .lookup(pool_guard, &key3, &mut res, 100)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);

        // 验证 scan_values 包含所有值
        res.clear();
        let mut values = vec![];
        index
            .scan_values(pool_guard, &mut values, 100)
            .await
            .unwrap();
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2

        // 验证insert覆盖
        let key4 = vec![Val::from(97i32)];
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
        res.clear();
        index
            .lookup(pool_guard, &key4, &mut res, 100)
            .await
            .unwrap();
        assert_eq!(res[0], row_id4);
    }
}
