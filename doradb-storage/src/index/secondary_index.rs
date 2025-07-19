use crate::buffer::guard::PageGuard;
use crate::buffer::FixedBufferPool;
use crate::index::btree::{BTree, BTreeDelete, BTreeInsert, BTreeUpdate};
use crate::index::btree_key::{BTreeKey, BTreeKeyEncoder};
use crate::index::btree_value::BTreeU64;
use crate::index::util::Maskable;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::{Val, ValType};
use doradb_catalog::IndexSpec;
use doradb_datatype::konst::{ValidF32, ValidF64};
use either::Either;
use futures::FutureExt;
use parking_lot::RwLock;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem::MaybeUninit;
use std::sync::Arc;

pub struct SecondaryIndex {
    pub index_no: usize,
    pub kind: IndexKind,
}

impl SecondaryIndex {
    #[inline]
    pub async fn new<F: Fn(usize) -> ValType>(
        index_pool: &'static FixedBufferPool,
        index_no: usize,
        index_spec: &IndexSpec,
        ty_infer: F,
        ts: TrxID,
    ) -> Self {
        debug_assert!(!index_spec.index_cols.is_empty());
        if index_spec.unique() {
            let types: Vec<_> = index_spec
                .index_cols
                .iter()
                .map(|key| ty_infer(key.col_no as usize))
                .collect();
            let encoder = BTreeKeyEncoder::new(types);
            let tree = BTree::new(index_pool, true, ts).await;
            let kind = IndexKind::Unique(UniqueBTreeIndex { tree, encoder });
            SecondaryIndex { index_no, kind }
        } else {
            // create non-unique index
            SecondaryIndex {
                index_no,
                kind: IndexKind::non_unique(PartitionNonUniqueIndex {}),
            }
        }
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        matches!(self.kind, IndexKind::Unique(_))
    }

    #[inline]
    pub fn unique(&self) -> Option<&UniqueBTreeIndex> {
        match &self.kind {
            IndexKind::Unique(idx) => Some(idx),
            _ => None,
        }
    }

    #[inline]
    pub fn non_unique(&self) -> Option<&dyn NonUniqueIndex> {
        match &self.kind {
            IndexKind::NonUnique(idx) => Some(&**idx),
            _ => None,
        }
    }
}

pub enum IndexKind {
    Unique(UniqueBTreeIndex),
    NonUnique(Arc<dyn NonUniqueIndex>),
}

impl IndexKind {
    #[inline]
    pub fn non_unique<T: NonUniqueIndex>(index: T) -> Self {
        IndexKind::NonUnique(Arc::new(index))
    }
}

pub trait UniqueIndex: Send + Sync + 'static {
    /// Lookup unique key in this index.
    /// Return associated value and delete flag.
    fn lookup(&self, key: &[Val], ts: TrxID) -> impl Future<Output = Option<(RowID, bool)>>;

    /// Insert new key value pair into this index.
    /// If same key exists, return old key and its delete flag.
    fn insert_if_not_exists(
        &self,
        key: &[Val],
        row_id: RowID,
        ts: TrxID,
    ) -> impl Future<Output = Option<(RowID, bool)>>;

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

    #[inline]
    fn mask_as_deleted(&self, key: &[Val], row_id: RowID, ts: TrxID) -> impl Future<Output = bool> {
        debug_assert!(!row_id.is_deleted());
        let new_row_id = row_id.deleted();
        self.compare_exchange(key, row_id, new_row_id, ts)
            .map(|res| match res {
                IndexCompareExchange::Ok => true,
                IndexCompareExchange::Failure | IndexCompareExchange::NotExists => false,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexCompareExchange {
    Ok,
    Failure,
    NotExists,
}

impl IndexCompareExchange {
    #[inline]
    pub fn is_ok(self) -> bool {
        matches!(self, IndexCompareExchange::Ok)
    }
}

pub trait NonUniqueIndex: Send + Sync + 'static {
    fn lookup(&self, key: &[Val], res: &mut Vec<RowID>);

    fn insert(&self, key: &[Val], row_id: RowID);

    fn delete(&self, key: &[Val], row_id: RowID) -> bool;

    fn update(&self, key: &[Val], old_row_id: RowID, new_row_id: RowID) -> bool;
}

pub struct UniqueBTreeIndex {
    tree: BTree,
    encoder: BTreeKeyEncoder,
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
        ts: TrxID,
    ) -> Option<(RowID, bool)> {
        debug_assert!(!BTreeU64::from(row_id).is_deleted());
        let k = self.encoder.encode(key);
        match self
            .tree
            .insert::<BTreeU64>(k.as_bytes(), BTreeU64::from(row_id), ts)
            .await
        {
            BTreeInsert::Ok => None,
            BTreeInsert::DuplicateKey(res) => Some((res.value().to_u64(), res.is_deleted())),
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
        debug_assert!(!BTreeU64::from(row_id).is_deleted());
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
            BTreeUpdate::ValueMismatch(_) => IndexCompareExchange::Failure,
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

pub trait EncodeKeySelf {
    fn encode(key: &[Val]) -> Self;
}

macro_rules! impl_self_encode_number {
    ($ty:ty, $func:ident) => {
        impl EncodeKeySelf for $ty {
            #[inline]
            fn encode(key: &[Val]) -> Self {
                key[0].$func().unwrap()
            }
        }
    };
}

impl_self_encode_number!(i8, as_i8);
impl_self_encode_number!(u8, as_u8);
impl_self_encode_number!(i16, as_i16);
impl_self_encode_number!(u16, as_u16);
impl_self_encode_number!(i32, as_i32);
impl_self_encode_number!(u32, as_u32);
impl_self_encode_number!(i64, as_i64);
impl_self_encode_number!(u64, as_u64);
impl_self_encode_number!(f32, as_f32);
impl_self_encode_number!(f64, as_f64);
impl_self_encode_number!(ValidF32, as_valid_f32);
impl_self_encode_number!(ValidF64, as_valid_f64);

impl EncodeKeySelf for BTreeKey {
    #[inline]
    fn encode(key: &[Val]) -> Self {
        debug_assert!(key.len() == 1);
        let bs = key[0].as_bytes().unwrap();
        BTreeKey::from(bs)
    }
}

pub trait EncodeMultiKeys {
    fn encode(&self, key: &[Val]) -> Val;

    fn encode_pair(&self, prefix_key: &[Val], end_key: &Val) -> Val;
}

#[inline]
pub fn multi_key_encoder(types: Vec<ValType>) -> Either<FixLenEncoder, VarLenEncoder> {
    debug_assert!(types.len() > 1);
    if types.iter().all(|ty| ty.kind.is_fixed()) {
        let len = types
            .iter()
            .map(|ty| ty.memcmp_encoded_len().unwrap())
            .sum::<usize>();
        let encoder = FixLenEncoder::new(types, len);
        return Either::Left(encoder);
    }
    let min_len = types
        .iter()
        .map(|ty| ty.memcmp_encoded_len_maybe_var())
        .sum::<usize>();
    let encoder = VarLenEncoder::new(types, min_len);
    Either::Right(encoder)
}

pub struct FixLenEncoder {
    types: Box<[ValType]>,
    len: usize,
}

impl FixLenEncoder {
    #[inline]
    pub fn new(types: Vec<ValType>, len: usize) -> Self {
        debug_assert!(types.len() > 1);
        debug_assert!(types.iter().all(|ty| ty.kind.is_fixed()));
        debug_assert!(
            types
                .iter()
                .map(|ty| ty.memcmp_encoded_len().unwrap())
                .sum::<usize>()
                == len
        );
        FixLenEncoder {
            types: types.into_boxed_slice(),
            len,
        }
    }
}

impl EncodeMultiKeys for FixLenEncoder {
    #[inline]
    fn encode(&self, key: &[Val]) -> Val {
        debug_assert!(key.len() == self.types.len());
        let mut buf = Vec::with_capacity(self.len);
        for (ty, val) in self.types.iter().zip(key) {
            val.encode_memcmp(*ty, &mut buf);
        }
        Val::from(buf)
    }

    #[inline]
    fn encode_pair(&self, prefix_key: &[Val], end_key: &Val) -> Val {
        debug_assert!(prefix_key.len() + 1 == self.types.len());
        let mut buf = Vec::with_capacity(self.len);
        for (ty, val) in self.types.iter().zip(prefix_key) {
            val.encode_memcmp(*ty, &mut buf);
        }
        end_key.encode_memcmp(self.types.last().cloned().unwrap(), &mut buf);
        Val::from(buf)
    }
}

pub struct VarLenEncoder {
    types: Box<[ValType]>,
    min_len: usize,
}

impl VarLenEncoder {
    #[inline]
    pub fn new(types: Vec<ValType>, min_len: usize) -> Self {
        debug_assert!(types.len() > 1);
        debug_assert!(types.iter().any(|ty| !ty.kind.is_fixed()));
        VarLenEncoder {
            types: types.into_boxed_slice(),
            min_len,
        }
    }
}

impl VarLenEncoder {
    #[inline]
    fn encode(&self, key: &[Val]) -> Val {
        debug_assert!(key.len() == self.types.len());
        let mut buf = Vec::with_capacity(self.min_len);
        for (ty, val) in self.types.iter().zip(key) {
            val.encode_memcmp(*ty, &mut buf);
        }
        Val::from(buf)
    }

    #[inline]
    fn encode_pair(&self, prefix_key: &[Val], end_key: &Val) -> Val {
        debug_assert!(prefix_key.len() + 1 == self.types.len());
        let mut buf = Vec::with_capacity(self.min_len);
        for (ty, val) in self.types.iter().zip(prefix_key) {
            val.encode_memcmp(*ty, &mut buf);
        }
        end_key.encode_memcmp(self.types.last().cloned().unwrap(), &mut buf);
        Val::from(buf)
    }
}

pub const INDEX_PARTITIONS: usize = 64;

// Simple partitioned index implementation backed by BTreeMap in standard library.
pub struct PartitionSingleKeyIndex<T, const NULLABLE: bool>(
    [RwLock<BTreeMap<T, RowID>>; INDEX_PARTITIONS],
);

impl<T: Hash> PartitionSingleKeyIndex<T, false> {
    #[inline]
    pub fn empty() -> Self {
        let mut init: MaybeUninit<[RwLock<BTreeMap<T, RowID>>; INDEX_PARTITIONS]> =
            MaybeUninit::uninit();
        let array = unsafe {
            for ptr in init.assume_init_mut().iter_mut() {
                (ptr as *mut RwLock<BTreeMap<T, RowID>>).write(RwLock::new(BTreeMap::new()));
            }
            init.assume_init()
        };
        PartitionSingleKeyIndex(array)
    }

    #[inline]
    fn select(&self, key: &T) -> &RwLock<BTreeMap<T, RowID>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        &self.0[hash as usize % INDEX_PARTITIONS]
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
        _ts: TrxID,
    ) -> Option<(RowID, bool)> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.entry(key) {
            Entry::Occupied(occ) => {
                let v = *occ.get();
                Some((
                    BTreeU64::from(v).value().to_u64(),
                    BTreeU64::from(v).is_deleted(),
                ))
            }
            Entry::Vacant(vac) => {
                vac.insert(row_id);
                None
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
                    IndexCompareExchange::Failure
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

pub struct PartitionMultiKeyIndex {
    encoder: Either<FixLenEncoder, VarLenEncoder>,
    index: PartitionSingleKeyIndex<BTreeKey, false>,
}

impl PartitionMultiKeyIndex {
    #[inline]
    pub fn empty(encoder: Either<FixLenEncoder, VarLenEncoder>) -> Self {
        let index = PartitionSingleKeyIndex::empty();
        PartitionMultiKeyIndex { encoder, index }
    }

    #[inline]
    pub fn encode(&self, key: &[Val]) -> Val {
        match &self.encoder {
            Either::Left(fe) => fe.encode(key),
            Either::Right(ve) => ve.encode(key),
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
        ts: TrxID,
    ) -> Option<(RowID, bool)> {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index.insert_if_not_exists(key, row_id, ts).await
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

pub struct PartitionNonUniqueIndex {}

impl NonUniqueIndex for PartitionNonUniqueIndex {
    #[inline]
    fn lookup(&self, _key: &[Val], _res: &mut Vec<RowID>) {
        todo!()
    }

    #[inline]
    fn insert(&self, _key: &[Val], _row_id: RowID) {
        todo!()
    }

    #[inline]
    fn delete(&self, _key: &[Val], _row_id: RowID) -> bool {
        todo!()
    }

    #[inline]
    fn update(&self, _key: &[Val], _old_row_id: RowID, _new_row_id: RowID) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifetime::StaticLifetime;
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
            let pool = FixedBufferPool::with_capacity_static(1024usize * 1024 * 1024).unwrap();
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
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        });
    }

    #[test]
    fn test_multi_key_btree_unique_index() {
        smol::block_on(async {
            let pool = FixedBufferPool::with_capacity_static(1024usize * 1024 * 1024).unwrap();
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
            unsafe {
                StaticLifetime::drop_static(pool);
            }
        });
    }

    async fn run_test_suit_for_single_key_unique_index<T: UniqueIndex>(index: &T) {
        // 测试用例1：基本插入和查找操作
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert_eq!(index.insert_if_not_exists(&key, row_id, 100).await, None);

        // 测试查找
        assert_eq!(index.lookup(&key, 100).await, Some((row_id, false)));

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        assert_eq!(index.lookup(&non_existent_key, 100).await, None);

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index.insert_if_not_exists(&key, new_row_id, 100).await;
        assert_eq!(old_row_id, Some((row_id, false)));
        assert_eq!(index.lookup(&key, 100).await, old_row_id);

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(&key, old_row_id.unwrap().0, true, 100)
                .await
        );
        assert_eq!(index.lookup(&key, 100).await, None);

        // 测试删除不存在的键 still ok
        assert!(index.compare_delete(&key, new_row_id, false, 100).await);

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert_eq!(index.insert_if_not_exists(&key, row_id1, 100).await, None);

        // 测试成功的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await == IndexCompareExchange::Ok
        );
        assert_eq!(index.lookup(&key, 100).await, Some((row_id2, false)));

        // 测试失败的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await
                == IndexCompareExchange::Failure
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
        assert_eq!(index.insert_if_not_exists(&key1, row_id1, 100).await, None);
        assert_eq!(index.insert_if_not_exists(&key2, row_id2, 100).await, None);
        assert_eq!(index.insert_if_not_exists(&key3, row_id3, 100).await, None);

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
        assert_eq!(index.insert_if_not_exists(&key, row_id, 100).await, None);

        // 测试查找
        assert_eq!(index.lookup(&key, 100).await, Some((row_id, false)));

        // 测试不存在的键
        let non_existent_key = vec![Val::from("hello"), Val::from(43i32)];
        assert_eq!(index.lookup(&non_existent_key, 100).await, None);

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index.insert_if_not_exists(&key, new_row_id, 100).await;
        assert_eq!(old_row_id, Some((row_id, false)));
        assert_eq!(index.lookup(&key, 100).await, old_row_id);

        // 测试用例3：删除操作
        assert!(
            index
                .compare_delete(&key, old_row_id.unwrap().0, true, 100)
                .await
        );
        assert_eq!(index.lookup(&key, 100).await, None);

        // 测试删除不存在的键 still ok
        assert!(index.compare_delete(&key, new_row_id, false, 100).await);

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from("hello"), Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert_eq!(index.insert_if_not_exists(&key, row_id1, 100).await, None);

        // 测试成功的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await == IndexCompareExchange::Ok
        );
        assert_eq!(index.lookup(&key, 100).await, Some((row_id2, false)));

        // 测试失败的 compare_exchange
        assert!(
            index.compare_exchange(&key, row_id1, row_id2, 100).await
                == IndexCompareExchange::Failure
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
        assert_eq!(index.insert_if_not_exists(&key1, row_id1, 100).await, None);
        assert_eq!(index.insert_if_not_exists(&key2, row_id2, 100).await, None);
        assert_eq!(index.insert_if_not_exists(&key3, row_id3, 100).await, None);

        // 验证所有键都能正确查找
        assert_eq!(index.lookup(&key1, 100).await, Some((row_id1, false)));
        assert_eq!(index.lookup(&key2, 100).await, Some((row_id2, false)));
        assert_eq!(index.lookup(&key3, 100).await, Some((row_id3, false)));

        // 验证 scan_values 包含所有值
        let mut values = Vec::new();
        index.scan_values(&mut values, 100).await;
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2
    }
}
