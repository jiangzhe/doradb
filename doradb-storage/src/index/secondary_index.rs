use crate::index::smart_key::SmartKey;
use crate::row::RowID;
use crate::value::{Val, ValKind, ValType};
use doradb_catalog::IndexSpec;
use doradb_datatype::konst::{ValidF32, ValidF64};
use either::Either;
use parking_lot::RwLock;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem::MaybeUninit;
use std::sync::Arc;

pub const INDEX_KEY_MAX_LEN: usize = 255;

#[derive(Clone)]
pub struct SecondaryIndex {
    pub index_no: usize,
    pub kind: IndexKind,
}

impl SecondaryIndex {
    #[inline]
    pub fn new<F: Fn(usize) -> ValType>(
        index_no: usize,
        index_spec: &IndexSpec,
        ty_infer: F,
    ) -> Self {
        debug_assert!(!index_spec.index_cols.is_empty());
        if index_spec.unique() {
            // create unique index
            let kind = match &index_spec.index_cols[..] {
                [] => unreachable!(),
                [key] => {
                    // single-key index
                    let key_ty = ty_infer(key.col_no as usize);
                    match (key_ty.kind, key_ty.nullable) {
                        (ValKind::I8, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<i8, false>::empty())
                        }
                        (ValKind::U8, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<u8, false>::empty())
                        }
                        (ValKind::I16, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<i16, false>::empty())
                        }
                        (ValKind::U16, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<u16, false>::empty())
                        }
                        (ValKind::I32, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<i32, false>::empty())
                        }
                        (ValKind::U32, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<u32, false>::empty())
                        }
                        (ValKind::I64, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<i64, false>::empty())
                        }
                        (ValKind::U64, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<u64, false>::empty())
                        }
                        (ValKind::F32, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<ValidF32, false>::empty())
                        }
                        (ValKind::F64, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<ValidF64, false>::empty())
                        }
                        (ValKind::VarByte, false) => {
                            IndexKind::unique(PartitionSingleKeyIndex::<SmartKey, false>::empty())
                        }
                        _ => todo!("nullable index not supported"),
                    }
                }
                keys => {
                    // multi-key index
                    let types: Vec<_> = keys
                        .iter()
                        .map(|key| ty_infer(key.col_no as usize))
                        .collect();
                    let encoder = multi_key_encoder(types);
                    IndexKind::unique(PartitionMultiKeyIndex::empty(encoder))
                }
            };
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
    pub fn unique(&self) -> Option<&dyn UniqueIndex> {
        match &self.kind {
            IndexKind::Unique(idx) => Some(&**idx),
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

#[derive(Clone)]
pub enum IndexKind {
    Unique(Arc<dyn UniqueIndex>),
    NonUnique(Arc<dyn NonUniqueIndex>),
}

impl IndexKind {
    #[inline]
    pub fn unique<T: UniqueIndex>(index: T) -> Self {
        IndexKind::Unique(Arc::new(index))
    }

    #[inline]
    pub fn non_unique<T: NonUniqueIndex>(index: T) -> Self {
        IndexKind::NonUnique(Arc::new(index))
    }
}

pub trait UniqueIndex: Send + Sync + 'static {
    fn lookup(&self, key: &[Val]) -> Option<RowID>;

    fn insert(&self, key: &[Val], row_id: RowID) -> Option<RowID>;

    fn insert_if_not_exists(&self, key: &[Val], row_id: RowID) -> Option<RowID>;

    fn compare_delete(&self, key: &[Val], old_row_id: RowID) -> bool;

    /// atomically update an existing value associated to given key to another value.
    /// if not exists, returns specified bool value.
    fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> IndexCompareExchange;

    fn scan_values(&self, values: &mut Vec<RowID>);
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

pub trait EncodeKeySelf {
    fn encode(key: &[Val]) -> Self;
}

macro_rules! impl_self_encode_int {
    ($ty:ty, $func:ident) => {
        impl EncodeKeySelf for $ty {
            #[inline]
            fn encode(key: &[Val]) -> Self {
                key[0].$func().unwrap()
            }
        }
    };
}

impl_self_encode_int!(i8, as_i8);
impl_self_encode_int!(u8, as_u8);
impl_self_encode_int!(i16, as_i16);
impl_self_encode_int!(u16, as_u16);
impl_self_encode_int!(i32, as_i32);
impl_self_encode_int!(u32, as_u32);
impl_self_encode_int!(i64, as_i64);
impl_self_encode_int!(u64, as_u64);

macro_rules! impl_self_encode_float {
    ($ty:ty, $func:ident) => {
        impl EncodeKeySelf for $ty {
            #[inline]
            fn encode(key: &[Val]) -> Self {
                key[0].$func().unwrap()
            }
        }
    };
}

impl_self_encode_float!(ValidF32, as_f32);
impl_self_encode_float!(ValidF64, as_f64);

impl EncodeKeySelf for SmartKey {
    #[inline]
    fn encode(key: &[Val]) -> Self {
        debug_assert!(key.len() == 1);
        let bs = key[0].as_bytes().unwrap();
        SmartKey::from(bs)
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
    fn lookup(&self, key: &[Val]) -> Option<RowID> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let g = tree.read();
        g.get(&key).cloned()
    }

    #[inline]
    fn insert(&self, key: &[Val], row_id: RowID) -> Option<RowID> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        g.insert(key, row_id)
    }

    #[inline]
    fn insert_if_not_exists(&self, key: &[Val], row_id: RowID) -> Option<RowID> {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.entry(key) {
            Entry::Occupied(occ) => Some(*occ.get()),
            Entry::Vacant(vac) => {
                vac.insert(row_id);
                None
            }
        }
    }

    #[inline]
    fn compare_delete(&self, key: &[Val], old_row_id: RowID) -> bool {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.entry(key) {
            Entry::Occupied(occ) => {
                if occ.get() == &old_row_id {
                    occ.remove();
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(_) => false,
        }
    }

    #[inline]
    fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
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
    fn scan_values(&self, values: &mut Vec<RowID>) {
        for tree in &self.0 {
            let g = tree.read();
            values.extend(g.values());
        }
    }
}

pub struct PartitionMultiKeyIndex {
    encoder: Either<FixLenEncoder, VarLenEncoder>,
    index: PartitionSingleKeyIndex<SmartKey, false>,
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
    fn lookup(&self, keys: &[Val]) -> Option<RowID> {
        let encoded = self.encode(keys);
        let key = std::slice::from_ref(&encoded);
        self.index.lookup(key)
    }

    #[inline]
    fn insert(&self, key: &[Val], row_id: RowID) -> Option<RowID> {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index.insert(key, row_id)
    }

    #[inline]
    fn insert_if_not_exists(&self, key: &[Val], row_id: RowID) -> Option<RowID> {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index.insert_if_not_exists(key, row_id)
    }

    #[inline]
    fn compare_delete(&self, key: &[Val], old_row_id: RowID) -> bool {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index.compare_delete(key, old_row_id)
    }

    #[inline]
    fn compare_exchange(
        &self,
        key: &[Val],
        old_row_id: RowID,
        new_row_id: RowID,
    ) -> IndexCompareExchange {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index.compare_exchange(key, old_row_id, new_row_id)
    }

    #[inline]
    fn scan_values(&self, values: &mut Vec<RowID>) {
        self.index.scan_values(values);
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

    #[test]
    fn test_unique_index() {
        // 测试用例1：基本插入和查找操作
        let index = PartitionSingleKeyIndex::<i32, false>::empty();
        let key = vec![Val::from(42i32)];
        let row_id = 100u64;

        // 测试插入
        assert_eq!(index.insert(&key, row_id), None);

        // 测试查找
        assert_eq!(index.lookup(&key), Some(row_id));

        // 测试不存在的键
        let non_existent_key = vec![Val::from(43i32)];
        assert_eq!(index.lookup(&non_existent_key), None);

        // 测试用例2：重复插入
        let new_row_id = 200u64;
        let old_row_id = index.insert(&key, new_row_id);
        assert_eq!(old_row_id, Some(row_id));
        assert_eq!(index.lookup(&key), Some(new_row_id));

        // 测试用例3：删除操作
        assert!(index.compare_delete(&key, new_row_id));
        assert_eq!(index.lookup(&key), None);

        // 测试删除不存在的键
        assert!(!index.compare_delete(&key, new_row_id));

        // 测试用例4：compare_exchange 操作
        let key = vec![Val::from(100i32)];
        let row_id1 = 300u64;
        let row_id2 = 400u64;

        // 先插入一个值
        assert_eq!(index.insert(&key, row_id1), None);

        // 测试成功的 compare_exchange
        assert!(index.compare_exchange(&key, row_id1, row_id2) == IndexCompareExchange::Ok);
        assert_eq!(index.lookup(&key), Some(row_id2));

        // 测试失败的 compare_exchange
        assert!(index.compare_exchange(&key, row_id1, row_id2) == IndexCompareExchange::Failure);

        // 测试用例5：scan_values 操作
        let mut values = Vec::new();
        index.scan_values(&mut values);
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
        assert_eq!(index.insert(&key1, row_id1), None);
        assert_eq!(index.insert(&key2, row_id2), None);
        assert_eq!(index.insert(&key3, row_id3), None);

        // 验证所有键都能正确查找
        assert_eq!(index.lookup(&key1), Some(row_id1));
        assert_eq!(index.lookup(&key2), Some(row_id2));
        assert_eq!(index.lookup(&key3), Some(row_id3));

        // 验证 scan_values 包含所有值
        let mut values = Vec::new();
        index.scan_values(&mut values);
        assert_eq!(values.len(), 4); // 包含之前插入的 row_id2
    }
}
