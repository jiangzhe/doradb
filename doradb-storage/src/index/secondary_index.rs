use crate::catalog::IndexSchema;
use crate::index::smart_key::SmartKey;
use crate::row::RowID;
use crate::value::{Val, ValKind, ValType};
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
    pub fn new(index_no: usize, index_schema: &IndexSchema, user_col_types: &[ValType]) -> Self {
        debug_assert!(!index_schema.keys.is_empty());
        if index_schema.unique {
            // create unique index
            let kind = match &index_schema.keys[..] {
                [] => unreachable!(),
                [key] => {
                    // single-key index
                    let key_ty = user_col_types[key.user_col_idx as usize];
                    match key_ty.kind {
                        ValKind::I8 => IndexKind::unique(PartitionSingleKeyIndex::<i8>::empty()),
                        ValKind::U8 => IndexKind::unique(PartitionSingleKeyIndex::<u8>::empty()),
                        ValKind::I16 => IndexKind::unique(PartitionSingleKeyIndex::<i16>::empty()),
                        ValKind::U16 => IndexKind::unique(PartitionSingleKeyIndex::<u16>::empty()),
                        ValKind::I32 => IndexKind::unique(PartitionSingleKeyIndex::<i32>::empty()),
                        ValKind::U32 => IndexKind::unique(PartitionSingleKeyIndex::<u32>::empty()),
                        ValKind::I64 => IndexKind::unique(PartitionSingleKeyIndex::<i64>::empty()),
                        ValKind::U64 => IndexKind::unique(PartitionSingleKeyIndex::<u64>::empty()),
                        ValKind::F32 => {
                            IndexKind::unique(PartitionSingleKeyIndex::<ValidF32>::empty())
                        }
                        ValKind::F64 => {
                            IndexKind::unique(PartitionSingleKeyIndex::<ValidF64>::empty())
                        }
                        ValKind::VarByte => {
                            IndexKind::unique(PartitionSingleKeyIndex::<SmartKey>::empty())
                        }
                    }
                }
                keys => {
                    // multi-key index
                    let types: Vec<_> = keys
                        .iter()
                        .map(|key| user_col_types[key.user_col_idx as usize])
                        .collect();
                    let encoder = multi_key_encoder(types);
                    IndexKind::unique(PartitionMultiKeyIndex::empty(encoder))
                }
            };
            return SecondaryIndex { index_no, kind };
        }
        // create non-unique index
        todo!()
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
}

pub trait UniqueIndex: Send + Sync + 'static {
    fn lookup(&self, key: &[Val]) -> Option<RowID>;

    fn insert(&self, key: &[Val], row_id: RowID) -> Option<RowID>;

    fn insert_if_not_exists(&self, key: &[Val], row_id: RowID) -> Option<RowID>;

    fn compare_delete(&self, key: &[Val], old_row_id: RowID) -> bool;

    fn compare_exchange(&self, key: &[Val], old_row_id: RowID, new_row_id: RowID) -> bool;

    fn scan_values(&self, values: &mut Vec<RowID>);
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
        let bs = key[0].as_bytes().unwrap();
        SmartKey::from(bs)
    }
}

pub trait EncodeMultiKeys {
    fn encode(&self, key: &[Val]) -> Val;
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
}

pub const INDEX_PARTITIONS: usize = 64;

// Simple partitioned index implementation backed by BTreeMap in standard library.
pub struct PartitionSingleKeyIndex<T>([RwLock<BTreeMap<T, RowID>>; INDEX_PARTITIONS]);

impl<T: Hash> PartitionSingleKeyIndex<T> {
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
    for PartitionSingleKeyIndex<T>
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
    fn compare_exchange(&self, key: &[Val], old_row_id: RowID, new_row_id: RowID) -> bool {
        let key = T::encode(key);
        let tree = self.select(&key);
        let mut g = tree.write();
        match g.get_mut(&key) {
            Some(row_id) => {
                if *row_id == old_row_id {
                    *row_id = new_row_id;
                    true
                } else {
                    false
                }
            }
            None => false,
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
    index: PartitionSingleKeyIndex<SmartKey>,
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
    fn lookup(&self, key: &[Val]) -> Option<RowID> {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
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
    fn compare_exchange(&self, key: &[Val], old_row_id: RowID, new_row_id: RowID) -> bool {
        let key = self.encode(key);
        let key = std::slice::from_ref(&key);
        self.index.compare_exchange(key, old_row_id, new_row_id)
    }

    #[inline]
    fn scan_values(&self, values: &mut Vec<RowID>) {
        self.index.scan_values(values);
    }
}
