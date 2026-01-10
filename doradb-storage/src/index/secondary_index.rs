use crate::buffer::FixedBufferPool;
use crate::catalog::IndexSpec;
use crate::index::btree::BTree;
use crate::index::btree_key::{BTreeKey, BTreeKeyEncoder};
use crate::index::non_unique_index::NonUniqueBTreeIndex;
use crate::index::unique_index::UniqueBTreeIndex;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::{Val, ValKind, ValType};
use either::Either;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};

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
        let mut types: Vec<_> = index_spec
            .index_cols
            .iter()
            .map(|key| ty_infer(key.col_no as usize))
            .collect();
        if index_spec.unique() {
            let encoder = BTreeKeyEncoder::new(types);
            let tree = BTree::new(index_pool, true, ts).await;
            let kind = IndexKind::Unique(UniqueBTreeIndex::new(tree, encoder));
            SecondaryIndex { index_no, kind }
        } else {
            // non-unique index always encodes RowID as last key to
            // ensure uniqueness(which is required by BTree implementation).
            types.push(ValType::new(ValKind::U64, false));
            let encoder = BTreeKeyEncoder::new(types);
            let tree = BTree::new(index_pool, true, ts).await;
            let kind = IndexKind::NonUnique(NonUniqueBTreeIndex::new(tree, encoder));
            SecondaryIndex { index_no, kind }
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
    pub fn non_unique(&self) -> Option<&NonUniqueBTreeIndex> {
        match &self.kind {
            IndexKind::NonUnique(idx) => Some(idx),
            _ => None,
        }
    }
}

pub enum IndexKind {
    Unique(UniqueBTreeIndex),
    NonUnique(NonUniqueBTreeIndex),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexInsert {
    /// if flag set to true, means Old entry is masked as deleted,
    /// value match and merge is enabled.
    Ok(bool),
    // Old row id and its delete flag.
    DuplicateKey(RowID, bool),
}

impl IndexInsert {
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, IndexInsert::Ok(false))
    }

    #[inline]
    pub fn is_merged(&self) -> bool {
        matches!(self, IndexInsert::Ok(true))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexCompareExchange {
    Ok,
    Mismatch,
    NotExists,
}

impl IndexCompareExchange {
    #[inline]
    pub fn is_ok(self) -> bool {
        matches!(self, IndexCompareExchange::Ok)
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
    pub(super) Box<[RwLock<BTreeMap<T, RowID>>]>,
);

impl<T: Hash> PartitionSingleKeyIndex<T, false> {
    #[inline]
    pub(super) fn empty() -> Self {
        let partitions: Vec<_> = (0..INDEX_PARTITIONS)
            .map(|_| RwLock::new(BTreeMap::new()))
            .collect();
        PartitionSingleKeyIndex(partitions.into_boxed_slice())
    }

    #[inline]
    pub(super) fn select(&self, key: &T) -> &RwLock<BTreeMap<T, RowID>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        &self.0[hash as usize % INDEX_PARTITIONS]
    }
}

pub struct PartitionMultiKeyIndex {
    pub(super) encoder: Either<FixLenEncoder, VarLenEncoder>,
    pub(super) index: PartitionSingleKeyIndex<BTreeKey, false>,
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

// libaio is required for secondary index test.
#[cfg(all(test, feature = "libaio"))]
mod tests {
    use super::*;
    use crate::buffer::EvictableBufferPoolConfig;
    use crate::catalog::tests::table4;
    use crate::engine::EngineConfig;
    use crate::row::ops::{SelectKey, UpdateCol};
    use crate::table::TableAccess;
    use crate::trx::sys_conf::TrxSysConfig;
    use tempfile::TempDir;

    #[test]
    fn test_secondary_index_common() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default().file_path("databuffer_secidx1.bin"),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_secidx1")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            let table_id = table4(&engine).await;
            {
                let table = engine.catalog().get_table(table_id).await.unwrap();

                let mut session = engine.new_session();
                let user_read_set = &[0usize, 1];
                // insert row.
                // 0,0; 1,1; 2,2; 3,3; 4,4
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                for i in 0i32..5i32 {
                    let res = table
                        .insert_mvcc(
                            engine.data_pool,
                            &mut stmt,
                            vec![Val::from(i), Val::from(i)],
                        )
                        .await;
                    assert!(res.is_ok());
                }
                stmt.succeed().commit().await.unwrap();
                // select ... where id = 1
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let res = table
                    .index_lookup_unique_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.is_ok());
                // select ... where val = 1
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(1, vec![Val::from(1i32)]);
                let res = table
                    .index_scan_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.len() == 1);
                // update val = 0 where id = 1
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(0i32),
                }];
                let res = table
                    .update_unique_mvcc(engine.data_pool, &mut stmt, &key, update, false)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.is_ok());
                // select ... where val = 0
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(1, vec![Val::from(0i32)]);
                let res = table
                    .index_scan_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.len() == 2);
                // delete where id = 0
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = table
                    .delete_unique_mvcc(engine.data_pool, &mut stmt, &key, false)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.is_ok());
                // select ... where val = 0
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(1, vec![Val::from(0i32)]);
                let res = table
                    .index_scan_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                _ = stmt.succeed().commit().await.unwrap();
                assert!(res.len() == 1);
            }
            drop(engine);
        })
    }

    #[test]
    fn test_secondary_index_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_string_lossy().to_string();
            let engine = EngineConfig::default()
                .main_dir(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default().file_path("databuffer_secidx2.bin"),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_prefix("redo_secidx2")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            let table_id = table4(&engine).await;
            {
                let table = engine.catalog().get_table(table_id).await.unwrap();

                let mut session = engine.new_session();
                let user_read_set = &[0usize, 1];
                // insert row.
                // 0,0; 1,1; 2,2; 3,3; 4,4
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                for i in 0i32..5i32 {
                    let res = table
                        .insert_mvcc(
                            engine.data_pool,
                            &mut stmt,
                            vec![Val::from(i), Val::from(i)],
                        )
                        .await;
                    assert!(res.is_ok());
                }
                stmt.succeed().commit().await.unwrap();
                // insert 5,5 and rollback
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let res = table
                    .insert_mvcc(
                        engine.data_pool,
                        &mut stmt,
                        vec![Val::from(5i32), Val::from(5i32)],
                    )
                    .await;
                assert!(res.is_ok());
                stmt.succeed().rollback().await;
                // select ... where id = 5
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(5i32)]);
                let res = table
                    .index_lookup_unique_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.not_found());
                // update val = 0 where id = 1
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(0i32),
                }];
                let res = table
                    .update_unique_mvcc(engine.data_pool, &mut stmt, &key, update, false)
                    .await;
                assert!(res.is_ok());
                stmt.succeed().rollback().await;
                // select ... where id = 1
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let res = table
                    .index_lookup_unique_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.is_ok());
                let vals = res.unwrap();
                assert!(vals[0] == Val::from(1i32) && vals[1] == Val::from(1i32));
                // delete where id = 0
                let trx = session.begin_trx().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = table
                    .delete_unique_mvcc(engine.data_pool, &mut stmt, &key, false)
                    .await;
                assert!(res.is_ok());
                stmt.succeed().rollback().await;
                // select ... where val = 0
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = table
                    .index_lookup_unique_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.is_ok());
                let vals = res.unwrap();
                assert!(vals[0] == Val::from(0i32) && vals[1] == Val::from(0i32));

                // delete where id = 3, then insert 3,3, then rollback
                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(3i32)]);
                let mut stmt = trx.start_stmt();
                let res = table
                    .delete_unique_mvcc(engine.data_pool, &mut stmt, &key, false)
                    .await;
                assert!(res.is_ok());
                trx = stmt.succeed();
                stmt = trx.start_stmt();
                let res = table
                    .insert_mvcc(
                        engine.data_pool,
                        &mut stmt,
                        vec![Val::from(3), Val::from(3)],
                    )
                    .await;
                assert!(res.is_ok());
                trx = stmt.succeed();
                // manual rollback.
                trx.rollback().await;
                // select ... where id = 3
                let trx = session.begin_trx().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(3i32)]);
                let res = table
                    .index_lookup_unique_mvcc(engine.data_pool, &stmt, &key, user_read_set)
                    .await;
                _ = stmt.succeed().commit().await.unwrap();
                assert!(res.is_ok());
                let vals = res.unwrap();
                assert!(vals[0] == Val::from(3i32) && vals[1] == Val::from(3i32));
            }
            drop(engine);
        })
    }
}
