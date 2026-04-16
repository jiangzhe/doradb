use crate::buffer::{BufferPool, PoolGuard};
use crate::catalog::IndexSpec;
use crate::index::btree::GenericBTree;
use crate::index::btree_key::BTreeKeyEncoder;
use crate::index::non_unique_index::NonUniqueMemIndex;
use crate::index::unique_index::UniqueMemIndex;
use crate::quiescent::QuiescentGuard;
use crate::trx::TrxID;
use crate::value::{ValKind, ValType};

/// Generic secondary-index container for one table index definition.
pub struct MemIndex<P: 'static> {
    pub index_no: usize,
    pub kind: MemIndexKind<P>,
}

impl<P: BufferPool> MemIndex<P> {
    /// Build a secondary index from catalog `IndexSpec`.
    #[inline]
    pub async fn new<F: Fn(usize) -> ValType>(
        index_pool: QuiescentGuard<P>,
        index_pool_guard: &PoolGuard,
        index_no: usize,
        index_spec: &IndexSpec,
        ty_infer: F,
        ts: TrxID,
    ) -> crate::error::Result<Self> {
        debug_assert!(!index_spec.index_cols.is_empty());
        let mut types: Vec<_> = index_spec
            .index_cols
            .iter()
            .map(|key| ty_infer(key.col_no as usize))
            .collect();
        if index_spec.unique() {
            let encoder = BTreeKeyEncoder::new(types);
            let tree = GenericBTree::new(index_pool, index_pool_guard, true, ts).await?;
            let kind = MemIndexKind::Unique(UniqueMemIndex::new(tree, encoder));
            Ok(MemIndex { index_no, kind })
        } else {
            // non-unique index always encodes RowID as last key to
            // ensure uniqueness(which is required by BTree implementation).
            types.push(ValType::new(ValKind::U64, false));
            let encoder = BTreeKeyEncoder::new(types);
            let tree = GenericBTree::new(index_pool, index_pool_guard, true, ts).await?;
            let kind = MemIndexKind::NonUnique(NonUniqueMemIndex::new(tree, encoder));
            Ok(MemIndex { index_no, kind })
        }
    }

    /// Destroy this secondary index and reclaim all pages it owns.
    #[inline]
    pub(crate) async fn destroy(self, pool_guard: &PoolGuard) -> crate::error::Result<()> {
        match self.kind {
            MemIndexKind::Unique(idx) => idx.destroy(pool_guard).await,
            MemIndexKind::NonUnique(idx) => idx.destroy(pool_guard).await,
        }
    }

    /// Returns whether this index is unique.
    #[inline]
    pub fn is_unique(&self) -> bool {
        matches!(self.kind, MemIndexKind::Unique(_))
    }

    /// Returns the unique-index view when the index is unique.
    #[inline]
    pub fn unique(&self) -> Option<&UniqueMemIndex<P>> {
        match &self.kind {
            MemIndexKind::Unique(idx) => Some(idx),
            _ => None,
        }
    }

    /// Returns the non-unique-index view when the index is non-unique.
    #[inline]
    pub fn non_unique(&self) -> Option<&NonUniqueMemIndex<P>> {
        match &self.kind {
            MemIndexKind::NonUnique(idx) => Some(idx),
            _ => None,
        }
    }

    /// Consume this container into its concrete unique MemIndex backend.
    #[inline]
    pub(crate) fn into_unique(self) -> Option<(usize, UniqueMemIndex<P>)> {
        match self.kind {
            MemIndexKind::Unique(idx) => Some((self.index_no, idx)),
            MemIndexKind::NonUnique(_) => None,
        }
    }

    /// Consume this container into its concrete non-unique MemIndex backend.
    #[inline]
    pub(crate) fn into_non_unique(self) -> Option<(usize, NonUniqueMemIndex<P>)> {
        match self.kind {
            MemIndexKind::Unique(_) => None,
            MemIndexKind::NonUnique(idx) => Some((self.index_no, idx)),
        }
    }
}

/// Generic variants of secondary-index backends.
pub enum MemIndexKind<P: 'static> {
    Unique(UniqueMemIndex<P>),
    NonUnique(NonUniqueMemIndex<P>),
}

#[cfg(test)]
mod tests {
    use crate::catalog::tests::table4;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::row::ops::{DeleteMvcc, InsertMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::table::TableAccess;
    use crate::value::Val;
    use tempfile::TempDir;

    #[test]
    fn test_secondary_index_common() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default().role(crate::buffer::PoolRole::Mem),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("redo_secidx1")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            let table_id = table4(&engine).await;
            {
                let table = engine.catalog().get_table(table_id).await.unwrap();

                let mut session = engine.try_new_session().unwrap();
                let user_read_set = &[0usize, 1];
                // insert row.
                // 0,0; 1,1; 2,2; 3,3; 4,4
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                for i in 0i32..5i32 {
                    let res = table
                        .accessor()
                        .insert_mvcc(&mut stmt, vec![Val::from(i), Val::from(i)])
                        .await;
                    assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                }
                stmt.succeed().commit().await.unwrap();
                // select ... where id = 1
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let res = table
                    .accessor()
                    .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                // select ... where val = 1
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(1, vec![Val::from(1i32)]);
                let res = table
                    .accessor()
                    .index_scan_mvcc(&stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.unwrap().unwrap_rows().len() == 1);
                // update val = 0 where id = 1
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(0i32),
                }];
                let res = table
                    .accessor()
                    .update_unique_mvcc(&mut stmt, &key, update)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                // select ... where val = 0
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(1, vec![Val::from(0i32)]);
                let res = table
                    .accessor()
                    .index_scan_mvcc(&stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(res.unwrap().unwrap_rows().len() == 2);
                // delete where id = 0
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = table
                    .accessor()
                    .delete_unique_mvcc(&mut stmt, &key, false)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                // select ... where val = 0
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(1, vec![Val::from(0i32)]);
                let res = table
                    .accessor()
                    .index_scan_mvcc(&stmt, &key, user_read_set)
                    .await;
                _ = stmt.succeed().commit().await.unwrap();
                assert!(res.unwrap().unwrap_rows().len() == 1);
            }
            drop(engine);
        })
    }

    #[test]
    fn test_secondary_index_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = EngineConfig::default()
                .storage_root(main_dir)
                .data_buffer(
                    EvictableBufferPoolConfig::default().role(crate::buffer::PoolRole::Mem),
                )
                .trx(
                    TrxSysConfig::default()
                        .log_file_stem("redo_secidx2")
                        .skip_recovery(true),
                )
                .build()
                .await
                .unwrap();
            let table_id = table4(&engine).await;
            {
                let table = engine.catalog().get_table(table_id).await.unwrap();

                let mut session = engine.try_new_session().unwrap();
                let user_read_set = &[0usize, 1];
                // insert row.
                // 0,0; 1,1; 2,2; 3,3; 4,4
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                for i in 0i32..5i32 {
                    let res = table
                        .accessor()
                        .insert_mvcc(&mut stmt, vec![Val::from(i), Val::from(i)])
                        .await;
                    assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                }
                stmt.succeed().commit().await.unwrap();
                // insert 5,5 and rollback
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let res = table
                    .accessor()
                    .insert_mvcc(&mut stmt, vec![Val::from(5i32), Val::from(5i32)])
                    .await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                stmt.succeed().rollback().await.unwrap();
                // select ... where id = 5
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(5i32)]);
                let res = table
                    .accessor()
                    .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::NotFound)));
                // update val = 0 where id = 1
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(0i32),
                }];
                let res = table
                    .accessor()
                    .update_unique_mvcc(&mut stmt, &key, update)
                    .await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                stmt.succeed().rollback().await.unwrap();
                // select ... where id = 1
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let res = table
                    .accessor()
                    .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                let vals = res.unwrap().unwrap_found();
                assert!(vals[0] == Val::from(1i32) && vals[1] == Val::from(1i32));
                // delete where id = 0
                let trx = session.try_begin_trx().unwrap().unwrap();
                let mut stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = table
                    .accessor()
                    .delete_unique_mvcc(&mut stmt, &key, false)
                    .await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                stmt.succeed().rollback().await.unwrap();
                // select ... where val = 0
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = table
                    .accessor()
                    .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                    .await;
                stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                let vals = res.unwrap().unwrap_found();
                assert!(vals[0] == Val::from(0i32) && vals[1] == Val::from(0i32));

                // delete where id = 3, then insert 3,3, then rollback
                let mut trx = session.try_begin_trx().unwrap().unwrap();
                let key = SelectKey::new(0, vec![Val::from(3i32)]);
                let mut stmt = trx.start_stmt();
                let res = table
                    .accessor()
                    .delete_unique_mvcc(&mut stmt, &key, false)
                    .await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                trx = stmt.succeed();
                stmt = trx.start_stmt();
                let res = table
                    .accessor()
                    .insert_mvcc(&mut stmt, vec![Val::from(3), Val::from(3)])
                    .await;
                assert!(matches!(res, Ok(InsertMvcc::Inserted(_))));
                trx = stmt.succeed();
                // manual rollback.
                trx.rollback().await.unwrap();
                // select ... where id = 3
                let trx = session.try_begin_trx().unwrap().unwrap();
                let stmt = trx.start_stmt();
                let key = SelectKey::new(0, vec![Val::from(3i32)]);
                let res = table
                    .accessor()
                    .index_lookup_unique_mvcc(&stmt, &key, user_read_set)
                    .await;
                _ = stmt.succeed().commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                let vals = res.unwrap().unwrap_found();
                assert!(vals[0] == Val::from(3i32) && vals[1] == Val::from(3i32));
            }
            drop(engine);
        })
    }
}
