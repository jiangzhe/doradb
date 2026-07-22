use super::{MemTable, Table, TableRuntimeLayout};
use crate::buffer::{BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuard, PoolGuards};
use crate::catalog::CatalogTable;
use crate::error::RuntimeResult;
use crate::id::{RowID, TrxID};
use crate::index::util::Maskable;
use crate::index::{IndexCompareExchange, NonUniqueIndex, UniqueIndex};
use crate::row::ops::SelectKey;
use crate::trx::undo::{IndexUndo, IndexUndoKind};

/// Rollback adapter for table-specific secondary-index runtimes.
///
/// User tables route rollback through dual-tree secondary indexes, while
/// catalog tables keep using their in-memory generic indexes. Implementors
/// provide the primitive index operations; the shared rollback body applies
/// undo entries in reverse order and preserves the exact old index value
/// recorded in the undo log.
pub(crate) trait IndexRollback {
    /// Row buffer pool type owned by the table runtime.
    type RowPool: BufferPool + 'static;
    /// Secondary-index buffer pool type owned by the table runtime.
    type IndexPool: BufferPool + 'static;

    /// Returns the shared MemTable metadata and index-pool binding.
    fn mem_table(&self) -> &MemTable<Self::RowPool, Self::IndexPool>;

    /// Marks an existing unique entry as deleted.
    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool>;

    /// Removes a unique entry when the current value matches `row_id`.
    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool>;

    /// Atomically replaces a unique entry when the current value matches.
    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange>;

    /// Marks an existing non-unique exact entry as deleted.
    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool>;

    /// Marks an existing non-unique exact entry as active.
    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool>;

    /// Removes a non-unique exact entry when the current value matches.
    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool>;

    /// Rolls back one secondary-index undo entry against this table runtime.
    async fn rollback_index_entry(
        &self,
        entry: IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> RuntimeResult<()> {
        let table = self.mem_table();
        let index_pool_guard = table.index_pool_guard(guards);
        match entry.kind {
            IndexUndoKind::InsertUnique(key, merge_old_deleted) => {
                if merge_old_deleted {
                    // This insert reused a delete-masked unique entry for the
                    // same RowID, usually from repeated hot key changes in one
                    // transaction. Rollback restores the masked old owner
                    // instead of removing the entry.
                    let res = self
                        .unique_mask_as_deleted(index_pool_guard, &key, entry.row_id, ts)
                        .await?;
                    assert!(res);
                } else {
                    // The transaction created a new latest unique owner. If it
                    // aborts, remove only that claim; older owners are restored
                    // by separate update/defer-delete undo entries.
                    let res = self
                        .unique_compare_delete(index_pool_guard, &key, entry.row_id, true, ts)
                        .await?;
                    assert!(res);
                }
            }
            IndexUndoKind::InsertNonUnique(key, merge_old_deleted) => {
                if merge_old_deleted {
                    // Same exact `(key, row_id)` was unmasked during a hot
                    // key-change cycle. Rollback masks it back to deleted.
                    let res = self
                        .non_unique_mask_as_deleted(index_pool_guard, &key, entry.row_id, ts)
                        .await?;
                    assert!(res);
                } else {
                    // Remove the exact non-unique claim inserted by the
                    // aborted transaction.
                    let res = self
                        .non_unique_compare_delete(index_pool_guard, &key, entry.row_id, true, ts)
                        .await?;
                    assert!(res);
                }
            }
            IndexUndoKind::UpdateUnique(key, old_row_id, deleted) => {
                // Rollback restores the exact unique-index value that existed
                // before this transaction claimed the key. A delete-masked old
                // owner may already be stale, but rollback must not race purge by
                // inspecting row-page undo chains to prove that. Leaving the
                // stale marker is safe: it remains invisible to reads and normal
                // index purge or a later foreground update can remove it.
                let new_row_id = entry.row_id;
                let restored_row_id = if deleted {
                    old_row_id.deleted()
                } else {
                    old_row_id
                };
                let res = self
                    .unique_compare_exchange(
                        index_pool_guard,
                        &key,
                        new_row_id,
                        restored_row_id,
                        ts,
                    )
                    .await?;
                debug_assert!(res.is_ok());
            }
            IndexUndoKind::DeferDelete(key, unique) => {
                // Foreground hot/cold delete and update paths mask index
                // entries but leave them physically present for rollback and
                // old snapshots. Aborting the transaction unmasks the exact
                // owner that was deferred for deletion.
                if unique {
                    let res = self
                        .unique_compare_exchange(
                            index_pool_guard,
                            &key,
                            entry.row_id.deleted(),
                            entry.row_id,
                            ts,
                        )
                        .await?;
                    assert!(res.is_ok());
                } else {
                    let res = self
                        .non_unique_mask_as_active(index_pool_guard, &key, entry.row_id, ts)
                        .await?;
                    assert!(res);
                }
            }
        }
        Ok(())
    }
}

struct UserTableRollback<'a> {
    table: &'a Table,
    layout: &'a TableRuntimeLayout,
}

impl IndexRollback for UserTableRollback<'_> {
    type RowPool = EvictableBufferPool;
    type IndexPool = EvictableBufferPool;

    #[inline]
    fn mem_table(&self) -> &MemTable<Self::RowPool, Self::IndexPool> {
        &self.table.mem
    }

    #[inline]
    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self.layout.secondary_index(key.index_no)?;
        index
            .unique_mem()?
            .bind(index_pool_guard)
            .mask_as_deleted(&key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self.layout.secondary_index(key.index_no)?;
        index
            .unique_mem()?
            .bind(index_pool_guard)
            .compare_delete(&key.vals, row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        let index = self.layout.secondary_index(key.index_no)?;
        index
            .unique_mem()?
            .bind(index_pool_guard)
            .compare_exchange(&key.vals, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self.layout.secondary_index(key.index_no)?;
        index
            .non_unique_mem()?
            .bind(index_pool_guard)
            .mask_as_deleted(&key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self.layout.secondary_index(key.index_no)?;
        index
            .non_unique_mem()?
            .bind(index_pool_guard)
            .mask_as_active(&key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        let index = self.layout.secondary_index(key.index_no)?;
        index
            .non_unique_mem()?
            .bind(index_pool_guard)
            .compare_delete(&key.vals, row_id, ignore_del_mask, ts)
            .await
    }
}

impl Table {
    /// Roll back one secondary-index undo entry against a pinned runtime layout.
    #[inline]
    pub(crate) async fn rollback_index_entry_with_layout(
        &self,
        layout: &TableRuntimeLayout,
        entry: IndexUndo,
        guards: &PoolGuards,
        ts: TrxID,
    ) -> RuntimeResult<()> {
        UserTableRollback {
            table: self,
            layout,
        }
        .rollback_index_entry(entry, guards, ts)
        .await
    }
}

impl IndexRollback for CatalogTable {
    type RowPool = FixedBufferPool;
    type IndexPool = FixedBufferPool;

    #[inline]
    fn mem_table(&self) -> &MemTable<Self::RowPool, Self::IndexPool> {
        &self.mem
    }

    #[inline]
    async fn unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.mem
            .require_sec_idx(key.index_no)?
            .unique()
            // The undo variant is emitted from this index's immutable kind.
            .expect("unique rollback undo referenced a non-unique catalog index")
            .bind(index_pool_guard)
            .mask_as_deleted(&key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.mem
            .require_sec_idx(key.index_no)?
            .unique()
            // The undo variant is emitted from this index's immutable kind.
            .expect("unique rollback undo referenced a non-unique catalog index")
            .bind(index_pool_guard)
            .compare_delete(&key.vals, row_id, ignore_del_mask, ts)
            .await
    }

    #[inline]
    async fn unique_compare_exchange(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        old_row_id: RowID,
        new_row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<IndexCompareExchange> {
        self.mem
            .require_sec_idx(key.index_no)?
            .unique()
            // The undo variant is emitted from this index's immutable kind.
            .expect("unique rollback undo referenced a non-unique catalog index")
            .bind(index_pool_guard)
            .compare_exchange(&key.vals, old_row_id, new_row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_deleted(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.mem
            .require_sec_idx(key.index_no)?
            .non_unique()
            // The undo variant is emitted from this index's immutable kind.
            .expect("non-unique rollback undo referenced a unique catalog index")
            .bind(index_pool_guard)
            .mask_as_deleted(&key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_mask_as_active(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.mem
            .require_sec_idx(key.index_no)?
            .non_unique()
            // The undo variant is emitted from this index's immutable kind.
            .expect("non-unique rollback undo referenced a unique catalog index")
            .bind(index_pool_guard)
            .mask_as_active(&key.vals, row_id, ts)
            .await
    }

    #[inline]
    async fn non_unique_compare_delete(
        &self,
        index_pool_guard: &PoolGuard,
        key: &SelectKey,
        row_id: RowID,
        ignore_del_mask: bool,
        ts: TrxID,
    ) -> RuntimeResult<bool> {
        self.mem
            .require_sec_idx(key.index_no)?
            .non_unique()
            // The undo variant is emitted from this index's immutable kind.
            .expect("non-unique rollback undo referenced a unique catalog index")
            .bind(index_pool_guard)
            .compare_delete(&key.vals, row_id, ignore_del_mask, ts)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::PoolRole;
    use crate::catalog::tests::table4;
    use crate::conf::{EngineConfig, EvictableBufferPoolConfig, TrxSysConfig};
    use crate::engine::Engine;
    use crate::error::{DiscloseError, OperationError, Result};
    use crate::id::RowID;
    use crate::index::{RowLocation, UniqueIndex};
    use crate::row::ops::{DeleteMvcc, SelectKey, SelectMvcc, UpdateCol, UpdateMvcc};
    use crate::session::tests::{SessionTestExt, assert_checkpoint_published};
    use crate::table::CheckpointOutcome;
    use crate::table::tests::*;
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::value::Val;
    use error_stack::Report;
    use tempfile::TempDir;

    #[test]
    fn test_column_delete_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            assert_checkpoint_published(&mut session, table_id).await;

            let key = single_key(2i32);
            let mut reader_session = engine.new_session().unwrap();
            let trx = reader_session.begin_trx().unwrap();
            let old_row_id = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &reader_session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx.exec(async |stmt| {
                let res = stmt_delete_row_by_id(stmt, table_id, &key).await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                assert_unique_index_entry(
                    &table_for_internal_assertion(&engine, table_id),
                    &session.pool_guards(),
                    &key,
                    stmt.runtime().sts(),
                    old_row_id,
                    true,
                )
                .await;
                Ok(())
            })
            .await
            .unwrap();
            trx.rollback().await.unwrap();
            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                MAX_SNAPSHOT_TS,
                old_row_id,
                false,
            )
            .await;

            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_select(table_id, trx, &key, |row| {
                assert_eq!(row[0], Val::from(2i32));
            })
            .await;
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_column_delete_rollback_after_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;

            let key = single_key(3i32);
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            session
                .wait_for_gc_horizon_after(session.last_cts())
                .await
                .unwrap();
            let mut trx_delete = session.begin_trx().unwrap();
            let res = trx_delete_row_by_id(&mut trx_delete, table_id, &key).await;
            assert!(matches!(res, Ok(DeleteMvcc::Deleted)));

            let mut checkpoint_session = engine.new_session().unwrap();
            let outcome = checkpoint_session.checkpoint_table(table_id).await.unwrap();
            assert!(matches!(outcome, CheckpointOutcome::Published { .. }));

            let mut reader_session = engine.new_session().unwrap();
            let trx = reader_session.begin_trx().unwrap();
            let _ = assert_row_in_lwc(
                &table_for_internal_assertion(&engine, table_id),
                &reader_session.pool_guards(),
                &key,
                trx.sts(),
            )
            .await;
            trx.commit().await.unwrap();

            let res = trx_select_row_mvcc_by_id(&mut trx_delete, table_id, &key, &[0, 1]).await;
            assert!(matches!(res, Ok(SelectMvcc::NotFound)));
            trx_delete.rollback().await.unwrap();

            let mut trx = session.begin_trx().unwrap();
            trx = expect_trx_select(table_id, trx, &key, |row| {
                assert_eq!(row[0], Val::from(3i32));
            })
            .await;
            trx.commit().await.unwrap();
        });
    }

    #[test]
    fn test_unique_insert_rollback_restores_deleted_owner_even_when_row_missing() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let key = single_key(10_001i32);
            let stale_row_id = 10_001;

            assert!(matches!(
                table_for_internal_assertion(&engine, table_id)
                    .find_row(&session.pool_guards(), RowID::new(stale_row_id))
                    .await
                    .unwrap(),
                RowLocation::NotFound
            ));

            let pool_guards = session.pool_guards();
            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                key.index_no,
            );
            assert!(
                index
                    .insert_if_not_exists(
                        &key.vals,
                        RowID::new(stale_row_id),
                        false,
                        MAX_SNAPSHOT_TS,
                    )
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&key.vals, RowID::new(stale_row_id), MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );
            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                MAX_SNAPSHOT_TS,
                RowID::new(stale_row_id),
                true,
            )
            .await;

            let mut trx = session.begin_trx().unwrap();
            let res: Result<()> = trx
                .exec(async |stmt| {
                    let new_row_id = unwrap_insert_result(
                        stmt_insert_row_by_id(
                            stmt,
                            table_id,
                            vec![Val::from(10_001i32), Val::from("reborn")],
                        )
                        .await,
                    );
                    assert_ne!(new_row_id, RowID::new(stale_row_id));
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    Err(Report::new(OperationError::NotSupported).disclose())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::NotSupported)
            );
            trx.rollback().await.unwrap();

            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &key,
                MAX_SNAPSHOT_TS,
                RowID::new(stale_row_id),
                true,
            )
            .await;
            expect_select_not_found_committed(table_id, &mut session, &key).await;
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(10_001i32), Val::from("reclaimed")],
            )
            .await;
            expect_select_committed(table_id, &mut session, &key, |vals| {
                assert_eq!(vals, vec![Val::from(10_001i32), Val::from("reclaimed")]);
            })
            .await;
        });
    }

    #[test]
    fn test_unique_insert_rollback_restores_delete_marked_stale_hot_owner() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let live_key = single_key(1i32);
            let stale_key = single_key(2i32);
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(1i32), Val::from("one")],
            )
            .await;

            let reader = session.begin_trx().unwrap();
            let pool_guards = session.pool_guards();
            let old_row_id = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                live_key.index_no,
            )
            .lookup(&live_key.vals, reader.sts())
            .await
            .unwrap()
            .unwrap()
            .0;
            reader.commit().await.unwrap();
            assert!(matches!(
                table_for_internal_assertion(&engine, table_id)
                    .find_row(&session.pool_guards(), old_row_id)
                    .await
                    .unwrap(),
                RowLocation::RowPage(_)
            ));

            let index = bound_unique_index(
                &table_for_internal_assertion(&engine, table_id),
                &pool_guards,
                stale_key.index_no,
            );
            assert!(
                index
                    .insert_if_not_exists(&stale_key.vals, old_row_id, false, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
                    .is_ok()
            );
            assert!(
                index
                    .mask_as_deleted(&stale_key.vals, old_row_id, MAX_SNAPSHOT_TS,)
                    .await
                    .unwrap()
            );

            let mut trx = session.begin_trx().unwrap();
            let res: Result<()> = trx
                .exec(async |stmt| {
                    let new_row_id = unwrap_insert_result(
                        stmt_insert_row_by_id(
                            stmt,
                            table_id,
                            vec![Val::from(2i32), Val::from("two")],
                        )
                        .await,
                    );
                    assert_ne!(new_row_id, old_row_id);
                    assert_unique_index_entry(
                        &table_for_internal_assertion(&engine, table_id),
                        &session.pool_guards(),
                        &stale_key,
                        stmt.runtime().sts(),
                        new_row_id,
                        false,
                    )
                    .await;
                    Err(Report::new(OperationError::NotSupported).disclose())
                })
                .await;
            assert_eq!(
                res.unwrap_err()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::NotSupported)
            );
            trx.rollback().await.unwrap();

            assert_unique_index_entry(
                &table_for_internal_assertion(&engine, table_id),
                &session.pool_guards(),
                &stale_key,
                MAX_SNAPSHOT_TS,
                old_row_id,
                true,
            )
            .await;
            expect_select_committed(table_id, &mut session, &live_key, |vals| {
                assert_eq!(vals, vec![Val::from(1i32), Val::from("one")]);
            })
            .await;
            expect_select_not_found_committed(table_id, &mut session, &stale_key).await;
            expect_insert_committed(
                table_id,
                &mut session,
                vec![Val::from(2i32), Val::from("two-final")],
            )
            .await;
            expect_select_committed(table_id, &mut session, &stale_key, |vals| {
                assert_eq!(vals, vec![Val::from(2i32), Val::from("two-final")]);
            })
            .await;
        });
    }

    #[test]
    fn test_mvcc_rollback_insert_normal() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1 row
                let mut trx = session.begin_trx().unwrap();
                let insert = vec![Val::from(1i32), Val::from("hello")];
                trx = expect_trx_insert(table_id, trx, insert).await;
                // explicit rollback
                trx.rollback().await.unwrap();

                // select 1 row
                let key = single_key(1i32);
                _ = expect_select_not_found_committed(table_id, &mut session, &key).await;
            }
        });
    }

    #[test]
    fn test_mvcc_rollback_insert_link_unique_index() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                // insert 1 row
                let insert = vec![Val::from(1i32), Val::from("hello")];
                expect_insert_committed(table_id, &mut session, insert).await;

                // delete it
                let key = single_key(1i32);
                expect_delete_committed(table_id, &mut session, &key).await;

                // insert again, trigger insert+link
                let insert = vec![Val::from(1i32), Val::from("world")];
                let mut trx = session.begin_trx().unwrap();
                trx = expect_trx_insert(table_id, trx, insert).await;
                // explicit rollback
                trx.rollback().await.unwrap();

                // select 1 row
                let key = single_key(1i32);
                _ = expect_select_not_found_committed(table_id, &mut session, &key).await;
            }
        });
    }

    #[test]
    fn test_secondary_index_rollback() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(
                EngineConfig::default()
                    .storage_root(main_dir)
                    .data_buffer(EvictableBufferPoolConfig::default().role(PoolRole::Mem))
                    .trx(TrxSysConfig::default().log_file_stem("redo_secidx2")),
            )
            .await
            .unwrap();
            let table_id = table4(&engine).await;
            {
                let mut session = engine.new_session().unwrap();
                let user_read_set = &[0usize, 1];
                let mut trx = session.begin_trx().unwrap();
                for i in 0i32..5i32 {
                    let res =
                        trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(i), Val::from(i)])
                            .await;
                    assert!(res.is_ok());
                }
                trx.commit().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let res = trx_insert_row_by_id(
                    &mut trx,
                    table_id,
                    vec![Val::from(5i32), Val::from(5i32)],
                )
                .await;
                assert!(res.is_ok());
                trx.rollback().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(5i32)]);
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::NotFound)));

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let update = vec![UpdateCol {
                    idx: 1,
                    val: Val::from(0i32),
                }];
                let res = trx_update_row_by_id(&mut trx, table_id, &key, update).await;
                assert!(matches!(res, Ok(UpdateMvcc::Updated(_))));
                trx.rollback().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(1i32)]);
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                let vals = res.unwrap().unwrap_found();
                assert!(vals[0] == Val::from(1i32) && vals[1] == Val::from(1i32));

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                trx.rollback().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(0i32)]);
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
                trx.commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                let vals = res.unwrap().unwrap_found();
                assert!(vals[0] == Val::from(0i32) && vals[1] == Val::from(0i32));

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(3i32)]);
                let res = trx_delete_row_by_id(&mut trx, table_id, &key).await;
                assert!(matches!(res, Ok(DeleteMvcc::Deleted)));
                let res =
                    trx_insert_row_by_id(&mut trx, table_id, vec![Val::from(3), Val::from(3)])
                        .await;
                assert!(res.is_ok());
                trx.rollback().await.unwrap();

                let mut trx = session.begin_trx().unwrap();
                let key = SelectKey::new(0, vec![Val::from(3i32)]);
                let res = trx_select_row_mvcc_by_id(&mut trx, table_id, &key, user_read_set).await;
                _ = trx.commit().await.unwrap();
                assert!(matches!(res, Ok(SelectMvcc::Found(_))));
                let vals = res.unwrap().unwrap_found();
                assert!(vals[0] == Val::from(3i32) && vals[1] == Val::from(3i32));
            }
        })
    }
}
