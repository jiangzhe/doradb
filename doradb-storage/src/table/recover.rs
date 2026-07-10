use crate::buffer::PoolGuards;
use crate::error::{DataIntegrityError, RecoveryDuplicateKey, Result};
use crate::id::{PageID, RowID, TrxID};
use crate::index::IndexInsert;
use crate::index::{NonUniqueIndex, UniqueIndex};
use crate::row::RowRead;
use crate::row::ops::{ReadRow, UpdateCol};
use crate::table::{DeletionError, DmlValidationDomain, DmlValidator, Table};
use crate::trx::MIN_SNAPSHOT_TS;
use crate::trx::row::ReadAllRows;
use crate::value::Val;
use error_stack::Report;

impl Table {
    /// Recover row insert from redo log.
    pub(crate) async fn recover_row_insert(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cols: &[Val],
        cts: TrxID,
        disable_dml_validation: bool,
    ) -> Result<()> {
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        if !disable_dml_validation {
            DmlValidator::new(
                metadata,
                self.table_id(),
                "recover_row_insert",
                DmlValidationDomain::Recovery,
            )
            .validate_full_row(cols)?;
        }
        debug_assert!(cols.len() == metadata.col.col_count());
        debug_assert!({
            cols.iter()
                .enumerate()
                .all(|(idx, val)| metadata.col.col_type_match(idx, val))
        });
        // Since we always dispatch rows of one page to same thread,
        // we can just hold exclusive lock on this page and process all rows in it.
        let mut page_guard = self
            .mem
            .must_get_row_page_exclusive(guards, page_id)
            .await?;

        self.recover_row_insert_to_page(metadata, &mut page_guard, row_id, cols, cts)?;
        page_guard.set_dirty(); // mark as dirty page.
        Ok(())
    }

    /// Recover row update from redo log.
    pub(crate) async fn recover_row_update(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        update: &[UpdateCol],
        cts: TrxID,
        disable_dml_validation: bool,
    ) -> Result<()> {
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        if !disable_dml_validation {
            DmlValidator::new(
                metadata,
                self.table_id(),
                "recover_row_update",
                DmlValidationDomain::Recovery,
            )
            .validate_sparse_update(update)?;
        }
        let mut page_guard = self
            .mem
            .must_get_row_page_exclusive(guards, page_id)
            .await?;

        self.recover_row_update_to_page(metadata, &mut page_guard, row_id, update, cts)?;
        page_guard.set_dirty(); // mark as dirty page.
        Ok(())
    }

    /// Recover row delete from redo log.
    pub(crate) async fn recover_row_delete(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
        row_id: RowID,
        cts: TrxID,
    ) -> Result<()> {
        // `recovery_bootstrap_unchecked`: restart recovery runs without
        // surviving user transactions, so it binds the current loaded root
        // directly for cold-row delete replay predicates.
        let active_root = self.file().active_root_unchecked();
        if row_id < active_root.pivot_row_id {
            if cts < active_root.deletion_cutoff_ts {
                return Ok(());
            }
            match self.deletion_buffer().put_committed(row_id, cts) {
                Ok(()) => return Ok(()),
                Err(DeletionError::AlreadyDeleted | DeletionError::WriteConflict) => {
                    return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach(format!("row_id={row_id}, cts={cts}"))
                        .into());
                }
            }
        }

        let mut page_guard = self
            .mem
            .must_get_row_page_exclusive(guards, page_id)
            .await?;

        self.recover_row_delete_to_page(&mut page_guard, row_id, cts)?;
        page_guard.set_dirty(); // mark as dirty page.
        Ok(())
    }

    /// Populate index using data on row page.
    pub(crate) async fn populate_index_via_row_page(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> Result<()> {
        let page_guard = self.mem.must_get_row_page_shared(guards, page_id).await?;
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        let index_pool_guard = self.mem.index_pool_guard(guards)?;
        let (ctx, page) = page_guard.ctx_and_page();
        for (index_no, index_spec) in metadata.idx.active_indexes() {
            let sec_idx = layout.secondary_index(index_no)?;
            let read_set: Vec<_> = index_spec.cols.iter().map(|c| c.col_no as usize).collect();
            for row_access in ReadAllRows::new(page, ctx) {
                let row_id = row_access.row().row_id();
                match row_access.read_row_latest(metadata, &read_set, None) {
                    ReadRow::Ok(vals) => {
                        if index_spec.unique() {
                            let index = sec_idx.unique_mem()?;
                            let res = index
                                .bind(index_pool_guard)
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await?;
                            ensure_recovery_index_insert(sec_idx.index_no(), res)?;
                        } else {
                            let index = sec_idx.non_unique_mem()?;
                            let res = index
                                .bind(index_pool_guard)
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await?;
                            ensure_recovery_index_insert(sec_idx.index_no(), res)?;
                        }
                    }
                    ReadRow::NotFound => (),
                    ReadRow::InvalidIndex => unreachable!(),
                }
            }
        }
        Ok(())
    }
}

/// Reject duplicate secondary-index entries during recovery rebuild.
#[inline]
pub(super) fn ensure_recovery_index_insert(index_no: usize, res: IndexInsert) -> Result<()> {
    match res {
        IndexInsert::Ok(_) => Ok(()),
        IndexInsert::DuplicateKey(row_id, deleted) => Err(Report::new(
            DataIntegrityError::UnexpectedRecoveryDuplicateKey,
        )
        .attach(RecoveryDuplicateKey {
            index_no,
            row_id,
            deleted,
        })
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_recovery_index_insert;
    use crate::buffer::guard::PageGuard;
    use crate::buffer::page::PAGE_SIZE;
    use crate::catalog::{TableMetadata, USER_TABLE_ID_START};
    use crate::error::{DataIntegrityError, Error, RecoveryDuplicateKey};
    use crate::id::RowID;
    use crate::id::{PageID, TrxID};
    use crate::index::IndexInsert;
    use crate::row::ops::UpdateCol;
    use crate::session::tests::SessionTestExt;
    use crate::table::tests::*;
    use crate::value::Val;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_ensure_recovery_index_insert_accepts_ok_variants() {
        assert!(ensure_recovery_index_insert(1, IndexInsert::Ok(false)).is_ok());
        assert!(ensure_recovery_index_insert(1, IndexInsert::Ok(true)).is_ok());
    }

    #[test]
    fn test_ensure_recovery_index_insert_rejects_duplicate_key() {
        let err = ensure_recovery_index_insert(3, IndexInsert::DuplicateKey(RowID::new(42), false))
            .unwrap_err();
        let duplicate = err
            .downcast_ref::<RecoveryDuplicateKey>()
            .unwrap_or_else(|| panic!("unexpected error: {err:?}"));
        assert_eq!(duplicate.index_no, 3);
        assert_eq!(duplicate.row_id, RowID::new(42));
        assert!(!duplicate.deleted);
    }

    #[test]
    fn test_recover_cold_delete_rejects_already_deleted_with_different_cts() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            insert_rows(table_id, &mut session, 0, 10, "name").await;
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
            checkpoint_published(table_id, &mut session).await;

            let key = single_key(6i32);
            let reader = session.begin_trx().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let row_id =
                assert_row_in_lwc(&table, &session.pool_guards(), &key, reader.sts()).await;
            reader.commit().await.unwrap();

            let active_root = table.file().active_root_unchecked().clone();
            assert!(row_id < active_root.pivot_row_id);
            let cts = active_root.deletion_cutoff_ts;
            table
                .recover_row_delete(&session.pool_guards(), PageID::from(0u64), row_id, cts)
                .await
                .unwrap();
            table
                .recover_row_delete(&session.pool_guards(), PageID::from(0u64), row_id, cts)
                .await
                .unwrap();

            let err = table
                .recover_row_delete(&session.pool_guards(), PageID::from(0u64), row_id, cts + 1)
                .await
                .unwrap_err();
            assert_eq!(
                err.report().downcast_ref::<DataIntegrityError>().copied(),
                Some(DataIntegrityError::InvalidRootInvariant)
            );
        });
    }

    #[test]
    fn test_recover_row_page_reports_invalid_replay_state() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();
            let metadata = table_for_internal_assertion(&engine, table_id).metadata();
            let mut page_guard = table_for_internal_assertion(&engine, table_id)
                .mem
                .get_insert_page_exclusive(&session.pool_guards(), 2, None)
                .await
                .unwrap();
            let row_id = page_guard.page().header.start_row_id;
            let assert_invalid_root = |err: Error, reason: &str| {
                let report = format!("{err:?}");
                assert_eq!(
                    err.data_integrity_error(),
                    Some(DataIntegrityError::InvalidRootInvariant),
                    "{report}"
                );
                assert!(report.contains(reason), "{report}");
                assert!(report.contains("recover row"), "{report}");
            };

            let err = table_for_internal_assertion(&engine, table_id)
                .recover_row_insert_to_page(
                    metadata.as_ref(),
                    &mut page_guard,
                    row_id,
                    &[Val::from(1i32), Val::from("name")],
                    TrxID::new(10),
                )
                .unwrap_err();
            assert_invalid_root(err, "missing recover map");

            page_guard.bf_mut().init_recover_map(TrxID::new(10));
            let err = table_for_internal_assertion(&engine, table_id)
                .recover_row_insert_to_page(
                    metadata.as_ref(),
                    &mut page_guard,
                    row_id,
                    &[Val::from(1i32), Val::from(vec![b'x'; PAGE_SIZE - 1])],
                    TrxID::new(11),
                )
                .unwrap_err();
            assert_invalid_root(err, "insufficient row page space");

            table_for_internal_assertion(&engine, table_id)
                .recover_row_insert_to_page(
                    metadata.as_ref(),
                    &mut page_guard,
                    row_id,
                    &[Val::from(1i32), Val::from("name")],
                    TrxID::new(12),
                )
                .unwrap();
            assert_eq!(page_guard.page().header.approx_non_deleted(), 1);

            let err = table_for_internal_assertion(&engine, table_id)
                .recover_row_insert_to_page(
                    metadata.as_ref(),
                    &mut page_guard,
                    row_id,
                    &[Val::from(2i32), Val::from("other")],
                    TrxID::new(13),
                )
                .unwrap_err();
            assert_invalid_root(err, "row slot is not vacant");

            let err = table_for_internal_assertion(&engine, table_id)
                .recover_row_update_to_page(
                    metadata.as_ref(),
                    &mut page_guard,
                    row_id + 1,
                    &[UpdateCol {
                        idx: 1,
                        val: Val::from("new"),
                    }],
                    TrxID::new(14),
                )
                .unwrap_err();
            assert_invalid_root(err, "row is deleted");

            table_for_internal_assertion(&engine, table_id)
                .recover_row_delete_to_page(&mut page_guard, row_id, TrxID::new(15))
                .unwrap();
            assert_eq!(page_guard.page().header.approx_non_deleted(), 0);

            let err = table_for_internal_assertion(&engine, table_id)
                .recover_row_delete_to_page(&mut page_guard, row_id, TrxID::new(16))
                .unwrap_err();
            assert_invalid_root(err, "row is already deleted");

            let err = table_for_internal_assertion(&engine, table_id)
                .recover_row_delete_to_page(&mut page_guard, row_id + 2, TrxID::new(17))
                .unwrap_err();
            assert_invalid_root(err, "row id outside page range");
        });
    }

    #[test]
    fn test_recover_row_dml_validation_rejects_malformed_payloads() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);

            let err = table
                .recover_row_insert(
                    &session.pool_guards(),
                    PageID::from(0u64),
                    RowID::new(0),
                    &[Val::from(1i32)],
                    TrxID::new(10),
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(report.contains("recover_row_insert"), "{report}");

            let err = table
                .recover_row_update(
                    &session.pool_guards(),
                    PageID::from(0u64),
                    RowID::new(0),
                    &[UpdateCol {
                        idx: 2,
                        val: Val::from("out-of-range"),
                    }],
                    TrxID::new(11),
                    false,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.data_integrity_error(),
                Some(DataIntegrityError::InvalidPayload)
            );
            let report = format!("{err:?}");
            assert!(report.contains("recover_row_update"), "{report}");
        });
    }

    #[test]
    fn test_drop_table_recovery_keeps_table_live_without_committed_drop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine =
                lightweight_test_engine_config(main_dir.clone(), "drop_recover_uncommitted")
                    .build()
                    .await
                    .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session.create_table(table_spec, index_specs).await.unwrap();
            let table_for_internal_lifecycle = engine
                .catalog()
                .get_table_now(table_id)
                .expect("created table should still be loaded");
            table_for_internal_lifecycle
                .start_drop_lifecycle()
                .unwrap()
                .wait()
                .await;

            drop(table_for_internal_lifecycle);
            drop(session);
            drop(engine);

            let engine = lightweight_test_engine_config(main_dir, "drop_recover_uncommitted")
                .build()
                .await
                .unwrap();
            assert!(engine.catalog().get_table(table_id).await.is_some());
        });
    }

    #[test]
    fn test_drop_table_recovery_replays_committed_drop_before_catalog_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir.clone(), "drop_recover_replay")
                .build()
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session.create_table(table_spec, index_specs).await.unwrap();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);

            session.drop_table(table_id).await.unwrap();
            assert!(std::path::Path::new(&table_file_path).exists());

            drop(session);
            drop(engine);

            let engine = lightweight_test_engine_config(main_dir, "drop_recover_replay")
                .build()
                .await
                .unwrap();
            assert!(engine.catalog().get_table(table_id).await.is_none());
            assert_eq!(
                engine.catalog().retained_dropped_table_ids_now(),
                vec![table_id]
            );
            assert!(std::path::Path::new(&table_file_path).exists());
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let _ = session.create_table(table_spec, index_specs).await.unwrap();
            engine
                .catalog()
                .checkpoint_now(&engine.inner().trx_sys)
                .await
                .unwrap();
            wait_path_exists(&table_file_path, false).await;
            assert!(engine.catalog().retained_dropped_table_ids_now().is_empty());
        });
    }

    #[test]
    fn test_recovery_cleans_post_replay_create_table_provisional_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = lightweight_test_engine_config(main_dir.clone(), "create_orphan_recover")
                .build()
                .await
                .unwrap();
            let table_id = USER_TABLE_ID_START + 99;
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);
            let (table_spec, index_specs) = drop_table_test_spec();
            let metadata =
                Arc::new(TableMetadata::try_new(table_spec.columns, index_specs).unwrap());
            let mutable = engine
                .inner()
                .table_fs
                .create_table_file(table_id, metadata, false)
                .unwrap();
            let (table_file, old_root) = mutable.commit(TrxID::new(1), false).await.unwrap();
            drop(old_root);
            drop(table_file);
            assert!(std::path::Path::new(&table_file_path).exists());

            drop(engine);

            let engine = lightweight_test_engine_config(main_dir, "create_orphan_recover")
                .build()
                .await
                .unwrap();
            assert!(engine.catalog().get_table(table_id).await.is_none());
            wait_path_exists(&table_file_path, false).await;
        });
    }
}
