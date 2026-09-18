use crate::buffer::PoolGuards;
use crate::buffer::guard::PageExclusiveGuard;
use crate::catalog::{IndexSlot, TableMetadata};
use crate::error::{
    DataIntegrityError, DataIntegrityResult, RecoveryDuplicateKey, RuntimeError,
    RuntimeOrFatalResult, RuntimeResult,
};
use crate::id::{PageID, RowID, TrxID};
use crate::index::IndexInsert;
use crate::log::redo::RowRedoKind;
use crate::recovery::{ReplayOp, RowReplayCounts, RowReplayState};
use crate::row::ops::ReadRow;
use crate::row::{RowPage, RowRead};
use crate::stats::recovery_add_count;
use crate::table::{DeletionError, DmlValidator, Table};
use crate::trx::MIN_SNAPSHOT_TS;
use error_stack::{Report, ResultExt};

impl Table {
    /// Apply one ordered hot-page batch through one exclusive page acquisition.
    /// The caller groups all operations for `replay.page_id()`.
    pub(crate) async fn recover_row_batch(
        &self,
        guards: &PoolGuards,
        replay: &mut RowReplayState,
        ops: &[ReplayOp],
        disable_dml_validation: bool,
    ) -> RuntimeOrFatalResult<RowReplayCounts> {
        let page_id = replay.page_id();
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        let mut page_guard = self
            .row_store
            .must_get_row_page_exclusive(guards, page_id)
            .await?;
        let mut counts = RowReplayCounts::default();
        // The job exclusively owns the bitmap. No scheduler wait occurs while
        // the latch is held, and every successful mutation is marked dirty even
        // if validation or mutation of a later operation fails.
        let result = self.recover_row_batch_to_page(
            metadata,
            &mut page_guard,
            replay,
            ops,
            disable_dml_validation,
            &mut counts,
        );
        if !counts.is_empty() {
            page_guard.set_dirty();
        }
        result?;
        Ok(counts)
    }

    /// Apply ordered operations to the latched page, retaining counts on failure.
    fn recover_row_batch_to_page(
        &self,
        metadata: &TableMetadata,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        replay: &mut RowReplayState,
        ops: &[ReplayOp],
        disable_dml_validation: bool,
        counts: &mut RowReplayCounts,
    ) -> RuntimeResult<()> {
        let page_id = replay.page_id();
        for op in ops {
            self.recover_row_op_to_page(
                metadata,
                page_guard,
                replay,
                op,
                disable_dml_validation,
                counts,
            )
            .change_context(RuntimeError::TableAccess)
            .attach_with(|| {
                format!(
                    "operation=recover_row_batch, kind={:?}, table_id={}, page_id={page_id}, row_id={}, cts={}",
                    op.row.kind.code(), self.table_id(), op.row.row_id, op.cts
                )
            })?;
        }
        Ok(())
    }

    /// Validate and apply one operation, counting only its successful mutation.
    fn recover_row_op_to_page(
        &self,
        metadata: &TableMetadata,
        page_guard: &mut PageExclusiveGuard<RowPage>,
        replay: &mut RowReplayState,
        op: &ReplayOp,
        disable_dml_validation: bool,
        counts: &mut RowReplayCounts,
    ) -> DataIntegrityResult<()> {
        let row_id = op.row.row_id;
        let cts = op.cts;
        match &op.row.kind {
            RowRedoKind::Insert(_, cols) => {
                if !disable_dml_validation {
                    DmlValidator::new(metadata)
                        .validate_full_row(cols)
                        .change_context(DataIntegrityError::InvalidPayload)?;
                }
                self.recover_row_insert_to_page(metadata, page_guard, replay, row_id, cols, cts)?;
                counts.inserts += 1;
            }
            RowRedoKind::Update(_, cols) => {
                if !disable_dml_validation {
                    DmlValidator::new(metadata)
                        .validate_sparse_update(cols)
                        .change_context(DataIntegrityError::InvalidPayload)?;
                }
                self.recover_row_update_to_page(metadata, page_guard, replay, row_id, cols, cts)?;
                counts.updates += 1;
            }
            RowRedoKind::Delete(_) => {
                self.recover_row_delete_to_page(page_guard, replay, row_id, cts)?;
                counts.deletes += 1;
            }
            RowRedoKind::DeleteByPrimaryKey(_) | RowRedoKind::UpdateByPrimaryKey(..) => {
                unreachable!("catalog redo in hot-page batch")
            }
        }
        Ok(())
    }

    /// Replays a committed cold-row deletion without hot-page replay state.
    pub(crate) fn recover_cold_row_delete(
        &self,
        row_id: RowID,
        cts: TrxID,
    ) -> DataIntegrityResult<()> {
        // `recovery_bootstrap_unchecked`: no surviving transactions exist at startup.
        let active_root = self.file().active_root_unchecked();
        if row_id >= active_root.pivot_row_id {
            return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                .attach("cold row delete requires a row below the table pivot"));
        }
        if cts < active_root.deletion_cutoff_ts {
            return Ok(());
        }
        self.deletion_buffer()
            .put_committed(row_id, cts)
            .map_err(|err| match err {
                DeletionError::AlreadyDeleted | DeletionError::WriteConflict => {
                    Report::new(DataIntegrityError::InvalidRootInvariant)
                        .attach("conflicting committed cold-row deletion")
                }
            })
    }

    /// Populate active indexes from one row page and return successful entry count
    /// plus an arithmetic saturation flag.
    pub(crate) async fn populate_index_via_row_page(
        &self,
        guards: &PoolGuards,
        page_id: PageID,
    ) -> RuntimeOrFatalResult<(u64, bool)> {
        let mut entries = 0;
        let mut saturated = false;
        let page_guard = self
            .row_store
            .must_get_row_page_shared(guards, page_id)
            .await?;
        let layout = self.layout_snapshot();
        let metadata = layout.metadata();
        let index_pool_guard = guards.index_guard();
        for (index_slot, index_spec) in metadata.idx.active_indexes() {
            let sec_idx = layout.expect_secondary_index(index_spec.index);
            let read_set: Vec<_> = index_spec
                .keys
                .iter()
                .map(|c| c.column_ordinal.as_usize())
                .collect();
            for row_access in page_guard.read_all_rows() {
                let row_id = row_access.row().row_id();
                match row_access.read_row_latest(metadata, &read_set, None) {
                    ReadRow::Ok(vals) => {
                        if index_spec.unique() {
                            let index = sec_idx
                                .unique_mem()
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=populate_index_via_row_page, table_id={}, page_id={page_id}, index_slot={index_slot}",
                                        self.table_id()
                                    )
                                })?;
                            let res = index
                                .bind(index_pool_guard)
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await?;
                            ensure_recovery_index_insert(sec_idx.index_slot(), res)
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=populate_index_via_row_page, table_id={}, page_id={page_id}, index_slot={index_slot}, row_id={row_id}",
                                        self.table_id()
                                    )
                                })?;
                        } else {
                            let index = sec_idx
                                .non_unique_mem()
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=populate_index_via_row_page, table_id={}, page_id={page_id}, index_slot={index_slot}",
                                        self.table_id()
                                    )
                                })?;
                            let res = index
                                .bind(index_pool_guard)
                                .insert_if_not_exists(&vals, row_id, false, MIN_SNAPSHOT_TS)
                                .await?;
                            ensure_recovery_index_insert(sec_idx.index_slot(), res)
                                .change_context(RuntimeError::TableAccess)
                                .attach_with(|| {
                                    format!(
                                        "operation=populate_index_via_row_page, table_id={}, page_id={page_id}, index_slot={index_slot}, row_id={row_id}",
                                        self.table_id()
                                    )
                                })?;
                        }
                        recovery_add_count(&mut entries, 1, &mut saturated);
                    }
                    ReadRow::NotFound => (),
                    ReadRow::InvalidIndex => unreachable!(),
                }
            }
        }
        Ok((entries, saturated))
    }
}

/// Reject duplicate secondary-index entries during recovery rebuild.
#[inline]
pub(super) fn ensure_recovery_index_insert(
    index_slot: IndexSlot,
    res: IndexInsert,
) -> DataIntegrityResult<()> {
    match res {
        IndexInsert::Ok(_) => Ok(()),
        IndexInsert::DuplicateKey(row_id, deleted) => Err(Report::new(
            DataIntegrityError::UnexpectedRecoveryDuplicateKey,
        )
        .attach(RecoveryDuplicateKey {
            index_slot: index_slot.as_usize(),
            row_id,
            deleted,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_recovery_index_insert;
    use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
    use crate::buffer::page::PAGE_SIZE;
    use crate::catalog::tests::{
        assert_dropped_table_floor, assert_no_dropped_table_operational_state,
        wait_for_no_dropped_table_operational_state,
    };
    use crate::catalog::{IndexSlot, TableMetadata, USER_TABLE_ID_START};
    use crate::engine::Engine;
    use crate::error::RuntimeOrFatalError;
    use crate::error::{DataIntegrityError, RecoveryDuplicateKey, RuntimeError};
    use crate::id::RowID;
    use crate::id::TrxID;
    use crate::index::IndexInsert;
    use crate::log::redo::{RowRedo, RowRedoKind};
    use crate::recovery::{ReplayOp, RowReplayState};
    use crate::row::ops::UpdateCol;
    use crate::row::{RowPage, RowRead};
    use crate::session::tests::{SessionTestExt, assert_checkpoint_published};
    use crate::table::{DmlValidationError, tests::*};
    use crate::trx::MAX_SNAPSHOT_TS;
    use crate::value::Val;
    use error_stack::Report;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn replay_state(page: &PageExclusiveGuard<RowPage>) -> RowReplayState {
        RowReplayState::new(page.page_id(), page.page().header.max_row_count as usize)
    }

    fn assert_invalid_replay(err: Report<DataIntegrityError>, reason: &str) {
        assert_eq!(
            *err.current_context(),
            DataIntegrityError::InvalidRootInvariant
        );
        let report = format!("{err:?}");
        assert!(report.contains(reason), "{report}");
    }

    #[test]
    fn test_ensure_recovery_index_insert_accepts_ok_variants() {
        let index_slot = IndexSlot::new(1);
        assert!(ensure_recovery_index_insert(index_slot, IndexInsert::Ok(false)).is_ok());
        assert!(ensure_recovery_index_insert(index_slot, IndexInsert::Ok(true)).is_ok());
    }

    #[test]
    fn test_ensure_recovery_index_insert_rejects_duplicate_key() {
        let err = ensure_recovery_index_insert(
            IndexSlot::new(3),
            IndexInsert::DuplicateKey(RowID::new(42), false),
        )
        .unwrap_err();
        let duplicate = err
            .downcast_ref::<RecoveryDuplicateKey>()
            .unwrap_or_else(|| panic!("unexpected error: {err:?}"));
        assert_eq!(duplicate.index_slot, 3);
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
            assert_checkpoint_published(&mut session, table_id).await;

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
                .recover_cold_row_delete(row_id, TrxID::new(cts.as_u64() - 1))
                .unwrap();
            assert!(table.deletion_buffer().get(row_id).is_none());
            table.recover_cold_row_delete(row_id, cts).unwrap();
            table.recover_cold_row_delete(row_id, cts).unwrap();
            let err = table.recover_cold_row_delete(row_id, cts + 1).unwrap_err();
            assert_invalid_replay(err, "conflicting committed cold-row deletion");
            let err = table
                .recover_cold_row_delete(active_root.pivot_row_id, cts)
                .unwrap_err();
            assert_invalid_replay(err, "requires a row below the table pivot");
        });
    }

    #[test]
    fn test_recover_row_page_sparse_bitmap_boundaries_and_slot_history() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine =
                evictable_test_engine(&temp_dir, 64u64 * 1024 * 1024, "redo_testsys").await;
            let table_id = create_table2_for_test(&engine).await;
            let session = engine.new_session().unwrap();
            let table = table_for_internal_assertion(&engine, table_id);
            let metadata = table.metadata();
            let mut page = table
                .row_store
                .get_insert_page_exclusive(&session.pool_guards(), 70)
                .await
                .unwrap();
            let mut replay = replay_state(&page);
            let first = page.page().header.start_row_id;
            let vals = [Val::from(1i32), Val::from("name")];
            let update = [UpdateCol {
                idx: 1,
                val: Val::from("changed"),
            }];
            let mut cts = TrxID::new(10);
            for idx in [69, 64, 63, 0] {
                let row_id = first + idx as u64;
                table
                    .recover_row_insert_to_page(
                        &metadata,
                        &mut page,
                        &mut replay,
                        row_id,
                        &vals,
                        cts,
                    )
                    .unwrap();
                assert!(replay.is_inserted(idx));
                cts = cts + 1;
                let err = table
                    .recover_row_insert_to_page(
                        &metadata,
                        &mut page,
                        &mut replay,
                        row_id,
                        &vals,
                        cts,
                    )
                    .unwrap_err();
                assert_invalid_replay(err, "row slot was already inserted");
                cts = cts + 1;
                table
                    .recover_row_update_to_page(&metadata, &mut page, &replay, row_id, &update, cts)
                    .unwrap();
                assert_eq!(
                    page.page().row(idx).val(&metadata.col, 1),
                    Val::from("changed")
                );
                assert!(replay.is_inserted(idx));
                cts = cts + 1;
                table
                    .recover_row_delete_to_page(&mut page, &replay, row_id, cts)
                    .unwrap();
                assert!(replay.is_inserted(idx));
                cts = cts + 1;
                let err = table
                    .recover_row_insert_to_page(
                        &metadata,
                        &mut page,
                        &mut replay,
                        row_id,
                        &vals,
                        cts,
                    )
                    .unwrap_err();
                assert_invalid_replay(err, "row slot was already inserted");
                let err = table
                    .recover_row_update_to_page(&metadata, &mut page, &replay, row_id, &update, cts)
                    .unwrap_err();
                assert_invalid_replay(err, "row is deleted");
                let err = table
                    .recover_row_delete_to_page(&mut page, &replay, row_id, cts)
                    .unwrap_err();
                assert_invalid_replay(err, "row is already deleted");
                cts = cts + 1;
            }
            assert_eq!(page.page().header.row_count(), 70);
            assert!((0..70).all(|slot| page.page().is_deleted(slot)));
            for idx in 0..70 {
                assert_eq!(replay.is_inserted(idx), [0, 63, 64, 69].contains(&idx));
            }
            // Slot 70 is inside the rounded bitmap storage but outside this page.
            for row_id in [first + 70, first + 127] {
                let err = table
                    .recover_row_insert_to_page(
                        &metadata,
                        &mut page,
                        &mut replay,
                        row_id,
                        &vals,
                        cts,
                    )
                    .unwrap_err();
                assert_invalid_replay(err, "row id outside page range");
                let err = table
                    .recover_row_update_to_page(&metadata, &mut page, &replay, row_id, &update, cts)
                    .unwrap_err();
                assert_invalid_replay(err, "row id outside page range");
                let err = table
                    .recover_row_delete_to_page(&mut page, &replay, row_id, cts)
                    .unwrap_err();
                assert_invalid_replay(err, "row id outside page range");
            }
            let unused = first + 1;
            let err = table
                .recover_row_update_to_page(&metadata, &mut page, &replay, unused, &update, cts)
                .unwrap_err();
            assert_invalid_replay(err, "missing inserted state");
            let err = table
                .recover_row_delete_to_page(&mut page, &replay, unused, cts)
                .unwrap_err();
            assert_invalid_replay(err, "missing inserted state");
            let huge = [Val::from(1i32), Val::from(vec![b'x'; PAGE_SIZE - 1])];
            let err = table
                .recover_row_insert_to_page(&metadata, &mut page, &mut replay, unused, &huge, cts)
                .unwrap_err();
            assert_invalid_replay(err, "insufficient row page space");
            assert!(!replay.is_inserted(1));
            assert!(page.page().is_deleted(1));
            // A live physical row without its inserted bit is also invalid.
            page.page_mut().set_deleted_exclusive(1, false);
            let before = page.page().header.var_field_offset();
            let err = table
                .recover_row_insert_to_page(&metadata, &mut page, &mut replay, unused, &vals, cts)
                .unwrap_err();
            assert_invalid_replay(err, "row slot is not deleted");
            assert!(!replay.is_inserted(1));
            assert_eq!(page.page().header.var_field_offset(), before);
            page.page_mut().set_deleted_exclusive(1, true);
            table
                .recover_row_insert_to_page(&metadata, &mut page, &mut replay, unused, &vals, cts)
                .unwrap();
            let err = table
                .recover_row_update_to_page(
                    &metadata,
                    &mut page,
                    &replay,
                    unused,
                    &[UpdateCol {
                        idx: 1,
                        val: huge[1].clone(),
                    }],
                    cts + 1,
                )
                .unwrap_err();
            assert_invalid_replay(err, "insufficient row page space");
            assert!(replay.is_inserted(1));
            assert_eq!(page.page().row(1).val(&metadata.col, 1), vals[1]);
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
            let guards = session.pool_guards();
            let page = table
                .row_store
                .get_insert_page_exclusive(&guards, 2)
                .await
                .unwrap();
            let page_id = page.page_id();
            let row_id = page.page().header.start_row_id;
            let mut replay = replay_state(&page);
            drop(page);
            for kind in [
                RowRedoKind::Insert(page_id, vec![Val::from(1i32)]),
                RowRedoKind::Update(
                    page_id,
                    vec![UpdateCol {
                        idx: 2,
                        val: Val::from("out-of-range"),
                    }],
                ),
            ] {
                let ops = [ReplayOp {
                    cts: TrxID::new(10),
                    row: RowRedo { row_id, kind },
                }];
                let err = table
                    .recover_row_batch(&guards, &mut replay, &ops, false)
                    .await
                    .unwrap_err();
                let RuntimeOrFatalError::Runtime(err) = err else {
                    panic!("expected Runtime error, got {err:?}")
                };
                assert_eq!(*err.current_context(), RuntimeError::TableAccess);
                assert_eq!(
                    err.downcast_ref::<DataIntegrityError>().copied(),
                    Some(DataIntegrityError::InvalidPayload)
                );
                assert!(err.downcast_ref::<DmlValidationError>().is_some());
                let report = format!("{err:?}");
                assert!(report.contains("recover_row_batch"), "{report}");
                assert!(report.contains(&format!("table_id={table_id}")), "{report}");
                assert!(!replay.is_inserted(0));
            }
        });
    }

    #[test]
    fn test_drop_table_recovery_keeps_table_live_without_committed_drop() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "drop_recover_uncommitted",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();
            let table_for_internal_lifecycle = engine
                .inner()
                .core
                .catalog()
                .get_table(table_id)
                .expect("created table should still be loaded");
            table_for_internal_lifecycle
                .start_drop_lifecycle()
                .unwrap()
                .wait()
                .await;

            drop(table_for_internal_lifecycle);
            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "drop_recover_uncommitted",
            ))
            .await
            .unwrap();
            let current = engine
                .inner()
                .core
                .catalog()
                .resolve_user_table_current(table_id)
                .unwrap();
            assert_eq!(current.effective_cts(), TrxID::new(0));
            let table = current.live_table().unwrap();
            assert!(Arc::ptr_eq(
                table.layout_snapshot().metadata_arc(),
                &table.metadata()
            ));
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .user_table_history_version_count(table_id),
                Some(0)
            );
            assert_no_dropped_table_operational_state(engine.inner().core.catalog(), table_id);
        });
    }

    #[test]
    fn test_drop_table_recovery_replays_committed_drop_before_catalog_checkpoint() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "drop_recover_replay",
            ))
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let table_id = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();
            let table_file_path = engine.inner().table_fs.user_table_file_path(table_id);

            session.drop_table(table_id).await.unwrap();
            assert!(std::path::Path::new(&table_file_path).exists());

            drop(session);
            drop(engine);

            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "drop_recover_replay",
            ))
            .await
            .unwrap();
            assert!(engine.inner().core.catalog().get_table(table_id).is_none());
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_current(table_id)
                    .is_none()
            );
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .resolve_user_table_visible(table_id, MAX_SNAPSHOT_TS)
                    .is_none()
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .user_table_history_version_count(table_id),
                None
            );
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .retained_dropped_table_ids_now(),
                vec![table_id]
            );
            assert_dropped_table_floor(engine.inner().core.catalog(), table_id);
            assert!(std::path::Path::new(&table_file_path).exists());
            let mut session = engine.new_session().unwrap();
            let (table_spec, index_specs) = drop_table_test_spec();
            let _ = session
                .create_table(table_spec, index_specs)
                .await
                .unwrap()
                .table_id();
            engine
                .new_session()
                .unwrap()
                .checkpoint_catalog()
                .await
                .unwrap();
            wait_for_no_dropped_table_operational_state(&engine, table_id).await;
            assert!(!std::path::Path::new(&table_file_path).exists());
            assert!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .retained_dropped_table_ids_now()
                    .is_empty()
            );
            assert_no_dropped_table_operational_state(engine.inner().core.catalog(), table_id);
            assert_eq!(
                engine
                    .inner()
                    .core
                    .catalog()
                    .user_table_history_version_count(table_id),
                None
            );
        });
    }

    #[test]
    fn test_recovery_cleans_post_replay_create_table_provisional_file() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let main_dir = temp_dir.path().to_path_buf();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir.clone(),
                "create_orphan_recover",
            ))
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

            let engine = Engine::bootstrap(lightweight_test_engine_config(
                main_dir,
                "create_orphan_recover",
            ))
            .await
            .unwrap();
            assert!(engine.inner().core.catalog().get_table(table_id).is_none());
            wait_path_exists(&table_file_path, false).await;
        });
    }
}
