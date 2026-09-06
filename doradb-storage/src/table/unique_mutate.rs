//! Unique-point selection, retained ownership, and logical action dispatch.

use super::access::{
    ColdLatestRow, LazyRow, LazyRowBuffer, LazyRowSource, UserTableAccessor, WriteIndexKeySet,
};
use super::hot::{DeleteInternal, HotRowMutator};
use super::{DeletionClaim, DeletionError, DmlValidator};
use crate::catalog::IndexRef;
use crate::error::{
    CallbackResult, DataIntegrityError, DiscloseError, DiscloseResultExt, OperationError,
    OperationResult, RuntimeError,
};
use crate::index::RowLocation;
use crate::row::ops::{RowUpdateInput, UniqueMutation, UniqueMutationOutcome};
use crate::trx::TrxRuntime;
use crate::trx::row::LockRowForWrite;
use crate::trx::stmt::StmtEffects;
use crate::value::Val;
use error_stack::{Report, ResultExt};

#[cfg(test)]
pub(crate) use tests::record_point_disk_lookup;

/// Operation-scoped point executor; guards and roots remain attempt-local.
pub(super) struct UniquePointMutator<'a, 'op, 'r> {
    accessor: &'a UserTableAccessor<'op>,
    rt: TrxRuntime<'r>,
    effects: &'a mut StmtEffects,
    index: IndexRef,
    validator: Option<DmlValidator<'a>>,
}

impl<'a, 'op, 'r> UniquePointMutator<'a, 'op, 'r> {
    pub(super) fn new(
        accessor: &'a UserTableAccessor<'op>,
        rt: TrxRuntime<'r>,
        effects: &'a mut StmtEffects,
        index: IndexRef,
        validate: bool,
    ) -> Self {
        Self {
            accessor,
            rt,
            effects,
            index,
            validator: validate.then(|| DmlValidator::new(accessor.metadata())),
        }
    }

    /// Retries only selection; invoking the callback commits to the selected entry.
    #[inline]
    pub(super) async fn execute<F, E>(
        self,
        key_vals: &[Val],
        mutate_row: F,
    ) -> CallbackResult<UniqueMutationOutcome, E>
    where
        F: for<'row> FnOnce(Option<&mut LazyRow<'row>>) -> CallbackResult<UniqueMutation, E>,
    {
        let accessor = self.accessor;
        let rt = self.rt;
        let width = accessor.metadata().col.col_count();
        let mut buffer = LazyRowBuffer::new_deferred(width);
        'retry: loop {
            // This block releases the root/index handles before a preparing wait.
            let preparing = 'attempt: {
                let root = accessor.root_snapshot(rt.ctx());
                let handle = accessor
                    .snapshot_index_read_handle(rt.pool_guards(), &root, self.index)
                    .disclose()?;
                let index = handle.bind_unique().disclose()?;
                let Some((row_id, _)) = index.lookup(key_vals, rt.sts()).await.disclose()? else {
                    break 'retry;
                };
                match accessor
                    .resolve_row_location(rt.pool_guards(), row_id)
                    .await
                    .disclose()?
                {
                    RowLocation::NotFound => break 'retry,
                    RowLocation::RowPage(page_id) => {
                        let Some(page) = accessor
                            .mem()
                            .try_get_validated_row_page_shared_result(
                                rt.pool_guards(),
                                page_id,
                                row_id,
                            )
                            .await
                            .disclose()?
                        else {
                            continue 'retry;
                        };
                        let hot = HotRowMutator::new(
                            accessor.table_id(),
                            accessor.metadata(),
                            rt,
                            &page,
                            row_id,
                        );
                        let mut locked = hot
                            .lock_for_write(self.effects, Some((self.index.slot(), key_vals)))
                            .await
                            .disclose()?;
                        let access = match &mut locked {
                            LockRowForWrite::InvalidIndex => {
                                break 'retry;
                            }
                            LockRowForWrite::WriteConflict => {
                                return Err(Report::new(OperationError::WriteConflict)
                                    .attach("unique point hot-row ownership")
                                    .disclose()
                                    .into());
                            }
                            LockRowForWrite::RetryInTransition => {
                                drop(locked);
                                drop(page);
                                drop(root);
                                accessor
                                    .table()
                                    .wait_transition_route_or_poison(&rt.engine().poisoner, row_id)
                                    .await
                                    .disclose()?;
                                continue 'retry;
                            }
                            LockRowForWrite::Ok(access) => access
                                .take()
                                .expect("point ownership retains row write access"),
                        };
                        drop(locked);
                        let source = LazyRowSource::HotWrite {
                            access,
                            column_layout: accessor.metadata().col.as_ref(),
                        };
                        let mut row = LazyRow::new(source, &mut buffer, width);
                        let action = mutate_row(Some(&mut row))?;
                        let access = row.into_hot_write_access();
                        self.validate_action(true, &action, key_vals).disclose()?;
                        match action {
                            UniqueMutation::Skip => {
                                accessor.cancel_owned_hot_row(self.effects, access);
                                return Ok(UniqueMutationOutcome::Noop);
                            }
                            UniqueMutation::Update(ref cols) if cols.is_empty() => {
                                accessor.cancel_owned_hot_row(self.effects, access);
                                return Ok(UniqueMutationOutcome::Updated(row_id));
                            }
                            UniqueMutation::Update(input) => {
                                let result = accessor
                                    .update_owned_hot_row(
                                        rt,
                                        self.effects,
                                        &page,
                                        access,
                                        RowUpdateInput::Sparse(input),
                                        &root,
                                    )
                                    .await
                                    .disclose()?;
                                return Ok(UniqueMutationOutcome::Updated(result));
                            }
                            UniqueMutation::Delete => {
                                let result = hot.delete_owned_row(self.effects, access);
                                assert!(
                                    matches!(result, DeleteInternal::Ok),
                                    "retained unique point row must remain deletable: row_id={row_id}"
                                );
                                let keys =
                                    WriteIndexKeySet::from_physical_row(accessor, &page, row_id);
                                let proof =
                                    accessor.owned_row_page_index_set_proof(row_id, keys, &root);
                                drop(page);
                                accessor
                                    .defer_delete_owned_row_index_set(rt, self.effects, proof)
                                    .await
                                    .disclose()?;
                                return Ok(UniqueMutationOutcome::Deleted);
                            }
                            UniqueMutation::Insert(_) => {
                                unreachable!("validated occupied actions cannot insert")
                            }
                        }
                    }
                    RowLocation::LwcBlock(location) => {
                        match accessor.point_cold_state(rt, row_id, location.durable_deleted) {
                            ColdLatestRow::Readable => (),
                            ColdLatestRow::NotFound => {
                                break 'retry;
                            }
                            ColdLatestRow::WriteConflict => {
                                return Err(Report::new(OperationError::WriteConflict)
                                    .attach("unique point cold-row ownership")
                                    .disclose()
                                    .into());
                            }
                            ColdLatestRow::Preparing(listener) => break 'attempt listener,
                        }
                        let storage = accessor.column_storage();
                        let persisted = storage
                            .load_lwc_block(rt.pool_guards().disk_guard(), location.block_id)
                            .await
                            .change_context(RuntimeError::TableAccess)
                            .disclose()?;
                        let block = persisted.block();
                        if block.row_shape_fingerprint() != location.row_shape_fingerprint {
                            return Err(Report::new(DataIntegrityError::InvalidPayload)
                                .attach(format!(
                                    "unique point row shape mismatch: block_id={}",
                                    location.block_id
                                ))
                                .change_context(RuntimeError::TableAccess)
                                .disclose()
                                .into());
                        }
                        let spec = accessor
                            .metadata()
                            .idx
                            .index_spec(self.index.slot())
                            .expect("admitted unique index has matching metadata");
                        let actual_key = block
                            .decode_index_key_values(
                                accessor.metadata().col.as_ref(),
                                spec,
                                location.row_idx,
                            )
                            .change_context(RuntimeError::TableAccess)
                            .disclose()?;
                        if actual_key != key_vals {
                            break 'retry;
                        }
                        drop(actual_key);
                        match accessor.claim_cold_row_for_write(
                            rt,
                            self.effects,
                            row_id,
                            location.durable_deleted,
                        ) {
                            Ok(DeletionClaim::Acquired) => (),
                            Ok(DeletionClaim::Preparing(listener)) => break 'attempt listener,
                            Err(DeletionError::AlreadyDeleted) => {
                                break 'retry;
                            }
                            Err(DeletionError::WriteConflict) => {
                                return Err(Report::new(OperationError::WriteConflict)
                                    .attach("unique point cold-row claim")
                                    .disclose()
                                    .into());
                            }
                        }
                        let source = || LazyRowSource::Cold {
                            block,
                            column_layout: accessor.metadata().col.as_ref(),
                            row_idx: location.row_idx,
                            file_kind: storage.file().file_kind(),
                            block_id: location.block_id,
                        };
                        let action = {
                            let mut row = LazyRow::new(source(), &mut buffer, width);
                            mutate_row(Some(&mut row))?
                        };
                        self.validate_action(true, &action, key_vals).disclose()?;
                        match action {
                            UniqueMutation::Skip => {
                                accessor.cancel_owned_cold_row(rt, self.effects, row_id);
                                return Ok(UniqueMutationOutcome::Noop);
                            }
                            UniqueMutation::Update(ref cols) if cols.is_empty() => {
                                accessor.cancel_owned_cold_row(rt, self.effects, row_id);
                                return Ok(UniqueMutationOutcome::Updated(row_id));
                            }
                            UniqueMutation::Update(input) => {
                                let old = LazyRow::new_prepared(source(), &mut buffer, width)
                                    .into_full_row()
                                    .change_context(RuntimeError::TableAccess)
                                    .disclose()?;
                                drop(persisted);
                                let result = accessor
                                    .update_owned_cold_row(
                                        rt,
                                        self.effects,
                                        row_id,
                                        old,
                                        RowUpdateInput::Sparse(input),
                                        &root,
                                    )
                                    .await
                                    .disclose()?;
                                return Ok(UniqueMutationOutcome::Updated(result.row_id()));
                            }
                            UniqueMutation::Delete => {
                                // Decode indexed columns directly; a constant delete
                                // never initializes dense callback scratch.
                                let keys = WriteIndexKeySet::from_cold_row(
                                    accessor,
                                    block,
                                    location.row_idx,
                                )
                                .change_context(RuntimeError::TableAccess)
                                .disclose()?;
                                drop(persisted);
                                accessor
                                    .finish_owned_cold_delete_effects(
                                        rt,
                                        self.effects,
                                        row_id,
                                        keys,
                                        &root,
                                    )
                                    .await
                                    .disclose()?;
                                return Ok(UniqueMutationOutcome::Deleted);
                            }
                            UniqueMutation::Insert(_) => {
                                unreachable!("validated occupied actions cannot insert")
                            }
                        }
                    }
                }
            };
            rt.wait_prepare_or_poison(preparing).await.disclose()?;
        }
        let action = mutate_row(None)?;
        self.validate_action(false, &action, key_vals).disclose()?;
        match action {
            UniqueMutation::Skip => Ok(UniqueMutationOutcome::Noop),
            UniqueMutation::Insert(row) => self
                .accessor
                .insert_mvcc(self.rt, self.effects, row)
                .await
                .map(UniqueMutationOutcome::Inserted)
                .disclose()
                .map_err(Into::into),
            _ => unreachable!("validated missing actions can only skip or insert"),
        }
    }

    fn validate_action(
        &self,
        occupied: bool,
        action: &UniqueMutation,
        key: &[Val],
    ) -> OperationResult<()> {
        if matches!(
            (occupied, action),
            (true, UniqueMutation::Insert(_))
                | (false, UniqueMutation::Update(_) | UniqueMutation::Delete)
        ) {
            return Err(Report::new(OperationError::InvalidDmlInput)
                .attach("unique mutation action is invalid for the observed entry state"));
        }
        match action {
            UniqueMutation::Insert(row) => {
                if let Some(validator) = &self.validator {
                    validator
                        .validate_full_row(row)
                        .change_context(OperationError::InvalidDmlInput)?;
                }
                let spec = self
                    .accessor
                    .metadata()
                    .idx
                    .index_spec(self.index.slot())
                    .expect("admitted unique index has matching metadata");
                if spec.keys.len() != key.len()
                    || !spec.keys.iter().zip(key).all(|(column, expected)| {
                        row.get(column.column_ordinal.as_usize()) == Some(expected)
                    })
                {
                    return Err(Report::new(OperationError::InvalidDmlInput)
                        .attach("inserted row must match the selected unique key"));
                }
            }
            UniqueMutation::Update(update) => {
                if let Some(validator) = &self.validator {
                    validator
                        .validate_sparse_update(update)
                        .change_context(OperationError::InvalidDmlInput)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::IndexID;
    use crate::catalog::{
        IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, StorageTableSpec,
    };
    use crate::error::{CallbackError, CallbackResult, OperationError};
    use crate::id::RowID;
    use crate::index::RowLocation;
    use crate::lwc::test_decode_counts;
    use crate::row::ops::{UniqueMutation, UniqueMutationOutcome, UpdateCol};
    use crate::session::tests::{
        SessionTestExt, assert_checkpoint_published, wait_for_session_idle,
    };
    use crate::table::access::dense_initializations;
    use crate::table::tests::{
        assert_freeze_created, bound_unique_index, evictable_test_engine,
        table_for_internal_assertion,
    };
    use crate::trx::tests::{
        commit_preparing_shared_trx_status, prepare_event_is_installed, prepare_shared_trx_status,
        prepare_transaction, rollback_preparing_shared_trx_status,
        rollback_production_prepared_for_test, shared_trx_status, transaction_status_for_test,
    };
    use crate::trx::{MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID};
    use crate::{Engine, Session, TableIndex, Transaction, Val, ValKind};
    use smol::future::yield_now;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use tempfile::TempDir;

    thread_local! {
        static POINT_DISK_LOOKUPS: Cell<usize> = const { Cell::new(0) };
    }

    /// Records entry to the persistent side of a unique point lookup.
    pub(crate) fn record_point_disk_lookup() {
        POINT_DISK_LOOKUPS.set(POINT_DISK_LOOKUPS.get() + 1);
    }

    /// Returns point DiskTree lookups performed by this test thread.
    fn test_unique_point_disk_lookups() -> usize {
        POINT_DISK_LOOKUPS.get()
    }

    fn values(id: i32) -> Vec<Val> {
        vec![
            Val::from(id),
            Val::from(id),
            Val::from(10i32),
            Val::from("original"),
        ]
    }

    fn assignment(idx: usize, val: impl Into<Val>) -> UniqueMutation {
        UniqueMutation::Update(vec![UpdateCol {
            idx,
            val: val.into(),
        }])
    }

    async fn fixture(
        state: &str,
        count: i32,
    ) -> (TempDir, Engine, Session, TableIndex, Vec<RowID>) {
        let root = TempDir::new().unwrap();
        let engine = evictable_test_engine(&root, 64 * 1024 * 1024, "unique_callback").await;
        let mut session = engine.new_session().unwrap();
        let table_id = session
            .create_table(
                StorageTableSpec::new(vec![
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                    StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::NULLABLE),
                    StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                ]),
                vec![
                    StorageIndexSpec::new(vec![StorageIndexKey::new(0)], StorageIndexFlags::UK),
                    StorageIndexSpec::new(vec![StorageIndexKey::new(1)], StorageIndexFlags::UK),
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(3)],
                        StorageIndexFlags::empty(),
                    ),
                ],
            )
            .await
            .unwrap()
            .table_id();
        let mut trx = session.begin_trx().unwrap();
        let ids = trx
            .table_insert_batch_mvcc(table_id, (0..count).map(values).collect())
            .await
            .unwrap();
        trx.commit().await.unwrap();
        if state != "hot" {
            assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
        }
        if state == "cold" {
            assert_checkpoint_published(&mut session, table_id).await;
        }
        (
            root,
            engine,
            session,
            TableIndex(table_id, IndexID::new(0)),
            ids,
        )
    }

    async fn apply(
        trx: &mut Transaction,
        index: TableIndex,
        id: i32,
        action: UniqueMutation,
    ) -> CallbackResult<UniqueMutationOutcome> {
        trx.table_unique_mutate_mvcc(index, &[Val::from(id)], |_| Ok(action))
            .await
    }

    async fn read(trx: &mut Transaction, index: TableIndex, id: i32) -> Vec<Val> {
        trx.table_lookup_unique_mvcc(index, &[Val::from(id)], &[0, 1, 2, 3])
            .await
            .unwrap()
            .unwrap_found()
    }

    fn invalid(result: CallbackResult<UniqueMutationOutcome>) {
        let CallbackError::Engine(error) = result.unwrap_err();
        assert_eq!(
            error.operation_error(),
            Some(OperationError::InvalidDmlInput),
            "{error:?}"
        );
    }

    #[test]
    fn test_unique_callback_entry_state_matrix_and_trusted_contracts() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, _engine, mut session, index, ids) = fixture(state, 1).await;
                for trusted in [false, true] {
                    for occupied in [false, true] {
                        for action_no in 0..5 {
                            let mut trx = session.begin_trx().unwrap();
                            trx.disable_dml_validation(trusted);
                            let id = if occupied { 0 } else { 9 };
                            let action = match action_no {
                                0 => UniqueMutation::Skip,
                                1 => UniqueMutation::Insert(values(id)),
                                2 => assignment(2, 20i32),
                                3 => UniqueMutation::Delete,
                                _ => UniqueMutation::Update(vec![]),
                            };
                            let mut calls = 0;
                            let result = trx
                                .table_unique_mutate_mvcc(
                                    index,
                                    &[Val::from(id)],
                                    |row| -> CallbackResult<_> {
                                        calls += 1;
                                        assert_eq!(row.is_some(), occupied);
                                        Ok(action)
                                    },
                                )
                                .await;
                            assert_eq!(calls, 1);
                            match (occupied, action_no) {
                                (_, 0) => assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop),
                                (false, 1) => assert!(matches!(
                                    result.unwrap(),
                                    UniqueMutationOutcome::Inserted(_)
                                )),
                                (true, 2) => assert!(matches!(
                                    result.unwrap(),
                                    UniqueMutationOutcome::Updated(_)
                                )),
                                (true, 3) => {
                                    assert_eq!(result.unwrap(), UniqueMutationOutcome::Deleted)
                                }
                                (true, 4) => assert_eq!(
                                    result.unwrap(),
                                    UniqueMutationOutcome::Updated(ids[0])
                                ),
                                _ => invalid(result),
                            }
                            trx.rollback().await.unwrap();
                        }
                    }
                    let mut trx = session.begin_trx().unwrap();
                    trx.disable_dml_validation(trusted);
                    invalid(apply(&mut trx, index, 9, UniqueMutation::Insert(values(8))).await);
                    trx.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_callback_payload_validation_and_index_arguments() {
        smol::block_on(async {
            let (_root, _engine, mut session, index, _) = fixture("hot", 1).await;
            let mut trx = session.begin_trx().unwrap();
            let resolved = trx.resolve_table_index(index).await.unwrap();
            assert_eq!(
                trx.table_unique_mutate_mvcc(
                    resolved,
                    &[Val::from(0i32)],
                    |_| -> CallbackResult<_> { Ok(UniqueMutation::Skip) }
                )
                .await
                .unwrap(),
                UniqueMutationOutcome::Noop
            );
            let mut wrong_kind = values(9);
            wrong_kind[2] = Val::from("wrong");
            let mut wrong_null = values(9);
            wrong_null[1] = Val::Null;
            for row in [vec![], wrong_kind, wrong_null] {
                invalid(apply(&mut trx, index, 9, UniqueMutation::Insert(row)).await);
            }
            for update in [
                vec![UpdateCol {
                    idx: 4,
                    val: Val::from(1i32),
                }],
                vec![UpdateCol {
                    idx: 2,
                    val: Val::from("bad"),
                }],
                vec![
                    UpdateCol {
                        idx: 2,
                        val: Val::from(1i32),
                    },
                    UpdateCol {
                        idx: 1,
                        val: Val::from(1i32),
                    },
                ],
                vec![
                    UpdateCol {
                        idx: 2,
                        val: Val::from(1i32),
                    },
                    UpdateCol {
                        idx: 2,
                        val: Val::from(1i32),
                    },
                ],
            ] {
                invalid(apply(&mut trx, index, 0, UniqueMutation::Update(update)).await);
            }
            for bad_index in [
                TableIndex(index.0, IndexID::new(2)),
                TableIndex(index.0, IndexID::new(99)),
            ] {
                let calls = Cell::new(0);
                assert!(
                    trx.table_unique_mutate_mvcc(
                        bad_index,
                        &[Val::from(0i32)],
                        |_| -> CallbackResult<_> {
                            calls.set(calls.get() + 1);
                            Ok(UniqueMutation::Skip)
                        }
                    )
                    .await
                    .is_err()
                );
                assert_eq!(calls.get(), 0);
            }
            assert_eq!(read(&mut trx, index, 0).await, values(0));
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_current_read_and_branch_local_payload() {
        smol::block_on(async {
            let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
            let mut writer = session.begin_trx().unwrap();
            let mut concurrent = engine.new_session().unwrap();
            let mut update = concurrent.begin_trx().unwrap();
            apply(&mut update, index, 0, assignment(2, 25i32))
                .await
                .unwrap();
            update.commit().await.unwrap();
            let mut inserts = 0;
            let mut updates = 0;
            for id in [0i32, 9] {
                let mut missing = false;
                writer
                    .table_unique_mutate_mvcc(index, &[Val::from(id)], |row| -> CallbackResult<_> {
                        match row {
                            Some(row) => {
                                updates += 1;
                                let old = row.val(2)?.as_i32().unwrap();
                                assert_eq!(old, 25);
                                Ok(assignment(2, old.checked_add(1).unwrap()))
                            }
                            None => {
                                missing = true;
                                inserts += 1;
                                Ok(UniqueMutation::Insert(values(id)))
                            }
                        }
                    })
                    .await
                    .unwrap();
                assert_eq!(missing, id == 9);
            }
            assert_eq!((inserts, updates), (1, 1));
            assert_eq!(read(&mut writer, index, 0).await[2], Val::from(26i32));
            // FnOnce consumes a non-Clone payload and may capture non-Send state.
            struct Payload {
                row: Vec<Val>,
                _local: Rc<()>,
            }
            let payload = Payload {
                row: values(10),
                _local: Rc::new(()),
            };
            writer
                .table_unique_mutate_mvcc(
                    index,
                    &[Val::from(10i32)],
                    move |_| -> CallbackResult<_> {
                        let Payload { row, _local } = payload;
                        drop(_local);
                        Ok(UniqueMutation::Insert(row))
                    },
                )
                .await
                .unwrap();
            writer
                .table_unique_mutate_mvcc(index, &[Val::from(0i32)], |row| -> CallbackResult<_> {
                    Ok(if row.unwrap().val(2)?.as_i32().unwrap() > 20 {
                        UniqueMutation::Delete
                    } else {
                        UniqueMutation::Skip
                    })
                })
                .await
                .unwrap();
            writer.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_cancellation_releases_only_new_ownership() {
        smol::block_on(async {
            for state in ["hot", "frozen", "cold"] {
                let (_root, engine, mut session, index, ids) = fixture(state, 2).await;
                let mut trx = session.begin_trx().unwrap();
                for action in [UniqueMutation::Skip, UniqueMutation::Update(vec![])] {
                    apply(&mut trx, index, 0, action).await.unwrap();
                    let mut other = engine.new_session().unwrap();
                    let mut competing = other.begin_trx().unwrap();
                    assert!(matches!(
                        apply(&mut competing, index, 0, assignment(2, 20i32))
                            .await
                            .unwrap(),
                        UniqueMutationOutcome::Updated(_)
                    ));
                    competing.rollback().await.unwrap();
                }
                let first = apply(&mut trx, index, 0, assignment(2, 30i32))
                    .await
                    .unwrap();
                for action in [UniqueMutation::Skip, UniqueMutation::Update(vec![])] {
                    apply(&mut trx, index, 0, action).await.unwrap();
                    assert_eq!(read(&mut trx, index, 0).await[2], Val::from(30i32));
                }
                invalid(apply(&mut trx, index, 0, UniqueMutation::Insert(values(0))).await);
                let mut other = engine.new_session().unwrap();
                let mut competing = other.begin_trx().unwrap();
                let error = apply(&mut competing, index, 0, UniqueMutation::Skip)
                    .await
                    .unwrap_err();
                let CallbackError::Engine(error) = error;
                assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                competing.rollback().await.unwrap();
                if state == "cold" {
                    let table = table_for_internal_assertion(&engine, index.0);
                    assert!(table.deletion_buffer().get(ids[0]).is_some());
                }
                assert!(matches!(first, UniqueMutationOutcome::Updated(_)));
                trx.rollback().await.unwrap();
                let mut reader = session.begin_trx().unwrap();
                assert_eq!(read(&mut reader, index, 0).await, values(0));
                reader.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_moves_keys_and_rolls_back_index_conflicts() {
        smol::block_on(async {
            for commit in [false, true] {
                for state in ["hot", "frozen", "cold", "space"] {
                    let (_root, engine, mut session, index, ids) =
                        fixture(if state == "space" { "hot" } else { state }, 300).await;
                    let mut snapshot_session = engine.new_session().unwrap();
                    let mut snapshot = snapshot_session.begin_trx().unwrap();
                    let mut trx = session.begin_trx().unwrap();
                    let payload = if state == "space" {
                        Val::from(vec![b'x'; 48000])
                    } else {
                        Val::from("changed")
                    };
                    let result = apply(
                        &mut trx,
                        index,
                        0,
                        UniqueMutation::Update(vec![
                            UpdateCol {
                                idx: 0,
                                val: Val::from(500i32),
                            },
                            UpdateCol {
                                idx: 1,
                                val: Val::from(500i32),
                            },
                            UpdateCol {
                                idx: 3,
                                val: payload.clone(),
                            },
                        ]),
                    )
                    .await
                    .unwrap();
                    let UniqueMutationOutcome::Updated(result_id) = result else {
                        panic!("move must be a logical update")
                    };
                    if state == "hot" {
                        assert_eq!(result_id, ids[0]);
                    } else {
                        assert_ne!(result_id, ids[0]);
                    }
                    assert!(
                        trx.table_lookup_unique_mvcc(index, &[Val::from(0i32)], &[0])
                            .await
                            .unwrap()
                            .not_found()
                    );
                    assert_eq!(read(&mut trx, index, 500).await[3], payload);
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    let by_other = trx
                        .table_lookup_unique_mvcc(
                            TableIndex(index.0, IndexID::new(1)),
                            &[Val::from(500i32)],
                            &[0],
                        )
                        .await
                        .unwrap()
                        .unwrap_found();
                    assert_eq!(by_other, vec![Val::from(500i32)]);
                    assert_eq!(
                        trx.table_index_lookup_mvcc(
                            TableIndex(index.0, IndexID::new(2)),
                            &[payload],
                            &[0]
                        )
                        .await
                        .unwrap()
                        .unwrap_rows(),
                        vec![vec![Val::from(500i32)]]
                    );
                    assert!(
                        apply(&mut trx, index, 1, assignment(1, 2i32))
                            .await
                            .is_err()
                    );
                    assert_eq!(read(&mut trx, index, 1).await, values(1));
                    assert_eq!(read(&mut trx, index, 500).await[0], Val::from(500i32));
                    if commit {
                        trx.commit().await.unwrap();
                    } else {
                        trx.rollback().await.unwrap();
                    }
                    assert_eq!(read(&mut snapshot, index, 0).await, values(0));
                    snapshot.rollback().await.unwrap();
                    let mut reader = session.begin_trx().unwrap();
                    if commit {
                        assert_eq!(read(&mut reader, index, 500).await[0], Val::from(500i32));
                    } else {
                        assert_eq!(read(&mut reader, index, 0).await, values(0));
                    }
                    reader.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_callback_errors_preserve_earlier_statements() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, _engine, mut session, index, _) = fixture(state, 2).await;
                let mut trx = session.begin_trx().unwrap();
                apply(&mut trx, index, 0, assignment(2, 30i32))
                    .await
                    .unwrap();
                struct Failure<'a> {
                    message: &'a str,
                    identity: Rc<()>,
                }
                let message = String::from("borrowed failure");
                let identity = Rc::new(());
                let error = trx
                    .table_unique_mutate_mvcc(index, &[Val::from(1i32)], |row| {
                        assert_eq!(row.unwrap().val(2)?.as_i32(), Some(10));
                        Err(CallbackError::User(Failure {
                            message: &message,
                            identity: Rc::clone(&identity),
                        }))
                    })
                    .await;
                match error {
                    Err(CallbackError::User(error)) => {
                        assert_eq!(error.message, message);
                        assert!(Rc::ptr_eq(&error.identity, &identity));
                    }
                    _ => panic!("callback application error must retain its original payload"),
                }
                let error = trx
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(1i32)],
                        |row| -> CallbackResult<_> {
                            row.unwrap().val(99)?;
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await;
                invalid(error);
                assert_eq!(read(&mut trx, index, 0).await[2], Val::from(30i32));
                assert_eq!(read(&mut trx, index, 1).await, values(1));
                trx.commit().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_cold_marker_authority_and_consumed_images() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("cold", 1).await;
            let table = table_for_internal_assertion(&engine, index.0);
            let mut trx = session.begin_trx().unwrap();
            let status = transaction_status_for_test(&trx);
            table
                .deletion_buffer()
                .put_ref(ids[0], Arc::clone(&status), trx.sts())
                .unwrap();
            for action in [
                UniqueMutation::Skip,
                UniqueMutation::Update(vec![]),
                UniqueMutation::Delete,
                UniqueMutation::Insert(values(1)),
            ] {
                let result = trx
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            assert!(row.is_none(), "a consumed cold image is missing");
                            Ok(action)
                        },
                    )
                    .await;
                assert!(matches!(result, Ok(UniqueMutationOutcome::Noop) | Err(_)));
                assert!(
                    matches!(table.deletion_buffer().get(ids[0]), Some(crate::table::DeleteMarker::Ref(owner)) if Arc::ptr_eq(&owner, &status))
                );
            }
            table.deletion_buffer().remove(ids[0]);
            for newer in [false, true] {
                let cts = if newer { trx.sts() + 1 } else { trx.sts() };
                table.deletion_buffer().put_committed(ids[0], cts).unwrap();
                let calls = Cell::new(0);
                let result = trx
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(0i32)],
                        |row| -> CallbackResult<_> {
                            calls.set(calls.get() + 1);
                            assert!(row.is_none());
                            Ok(UniqueMutation::Skip)
                        },
                    )
                    .await;
                if newer {
                    let CallbackError::Engine(error) = result.unwrap_err();
                    assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                    assert_eq!(calls.get(), 0);
                } else {
                    assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop);
                    assert_eq!(calls.get(), 1);
                }
                table.deletion_buffer().remove(ids[0]);
            }
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_cold_preparing_retry_invokes_once() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, ids) = fixture("cold", 1).await;
                let table = table_for_internal_assertion(&engine, index.0);
                let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 100));
                table
                    .deletion_buffer()
                    .put_ref(ids[0], Arc::clone(&owner), MAX_SNAPSHOT_TS)
                    .unwrap();
                prepare_shared_trx_status(&owner);
                let mut trx = session.begin_trx().unwrap();
                let cts = trx.sts();
                let calls = Cell::new(0);
                let key = [Val::from(0i32)];
                let mutation =
                    trx.table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                        calls.set(calls.get() + 1);
                        assert_eq!(row.is_none(), commit);
                        Ok(UniqueMutation::Skip)
                    });
                let release = async {
                    while !prepare_event_is_installed(&owner) {
                        yield_now().await;
                    }
                    assert_eq!(calls.get(), 0);
                    if commit {
                        commit_preparing_shared_trx_status(&owner, cts);
                    } else {
                        table.deletion_buffer().remove(ids[0]);
                        rollback_preparing_shared_trx_status(&owner);
                    }
                };
                let (result, ()) = futures::join!(mutation, release);
                assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop);
                assert_eq!(calls.get(), 1);
                table.deletion_buffer().remove(ids[0]);
                trx.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_insert_race_is_not_retried() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
                let mut winner_session = engine.new_session().unwrap();
                let winner = winner_session.begin_trx().unwrap();
                let mut loser = session.begin_trx().unwrap();
                let calls = Cell::new(0);
                let mut winner = Some(winner);
                let result = loser
                    .table_unique_mutate_mvcc(
                        index,
                        &[Val::from(9i32)],
                        |row| -> CallbackResult<_> {
                            assert!(row.is_none());
                            calls.set(calls.get() + 1);
                            // The synchronous callback is the precise missing-observation
                            // barrier. Run the independent writer before returning Insert.
                            smol::block_on(
                                winner
                                    .as_mut()
                                    .unwrap()
                                    .table_insert_mvcc(index.0, values(9)),
                            )
                            .unwrap();
                            if commit {
                                smol::block_on(winner.take().unwrap().commit()).unwrap();
                            }
                            Ok(UniqueMutation::Insert(values(9)))
                        },
                    )
                    .await;
                let CallbackError::Engine(error) = result.unwrap_err();
                assert!(
                    matches!(
                        error.operation_error(),
                        Some(OperationError::WriteConflict | OperationError::DuplicateKey)
                    ),
                    "{error:?}"
                );
                assert_eq!(calls.get(), 1);
                loser.rollback().await.unwrap();
                if let Some(winner) = winner {
                    winner.rollback().await.unwrap();
                }
            }
        });
    }

    #[test]
    fn test_unique_callback_drop_waiting_future_cleans_transaction() {
        smol::block_on(async {
            let (_root, engine, mut session, index, ids) = fixture("cold", 2).await;
            let table = table_for_internal_assertion(&engine, index.0);
            let owner = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 101));
            table
                .deletion_buffer()
                .put_ref(ids[1], Arc::clone(&owner), MAX_SNAPSHOT_TS)
                .unwrap();
            prepare_shared_trx_status(&owner);
            let mut trx = session.begin_trx().unwrap();
            apply(&mut trx, index, 0, assignment(2, 30i32))
                .await
                .unwrap();
            let key = [Val::from(1i32)];
            let mut operation = Box::pin(trx.table_unique_mutate_mvcc(
                index,
                &key,
                |_| -> CallbackResult<_> { panic!("callback must not run while owner prepares") },
            ));
            while !prepare_event_is_installed(&owner) {
                assert!(futures::poll!(operation.as_mut()).is_pending());
                yield_now().await;
            }
            drop(operation);
            drop(trx);
            table.deletion_buffer().remove(ids[1]);
            rollback_preparing_shared_trx_status(&owner);
            wait_for_session_idle(&engine.inner().session_registry, session.id()).await;
            let mut reader = session.begin_trx().unwrap();
            assert_eq!(read(&mut reader, index, 0).await, values(0));
            reader.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_empty_update_preserves_row_before_nonempty_move() {
        smol::block_on(async {
            for state in ["frozen", "cold"] {
                let (_root, engine, mut session, index, ids) = fixture(state, 1).await;
                let mut trx = session.begin_trx().unwrap();
                assert_eq!(
                    apply(&mut trx, index, 0, UniqueMutation::Update(vec![]))
                        .await
                        .unwrap(),
                    UniqueMutationOutcome::Updated(ids[0])
                );
                let UniqueMutationOutcome::Updated(new_id) =
                    apply(&mut trx, index, 0, assignment(2, 42i32))
                        .await
                        .unwrap()
                else {
                    panic!("nonempty update must find row")
                };
                assert_ne!(ids[0], new_id);
                let table = table_for_internal_assertion(&engine, index.0);
                assert!(matches!(
                    table
                        .find_row(&session.pool_guards(), new_id)
                        .await
                        .unwrap(),
                    RowLocation::RowPage(_)
                ));
                trx.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_zero_read_actions_avoid_dense_cache() {
        smol::block_on(async {
            for state in ["hot", "cold"] {
                let (_root, _engine, mut session, index, _) = fixture(state, 1).await;
                for action in [
                    UniqueMutation::Skip,
                    UniqueMutation::Update(vec![]),
                    assignment(2, 20i32),
                    UniqueMutation::Delete,
                ] {
                    let mut trx = session.begin_trx().unwrap();
                    let before = dense_initializations();
                    apply(&mut trx, index, 0, action).await.unwrap();
                    assert_eq!(dense_initializations(), before);
                    trx.rollback().await.unwrap();
                }
                let mut trx = session.begin_trx().unwrap();
                let before = dense_initializations();
                trx.table_unique_mutate_mvcc(
                    index,
                    &[Val::from(0i32)],
                    |row| -> CallbackResult<_> {
                        let row = row.unwrap();
                        row.val(0)?;
                        row.val(0)?;
                        row.val(1)?;
                        row.val(2)?;
                        Ok(assignment(2, 30i32))
                    },
                )
                .await
                .unwrap();
                assert_eq!(dense_initializations(), before + 1);
                trx.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_active_hot_delete_and_preparing_settlement() {
        smol::block_on(async {
            for commit in [false, true] {
                let (_root, engine, mut session, index, _) = fixture("hot", 1).await;
                let mut owner = session.begin_trx().unwrap();
                apply(&mut owner, index, 0, UniqueMutation::Delete)
                    .await
                    .unwrap();
                let mut competitor_session = engine.new_session().unwrap();
                let mut competitor = competitor_session.begin_trx().unwrap();
                let calls = Cell::new(0);
                let result = competitor
                    .table_unique_mutate_mvcc(index, &[Val::from(0i32)], |_| -> CallbackResult<_> {
                        calls.set(calls.get() + 1);
                        Ok(UniqueMutation::Skip)
                    })
                    .await;
                let CallbackError::Engine(error) = result.unwrap_err();
                assert_eq!(error.operation_error(), Some(OperationError::WriteConflict));
                assert_eq!(calls.get(), 0);
                let status = transaction_status_for_test(&owner);
                let prepared = prepare_transaction(owner).unwrap();
                let key = [Val::from(0i32)];
                let mutate =
                    competitor.table_unique_mutate_mvcc(index, &key, |row| -> CallbackResult<_> {
                        calls.set(calls.get() + 1);
                        assert_eq!(row.is_none(), commit);
                        Ok(UniqueMutation::Skip)
                    });
                let settle = async {
                    while !prepare_event_is_installed(&status) {
                        yield_now().await;
                    }
                    assert_eq!(calls.get(), 0);
                    if commit {
                        engine
                            .inner()
                            .trx_sys
                            .commit_prepared(prepared)
                            .await
                            .unwrap();
                    } else {
                        rollback_production_prepared_for_test(prepared).await;
                    }
                };
                let (result, ()) = futures::join!(mutate, settle);
                assert_eq!(result.unwrap(), UniqueMutationOutcome::Noop);
                assert_eq!(calls.get(), 1);
                competitor.rollback().await.unwrap();
            }
        });
    }

    #[test]
    fn test_unique_callback_cold_delete_only_decodes_index_columns() {
        smol::block_on(async {
            let (_root, _engine, mut session, index, _) = fixture("cold", 1).await;
            let mut trx = session.begin_trx().unwrap();
            let before = test_decode_counts();
            apply(&mut trx, index, 0, UniqueMutation::Delete)
                .await
                .unwrap();
            let after = test_decode_counts();
            assert!(after[0] > before[0]);
            assert!(after[1] > before[1]);
            assert_eq!(
                after[2], before[2],
                "unindexed counter must remain undecoded"
            );
            assert!(after[3] > before[3]);
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_unique_callback_mem_hit_short_circuits_disk() {
        smol::block_on(async {
            let (_root, engine, mut session, index, _) = fixture("cold", 1).await;
            let mut trx = session.begin_trx().unwrap();
            apply(&mut trx, index, 0, assignment(2, 20i32))
                .await
                .unwrap();
            let table = table_for_internal_assertion(&engine, index.0);
            assert!(
                bound_unique_index(&table, &session.pool_guards(), IndexSlot::new(0))
                    .lookup(&[Val::from(0i32)], MAX_SNAPSHOT_TS)
                    .await
                    .unwrap()
                    .is_some()
            );
            let before = test_unique_point_disk_lookups();
            apply(&mut trx, index, 0, UniqueMutation::Skip)
                .await
                .unwrap();
            assert_eq!(test_unique_point_disk_lookups(), before);
            trx.rollback().await.unwrap();
        });
    }
}
