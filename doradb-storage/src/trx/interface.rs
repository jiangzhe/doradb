use crate::catalog::{ResolvedTableIndex, TableIndex, TableIndexArgument};
use crate::error::{CallbackResult, DiscloseResultExt, MultiDomainResultExt, Result};
use crate::id::{RowID, TableID};
use crate::row::ops::{
    DeleteMvcc, RowMutation, ScanMvcc, ScanRowDecision, SelectMvcc, TableMutationOutcome,
    UpdateCol, UpdateMvcc, UpsertMvcc,
};
use crate::table::LazyRow;
use crate::trx::{IndexScanMvccStream, TableScanMvccStream, Transaction};
use crate::value::Val;
use std::ops::RangeBounds;

use super::stream_stmt::{
    INDEX_SCAN_STREAM_OPERATION, StreamStmtState, TABLE_SCAN_STREAM_OPERATION,
};

impl Transaction {
    /// Executes one empty statement through the normal transaction settlement path.
    #[inline]
    pub async fn noop(&mut self) -> Result<()> {
        self.exec(async |_| Ok(())).await
    }

    /// Looks up one visible row by a unique secondary-index key.
    #[inline]
    pub async fn table_lookup_unique_mvcc<I: TableIndexArgument>(
        &mut self,
        index: I,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        let selector = index.into_selector();
        self.exec(async move |stmt| {
            stmt.table_lookup_unique_mvcc(selector, key_vals, user_read_set)
                .await
        })
        .await
    }

    /// Looks up visible rows by one secondary-index key.
    #[inline]
    pub async fn table_index_lookup_mvcc<I: TableIndexArgument>(
        &mut self,
        index: I,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        let selector = index.into_selector();
        self.exec(async move |stmt| {
            stmt.table_index_lookup_mvcc(selector, key_vals, user_read_set)
                .await
        })
        .await
    }

    /// Scans visible rows selected by one secondary-index range.
    #[inline]
    pub async fn table_index_scan_mvcc<'r, R, I>(
        &mut self,
        index: I,
        range: R,
        read_set: &[usize],
    ) -> Result<ScanMvcc>
    where
        R: RangeBounds<&'r [Val]>,
        I: TableIndexArgument,
    {
        let selector = index.into_selector();
        self.exec(async move |stmt| stmt.table_index_scan_mvcc(selector, range, read_set).await)
            .await
    }

    /// Mutates callback-selected rows from a sequential latest-row traversal.
    ///
    /// Callback errors roll back this statement and return intact unless rollback
    /// itself fails, in which case the fatal engine error takes precedence. Use
    /// `CallbackResult<_>` for callbacks with engine failures only, or wrap
    /// application failures explicitly in `CallbackError::User`.
    #[inline]
    pub async fn table_mutate_mvcc<F, E>(
        &mut self,
        table_id: TableID,
        mutate_row: F,
    ) -> CallbackResult<TableMutationOutcome, E>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> CallbackResult<RowMutation, E>,
    {
        self.exec(async move |stmt| stmt.table_mutate_mvcc(table_id, mutate_row).await)
            .await
    }

    /// Mutates callback-selected rows from one secondary-index range.
    ///
    /// Callback errors stop traversal and roll back the statement, including
    /// deferred unique-key updates. Fatal rollback errors take engine precedence.
    /// Annotate engine-only callbacks with `CallbackResult<_>`.
    #[inline]
    pub async fn table_index_mutate_mvcc<'r, R, F, I, E>(
        &mut self,
        index: I,
        range: R,
        mutate_row: F,
    ) -> CallbackResult<TableMutationOutcome, E>
    where
        R: RangeBounds<&'r [Val]>,
        F: for<'row> FnMut(&mut LazyRow<'row>) -> CallbackResult<RowMutation, E>,
        I: TableIndexArgument,
    {
        let selector = index.into_selector();
        self.exec(async move |stmt| {
            stmt.table_index_mutate_mvcc(selector, range, mutate_row)
                .await
        })
        .await
    }

    /// Inserts one validated row into a user table.
    #[inline]
    pub async fn table_insert_mvcc(&mut self, table_id: TableID, cols: Vec<Val>) -> Result<RowID> {
        self.exec(async move |stmt| stmt.table_insert_mvcc(table_id, cols).await)
            .await
    }

    /// Atomically inserts a validated batch into one user table in input order.
    #[inline]
    pub async fn table_insert_batch_mvcc(
        &mut self,
        table_id: TableID,
        rows: Vec<Vec<Val>>,
    ) -> Result<Vec<RowID>> {
        self.exec(async move |stmt| stmt.table_insert_batch_mvcc(table_id, rows).await)
            .await
    }

    /// Inserts or replaces one row selected by a unique secondary index.
    #[inline]
    pub async fn table_upsert_unique_mvcc<I: TableIndexArgument>(
        &mut self,
        index: I,
        cols: Vec<Val>,
    ) -> Result<UpsertMvcc> {
        let selector = index.into_selector();
        self.exec(async move |stmt| stmt.table_upsert_unique_mvcc(selector, cols).await)
            .await
    }

    /// Updates one row selected by a unique secondary-index key.
    #[inline]
    pub async fn table_update_unique_mvcc<I: TableIndexArgument>(
        &mut self,
        index: I,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        let selector = index.into_selector();
        self.exec(async move |stmt| {
            stmt.table_update_unique_mvcc(selector, key_vals, update)
                .await
        })
        .await
    }

    /// Deletes one row selected by a unique secondary-index key.
    #[inline]
    pub async fn table_delete_unique_mvcc<I: TableIndexArgument>(
        &mut self,
        index: I,
        key_vals: &[Val],
    ) -> Result<DeleteMvcc> {
        let selector = index.into_selector();
        self.exec(async move |stmt| stmt.table_delete_unique_mvcc(selector, key_vals).await)
            .await
    }

    /// Creates a validated caller-driven stream over one secondary-index range.
    #[inline]
    pub async fn table_index_scan_mvcc_stream<'trx, 'r, R, I>(
        &'trx mut self,
        index: I,
        range: R,
        read_set: &[usize],
    ) -> Result<IndexScanMvccStream<'trx>>
    where
        R: RangeBounds<&'r [Val]>,
        I: TableIndexArgument,
    {
        let selector = index.into_selector();
        let dml_validation_disabled = self.dml_validation_disabled;
        let checkout = self
            .checkout()
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))
            .disclose()?;
        StreamStmtState::new(
            checkout,
            dml_validation_disabled,
            INDEX_SCAN_STREAM_OPERATION,
        )
        .table_index_scan_mvcc_stream(selector, range, read_set)
        .await
    }

    /// Resolves one table-qualified stable index into a reusable non-pinning token.
    #[inline]
    pub async fn resolve_table_index(&mut self, index: TableIndex) -> Result<ResolvedTableIndex> {
        self.exec(async move |stmt| stmt.resolve_table_index(index).await)
            .await
    }

    /// Creates a caller-driven programmable stream over visible table rows.
    ///
    /// Construction errors use the engine arm. Callback and projection failures
    /// returned by `next` close the stream; later calls return `Ok(None)`.
    /// Annotate engine-only callbacks with `CallbackResult<_>`. The stream keeps
    /// its exclusive transaction borrow until dropped.
    #[inline]
    pub async fn table_scan_mvcc_stream<'trx, F, E>(
        &'trx mut self,
        table_id: TableID,
        read_set: &[usize],
        scan_row: F,
    ) -> CallbackResult<TableScanMvccStream<'trx, F>, E>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> CallbackResult<ScanRowDecision, E>,
    {
        let dml_validation_disabled = self.dml_validation_disabled;
        let checkout = self
            .checkout()
            .attach_with(|| format!("operation={TABLE_SCAN_STREAM_OPERATION}"))
            .disclose()?;
        StreamStmtState::new(
            checkout,
            dml_validation_disabled,
            TABLE_SCAN_STREAM_OPERATION,
        )
        .table_scan_mvcc_stream(table_id, read_set, scan_row)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{IndexID, StorageIndexFlags, StorageIndexKey, StorageIndexSpec};
    use crate::error::{ErrorKind, OperationError};
    use crate::lock::{LockMode, LockResource};
    use crate::row::ops::SelectMvcc;
    use crate::table::tests::{create_table2_for_test, lightweight_test_engine};
    use crate::table::{Table, TableRuntimeLayout};
    use tempfile::TempDir;

    fn row(id: i32, name: &str) -> Vec<Val> {
        vec![Val::from(id), Val::from(name)]
    }

    #[test]
    fn test_direct_batch_insert_preserves_order_and_visibility() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "direct_batch_order").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let row_ids = trx
                .table_insert_batch_mvcc(
                    table_id,
                    vec![row(1, "one"), row(2, "two"), row(3, "three")],
                )
                .await
                .unwrap();
            assert!(row_ids.windows(2).all(|pair| pair[0] < pair[1]));
            for (id, name) in [(1, "one"), (2, "two"), (3, "three")] {
                let selected = trx
                    .table_lookup_unique_mvcc(
                        TableIndex(table_id, IndexID::new(0)),
                        &[Val::from(id)],
                        &[0, 1],
                    )
                    .await
                    .unwrap();
                assert_eq!(selected, SelectMvcc::Found(row(id, name)));
            }
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_direct_batch_validates_every_row_before_insert() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "direct_batch_validation").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let err = trx
                .table_insert_batch_mvcc(table_id, vec![row(1, "one"), vec![Val::from(2)]])
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::InvalidDmlInput));
            assert!(format!("{:?}", err.report()).contains("batch_index=1"));
            {
                let checkout = trx.checkout().unwrap();
                assert!(checkout.inner().table_bindings.contains_key(&table_id));
                assert!(
                    !checkout
                        .inner()
                        .checked_lock_state()
                        .covers(LockResource::TableData(table_id), LockMode::IntentExclusive,)
                );
            }
            assert_eq!(
                trx.table_lookup_unique_mvcc(
                    TableIndex(table_id, IndexID::new(0)),
                    &[Val::from(1)],
                    &[0, 1]
                )
                .await
                .unwrap(),
                SelectMvcc::NotFound
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_direct_batch_duplicate_rolls_back_prefix_before_return() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "direct_batch_duplicate").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            let err = trx
                .table_insert_batch_mvcc(table_id, vec![row(1, "one"), row(1, "duplicate")])
                .await
                .unwrap_err();
            assert_eq!(err.operation_error(), Some(OperationError::DuplicateKey));
            assert!(format!("{:?}", err.report()).contains("batch_index=1"));
            assert_eq!(
                trx.table_lookup_unique_mvcc(
                    TableIndex(table_id, IndexID::new(0)),
                    &[Val::from(1)],
                    &[0, 1]
                )
                .await
                .unwrap(),
                SelectMvcc::NotFound
            );
            assert!(
                trx.table_insert_batch_mvcc(table_id, vec![row(2, "reused")])
                    .await
                    .is_ok()
            );
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_direct_noop_empty_batch_and_stream_reuse_transaction() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "direct_surface_reuse").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();

            trx.noop().await.unwrap();
            assert!(
                trx.table_insert_batch_mvcc(table_id, Vec::new())
                    .await
                    .unwrap()
                    .is_empty()
            );
            {
                let mut checkout = trx.checkout().unwrap();
                assert!(checkout.inner().table_bindings.contains_key(&table_id));
                assert!(
                    checkout
                        .inner()
                        .checked_lock_state()
                        .covers(LockResource::TableData(table_id), LockMode::IntentExclusive,)
                );
                assert_eq!(checkout.inner_mut().next_stmt_no(), 3);
            }
            assert_eq!(
                trx.table_insert_mvcc(table_id, row(1, "one"))
                    .await
                    .unwrap(),
                RowID::new(0),
                "empty batch must not allocate a row id",
            );
            let mut stream = trx
                .table_index_scan_mvcc_stream(TableIndex(table_id, IndexID::new(0)), .., &[0, 1])
                .await
                .unwrap();
            assert_eq!(stream.next().await.unwrap(), Some(row(1, "one")));
            assert_eq!(stream.next().await.unwrap(), None);
            drop(stream);
            trx.noop().await.unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn resolved_table_index_uses_direct_generation_validation() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "resolved_table_index").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();

            TableRuntimeLayout::reset_index_access_counters();
            Table::reset_retirement_registry_access_count();
            let mut insert = session.begin_trx().unwrap();
            insert
                .table_insert_mvcc(table_id, row(1, "one"))
                .await
                .unwrap();
            insert.commit().await.unwrap();
            let (map, direct, iterations) = TableRuntimeLayout::index_access_counters();
            assert_eq!(map, 0);
            assert_eq!(direct, 0);
            assert!(iterations >= 1);

            let non_unique_id = session
                .create_index(
                    table_id,
                    StorageIndexSpec::new(
                        vec![StorageIndexKey::new(1)],
                        StorageIndexFlags::empty(),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(non_unique_id, IndexID::new(1));
            Table::reset_retirement_registry_access_count();

            TableRuntimeLayout::reset_index_access_counters();
            let mut normal_point = session.begin_trx().unwrap();
            assert_eq!(
                normal_point
                    .table_lookup_unique_mvcc(
                        TableIndex(table_id, IndexID::new(0)),
                        &[Val::from(1)],
                        &[0, 1],
                    )
                    .await
                    .unwrap(),
                SelectMvcc::Found(row(1, "one"))
            );
            normal_point.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters().0, 1);

            TableRuntimeLayout::reset_index_access_counters();
            let mut normal_equality = session.begin_trx().unwrap();
            assert!(
                normal_equality
                    .table_index_lookup_mvcc(
                        TableIndex(table_id, non_unique_id),
                        &[Val::from("one")],
                        &[0, 1],
                    )
                    .await
                    .unwrap()
                    .has_rows()
            );
            normal_equality.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters().0, 1);

            TableRuntimeLayout::reset_index_access_counters();
            let mut normal_range = session.begin_trx().unwrap();
            assert!(
                normal_range
                    .table_index_scan_mvcc(TableIndex(table_id, non_unique_id), .., &[0, 1])
                    .await
                    .unwrap()
                    .has_rows()
            );
            normal_range.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters().0, 1);

            TableRuntimeLayout::reset_index_access_counters();
            let mut normal_stream = session.begin_trx().unwrap();
            let mut stream = normal_stream
                .table_index_scan_mvcc_stream(TableIndex(table_id, non_unique_id), .., &[0, 1])
                .await
                .unwrap();
            assert_eq!(stream.next().await.unwrap(), Some(row(1, "one")));
            assert_eq!(stream.next().await.unwrap(), None);
            drop(stream);
            normal_stream.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters().0, 1);

            TableRuntimeLayout::reset_index_access_counters();
            let mut normal_mutation = session.begin_trx().unwrap();
            assert_eq!(
                normal_mutation
                    .table_index_mutate_mvcc(
                        TableIndex(table_id, non_unique_id),
                        ..,
                        |_| -> CallbackResult<_> { Ok(RowMutation::Skip) }
                    )
                    .await
                    .unwrap(),
                TableMutationOutcome::default()
            );
            normal_mutation.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters().0, 1);

            TableRuntimeLayout::reset_index_access_counters();
            let mut resolver = session.begin_trx().unwrap();
            let resolved = resolver
                .resolve_table_index(TableIndex(table_id, IndexID::new(0)))
                .await
                .unwrap();
            resolver.rollback().await.unwrap();
            assert_eq!(resolved.table_id(), table_id);
            assert_eq!(resolved.index_id(), IndexID::new(0));
            assert_eq!(TableRuntimeLayout::index_access_counters(), (1, 0, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut equality_resolver = session.begin_trx().unwrap();
            let resolved_non_unique = equality_resolver
                .resolve_table_index(TableIndex(table_id, non_unique_id))
                .await
                .unwrap();
            equality_resolver.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (1, 0, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut read = session.begin_trx().unwrap();
            assert_eq!(
                read.table_lookup_unique_mvcc(resolved, &[Val::from(1)], &[0, 1],)
                    .await
                    .unwrap(),
                SelectMvcc::Found(row(1, "one"))
            );
            read.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 1, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut equality = session.begin_trx().unwrap();
            assert!(
                equality
                    .table_index_lookup_mvcc(resolved_non_unique, &[Val::from("one")], &[0, 1])
                    .await
                    .unwrap()
                    .has_rows()
            );
            equality.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 1, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut range = session.begin_trx().unwrap();
            assert!(
                range
                    .table_index_scan_mvcc(resolved_non_unique, .., &[0, 1])
                    .await
                    .unwrap()
                    .has_rows()
            );
            range.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 1, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut stream_trx = session.begin_trx().unwrap();
            let mut stream = stream_trx
                .table_index_scan_mvcc_stream(resolved_non_unique, .., &[0, 1])
                .await
                .unwrap();
            assert_eq!(stream.next().await.unwrap(), Some(row(1, "one")));
            assert_eq!(stream.next().await.unwrap(), None);
            drop(stream);
            stream_trx.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 1, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut mutation = session.begin_trx().unwrap();
            assert_eq!(
                mutation
                    .table_index_mutate_mvcc(resolved_non_unique, .., |_| -> CallbackResult<_> {
                        Ok(RowMutation::Skip)
                    })
                    .await
                    .unwrap(),
                TableMutationOutcome::default()
            );
            mutation.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 1, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut write = session.begin_trx().unwrap();
            assert!(
                write
                    .table_update_unique_mvcc(
                        resolved,
                        &[Val::from(1)],
                        vec![UpdateCol {
                            idx: 1,
                            val: Val::from("updated"),
                        }],
                    )
                    .await
                    .unwrap()
                    .is_updated()
            );
            write.rollback().await.unwrap();
            assert_eq!(TableRuntimeLayout::index_access_counters(), (0, 1, 0));

            TableRuntimeLayout::reset_index_access_counters();
            let mut delete = session.begin_trx().unwrap();
            assert!(
                delete
                    .table_delete_unique_mvcc(resolved, &[Val::from(1)])
                    .await
                    .unwrap()
                    .is_deleted()
            );
            delete.rollback().await.unwrap();
            let (map, direct, _) = TableRuntimeLayout::index_access_counters();
            assert_eq!(map, 0);
            assert_eq!(direct, 1);

            TableRuntimeLayout::reset_index_access_counters();
            let mut upsert = session.begin_trx().unwrap();
            assert!(
                upsert
                    .table_upsert_unique_mvcc(resolved, row(1, "upserted"))
                    .await
                    .unwrap()
                    .is_updated()
            );
            upsert.rollback().await.unwrap();
            let (map, direct, _) = TableRuntimeLayout::index_access_counters();
            assert_eq!(map, 0);
            assert_eq!(direct, 1);
            assert_eq!(Table::retirement_registry_access_count(), 0);
        });
    }

    #[test]
    fn test_table_scan_mvcc_stream_missing_table_preserves_typed_context() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "table_stream_missing_context").await;
            let mut session = engine.new_session().unwrap();
            let mut trx = session.begin_trx().unwrap();
            let table_id = TableID::new(91_225);

            let err = match trx
                .table_scan_mvcc_stream(table_id, &[0], |_| -> CallbackResult<_> {
                    Ok(ScanRowDecision::Include)
                })
                .await
            {
                Ok(_) => panic!("missing table must fail stream construction"),
                Err(err) => err,
            };
            assert_eq!(err.engine().unwrap().kind(), ErrorKind::Operation);
            assert_eq!(
                err.engine()
                    .unwrap()
                    .report()
                    .downcast_ref::<OperationError>()
                    .copied(),
                Some(OperationError::TableNotFound)
            );
            let rendered = format!("{err:?}");
            assert_eq!(
                rendered.matches("operation=table_scan_mvcc_stream").count(),
                1
            );
            assert_eq!(rendered.matches(&format!("table_id={table_id}")).count(), 1);
            trx.noop().await.unwrap();
            trx.rollback().await.unwrap();
        });
    }

    #[test]
    fn test_callback_infallible_inference_in_engine_result() {
        smol::block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp_dir, "callback_infallible").await;
            let table_id = create_table2_for_test(&engine).await;
            let mut session = engine.new_session().unwrap();
            let result: Result<()> = async {
                let mut trx = session.begin_trx()?;
                trx.table_insert_mvcc(table_id, vec![Val::from(1i32), Val::from("row")])
                    .await?;
                trx.table_mutate_mvcc(table_id, |row| -> CallbackResult<_> {
                    row.val(0)?;
                    Ok(RowMutation::Skip)
                })
                .await?;
                trx.table_index_mutate_mvcc(
                    TableIndex(table_id, IndexID::new(0)),
                    ..,
                    |row| -> CallbackResult<_> {
                        row.val(0)?;
                        Ok(RowMutation::Skip)
                    },
                )
                .await?;
                let mut stream = trx
                    .table_scan_mvcc_stream(table_id, &[0], |row| -> CallbackResult<_> {
                        row.val(0)?;
                        Ok(ScanRowDecision::Include)
                    })
                    .await?;
                assert_eq!(stream.next().await?, Some(vec![Val::from(1i32)]));
                assert_eq!(stream.next().await?, None);
                drop(stream);
                trx.rollback().await?;
                Ok(())
            }
            .await;
            result.unwrap();
        });
    }
}
