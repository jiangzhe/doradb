use crate::error::{DiscloseResultExt, MultiDomainResultExt, Result};
use crate::id::{RowID, TableID};
use crate::row::ops::{
    DeleteMvcc, RowMutation, ScanMvcc, SelectMvcc, TableMutationOutcome, UpdateCol, UpdateMvcc,
    UpsertMvcc,
};
use crate::table::LazyRow;
use crate::trx::{IndexScanMvccStream, Transaction};
use crate::value::Val;
use std::ops::RangeBounds;

use super::stream_stmt::{INDEX_SCAN_STREAM_OPERATION, StreamStmtState};

impl Transaction {
    /// Executes one empty statement through the normal transaction settlement path.
    #[inline]
    pub async fn noop(&mut self) -> Result<()> {
        self.exec(async |_| Ok(())).await
    }

    /// Scans visible rows in a user table and invokes `row_action` for each projection.
    #[inline]
    pub async fn table_scan_mvcc<F>(
        &mut self,
        table_id: TableID,
        read_set: &[usize],
        row_action: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<Val>) -> bool,
    {
        self.exec(async move |stmt| stmt.table_scan_mvcc(table_id, read_set, row_action).await)
            .await
    }

    /// Looks up one visible row by a unique secondary-index key.
    #[inline]
    pub async fn table_lookup_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<SelectMvcc> {
        self.exec(async move |stmt| {
            stmt.table_lookup_unique_mvcc(table_id, index_no, key_vals, user_read_set)
                .await
        })
        .await
    }

    /// Looks up visible rows by one secondary-index key.
    #[inline]
    pub async fn table_index_lookup_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        user_read_set: &[usize],
    ) -> Result<ScanMvcc> {
        self.exec(async move |stmt| {
            stmt.table_index_lookup_mvcc(table_id, index_no, key_vals, user_read_set)
                .await
        })
        .await
    }

    /// Scans visible rows selected by one secondary-index range.
    #[inline]
    pub async fn table_index_scan_mvcc<'r, R>(
        &mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<ScanMvcc>
    where
        R: RangeBounds<&'r [Val]>,
    {
        self.exec(async move |stmt| {
            stmt.table_index_scan_mvcc(table_id, index_no, range, read_set)
                .await
        })
        .await
    }

    /// Mutates callback-selected rows from a sequential latest-row traversal.
    #[inline]
    pub async fn table_mutate_mvcc<F>(
        &mut self,
        table_id: TableID,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        self.exec(async move |stmt| stmt.table_mutate_mvcc(table_id, mutate_row).await)
            .await
    }

    /// Mutates callback-selected rows from one secondary-index range.
    #[inline]
    pub async fn table_index_mutate_mvcc<'r, R, F>(
        &mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        mutate_row: F,
    ) -> Result<TableMutationOutcome>
    where
        R: RangeBounds<&'r [Val]>,
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<RowMutation>,
    {
        self.exec(async move |stmt| {
            stmt.table_index_mutate_mvcc(table_id, index_no, range, mutate_row)
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
    pub async fn table_upsert_unique_mvcc(
        &mut self,
        table_id: TableID,
        unique_index_no: usize,
        cols: Vec<Val>,
    ) -> Result<UpsertMvcc> {
        self.exec(async move |stmt| {
            stmt.table_upsert_unique_mvcc(table_id, unique_index_no, cols)
                .await
        })
        .await
    }

    /// Updates one row selected by a unique secondary-index key.
    #[inline]
    pub async fn table_update_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
        update: Vec<UpdateCol>,
    ) -> Result<UpdateMvcc> {
        self.exec(async move |stmt| {
            stmt.table_update_unique_mvcc(table_id, index_no, key_vals, update)
                .await
        })
        .await
    }

    /// Deletes one row selected by a unique secondary-index key.
    #[inline]
    pub async fn table_delete_unique_mvcc(
        &mut self,
        table_id: TableID,
        index_no: usize,
        key_vals: &[Val],
    ) -> Result<DeleteMvcc> {
        self.exec(async move |stmt| {
            stmt.table_delete_unique_mvcc(table_id, index_no, key_vals)
                .await
        })
        .await
    }

    /// Creates a validated caller-driven stream over one secondary-index range.
    #[inline]
    pub async fn table_index_scan_mvcc_stream<'trx, 'r, R>(
        &'trx mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<IndexScanMvccStream<'trx>>
    where
        R: RangeBounds<&'r [Val]>,
    {
        let dml_validation_disabled = self.dml_validation_disabled;
        let checkout = self
            .checkout()
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))
            .disclose()?;
        StreamStmtState::new(checkout, dml_validation_disabled)
            .table_index_scan_mvcc_stream(table_id, index_no, range, read_set)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OperationError;
    use crate::lock::{LockMode, LockResource};
    use crate::row::ops::SelectMvcc;
    use crate::table::tests::{create_table2_for_test, lightweight_test_engine};
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
                    .table_lookup_unique_mvcc(table_id, 0, &[Val::from(id)], &[0, 1])
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
                trx.table_lookup_unique_mvcc(table_id, 0, &[Val::from(1)], &[0, 1])
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
                trx.table_lookup_unique_mvcc(table_id, 0, &[Val::from(1)], &[0, 1])
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
                .table_index_scan_mvcc_stream(table_id, 0, .., &[0, 1])
                .await
                .unwrap();
            assert_eq!(stream.next().await.unwrap(), Some(row(1, "one")));
            assert_eq!(stream.next().await.unwrap(), None);
            drop(stream);
            trx.noop().await.unwrap();
            trx.rollback().await.unwrap();
        });
    }
}
