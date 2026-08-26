#[cfg(test)]
use crate::error::RuntimeError;
use crate::error::{DiscloseError, DiscloseResultExt, Error, OperationError, Result};
use crate::id::{BlockID, RowID, TableID};
use crate::row::ops::{ScanRowDecision, ScanRowDecision::Include};
use crate::table::{
    LazyRow, Table, TableRuntimeLayout, TableScanCursor, TableScanCursorAdvance,
    TableScanRangeCursor, TableScanRuntime, TableScanUnit,
};
use crate::trx::read_snapshot::{
    ReadSnapshotExecutionCheckout, ReadSnapshotExecutionTable, SnapshotExecutionFailure,
};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::sync::Arc;

const _: fn() = || {
    fn assert_send_static<T: Send + 'static>() {}
    assert_send_static::<TableScanPartitionStream>();
};

type PartitionCursor = TableScanCursor<TableScanRangeCursor<Arc<[TableScanUnit]>>>;

struct TableScanPartitionStreamState {
    cursor: PartitionCursor,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    projection: Arc<[usize]>,
    table_id: TableID,
    partition_idx: usize,
    // Keep the checkout last so all local scan resources and pins drop first.
    checkout: ReadSnapshotExecutionCheckout,
}

impl TableScanPartitionStreamState {
    #[inline]
    fn advance(&mut self) -> Result<TableScanCursorAdvance> {
        let Self {
            cursor,
            table,
            layout,
            projection,
            checkout,
            ..
        } = self;
        cursor.advance(
            table,
            layout,
            checkout.read_view(),
            projection,
            &mut include_visible_row,
        )
    }

    async fn load_pending(&mut self) -> Result<()> {
        let pool_guards = self.checkout.pool_guards_owned();
        self.cursor
            .load_pending(
                TableScanRuntime::new(&pool_guards),
                &self.table,
                &self.layout,
            )
            .await
            .attach_with(|| {
                format!(
                    "operation=scan_snapshot_partition, table_id={}, partition_idx={}, phase=load_unit",
                    self.table_id, self.partition_idx
                )
            })
            .disclose()
    }

    #[inline]
    fn failure(&self) -> Option<SnapshotExecutionFailure> {
        self.checkout.failure()
    }
}

/// Fully owned row-oriented stream for one deterministic table-scan partition.
///
/// The stream may be moved into a spawned task and retains at most one loaded
/// persisted block or guarded hot row page. A paused stream can therefore
/// delay exclusive work on its current hot page until it is polled or dropped.
pub struct TableScanPartitionStream {
    state: Option<TableScanPartitionStreamState>,
}

impl TableScanPartitionStream {
    /// Create a stream over one planned partition and execution checkout.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        units: Arc<[TableScanUnit]>,
        start: usize,
        end: usize,
        column_root: BlockID,
        pivot_row_id: RowID,
        projection: Arc<[usize]>,
        table_id: TableID,
        partition_idx: usize,
        table: ReadSnapshotExecutionTable,
        checkout: ReadSnapshotExecutionCheckout,
    ) -> Self {
        let column_count = table.layout.metadata().col.col_count();
        Self {
            state: Some(TableScanPartitionStreamState {
                cursor: TableScanCursor::new(
                    TableScanRangeCursor::new(units, start, end),
                    column_root,
                    pivot_row_id,
                    column_count,
                ),
                table: table.table,
                layout: table.layout,
                projection,
                table_id,
                partition_idx,
                checkout,
            }),
        }
    }

    /// Return the next visible projected row, or `None` after a terminal state.
    pub async fn next(&mut self) -> Result<Option<Vec<Val>>> {
        if self.state.is_none() {
            return Ok(None);
        }
        loop {
            let advance = self
                .state
                .as_mut()
                .expect("partition stream state is present while advancing")
                .advance();
            let advance = match advance {
                Ok(advance) => advance,
                Err(error) => return self.original_error(error),
            };
            match advance {
                TableScanCursorAdvance::Row(row) => return Ok(Some(row)),
                TableScanCursorAdvance::NeedsLoad => {
                    if let Some(failure) = self.failure() {
                        return self.peer_abort(failure);
                    }
                    let loaded = self
                        .state
                        .as_mut()
                        .expect("partition stream state is present while loading")
                        .load_pending()
                        .await;
                    if let Err(error) = loaded {
                        return self.original_error(error);
                    }
                    if let Some(failure) = self.failure() {
                        return self.peer_abort(failure);
                    }
                }
                TableScanCursorAdvance::Exhausted => {
                    if let Some(failure) = self.failure() {
                        return self.peer_abort(failure);
                    }
                    self.close();
                    return Ok(None);
                }
                TableScanCursorAdvance::Stop => {
                    unreachable!("snapshot partition callback always includes visible rows")
                }
            }
        }
    }

    #[inline]
    fn failure(&self) -> Option<SnapshotExecutionFailure> {
        self.state
            .as_ref()
            .expect("partition stream state is present while checking execution failure")
            .failure()
    }

    fn original_error(&mut self, error: Error) -> Result<Option<Vec<Val>>> {
        let state = self
            .state
            .as_ref()
            .expect("partition stream state is present while publishing execution failure");
        if state
            .checkout
            .publish_failure(state.table_id, state.partition_idx)
        {
            state.checkout.request_failed_drain();
        }
        self.close();
        Err(error)
    }

    fn peer_abort(&mut self, failure: SnapshotExecutionFailure) -> Result<Option<Vec<Val>>> {
        let (table_id, partition_idx) = self
            .state
            .as_ref()
            .map(|state| (state.table_id, state.partition_idx))
            .expect("partition stream state is present while reporting peer abort");
        self.close();
        Err(Report::new(OperationError::SnapshotScanAborted)
            .attach(format!(
                "operation=scan_snapshot_partition, table_id={table_id}, partition_idx={partition_idx}, first_failure_table_id={}, first_failure_partition_idx={}",
                failure.table_id, failure.partition_idx
            ))
            .disclose())
    }

    /// Inject an execution error for cooperative-failure tests.
    #[cfg(test)]
    pub(crate) fn inject_execution_error(&mut self) -> Result<Option<Vec<Val>>> {
        self.original_error(
            Report::new(RuntimeError::TableAccess)
                .attach("operation=scan_snapshot_partition, reason=injected_execution_error")
                .disclose(),
        )
    }

    #[inline]
    fn close(&mut self) {
        self.state.take();
    }
}

impl Drop for TableScanPartitionStream {
    #[inline]
    fn drop(&mut self) {
        self.close();
    }
}

#[inline]
fn include_visible_row(_row: &mut LazyRow<'_>) -> Result<ScanRowDecision> {
    Ok(Include)
}
