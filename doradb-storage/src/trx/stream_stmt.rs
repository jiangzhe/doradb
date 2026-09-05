use crate::buffer::EvictableBufferPool;
use crate::catalog::{IndexRef, TableIndexSelector};
use crate::error::{
    CallbackResult, DiscloseResultExt, OperationError, OperationOrFatalResult, Result,
    RuntimeResult,
};
use crate::id::TableID;
use crate::index::{
    BTreeKeyEncoder, IndexBatchStream, IndexLookupCandidate, OwnedIndexCandidateStream,
};
use crate::row::ops::{ScanRowDecision, SelectMvcc};
use crate::table::{
    DmlValidator, LazyRow, Table, TableRuntimeLayout, TableScanCursor, TableScanCursorAdvance,
    TableScanRuntime, TableScanWorklist, TableScanWorklistCursor,
};
use crate::trx::row::BoundIndexCandidate;
use crate::trx::{MvccReadView, SessionOperationCheckout, StmtNo, Transaction, TrxRuntime};
use crate::value::Val;
use error_stack::ResultExt;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::admission::{AdmittedUserIndex, AdmittedUserTable, admit_user_index, admit_user_table};

pub(super) const INDEX_SCAN_STREAM_OPERATION: &str = "table_index_scan_mvcc";
pub(super) const TABLE_SCAN_STREAM_OPERATION: &str = "table_scan_mvcc_stream";

pub(super) struct StreamStmtState {
    checkout: SessionOperationCheckout,
    _stmt_no: StmtNo,
    dml_validation_disabled: bool,
    operation: &'static str,
}

impl StreamStmtState {
    #[inline]
    pub(super) fn new(
        mut checkout: SessionOperationCheckout,
        dml_validation_disabled: bool,
        operation: &'static str,
    ) -> Self {
        let stmt_no = checkout.inner_mut().next_stmt_no();
        Self {
            checkout,
            _stmt_no: stmt_no,
            dml_validation_disabled,
            operation,
        }
    }

    #[inline]
    fn runtime(&self) -> TrxRuntime<'_> {
        TrxRuntime::new(
            self.checkout.inner().ctx(),
            self.checkout.attachment(),
            self.checkout.inner().checked_lock_state(),
        )
    }

    #[inline]
    async fn admit_user_table(
        &mut self,
        table_id: TableID,
        write: bool,
    ) -> OperationOrFatalResult<AdmittedUserTable> {
        let operation = self.operation;
        let Self { checkout, .. } = self;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        admit_user_table(inner, attachment, table_id, write, operation).await
    }

    #[inline]
    async fn admit_user_index(
        &mut self,
        selector: TableIndexSelector,
    ) -> OperationOrFatalResult<AdmittedUserIndex> {
        let operation = self.operation;
        let Self { checkout, .. } = self;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        admit_user_index(inner, attachment, selector, false, operation).await
    }

    /// Creates a validated MVCC secondary-index row stream for a user table.
    #[inline]
    pub(super) async fn table_index_scan_mvcc_stream<'trx, 'r, R>(
        mut self,
        selector: TableIndexSelector,
        range: R,
        read_set: &[usize],
    ) -> Result<IndexScanMvccStream<'trx>>
    where
        R: RangeBounds<&'r [Val]>,
    {
        let table_id = selector.table_id();
        let AdmittedUserIndex {
            table,
            layout,
            index,
        } = self.admit_user_index(selector).await.disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_index_scan(index.slot(), &range, read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={INDEX_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })
                .disclose()?;
        }
        let runtime = layout.secondary_index(index).disclose()?;
        let unique = runtime.is_unique();
        let encoder = runtime.key_encoder_arc();
        let range = if unique {
            encoder.encode_range(range)
        } else {
            encoder.encode_non_unique_range(range)
        };
        let rt = self.runtime();
        let accessor = table.accessor_with_layout(&layout);
        let candidate_stream = accessor
            .index_scan_candidates(rt, index, range)
            .disclose()?;
        debug_assert_eq!(candidate_stream.index_ref(), index);
        let state = IndexScanMvccStreamState {
            candidate_stream,
            table,
            layout,
            index,
            unique,
            encoder,
            read_set: read_set.to_vec(),
            stmt_state: self,
        };
        Ok(IndexScanMvccStream::new(state))
    }

    /// Creates a caller-driven programmable MVCC full-table scan stream.
    #[inline]
    pub(super) async fn table_scan_mvcc_stream<'trx, F, E>(
        mut self,
        table_id: TableID,
        read_set: &[usize],
        scan_row: F,
    ) -> CallbackResult<TableScanMvccStream<'trx, F>, E>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> CallbackResult<ScanRowDecision, E>,
    {
        let AdmittedUserTable { table, layout } =
            self.admit_user_table(table_id, false).await.disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_projection(read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={TABLE_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })
                .disclose()?;
        }
        let (read_view, worklist) = {
            let rt = self.runtime();
            let read_view = MvccReadView::from_transaction(rt.ctx());
            let accessor = table.accessor_with_layout(&layout);
            let root = accessor.root_snapshot(rt.ctx());
            let worklist = accessor
                .table_scan_mvcc_worklist(TableScanRuntime::from_transaction(rt), &root, &read_view)
                .await
                .attach_with(|| {
                    format!("operation={TABLE_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })
                .disclose()?;
            (read_view, worklist)
        };
        Ok(TableScanMvccStream::new(TableScanMvccStreamState::new(
            read_view,
            scan_row,
            table,
            layout,
            read_set.to_vec(),
            worklist,
            self,
        )))
    }
}

struct IndexScanMvccStreamState {
    candidate_stream: OwnedIndexCandidateStream<EvictableBufferPool>,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    index: IndexRef,
    unique: bool,
    encoder: Arc<BTreeKeyEncoder>,
    read_set: Vec<usize>,
    // Keep checkout last so cursor/root state is destroyed before transaction check-in.
    stmt_state: StreamStmtState,
}

/// Public caller-driven MVCC secondary-index scan stream.
pub struct IndexScanMvccStream<'trx> {
    state: Option<IndexScanMvccStreamState>,
    candidates: VecDeque<IndexLookupCandidate>,
    exhausted: bool,
    _trx: PhantomData<&'trx mut Transaction>,
}

impl<'trx> IndexScanMvccStream<'trx> {
    #[inline]
    fn new(state: IndexScanMvccStreamState) -> Self {
        Self {
            state: Some(state),
            candidates: VecDeque::new(),
            exhausted: false,
            _trx: PhantomData,
        }
    }

    /// Returns the next visible projected row, or `None` after exhaustion.
    #[inline]
    pub async fn next(&mut self) -> Result<Option<Vec<Val>>> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            let candidate = match self.next_candidate().await.disclose() {
                Ok(Some(candidate)) => candidate,
                Ok(None) => {
                    self.exhausted = true;
                    self.close();
                    return Ok(None);
                }
                Err(err) => {
                    self.close();
                    return Err(err);
                }
            };
            match self.lookup_candidate(candidate).await.disclose() {
                Ok(SelectMvcc::Found(vals)) => return Ok(Some(vals)),
                Ok(SelectMvcc::NotFound) => (),
                Err(err) => {
                    self.close();
                    return Err(err);
                }
            }
        }
    }

    #[inline]
    async fn fill_candidates(&mut self) -> RuntimeResult<bool> {
        if !self.candidates.is_empty() {
            return Ok(true);
        }
        let state = self
            .state
            .as_mut()
            .expect("stream state is present until exhaustion or error");
        loop {
            let Some(batch) = state.candidate_stream.next_batch().await? else {
                return Ok(false);
            };
            if batch.is_empty() {
                continue;
            }
            self.candidates = VecDeque::from(batch);
            return Ok(true);
        }
    }

    #[inline]
    async fn next_candidate(&mut self) -> RuntimeResult<Option<IndexLookupCandidate>> {
        if self.fill_candidates().await? {
            Ok(self.candidates.pop_front())
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn lookup_candidate(
        &mut self,
        candidate: IndexLookupCandidate,
    ) -> RuntimeResult<SelectMvcc> {
        let state = self
            .state
            .as_mut()
            .expect("stream state is present until exhaustion or error");
        let rt = state.stmt_state.runtime();
        let accessor = state.table.accessor_with_layout(&state.layout);
        let row_id = candidate.row_id;
        let candidate =
            BoundIndexCandidate::new(state.index, state.unique, state.encoder.as_ref(), candidate);
        accessor
            .index_lookup_candidate_row_mvcc(rt, candidate, &state.read_set)
            .await
            .attach_with(|| {
                format!(
                    "operation={INDEX_SCAN_STREAM_OPERATION}, table_id={}, index={}, row_id={}",
                    state.table.table_id(),
                    state.index,
                    row_id
                )
            })
    }

    #[inline]
    fn close(&mut self) {
        self.state.take();
        self.candidates.clear();
        self.exhausted = true;
    }
}

impl Drop for IndexScanMvccStream<'_> {
    #[inline]
    fn drop(&mut self) {
        self.close();
    }
}

struct TableScanMvccStreamState<F> {
    read_view: MvccReadView,
    scan_row: F,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    read_set: Vec<usize>,
    cursor: TableScanCursor<TableScanWorklistCursor>,
    // Keep checkout last so callback, page, and worklist state drops first.
    stmt_state: StreamStmtState,
}

impl<F> TableScanMvccStreamState<F> {
    #[inline]
    fn new(
        read_view: MvccReadView,
        scan_row: F,
        table: Arc<Table>,
        layout: Arc<TableRuntimeLayout>,
        read_set: Vec<usize>,
        worklist: TableScanWorklist,
        stmt_state: StreamStmtState,
    ) -> Self {
        let column_count = layout.metadata().col.col_count();
        let (_, _, units) = TableScanWorklistCursor::from_worklist(worklist);
        Self {
            read_view,
            scan_row,
            table,
            layout,
            read_set,
            cursor: TableScanCursor::new(units, column_count),
            stmt_state,
        }
    }

    async fn load_pending(&mut self) -> Result<()> {
        let runtime = TableScanRuntime::from_transaction(self.stmt_state.runtime());
        self.cursor
            .load_pending(runtime, &self.table, &self.layout)
            .await
            .attach_with(|| {
                format!(
                    "operation={TABLE_SCAN_STREAM_OPERATION}, table_id={}, phase=load_unit",
                    self.table.table_id()
                )
            })
            .disclose()
    }
}

impl<F, E> TableScanMvccStreamState<F>
where
    F: for<'row> FnMut(&mut LazyRow<'row>) -> CallbackResult<ScanRowDecision, E>,
{
    fn advance(&mut self) -> CallbackResult<TableScanCursorAdvance, E> {
        self.cursor.advance(
            &self.table,
            &self.layout,
            &self.read_view,
            &self.read_set,
            &mut self.scan_row,
        )
    }
}

/// Public caller-driven programmable MVCC full-table scan stream.
///
/// The stream retains a shared guard for its current hot page across returned
/// rows until that page is exhausted or the stream closes. Ordinary row
/// updates remain compatible, but work requiring the same page latch
/// exclusively may wait. A caller paused mid-page must not wait for external
/// work that requires that exclusive latch.
pub struct TableScanMvccStream<'trx, F> {
    state: Option<TableScanMvccStreamState<F>>,
    _trx: PhantomData<&'trx mut Transaction>,
}

impl<'trx, F, E> TableScanMvccStream<'trx, F>
where
    F: for<'row> FnMut(&mut LazyRow<'row>) -> CallbackResult<ScanRowDecision, E>,
{
    #[inline]
    fn new(state: TableScanMvccStreamState<F>) -> Self {
        Self {
            state: Some(state),
            _trx: PhantomData,
        }
    }

    /// Returns the next included projected row, or `None` after a terminal state.
    pub async fn next(&mut self) -> CallbackResult<Option<Vec<Val>>, E> {
        if self.state.is_none() {
            return Ok(None);
        }
        let result = self.next_inner().await;
        if result.is_err() || matches!(result, Ok(None)) {
            self.close();
        }
        result
    }

    async fn next_inner(&mut self) -> CallbackResult<Option<Vec<Val>>, E> {
        loop {
            let advance = self
                .state
                .as_mut()
                .expect("stream state is present while advancing the cursor")
                .advance()?;
            match advance {
                TableScanCursorAdvance::Row(vals) => return Ok(Some(vals)),
                TableScanCursorAdvance::NeedsLoad => {
                    self.state
                        .as_mut()
                        .expect("stream state is present while loading the pending unit")
                        .load_pending()
                        .await?;
                }
                TableScanCursorAdvance::Stop | TableScanCursorAdvance::Exhausted => {
                    return Ok(None);
                }
            }
        }
    }

    #[inline]
    fn close(&mut self) {
        self.state.take();
    }
}

impl<F> Drop for TableScanMvccStream<'_, F> {
    #[inline]
    fn drop(&mut self) {
        self.state.take();
    }
}
