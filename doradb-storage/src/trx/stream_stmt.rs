use crate::buffer::EvictableBufferPool;
use crate::buffer::guard::PageGuard;
use crate::error::{
    DiscloseResultExt, OperationError, OperationOrFatalResult, Result, RuntimeResult,
};
use crate::id::{BlockID, RowID, TableID};
use crate::index::{
    BTreeKeyEncoder, ColumnLeafEntry, IndexBatchStream, IndexLookupCandidate,
    OwnedIndexCandidateStream,
};
use crate::row::ops::{ScanRowDecision, SelectMvcc};
use crate::table::{
    DmlValidator, LazyRow, LazyRowBuffer, RowPageDescriptor, Table, TableRuntimeLayout,
    TableScanColdPage, TableScanWorklist,
};
use crate::trx::row::BoundIndexCandidate;
use crate::trx::{
    SessionOperationCheckout, StmtNo, TableAdmissionRequest, Transaction, TrxRuntime,
};
use crate::value::Val;
use error_stack::ResultExt;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::admission::admit_user_table;

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
        request: TableAdmissionRequest,
    ) -> OperationOrFatalResult<(Arc<Table>, Arc<TableRuntimeLayout>)> {
        let operation = self.operation;
        let Self { checkout, .. } = self;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        admit_user_table(inner, attachment, table_id, request, operation).await
    }

    /// Creates a validated MVCC secondary-index row stream for a user table.
    #[inline]
    pub(super) async fn table_index_scan_mvcc_stream<'trx, 'r, R>(
        mut self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<IndexScanMvccStream<'trx>>
    where
        R: RangeBounds<&'r [Val]>,
    {
        let (table, layout) = self
            .admit_user_table(table_id, TableAdmissionRequest::IndexRead { index_no })
            .await
            .disclose()?;
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_index_scan(index_no, &range, read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={INDEX_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })
                .disclose()?;
        }
        let index = layout.secondary_index(index_no).disclose()?;
        let unique = index.is_unique();
        let encoder = index.key_encoder_arc();
        let range = if unique {
            encoder.encode_range(range)
        } else {
            encoder.encode_non_unique_range(range)
        };
        let rt = self.runtime();
        let accessor = table.accessor_with_layout(&layout);
        let candidate_stream = accessor
            .index_scan_candidates(rt, index_no, range)
            .disclose()?;
        let state = IndexScanMvccStreamState {
            candidate_stream,
            table,
            layout,
            index_no,
            unique,
            encoder,
            read_set: read_set.to_vec(),
            stmt_state: self,
        };
        Ok(IndexScanMvccStream::new(state))
    }

    /// Creates a caller-driven programmable MVCC full-table scan stream.
    #[inline]
    pub(super) async fn table_scan_mvcc_stream<'trx, F>(
        mut self,
        table_id: TableID,
        read_set: &[usize],
        scan_row: F,
    ) -> Result<TableScanMvccStream<'trx, F>>
    where
        F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>,
    {
        let (table, layout) = self
            .admit_user_table(table_id, TableAdmissionRequest::TableRead)
            .await
            .disclose()?;
        let worklist = {
            let rt = self.runtime();
            table
                .accessor_with_layout(&layout)
                .table_scan_mvcc_worklist(rt)
                .await
                .attach_with(|| {
                    format!("operation={TABLE_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })
                .disclose()?
        };
        Ok(TableScanMvccStream::new(TableScanMvccStreamState::new(
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
    index_no: usize,
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
        let candidate = BoundIndexCandidate::new(
            state.index_no,
            state.unique,
            state.encoder.as_ref(),
            candidate,
        );
        accessor
            .index_lookup_candidate_row_mvcc(rt, candidate, &state.read_set)
            .await
            .attach_with(|| {
                format!(
                    "operation={INDEX_SCAN_STREAM_OPERATION}, table_id={}, index_no={}, row_id={}",
                    state.table.table_id(),
                    state.index_no,
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
    scan_row: F,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    read_set: Vec<usize>,
    column_root: BlockID,
    pivot_row_id: RowID,
    cold_entries: Vec<ColumnLeafEntry>,
    next_cold_entry: usize,
    cold_page: Option<TableScanColdPage>,
    cold_row_idx: usize,
    hot_pages: Vec<RowPageDescriptor>,
    hot_page_idx: usize,
    hot_row_idx: usize,
    row_buffer: LazyRowBuffer,
    // Keep checkout last so callback, page, and worklist state drops first.
    stmt_state: StreamStmtState,
}

impl<F> TableScanMvccStreamState<F> {
    #[inline]
    fn new(
        scan_row: F,
        table: Arc<Table>,
        layout: Arc<TableRuntimeLayout>,
        read_set: Vec<usize>,
        worklist: TableScanWorklist,
        stmt_state: StreamStmtState,
    ) -> Self {
        let TableScanWorklist {
            column_root,
            pivot_row_id,
            cold_entries,
            hot_pages,
        } = worklist;
        let column_count = layout.metadata().col.col_count();
        Self {
            scan_row,
            table,
            layout,
            read_set,
            column_root,
            pivot_row_id,
            cold_entries,
            next_cold_entry: 0,
            cold_page: None,
            cold_row_idx: 0,
            hot_pages,
            hot_page_idx: 0,
            hot_row_idx: 0,
            row_buffer: LazyRowBuffer::new(column_count),
            stmt_state,
        }
    }
}

/// Public caller-driven programmable MVCC full-table scan stream.
pub struct TableScanMvccStream<'trx, F> {
    state: Option<TableScanMvccStreamState<F>>,
    _trx: PhantomData<&'trx mut Transaction>,
}

impl<'trx, F> TableScanMvccStream<'trx, F>
where
    F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>,
{
    #[inline]
    fn new(state: TableScanMvccStreamState<F>) -> Self {
        Self {
            state: Some(state),
            _trx: PhantomData,
        }
    }

    /// Returns the next included projected row, or `None` after a terminal state.
    pub async fn next(&mut self) -> Result<Option<Vec<Val>>> {
        if self.state.is_none() {
            return Ok(None);
        }
        let result = self.next_inner().await;
        if result.is_err() || matches!(result, Ok(None)) {
            self.close();
        }
        result
    }

    async fn next_inner(&mut self) -> Result<Option<Vec<Val>>> {
        loop {
            let load_cold = {
                let state = self
                    .state
                    .as_ref()
                    .expect("stream state is present while scanning");
                if state.cold_page.is_none() && state.next_cold_entry < state.cold_entries.len() {
                    Some(state.cold_entries[state.next_cold_entry])
                } else {
                    None
                }
            };
            if let Some(entry) = load_cold {
                let page = {
                    let state = self
                        .state
                        .as_ref()
                        .expect("stream state is present while loading a cold page");
                    let rt = state.stmt_state.runtime();
                    state
                        .table
                        .accessor_with_layout(&state.layout)
                        .load_table_scan_cold_page(
                            rt,
                            state.column_root,
                            state.pivot_row_id,
                            &entry,
                        )
                        .await
                        .disclose()?
                };
                let state = self
                    .state
                    .as_mut()
                    .expect("stream state is present after loading a cold page");
                state.cold_page = Some(page);
                state.cold_row_idx = 0;
                state.next_cold_entry += 1;
            }

            let has_cold_page = self
                .state
                .as_ref()
                .expect("stream state is present while scanning")
                .cold_page
                .is_some();
            if has_cold_page {
                let state = self
                    .state
                    .as_mut()
                    .expect("stream state is present while scanning a cold page");
                let accessor = state.table.accessor_with_layout(&state.layout);
                let page = state
                    .cold_page
                    .as_ref()
                    .expect("loaded cold page is present");
                while state.cold_row_idx < page.row_count() {
                    let row_idx = state.cold_row_idx;
                    state.cold_row_idx += 1;
                    let rt = state.stmt_state.runtime();
                    let Some(mut lazy_row) =
                        accessor.table_scan_cold_row(rt, page, row_idx, &mut state.row_buffer)
                    else {
                        continue;
                    };
                    match (state.scan_row)(&mut lazy_row)? {
                        ScanRowDecision::Include => {
                            let vals = lazy_row.project(&state.read_set)?;
                            lazy_row.reset();
                            return Ok(Some(vals));
                        }
                        ScanRowDecision::Skip => lazy_row.reset(),
                        ScanRowDecision::Stop => {
                            lazy_row.reset();
                            return Ok(None);
                        }
                    }
                }
                state.cold_page.take();
                state.cold_row_idx = 0;
                continue;
            }

            let descriptor = {
                let state = self
                    .state
                    .as_ref()
                    .expect("stream state is present while selecting a hot page");
                state.hot_pages.get(state.hot_page_idx).copied()
            };
            let Some(descriptor) = descriptor else {
                return Ok(None);
            };
            let page_guard = {
                let state = self
                    .state
                    .as_ref()
                    .expect("stream state is present while loading a hot page");
                let rt = state.stmt_state.runtime();
                state
                    .table
                    .accessor_with_layout(&state.layout)
                    .load_table_scan_hot_page(rt, descriptor)
                    .await
                    .disclose()?
            };
            let row_count = page_guard.page().header.row_count();
            let state = self
                .state
                .as_mut()
                .expect("stream state is present while scanning a hot page");
            let accessor = state.table.accessor_with_layout(&state.layout);
            while state.hot_row_idx < row_count {
                let row_idx = state.hot_row_idx;
                state.hot_row_idx += 1;
                let access = page_guard.read_row(row_idx);
                let Some(mut lazy_row) = accessor.table_scan_hot_row(
                    state.stmt_state.runtime().ctx(),
                    access,
                    &mut state.row_buffer,
                ) else {
                    continue;
                };
                match (state.scan_row)(&mut lazy_row)? {
                    ScanRowDecision::Include => {
                        let vals = lazy_row.project(&state.read_set)?;
                        lazy_row.reset();
                        return Ok(Some(vals));
                    }
                    ScanRowDecision::Skip => lazy_row.reset(),
                    ScanRowDecision::Stop => {
                        lazy_row.reset();
                        return Ok(None);
                    }
                }
            }
            state.hot_page_idx += 1;
            state.hot_row_idx = 0;
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
