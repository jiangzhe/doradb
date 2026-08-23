use crate::buffer::EvictableBufferPool;
use crate::buffer::guard::{PageGuard, PageSharedGuard};
use crate::error::{
    DiscloseResultExt, OperationError, OperationOrFatalResult, Result, RuntimeResult,
};
use crate::id::{BlockID, RowID, TableID};
use crate::index::{
    BTreeKeyEncoder, ColumnLeafEntry, IndexBatchStream, IndexLookupCandidate,
    OwnedIndexCandidateStream,
};
use crate::row::RowPage;
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
        if !self.dml_validation_disabled {
            DmlValidator::new(layout.metadata())
                .validate_projection(read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={TABLE_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })
                .disclose()?;
        }
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

enum TableScanPageState {
    ColdPending(ColumnLeafEntry),
    Cold {
        page: TableScanColdPage,
        next_row: usize,
    },
    HotPending(RowPageDescriptor),
    Hot {
        page_guard: PageSharedGuard<RowPage>,
        next_row: usize,
    },
}

#[derive(Clone, Copy)]
enum TableScanPageLoad {
    Cold(ColumnLeafEntry),
    Hot(RowPageDescriptor),
}

enum TableScanRowAdvance {
    Include(Vec<Val>),
    Skip,
    Stop,
}

enum TableScanAdvance {
    Include(Vec<Val>),
    Continue,
    Stop,
    End,
}

struct TableScanMvccStreamState<F> {
    scan_row: F,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    read_set: Vec<usize>,
    column_root: BlockID,
    pivot_row_id: RowID,
    pages: VecDeque<TableScanPageState>,
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
        let mut pages = VecDeque::with_capacity(cold_entries.len() + hot_pages.len());
        pages.extend(
            cold_entries
                .into_iter()
                .map(TableScanPageState::ColdPending),
        );
        pages.extend(hot_pages.into_iter().map(TableScanPageState::HotPending));
        Self {
            scan_row,
            table,
            layout,
            read_set,
            column_root,
            pivot_row_id,
            pages,
            row_buffer: LazyRowBuffer::new(column_count),
            stmt_state,
        }
    }

    #[inline]
    fn pending_front_load(&self) -> Option<TableScanPageLoad> {
        match self.pages.front() {
            Some(TableScanPageState::ColdPending(entry)) => Some(TableScanPageLoad::Cold(*entry)),
            Some(TableScanPageState::HotPending(descriptor)) => {
                Some(TableScanPageLoad::Hot(*descriptor))
            }
            Some(TableScanPageState::Cold { .. } | TableScanPageState::Hot { .. }) | None => None,
        }
    }

    async fn load_pending_front(&self) -> Result<Option<TableScanPageState>> {
        let Some(load) = self.pending_front_load() else {
            return Ok(None);
        };
        let rt = self.stmt_state.runtime();
        let accessor = self.table.accessor_with_layout(&self.layout);
        let loaded = match load {
            TableScanPageLoad::Cold(entry) => {
                let page = accessor
                    .load_table_scan_cold_page(rt, self.column_root, self.pivot_row_id, &entry)
                    .await
                    .disclose()?;
                TableScanPageState::Cold { page, next_row: 0 }
            }
            TableScanPageLoad::Hot(descriptor) => {
                let page_guard = accessor
                    .load_table_scan_hot_page(rt, descriptor)
                    .await
                    .disclose()?;
                TableScanPageState::Hot {
                    page_guard,
                    next_row: 0,
                }
            }
        };
        Ok(Some(loaded))
    }

    #[inline]
    fn install_loaded_front(&mut self, loaded: TableScanPageState) {
        let matches_pending = matches!(
            (&loaded, self.pages.front()),
            (
                TableScanPageState::Cold { .. },
                Some(TableScanPageState::ColdPending(_))
            ) | (
                TableScanPageState::Hot { .. },
                Some(TableScanPageState::HotPending(_))
            )
        );
        if !matches_pending {
            unreachable!("loaded scan page must replace its matching pending queue front");
        }
        self.pages.pop_front();
        self.pages.push_front(loaded);
    }
}

impl<F> TableScanMvccStreamState<F>
where
    F: for<'row> FnMut(&mut LazyRow<'row>) -> Result<ScanRowDecision>,
{
    #[inline]
    fn apply_row(
        scan_row: &mut F,
        read_set: &[usize],
        mut lazy_row: LazyRow<'_>,
    ) -> Result<TableScanRowAdvance> {
        match scan_row(&mut lazy_row)? {
            ScanRowDecision::Include => {
                let vals = lazy_row.project(read_set)?;
                lazy_row.reset();
                Ok(TableScanRowAdvance::Include(vals))
            }
            ScanRowDecision::Skip => {
                lazy_row.reset();
                Ok(TableScanRowAdvance::Skip)
            }
            ScanRowDecision::Stop => {
                lazy_row.reset();
                Ok(TableScanRowAdvance::Stop)
            }
        }
    }

    fn advance_loaded_front(&mut self) -> Result<TableScanAdvance> {
        let accessor = self.table.accessor_with_layout(&self.layout);
        let advance = match self.pages.front_mut() {
            Some(TableScanPageState::Cold { page, next_row }) => {
                while *next_row < page.row_count() {
                    let row_idx = *next_row;
                    *next_row += 1;
                    let rt = self.stmt_state.runtime();
                    let Some(lazy_row) =
                        accessor.table_scan_cold_row(rt, page, row_idx, &mut self.row_buffer)
                    else {
                        continue;
                    };
                    match Self::apply_row(&mut self.scan_row, &self.read_set, lazy_row)? {
                        TableScanRowAdvance::Include(vals) => {
                            return Ok(TableScanAdvance::Include(vals));
                        }
                        TableScanRowAdvance::Skip => {}
                        TableScanRowAdvance::Stop => return Ok(TableScanAdvance::Stop),
                    }
                }
                TableScanAdvance::Continue
            }
            Some(TableScanPageState::Hot {
                page_guard,
                next_row,
            }) => {
                let row_count = page_guard.page().header.row_count();
                while *next_row < row_count {
                    let row_idx = *next_row;
                    *next_row += 1;
                    let access = page_guard.read_row(row_idx);
                    let Some(lazy_row) = accessor.table_scan_hot_row(
                        self.stmt_state.runtime().ctx(),
                        access,
                        &mut self.row_buffer,
                    ) else {
                        continue;
                    };
                    match Self::apply_row(&mut self.scan_row, &self.read_set, lazy_row)? {
                        TableScanRowAdvance::Include(vals) => {
                            return Ok(TableScanAdvance::Include(vals));
                        }
                        TableScanRowAdvance::Skip => {}
                        TableScanRowAdvance::Stop => return Ok(TableScanAdvance::Stop),
                    }
                }
                TableScanAdvance::Continue
            }
            Some(TableScanPageState::ColdPending(_) | TableScanPageState::HotPending(_)) => {
                unreachable!("pending scan pages are loaded before row processing")
            }
            None => TableScanAdvance::End,
        };
        if matches!(advance, TableScanAdvance::Continue) {
            match self.pages.pop_front() {
                Some(TableScanPageState::Cold { .. } | TableScanPageState::Hot { .. }) => {}
                _ => unreachable!("only an exhausted loaded page can continue the scan"),
            }
        }
        Ok(advance)
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
            let loaded = self
                .state
                .as_ref()
                .expect("stream state is present while loading the queue front")
                .load_pending_front()
                .await?;
            if let Some(loaded) = loaded {
                self.state
                    .as_mut()
                    .expect("stream state is present after loading the queue front")
                    .install_loaded_front(loaded);
                continue;
            }

            let advance = self
                .state
                .as_mut()
                .expect("stream state is present while advancing the queue front")
                .advance_loaded_front()?;
            match advance {
                TableScanAdvance::Include(vals) => return Ok(Some(vals)),
                TableScanAdvance::Continue => {}
                TableScanAdvance::Stop | TableScanAdvance::End => return Ok(None),
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
