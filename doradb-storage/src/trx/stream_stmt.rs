use crate::buffer::EvictableBufferPool;
use crate::error::{
    DiscloseResultExt, OperationError, OperationOrFatalResult, Result, RuntimeResult,
};
use crate::id::TableID;
use crate::index::{
    BTreeKeyEncoder, IndexBatchStream, IndexLookupCandidate, OwnedSecondaryIndexCandidateStream,
};
use crate::lock::LockScopeState;
use crate::row::ops::SelectMvcc;
use crate::table::{DmlValidator, Table, TableRuntimeLayout};
use crate::trx::{SessionOperationCheckout, TableAdmissionRequest, Transaction, TrxRuntime};
use crate::value::Val;
use error_stack::ResultExt;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use super::admission::admit_user_table;

const INDEX_SCAN_STREAM_OPERATION: &str = "table_index_scan_mvcc";

struct StreamStmtState {
    checkout: SessionOperationCheckout,
    curr_scope: Option<LockScopeState>,
}

impl StreamStmtState {
    #[inline]
    fn new(checkout: SessionOperationCheckout, curr_scope: LockScopeState) -> Self {
        Self {
            checkout,
            curr_scope: Some(curr_scope),
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
        let Self {
            checkout,
            curr_scope,
        } = self;
        let (inner, attachment) = checkout.inner_and_attachment_mut();
        admit_user_table(
            inner,
            attachment,
            curr_scope
                .as_mut()
                .expect("active stream statement must retain curr_scope"),
            table_id,
            request,
            INDEX_SCAN_STREAM_OPERATION,
        )
        .await
    }
}

impl Drop for StreamStmtState {
    #[inline]
    fn drop(&mut self) {
        if let Some(mut curr_scope) = self.curr_scope.take() {
            let lock_manager = self.checkout.attachment().engine().lock_manager().clone();
            let family = self
                .checkout
                .inner_mut()
                .checked_lock_state_mut()
                .family_mut();
            family.close_scope(&mut curr_scope, &lock_manager);
        }
    }
}

struct IndexScanMvccStreamState<'trx> {
    candidate_stream: OwnedSecondaryIndexCandidateStream<'trx, EvictableBufferPool>,
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
    state: Option<IndexScanMvccStreamState<'trx>>,
    candidates: VecDeque<IndexLookupCandidate>,
    exhausted: bool,
    _trx: PhantomData<&'trx mut Transaction>,
}

impl<'trx> IndexScanMvccStream<'trx> {
    #[inline]
    fn new(state: IndexScanMvccStreamState<'trx>) -> Self {
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
            match self.lookup_candidate(&candidate).await.disclose() {
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
        candidate: &IndexLookupCandidate,
    ) -> RuntimeResult<SelectMvcc> {
        let state = self
            .state
            .as_mut()
            .expect("stream state is present until exhaustion or error");
        let rt = state.stmt_state.runtime();
        let accessor = state.table.accessor_with_layout(&state.layout);
        accessor
            .index_lookup_candidate_row_mvcc(
                rt,
                state.index_no,
                state.unique,
                &state.encoder,
                candidate,
                &state.read_set,
            )
            .await
            .attach_with(|| {
                format!(
                    "operation={INDEX_SCAN_STREAM_OPERATION}, table_id={}, index_no={}, row_id={}",
                    state.table.table_id(),
                    state.index_no,
                    candidate.row_id
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

/// Statement facade for public caller-driven transaction streams.
pub struct StreamStmt<'trx> {
    trx: &'trx mut Transaction,
    disable_validation: bool,
}

impl<'trx> StreamStmt<'trx> {
    #[inline]
    pub(super) fn new(trx: &'trx mut Transaction) -> Self {
        Self {
            trx,
            disable_validation: false,
        }
    }

    /// Disable default DML shape, type, and read-set validation for this stream.
    ///
    /// Validation is enabled by default. Disable it only when the caller has
    /// already validated every `table_index_scan_mvcc` argument against the
    /// target table metadata for this statement:
    ///
    /// - `index_no` names an active secondary index on the target table.
    /// - Every bounded range side has exactly the target index key column count.
    /// - Every bounded range value matches the corresponding indexed column type.
    /// - `read_set` is non-empty, strictly increasing, and contains only
    ///   in-range table column numbers.
    ///
    /// Violating these preconditions may surface as debug assertions or
    /// internal errors instead of `InvalidDmlInput`.
    #[inline]
    pub fn disable_validation(mut self) -> Self {
        self.disable_validation = true;
        self
    }

    /// Creates a public MVCC secondary-index row stream for a user table.
    #[inline]
    pub async fn table_index_scan_mvcc<'r, R>(
        self,
        table_id: TableID,
        index_no: usize,
        range: R,
        read_set: &[usize],
    ) -> Result<IndexScanMvccStream<'trx>>
    where
        R: RangeBounds<&'r [Val]>,
    {
        let mut checkout = self
            .trx
            .checkout()
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))
            .disclose()?;
        let stmt_owner = checkout.inner_mut().next_statement_owner();
        let curr_scope = LockScopeState::new(stmt_owner);
        let mut stmt_state = StreamStmtState::new(checkout, curr_scope);
        let (table, layout) = stmt_state
            .admit_user_table(table_id, TableAdmissionRequest::IndexRead { index_no })
            .await
            .disclose()?;
        if !self.disable_validation {
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
        let encoder = index.key_encoder();
        let range = if unique {
            encoder.encode_range(range)
        } else {
            encoder.encode_non_unique_range(range)
        };
        let rt = stmt_state.runtime();
        let accessor = table.accessor_with_layout(&layout);
        let candidate_stream = accessor
            .index_scan_candidates(rt, index_no, range, PhantomData)
            .disclose()?;
        let state = IndexScanMvccStreamState {
            candidate_stream,
            table,
            layout,
            index_no,
            unique,
            encoder,
            read_set: read_set.to_vec(),
            stmt_state,
        };
        Ok(IndexScanMvccStream::new(state))
    }
}
