use crate::buffer::EvictableBufferPool;
use crate::catalog::is_catalog_table;
use crate::error::{OperationError, OperationResult, Result};
use crate::id::TableID;
use crate::index::{
    BTreeKeyEncoder, IndexBatchStream, IndexLookupCandidate, OwnedSecondaryIndexCandidateStream,
};
use crate::lock::{LockMode, LockOwner, LockResource, OwnerLockState};
use crate::row::ops::SelectMvcc;
use crate::table::{DmlValidator, Table, TableRuntimeLayout};
use crate::trx::{Transaction, TrxCheckout, TrxRuntime};
use crate::value::Val;
use error_stack::{Report, ResultExt};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

const INDEX_SCAN_STREAM_OPERATION: &str = "table_index_scan_mvcc";

struct StreamStmtState {
    checkout: TrxCheckout,
    stmt_locks: OwnerLockState,
}

impl StreamStmtState {
    #[inline]
    fn new(checkout: TrxCheckout, stmt_locks: OwnerLockState) -> Self {
        Self {
            checkout,
            stmt_locks,
        }
    }

    #[inline]
    fn runtime(&self) -> TrxRuntime<'_> {
        TrxRuntime::new(self.checkout.inner().ctx(), self.checkout.attachment())
    }

    #[inline]
    async fn acquire_table_read_lock(&mut self, table_id: TableID) -> OperationResult<()> {
        self.stmt_locks
            .acquire(
                self.checkout.attachment().engine().lock_manager(),
                LockResource::TableMetadata(table_id),
                LockMode::Shared,
            )
            .await
    }

    #[inline]
    fn resolve_user_table(&mut self, table_id: TableID) -> OperationResult<Arc<Table>> {
        if !is_catalog_table(table_id) {
            let (inner, attachment) = self.checkout.inner_and_attachment_mut();
            if let Some(table) = inner.cached_user_table(table_id) {
                return Ok(table);
            }
            if let Some(table) = attachment.cached_user_table(table_id) {
                inner.cache_user_table(&table);
                return Ok(table);
            }
            let engine = attachment.engine();
            if let Some(table) = engine.catalog().get_table_now(table_id) {
                attachment.cache_user_table(&table);
                inner.cache_user_table(&table);
                return Ok(table);
            }
        }
        Err(Report::new(OperationError::TableNotFound).attach(format!("table_id={table_id}")))
    }
}

impl Drop for StreamStmtState {
    #[inline]
    fn drop(&mut self) {
        self.stmt_locks
            .release_all(self.checkout.attachment().engine().lock_manager());
    }
}

struct IndexScanMvccStreamState {
    stmt_state: StreamStmtState,
    table: Arc<Table>,
    layout: Arc<TableRuntimeLayout>,
    index_no: usize,
    unique: bool,
    encoder: Arc<BTreeKeyEncoder>,
    candidate_stream: OwnedSecondaryIndexCandidateStream<EvictableBufferPool>,
    read_set: Vec<usize>,
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
            let candidate = match self.next_candidate().await {
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
            match self.lookup_candidate(&candidate).await {
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
    async fn fill_candidates(&mut self) -> Result<bool> {
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
    async fn next_candidate(&mut self) -> Result<Option<IndexLookupCandidate>> {
        if self.fill_candidates().await? {
            Ok(self.candidates.pop_front())
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn lookup_candidate(&mut self, candidate: &IndexLookupCandidate) -> Result<SelectMvcc> {
        let state = self
            .state
            .as_mut()
            .expect("stream state is present until exhaustion or error");
        let rt = state.stmt_state.runtime();
        let accessor = state.table.accessor_with_layout(&state.layout);
        Ok(accessor
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
            })?)
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
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))?;
        let trx_id = checkout.inner().trx_id();
        let stmt_no = checkout
            .inner_mut()
            .next_stmt_no()
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))?;
        let stmt_owner = LockOwner::Statement(trx_id, stmt_no);
        let owner_group = checkout
            .inner()
            .checked_lock_state()
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))?
            .owner_group();
        let stmt_locks = match owner_group {
            Some(owner_group) => OwnerLockState::new_grouped(stmt_owner, owner_group),
            None => OwnerLockState::new(stmt_owner),
        };
        let mut stmt_state = StreamStmtState::new(checkout, stmt_locks);
        let table = stmt_state
            .resolve_user_table(table_id)
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))?;
        stmt_state
            .acquire_table_read_lock(table_id)
            .await
            .attach_with(|| {
                format!("operation={INDEX_SCAN_STREAM_OPERATION}, table_id={table_id}")
            })?;
        table
            .check_foreground_live()
            .attach_with(|| format!("operation={INDEX_SCAN_STREAM_OPERATION}"))?;
        let layout = table.layout_snapshot();
        if !self.disable_validation {
            DmlValidator::new(layout.metadata())
                .validate_index_scan(index_no, &range, read_set)
                .change_context(OperationError::InvalidDmlInput)
                .attach_with(|| {
                    format!("operation={INDEX_SCAN_STREAM_OPERATION}, table_id={table_id}")
                })?;
        }
        let index = layout.secondary_index(index_no)?;
        let unique = index.is_unique();
        let encoder = index.key_encoder();
        let range = if unique {
            encoder.encode_range(range)
        } else {
            encoder.encode_non_unique_range(range)
        };
        let rt = stmt_state.runtime();
        let accessor = table.accessor_with_layout(&layout);
        let candidate_stream = accessor.index_scan_candidates(rt, index_no, range)?;
        let state = IndexScanMvccStreamState {
            stmt_state,
            table,
            layout,
            index_no,
            unique,
            encoder,
            candidate_stream,
            read_set: read_set.to_vec(),
        };
        Ok(IndexScanMvccStream::new(state))
    }
}
