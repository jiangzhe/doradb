#[cfg(test)]
pub(super) use super::tests::WorkerHooks;
use super::{BudgetedVec, DuplicateCheck, HotBuildPolicy, MemoryBudget};
use crate::buffer::PoolGuards;
use crate::catalog::index::IndexDdlGateScope;
use crate::catalog::{IndexRef, TableIndexMetadata};
use crate::error::{DataIntegrityError, RuntimeError, RuntimeOrFatalResult, RuntimeResult};
use crate::id::{RowID, TrxID};
use crate::index::{BTreeKeyEncoder, secondary_index_encoder};
use crate::map::{FastHashSet, FastRandomState};
#[cfg(feature = "profiling")]
use crate::profiling::HotIndexBuildProfiler;
use crate::table::{RowPageDescriptor, Table, TableRuntimeLayout};
use error_stack::{Report, ResultExt};
use std::sync::Arc;

/// Selected physical key shape, timestamp, and caller-required duplicate policy.
pub(crate) struct HotBuildKeySpec {
    /// Exact selected index identity.
    pub(crate) index: IndexRef,
    /// Owned ordered column ordinals used by the reusable projection.
    pub(crate) columns: Vec<usize>,
    /// Shared existing physical key encoder for all workers.
    pub(crate) encoder: BTreeKeyEncoder,
    /// Whether physical encoding omits the RowID suffix.
    pub(crate) unique: bool,
    /// Caller timestamp retained for later tree construction.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "RFC 0032 phase 3 timestamps built pages")
    )]
    pub(crate) build_ts: TrxID,
    /// Local evidence selected by the invocation duplicate policy.
    pub(crate) duplicates: DuplicateCheck,
}

/// Caller-owned stable layout, pivot, pool roots, and optional DDL gate.
pub(crate) struct HotBuildCapture {
    /// Stable table owner supplied by the capture factory.
    pub(crate) table: Arc<Table>,
    /// Layout captured under the caller's exclusion or bootstrap authority.
    pub(crate) layout: Arc<TableRuntimeLayout>,
    /// Pool roots retained by all accepted page reads.
    pub(crate) guards: PoolGuards,
    /// Cold/hot boundary captured with the layout/root.
    pub(crate) pivot: RowID,
    /// CREATE metadata gate retained through child completion.
    pub(crate) ddl: Option<Arc<IndexDdlGateScope>>,
    /// Engine recorder retained by this source.
    #[cfg(feature = "profiling")]
    pub(crate) profiler: Arc<HotIndexBuildProfiler>,
}

/// Captured physical source owned through all accepted child completions.
pub(crate) struct HotBuildSource {
    /// Stable table owner retained by accepted workers.
    pub(crate) table: Arc<Table>,
    /// Captured layout used for row interpretation.
    pub(crate) layout: Arc<TableRuntimeLayout>,
    /// Owner-scoped pool roots retained through guarded page access.
    pub(crate) guards: PoolGuards,
    /// Ordered disjoint page descriptors with charged capacity.
    pub(crate) pages: BudgetedVec<RowPageDescriptor>,
    /// Selected encoding, projection, timestamp, and duplicate policy.
    pub(crate) key: HotBuildKeySpec,
    /// Shared admission retained for run ownership and downstream phases.
    pub(crate) budget: MemoryBudget,
    /// Captured cold/hot boundary.
    pub(crate) pivot: RowID,
    /// Wall duration of source and descriptor capture, in nanoseconds.
    #[cfg(feature = "profiling")]
    pub(crate) capture_elapsed_nanos: u64,
    /// Shared publication target for completed extraction samples.
    #[cfg(feature = "profiling")]
    pub(crate) profiler: Arc<HotIndexBuildProfiler>,
    // DDL admission cannot retire while any accepted worker holds this source.
    _ddl: Option<Arc<IndexDdlGateScope>>,
    /// Component-only scheduling and failure controls.
    #[cfg(test)]
    pub(super) test: WorkerHooks,
}

impl HotBuildSource {
    /// Initialize a source only from the catalog/recovery factory owning stability.
    /// The caller admits descriptor capacity through `push_page` before capture.
    pub(crate) fn new(
        capture: HotBuildCapture,
        spec: &TableIndexMetadata,
        build_ts: TrxID,
        duplicates: DuplicateCheck,
        policy: HotBuildPolicy,
    ) -> Self {
        let HotBuildCapture {
            table,
            layout,
            guards,
            pivot,
            ddl,
            #[cfg(feature = "profiling")]
            profiler,
        } = capture;
        let budget = MemoryBudget::new(policy.max_scratch_bytes);
        let columns = spec
            .keys
            .iter()
            .map(|key| key.column_ordinal.as_usize())
            .collect();
        let unique = spec.unique();
        let encoder = secondary_index_encoder(layout.metadata(), spec, !unique);
        let pages = BudgetedVec::new(&budget);
        Self {
            table,
            layout,
            guards,
            pages,
            key: HotBuildKeySpec {
                index: spec.index,
                columns,
                encoder,
                unique,
                build_ts,
                duplicates,
            },
            budget,
            pivot,
            #[cfg(feature = "profiling")]
            capture_elapsed_nanos: 0,
            #[cfg(feature = "profiling")]
            profiler,
            _ddl: ddl,
            #[cfg(test)]
            test: WorkerHooks::default(),
        }
    }

    /// Admit one descriptor, excluding fully checkpointed prefixes.
    pub(crate) fn push_page(&mut self, page: RowPageDescriptor) -> RuntimeResult<()> {
        if page.start_row_id >= page.end_row_id
            || (page.start_row_id < self.pivot && page.end_row_id > self.pivot)
        {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                .attach(format!(
                    "hot-build invalid range or pivot: page={page:?}, pivot={}",
                    self.pivot
                ))
                .change_context(RuntimeError::IndexAccess));
        }
        if page.end_row_id <= self.pivot {
            return Ok(());
        }
        self.pages
            .push(page, "page descriptors")
            .change_context(RuntimeError::IndexAccess)
    }

    /// Validate finalized registry coverage without reopening or copying row pages.
    pub(crate) fn finish_capture(&mut self) -> RuntimeResult<()> {
        self.pages.sort_unstable_by_key(|page| page.start_row_id);
        // Temporary validation metadata is outside the bulk scratch budget.
        // Its size is bounded by the admitted descriptor count, and it is
        // released before any extraction jobs start.
        let mut page_ids =
            FastHashSet::with_capacity_and_hasher(self.pages.len(), FastRandomState::default());
        let mut previous: Option<&RowPageDescriptor> = None;
        for page in self.pages.iter() {
            if let Some(left) = previous
                && left.end_row_id > page.start_row_id
            {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "hot-build overlapping descriptors: left={left:?}, right={page:?}"
                    ))
                    .change_context(RuntimeError::IndexAccess));
            }
            if !page_ids.insert(page.page_id) {
                return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "hot-build repeated page identity: page_id={}",
                        page.page_id
                    ))
                    .change_context(RuntimeError::IndexAccess));
            }
            previous = Some(page);
        }
        Ok(())
    }

    /// Capture CREATE's original pages directly through the budgeted sink.
    pub(crate) async fn capture_pages(&mut self) -> RuntimeOrFatalResult<()> {
        let table = self.table.clone();
        let guards = self.guards.clone();
        table
            .row_store
            .visit_original_row_pages_from(&guards, self.pivot, |page| self.push_page(page))
            .await?;
        self.finish_capture()?;
        Ok(())
    }
}
