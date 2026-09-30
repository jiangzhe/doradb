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
    pub(crate) build_ts: TrxID,
    /// Local evidence selected by the invocation duplicate policy.
    pub(crate) duplicates: DuplicateCheck,
}

/// Caller-owned stable layout, pivot, pool roots, and optional DDL gate.
#[derive(Clone)]
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
    /// Ordered contiguous page descriptors with charged capacity.
    pub(crate) pages: Arc<BudgetedVec<RowPageDescriptor>>,
    /// Selected encoding, projection, timestamp, and duplicate policy.
    pub(crate) key: HotBuildKeySpec,
    /// Shared admission retained for run ownership and downstream phases.
    pub(crate) budget: MemoryBudget,
    /// Captured cold/hot boundary.
    pub(crate) pivot: RowID,
    // DDL admission cannot retire while any accepted worker holds this source.
    _ddl: Option<Arc<IndexDdlGateScope>>,
    /// Component-only scheduling and failure controls.
    #[cfg(test)]
    pub(super) test: WorkerHooks,
    /// Wall duration of source and descriptor capture, in nanoseconds.
    #[cfg(feature = "profiling")]
    pub(crate) capture_elapsed_nanos: u64,
    /// Shared publication target for completed extraction samples.
    #[cfg(feature = "profiling")]
    pub(crate) profiler: Arc<HotIndexBuildProfiler>,
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
        HotBuildTableSource::new(capture, policy).select(spec, build_ts, duplicates)
    }

    /// Admit one descriptor before source sharing begins.
    #[inline]
    pub(crate) fn push_page(&mut self, page: RowPageDescriptor) -> RuntimeResult<()> {
        push_page(&mut self.pages, self.pivot, page)
    }

    /// Sort and validate descriptor contiguity and identity before source sharing.
    /// The caller must supply every original hot page.
    #[inline]
    pub(crate) fn finish_capture(&mut self) -> RuntimeResult<()> {
        finish_capture(&mut self.pages, self.pivot)
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

/// Index-neutral captured table; descriptor admission survives every selected index.
pub(crate) struct HotBuildTableSource {
    capture: HotBuildCapture,
    pages: Arc<BudgetedVec<RowPageDescriptor>>,
    /// Shared table-local budget, reset only between settled index builds.
    pub(crate) budget: MemoryBudget,
}

impl HotBuildTableSource {
    /// Begin source capture under the caller's bootstrap or DDL exclusion.
    pub(crate) fn new(capture: HotBuildCapture, policy: HotBuildPolicy) -> Self {
        let budget = MemoryBudget::new(policy.max_scratch_bytes);
        Self {
            capture,
            pages: Arc::new(BudgetedVec::new(&budget)),
            budget,
        }
    }

    /// Admit an authoritative replay descriptor, excluding checkpointed prefixes.
    #[inline]
    pub(crate) fn push_page(&mut self, page: RowPageDescriptor) -> RuntimeResult<()> {
        push_page(&mut self.pages, self.capture.pivot, page)
    }

    /// Sort and validate descriptor contiguity and identity before source sharing.
    /// The replay owner must supply every surviving hot page after draining jobs.
    #[inline]
    pub(crate) fn finish_capture(&mut self) -> RuntimeResult<()> {
        finish_capture(&mut self.pages, self.capture.pivot)
    }

    /// Count final hot pages once, independently of the number of active indexes.
    #[inline]
    #[cfg(feature = "profiling")]
    pub(crate) fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Bind immutable descriptors to one physical key shape without reallocation.
    pub(crate) fn select(
        &self,
        spec: &TableIndexMetadata,
        build_ts: TrxID,
        duplicates: DuplicateCheck,
    ) -> HotBuildSource {
        let capture = &self.capture;
        let unique = spec.unique();
        HotBuildSource {
            table: capture.table.clone(),
            layout: capture.layout.clone(),
            guards: capture.guards.clone(),
            pages: self.pages.clone(),
            key: HotBuildKeySpec {
                index: spec.index,
                columns: spec
                    .keys
                    .iter()
                    .map(|key| key.column_ordinal.as_usize())
                    .collect(),
                encoder: secondary_index_encoder(capture.layout.metadata(), spec, !unique),
                unique,
                build_ts,
                duplicates,
            },
            budget: self.budget.clone(),
            pivot: capture.pivot,
            _ddl: capture.ddl.clone(),
            #[cfg(test)]
            test: WorkerHooks::default(),
            #[cfg(feature = "profiling")]
            capture_elapsed_nanos: 0,
            #[cfg(feature = "profiling")]
            profiler: capture.profiler.clone(),
        }
    }
}

#[inline]
fn push_page(
    pages: &mut Arc<BudgetedVec<RowPageDescriptor>>,
    pivot: RowID,
    page: RowPageDescriptor,
) -> RuntimeResult<()> {
    if page.start_row_id >= page.end_row_id
        || (page.start_row_id < pivot && page.end_row_id > pivot)
    {
        return Err(Report::new(DataIntegrityError::InvalidPayload)
            .attach(format!(
                "hot-build invalid range or pivot: page={page:?}, pivot={}",
                pivot
            ))
            .change_context(RuntimeError::IndexAccess));
    }
    if page.end_row_id <= pivot {
        return Ok(());
    }
    Arc::get_mut(pages)
        .unwrap_or_else(|| unreachable!("descriptor capture precedes source sharing"))
        .push(page, "page descriptors")
        .change_context(RuntimeError::IndexAccess)
}

fn finish_capture(
    pages: &mut Arc<BudgetedVec<RowPageDescriptor>>,
    pivot: RowID,
) -> RuntimeResult<()> {
    Arc::get_mut(pages)
        .unwrap_or_else(|| unreachable!("descriptor validation precedes source sharing"))
        .sort_unstable_by_key(|page| page.start_row_id);
    // Temporary validation metadata is outside the bulk scratch budget.
    // Its size is bounded by the admitted descriptor count, and it is
    // released before any extraction jobs start.
    let mut page_ids =
        FastHashSet::with_capacity_and_hasher(pages.len(), FastRandomState::default());
    let mut next_row_id = pivot;
    for page in pages.iter() {
        if page.start_row_id != next_row_id {
            return Err(Report::new(DataIntegrityError::InvalidPayload)
                    .attach(format!(
                        "hot-build non-contiguous descriptors: expected_start={next_row_id}, page={page:?}, pivot={pivot}"
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
        next_row_id = page.end_row_id;
    }
    Ok(())
}
