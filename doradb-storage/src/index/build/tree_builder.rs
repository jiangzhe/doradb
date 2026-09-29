//! Private streaming construction with globally planned branch levels.
use super::merge::{
    CompletedPartition, HotDuplicate, HotEntryRef, HotMergeCompletion, HotMergeConsumption,
    HotPartitionConsumer, PartitionMergeStream, PreparedHotMerge, execution_error, observe_stop,
};
use super::page_cleanup::{PageProducer, StagedPageOwner};
use super::{BudgetedVec, HotBuildSource, MemoryBudget, SortedHotRuns};
use crate::buffer::guard::{PageExclusiveGuard, PageGuard};
use crate::buffer::{BufferPool, EvictableBufferPool, PoolGuard};
use crate::completion::Completion;
use crate::component::panic_payload_description;
use crate::error::{
    FatalError, MultiDomainResultExt, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult,
};
use crate::id::{PageID, RowID, TrxID};
use crate::index::btree::algo::{
    KnownFenceNodeParams, PackedNodeEntry, PackedNodePlanParams, pack_fixed_entries,
    try_plan_sibling_node,
};
use crate::index::btree::{
    BTREE_BYTE_ZERO, BTREE_NODE_USABLE_SIZE, BTreeByte, BTreeNode, BTreeSlot, BTreeU64, BTreeValue,
    PackedNodeSpace,
};
use crate::index::mem_index::MemIndex;
use crate::index::util::Maskable;
use crate::latch::LatchFallbackMode;
use crate::poison::EnginePoisoner;
use crate::quiescent::QuiescentGuard;
use crate::runtime::{thread_pool::ThreadPool, yield_now};
use crate::value::{ValKind, ValType};
use error_stack::{Report, ResultExt};
use futures::FutureExt;
use std::borrow::Borrow;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) use super::page_cleanup::StagedPageCleanup;

#[cfg(test)]
pub(crate) use tests::{assert_recovery_root, gate_recovery_allocation, panic_recovery_cleanup};

#[cfg(feature = "profiling")]
use crate::profiling::{HotMergeMeasurements, HotPackedLevel, HotPackedMeasurements};
#[cfg(feature = "profiling")]
use std::{mem::take, time::Instant};

const INITIAL_CANDIDATES: usize = 64;
const PARENT_WINDOW: usize = max_node_slots::<BTreeU64>() + 2;

#[cfg(feature = "profiling")]
#[derive(Clone, Copy, Debug, Default)]
struct PageMeasurement {
    planning: u64,
    allocation: u64,
    packing: u64,
    occupied: usize,
}

// One restartable parent-planning attempt. The interval continues across helper
// returns and is restarted only after an actual cooperative yield resumes.
#[cfg(feature = "profiling")]
struct ParentPlanningProfile {
    started: Instant,
    interval_started: Instant,
    max_sync_nanos: u64,
}

#[cfg(feature = "profiling")]
impl ParentPlanningProfile {
    fn new(started: Instant) -> Self {
        Self {
            started,
            interval_started: started,
            max_sync_nanos: 0,
        }
    }

    fn record(&mut self, now: Instant) {
        self.max_sync_nanos = self
            .max_sync_nanos
            .max(now.duration_since(self.interval_started).as_nanos() as u64);
    }

    fn resume(&mut self, now: Instant) {
        self.interval_started = now;
    }

    fn finish(mut self, now: Instant, measurements: &mut HotPackedMeasurements) {
        self.record(now);
        measurements.parent_planning_nanos += now.duration_since(self.started).as_nanos() as u64;
        measurements.max_sync_nanos = measurements.max_sync_nanos.max(self.max_sync_nanos);
    }
}

/// Unpublished build destination, either owned staging or a borrowed bootstrap index.
/// Construction borrows this wrapper exclusively through root installation.
/// Only owned staging can release its index with finish or reclaim it with destroy;
/// recovery leaves index ownership with its captured table/layout.
pub(crate) struct StagingMemIndex<P: 'static, I = MemIndex<P>> {
    index: I,
    pool: QuiescentGuard<P>,
    guard: PoolGuard,
    unique: bool,
    ts: TrxID,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "CREATE INDEX staging integration remains RFC 0032 phase 5"
    )
)]
impl<P: BufferPool + 'static> StagingMemIndex<P> {
    /// Create an unpublished destination; borrowing this capability excludes all readers.
    pub(crate) async fn new(
        pool: QuiescentGuard<P>,
        guard: PoolGuard,
        mut types: Vec<ValType>,
        unique: bool,
        ts: TrxID,
    ) -> RuntimeOrFatalResult<Self> {
        if !unique {
            types.push(ValType::new(ValKind::U64, false));
        }
        let index = MemIndex::new_with_types(pool.clone(), &guard, types, ts)
            .await
            .map_err(Into::into)?;
        Ok(Self {
            index,
            pool,
            guard,
            unique,
            ts,
        })
    }

    /// Hand the completed index to the caller for publication.
    /// The caller must first install the ready tree and successfully run cleanup.
    /// This synchronous handoff does not execute cleanup or publish the index.
    #[inline]
    pub(crate) fn finish(self) -> MemIndex<P> {
        self.index
    }

    /// Destroy the private index after caller-driven detached-page cleanup.
    /// Use this path when construction or validation fails or the caller aborts.
    #[inline]
    pub(crate) async fn destroy(self) -> RuntimeOrFatalResult<()> {
        self.index.destroy(&self.guard).await.map_err(Into::into)
    }
}

impl<'a> StagingMemIndex<EvictableBufferPool, &'a MemIndex<EvictableBufferPool>> {
    /// Borrow recovery's captured active index under exclusive bootstrap authority.
    /// The source pins the table/layout and exact key representation; foreground
    /// and maintenance admission must remain closed through cleanup and join.
    pub(crate) fn for_recovery(
        source: &'a HotBuildSource,
        pool: QuiescentGuard<EvictableBufferPool>,
    ) -> RuntimeOrFatalResult<Self> {
        let selected = source.layout.expect_secondary_index(source.key.index);
        let index = if source.key.unique {
            &**selected.unique_mem()?
        } else {
            &**selected.non_unique_mem()?
        };
        Ok(Self {
            index,
            pool,
            guard: source.guards.index_guard().clone(),
            unique: source.key.unique,
            ts: source.key.build_ts,
        })
    }
}

impl<P: BufferPool + 'static, I: Borrow<MemIndex<P>>> StagingMemIndex<P, I> {
    /// Bind a plan to an exclusively borrowed target for direct component use.
    /// Retain cleanup before calling execute; run it after install, abort, or drop.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "production uses retained pipeline state")
    )]
    pub(crate) fn start_build(
        &mut self,
        plan: Arc<PreparedHotMerge>,
        workers: QuiescentGuard<ThreadPool>,
        poisoner: QuiescentGuard<EnginePoisoner>,
    ) -> (HotPackedBuild<'_, P, I>, StagedPageCleanup<P>) {
        let (state, cleanup) = self.start_build_state(plan, workers, poisoner);
        (
            HotPackedBuild {
                staging: Some(self),
                state,
            },
            cleanup,
        )
    }

    /// Create packed job state that a pipeline can retain across borrowed-future cancellation.
    pub(super) fn start_build_state(
        &mut self,
        plan: Arc<PreparedHotMerge>,
        workers: QuiescentGuard<ThreadPool>,
        poisoner: QuiescentGuard<EnginePoisoner>,
    ) -> (PackedBuildState<P>, StagedPageCleanup<P>) {
        let (owner, cleanup) = StagedPageOwner::new(
            self.pool.clone(),
            self.guard.clone(),
            poisoner.clone(),
            &plan.runs().budget,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let packing = Arc::new(Packing {
            producer: owner.producer(),
            runs: plan.runs().clone(),
            ts: self.ts,
        });
        let max_workers = plan.workers();
        let leaves = HotMergeConsumption::new(
            plan,
            workers.clone(),
            PackedLeafConsumer {
                packing: packing.clone(),
                stop: stop.clone(),
                unique: self.unique,
            },
        );
        let build = PackedBuildState {
            owner: Some(owner),
            thread_pool: workers,
            poisoner,
            packing: Some(packing),
            leaves: Some(leaves),
            children: None,
            parent_level: None,
            completion: None,
            outcome: None,
            stop,
            max_workers,
            finished: false,
            #[cfg(feature = "profiling")]
            measurements: HotPackedMeasurements::default(),
            #[cfg(feature = "profiling")]
            merge: HotMergeMeasurements::default(),
        };
        (build, cleanup)
    }

    /// Validate the empty destination and retain its exclusive root latch.
    async fn check_empty(&self) -> RuntimeOrFatalResult<PageExclusiveGuard<BTreeNode>> {
        self.index
            .borrow()
            .tree()
            .check_empty_private_root(&self.guard)
            .await
            .map_err(Into::into)
    }

    /// Install complete assembly and transfer ownership at one synchronous edge.
    async fn install_root(
        &mut self,
        assembly: &Assembly,
        owner: &StagedPageOwner<P>,
    ) -> RuntimeOrFatalResult<()> {
        // The pending decision blocks cleanup. The ready tree's exclusive
        // borrow prevents abort/drop until installation returns or is cancelled.
        let tree = self.index.borrow().tree();
        let mut destination = self.check_empty().await?;
        if let Some(root) = assembly.root {
            if assembly.completion.entries() == 0 || root.lower.is_some() || root.upper.is_some() {
                return Err(execution_error(
                    "packed root does not cover completed input",
                ));
            }
            let source = self
                .pool
                .get_page::<BTreeNode>(&self.guard, root.page_id, LatchFallbackMode::Exclusive)
                .await
                .map_err(Into::into)?
                .lock_exclusive_async()
                .await
                .unwrap_or_else(|| unreachable!("exclusive temporary root latch"));
            // No await, allocation, or fallible operation from this edge.
            tree.install_private_root(&mut destination, source.page());
            self.pool.deallocate_page(source);
        }
        owner.transferred();
        destination.set_dirty();
        Ok(())
    }
}

/// Page identity and borrowed fence coordinates; ownership lives only in the page tracker.
#[derive(Clone, Copy, Debug)]
struct ChildDescriptor {
    page_id: PageID,
    #[cfg(feature = "profiling")]
    measurement: PageMeasurement,
    height: u16,
    lower: Option<HotEntryRef>,
    upper: Option<HotEntryRef>,
}

/// Completed construction or settled duplicate evidence for caller-owned classification.
pub(crate) enum HotPackedOutcome<T> {
    /// Construction completed with all required hot-key validation satisfied.
    Complete(T),
    /// Earliest duplicate found after consuming all required input.
    Duplicate(HotDuplicate),
}

struct Assembly {
    root: Option<ChildDescriptor>,
    completion: HotMergeCompletion,
    #[cfg(feature = "profiling")]
    measurements: HotPackedMeasurements,
    #[cfg(feature = "profiling")]
    merge: HotMergeMeasurements,
}

type AssemblyResult = RuntimeOrFatalResult<HotPackedOutcome<Assembly>>;

/// Exclusively borrowed destination for direct packed-stage callers.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "production uses retained pipeline state")
)]
pub(crate) struct HotPackedBuild<'a, P: BufferPool + 'static, I = MemIndex<P>> {
    staging: Option<&'a mut StagingMemIndex<P, I>>,
    state: PackedBuildState<P>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "production uses retained pipeline state")
)]
impl<'a, P: BufferPool + 'static, I: Borrow<MemIndex<P>>> HotPackedBuild<'a, P, I> {
    /// Construct privately while retaining the destination borrow across cancellation.
    pub(crate) async fn execute(
        &mut self,
    ) -> RuntimeOrFatalResult<HotPackedOutcome<ReadyHotTree<'a, P, I>>> {
        let staging = self
            .staging
            .as_ref()
            .unwrap_or_else(|| unreachable!("packed build owns its staging index"));
        let outcome = self.state.execute(staging).await?;
        let staging = self
            .staging
            .take()
            .unwrap_or_else(|| unreachable!("packed build owns its staging index"));
        Ok(self.state.ready(staging, outcome))
    }

    /// Stop and drain construction before the caller runs detached-page cleanup.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.state.settle().await
    }
}

/// Packed construction ledgers independent of the destination borrow.
pub(super) struct PackedBuildState<P: BufferPool + 'static> {
    owner: Option<StagedPageOwner<P>>,
    thread_pool: QuiescentGuard<ThreadPool>,
    poisoner: QuiescentGuard<EnginePoisoner>,
    packing: Option<Arc<Packing<P>>>,
    // Present until leaf results are collected or accepted leaf jobs settle.
    leaves: Option<HotMergeConsumption<PackedLeafConsumer<P>>>,
    // Keep the completed child level until all of its parents are collected.
    children: Option<Arc<BudgetedVec<ChildDescriptor>>>,
    parent_level: Option<ParentLevel>,
    completion: Option<HotMergeCompletion>,
    outcome: Option<AssemblyResult>,
    stop: Arc<AtomicBool>,
    max_workers: usize,
    finished: bool,
    #[cfg(feature = "profiling")]
    measurements: HotPackedMeasurements,
    #[cfg(feature = "profiling")]
    merge: HotMergeMeasurements,
}

impl<P: BufferPool + 'static> PackedBuildState<P> {
    /// Construct against a borrowed destination while the pipeline retains every job ledger.
    pub(super) async fn build<'a, I: Borrow<MemIndex<P>>>(
        &mut self,
        staging: &'a mut StagingMemIndex<P, I>,
    ) -> RuntimeOrFatalResult<HotPackedOutcome<ReadyHotTree<'a, P, I>>> {
        let outcome = self.execute(staging).await?;
        Ok(self.ready(staging, outcome))
    }

    async fn execute<I: Borrow<MemIndex<P>>>(
        &mut self,
        staging: &StagingMemIndex<P, I>,
    ) -> AssemblyResult {
        assert!(!self.finished, "packed build reused after settlement");
        if self.outcome.is_none() {
            self.run_build(staging).await;
        }
        // Retain the outcome across draining so cancellation cannot lose it.
        if !matches!(&self.outcome, Some(Ok(HotPackedOutcome::Complete(_)))) {
            self.stop_and_drain().await;
        }
        self.finished = true;
        self.outcome
            .take()
            .unwrap_or_else(|| unreachable!("finished build retains its outcome"))
    }

    fn ready<'a, I>(
        &mut self,
        staging: &'a mut StagingMemIndex<P, I>,
        outcome: HotPackedOutcome<Assembly>,
    ) -> HotPackedOutcome<ReadyHotTree<'a, P, I>> {
        match outcome {
            HotPackedOutcome::Complete(assembly) => HotPackedOutcome::Complete(ReadyHotTree {
                staging,
                owner: self
                    .owner
                    .take()
                    .unwrap_or_else(|| unreachable!("finished build owns its page tracker")),
                assembly,
                installed: false,
                aborted: false,
            }),
            HotPackedOutcome::Duplicate(conflict) => HotPackedOutcome::Duplicate(conflict),
        }
    }

    /// Stop submission, drain accepted jobs, and request detached-page cleanup.
    /// The caller runs cleanup separately; duplicate outcomes belong to execute.
    /// Pool jobs own their accepted execution through poison and shutdown; their
    /// retained completion slots survive cancellation of this wait. The staged
    /// cleanup object reclaims only after those jobs and our leases settle.
    pub(super) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        assert!(!self.finished, "packed build reused after settlement");
        self.stop.store(true, Ordering::Release);
        if self.outcome.is_none() {
            self.record_failure(execution_error("hot packed build cancelled"));
        }
        self.stop_and_drain().await;
        self.finished = true;
        self.outcome
            .take()
            .unwrap_or_else(|| unreachable!("settled build retains its outcome"))
            .map(|_| ())
    }

    async fn run_build<I: Borrow<MemIndex<P>>>(&mut self, staging: &StagingMemIndex<P, I>) {
        let result = AssertUnwindSafe(self.build_tree(staging))
            .catch_unwind()
            .await;
        self.outcome = Some(match result {
            Ok(result) => result,
            Err(payload) => {
                let report = Report::new(FatalError::ThreadPoolTaskPanic).attach(format!(
                    "operation=hot_packed_build, phase=assembly, panic={}",
                    panic_payload_description(payload.as_ref())
                ));
                Err(self.poisoner.poison(report).into_report().into())
            }
        });
    }

    async fn build_tree<I: Borrow<MemIndex<P>>>(
        &mut self,
        staging: &StagingMemIndex<P, I>,
    ) -> AssemblyResult {
        self.check_staging(staging).await?;
        observe_stop(&self.stop)?;
        match self.build_leaves().await? {
            HotPackedOutcome::Complete(()) => (),
            HotPackedOutcome::Duplicate(conflict) => {
                return Ok(HotPackedOutcome::Duplicate(conflict));
            }
        }
        while self
            .children
            .as_ref()
            .unwrap_or_else(|| unreachable!("completed leaves own child descriptors"))
            .len()
            > 1
        {
            observe_stop(&self.stop)?;
            if self.parent_level.is_none() {
                self.plan_parent_level().await?;
            }
            self.build_parent_level().await?;
        }
        Ok(HotPackedOutcome::Complete(self.finish_build()))
    }

    async fn check_staging<I: Borrow<MemIndex<P>>>(
        &self,
        staging: &StagingMemIndex<P, I>,
    ) -> RuntimeOrFatalResult<()> {
        if let Some(error) = self.poisoner.poison_error() {
            return Err(error.into());
        }
        staging.check_empty().await.map(|_| ())
    }

    async fn build_leaves(&mut self) -> RuntimeOrFatalResult<HotPackedOutcome<()>> {
        if self.completion.is_some() {
            return Ok(HotPackedOutcome::Complete(()));
        }
        let leaves = self
            .leaves
            .as_mut()
            .unwrap_or_else(|| unreachable!("pending leaves own merge consumption"));
        let outcome = leaves.execute().await?;
        // No await between consuming the merge result and retaining its output.
        self.leaves = None;
        // Merge has consumed all required input and selected the earliest conflict.
        let completion = match outcome.validation {
            Ok(completion) => completion,
            Err(conflict) => return Ok(HotPackedOutcome::Duplicate(conflict)),
        };
        let packing = self
            .packing
            .as_ref()
            .unwrap_or_else(|| unreachable!("leaf construction owns packing resources"));
        let mut children = BudgetedVec::new(&packing.runs.budget);
        children
            .ensure_capacity(
                outcome.outputs.iter().map(|v| v.len()).sum(),
                "child descriptors",
            )
            .change_context(RuntimeError::IndexAccess)?;
        for partition in outcome.outputs {
            for &child in partition.iter() {
                children.push_reserved(child);
            }
        }
        #[cfg(feature = "profiling")]
        {
            self.merge = outcome.measurements;
            self.measurements.max_job_nanos = outcome.measurements.max_job_nanos;
            collect_level(&mut self.measurements, &children);
        }
        self.children = Some(Arc::new(children));
        self.completion = Some(completion);
        Ok(HotPackedOutcome::Complete(()))
    }

    async fn plan_parent_level(&mut self) -> RuntimeOrFatalResult<()> {
        #[cfg(feature = "profiling")]
        let mut profile = ParentPlanningProfile::new(Instant::now());
        let packing = self
            .packing
            .as_ref()
            .unwrap_or_else(|| unreachable!("parent planning owns packing resources"));
        let children = self
            .children
            .as_ref()
            .unwrap_or_else(|| unreachable!("parent planning owns its child level"));
        // Planning allocates no pages and may restart after observer cancellation.
        let (groups, direct) = if root_fits(
            &packing.runs,
            children,
            #[cfg(feature = "profiling")]
            &mut profile,
        )
        .await
        {
            let mut groups = BudgetedVec::new(&packing.runs.budget);
            groups
                .ensure_capacity(1, "parent group plans")
                .change_context(RuntimeError::IndexAccess)?;
            groups.push_reserved(0..children.len());
            (groups, false)
        } else {
            let groups = plan_groups(
                &packing.runs,
                children,
                #[cfg(feature = "profiling")]
                &mut profile,
            )
            .await?;
            if groups.len() >= children.len() {
                return Err(execution_error("packed parent level made no progress"));
            }
            (groups, children[0].height == 0)
        };
        #[cfg(feature = "profiling")]
        profile.finish(Instant::now(), &mut self.measurements);
        let workers = if direct { self.max_workers } else { 1 };
        self.parent_level = Some(ParentLevel::new(
            children.clone(),
            groups,
            workers,
            direct,
            &packing.runs.budget,
        )?);
        Ok(())
    }

    async fn build_parent_level(&mut self) -> RuntimeOrFatalResult<()> {
        let level = self
            .parent_level
            .as_mut()
            .unwrap_or_else(|| unreachable!("parent construction owns a planned level"));
        let packing = self
            .packing
            .as_ref()
            .unwrap_or_else(|| unreachable!("parent construction owns packing resources"));
        level
            .execute(&self.thread_pool, packing, &self.stop)
            .await?;
        let level = self
            .parent_level
            .take()
            .unwrap_or_else(|| unreachable!("completed parent level remains owned"));
        #[cfg(feature = "profiling")]
        {
            let elapsed = level.started.elapsed().as_nanos() as u64;
            if level.direct {
                self.measurements.direct_parent_nanos += elapsed;
            } else {
                self.measurements.serial_upper_nanos += elapsed;
            }
            collect_level(&mut self.measurements, &level.parents);
        }
        self.children = Some(Arc::new(level.parents));
        Ok(())
    }

    fn finish_build(&mut self) -> Assembly {
        let children = self
            .children
            .take()
            .unwrap_or_else(|| unreachable!("finished build owns its root descriptors"));
        #[cfg(feature = "profiling")]
        {
            self.measurements.scratch_peak_bytes = self
                .packing
                .as_ref()
                .unwrap_or_else(|| unreachable!("finished build owns packing resources"))
                .runs
                .budget
                .peak();
        }
        // All completions were collected; release the coordinator's final lease.
        self.packing = None;
        Assembly {
            root: children.first().copied(),
            completion: self
                .completion
                .take()
                .unwrap_or_else(|| unreachable!("finished build owns merge completion")),
            #[cfg(feature = "profiling")]
            measurements: take(&mut self.measurements),
            #[cfg(feature = "profiling")]
            merge: self.merge,
        }
    }

    async fn stop_and_drain(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(leaves) = self.leaves.as_mut() {
            let result = leaves.settle().await;
            self.leaves = None;
            if let Err(error) = result {
                self.record_failure(error);
            }
        }
        if let Some(level) = self.parent_level.as_mut() {
            let result = level.settle().await;
            self.parent_level = None;
            if let Err(error) = result {
                self.record_failure(error);
            }
        }
        self.children = None;
        self.completion = None;
        self.packing = None;
        let owner = self
            .owner
            .as_ref()
            .unwrap_or_else(|| unreachable!("stopping build owns staging"));
        owner.abort();
    }

    fn record_failure(&mut self, error: RuntimeOrFatalError) {
        self.stop.store(true, Ordering::Release);
        self.outcome = Some(Err(match self.outcome.take() {
            Some(Err(old)) => old.merge_cleanup(error),
            // Execution failures take precedence over a duplicate rejection.
            _ => error,
        }));
    }
}

impl<P: BufferPool + 'static> Drop for PackedBuildState<P> {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Release coordinator leases before the staging owner requests cleanup.
        // Accepted jobs retain their own captures until supervised completion.
        self.leaves = None;
        self.parent_level = None;
        self.packing = None;
    }
}

/// Complete private tree, bound to its destination and exhaustive hot consumption.
/// Dropping or aborting requests reclamation by the caller's cleanup object.
pub(crate) struct ReadyHotTree<'a, P: 'static, I = MemIndex<P>> {
    staging: &'a mut StagingMemIndex<P, I>,
    owner: StagedPageOwner<P>,
    assembly: Assembly,
    installed: bool,
    aborted: bool,
}

impl<P: BufferPool + 'static, I: Borrow<MemIndex<P>>> ReadyHotTree<'_, P, I> {
    /// Install into the fixed root after acquiring every fallible guard/check.
    /// Cancellation before transfer retains this ready tree; after transfer the
    /// index owns descendants and the caller runs cleanup before publication.
    pub(crate) async fn install(&mut self) -> RuntimeOrFatalResult<()> {
        if self.aborted || self.installed {
            return Err(execution_error("ready packed tree was already settled"));
        }
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        self.staging
            .install_root(&self.assembly, &self.owner)
            .await?;
        self.installed = true;
        #[cfg(feature = "profiling")]
        {
            self.assembly.measurements.install_nanos = started.elapsed().as_nanos() as u64;
        }
        Ok(())
    }

    /// Return exhaustive consumed entries after construction completes.
    #[inline]
    pub(crate) fn entries(&self) -> usize {
        self.assembly.completion.entries()
    }

    /// Borrow completed component measurements without exporting builder state.
    #[cfg(feature = "profiling")]
    #[inline]
    pub(crate) fn measurements(&self) -> (&HotMergeMeasurements, &HotPackedMeasurements) {
        (&self.assembly.merge, &self.assembly.measurements)
    }

    /// Reject an otherwise ready tree without publishing any destination state.
    /// The caller must run its cleanup object to reclaim detached pages.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "late DDL validation is integrated in RFC 0032 phase 5"
        )
    )]
    pub(crate) fn abort(&mut self) -> RuntimeOrFatalResult<()> {
        if self.installed {
            return Err(execution_error("installed packed tree cannot be aborted"));
        }
        self.aborted = true;
        self.owner.abort();
        Ok(())
    }
}

type ParentCompletion = Arc<Completion<RuntimeOrFatalResult<ChildDescriptor>>>;

/// One level's ordered results and bounded completion slots survive observer cancellation.
struct ParentLevel {
    children: Arc<BudgetedVec<ChildDescriptor>>,
    groups: BudgetedVec<Range<usize>>,
    parents: BudgetedVec<ChildDescriptor>,
    jobs: BudgetedVec<Option<ParentCompletion>>,
    submitted: usize,
    collected: usize,
    failure: Option<RuntimeOrFatalError>,
    #[cfg(feature = "profiling")]
    direct: bool,
    #[cfg(feature = "profiling")]
    started: Instant,
}

impl ParentLevel {
    fn new(
        children: Arc<BudgetedVec<ChildDescriptor>>,
        groups: BudgetedVec<Range<usize>>,
        workers: usize,
        direct: bool,
        budget: &MemoryBudget,
    ) -> RuntimeOrFatalResult<Self> {
        let mut parents = BudgetedVec::new(budget);
        parents
            .ensure_capacity(
                groups.len(),
                if direct {
                    "direct parent descriptors"
                } else {
                    "upper descriptors"
                },
            )
            .change_context(RuntimeError::IndexAccess)?;
        let mut jobs = BudgetedVec::new(budget);
        let workers = workers.min(groups.len());
        jobs.ensure_capacity(workers, "parent job ledger")
            .change_context(RuntimeError::IndexAccess)?;
        for _ in 0..workers {
            jobs.push_reserved(None);
        }
        Ok(Self {
            children,
            groups,
            parents,
            jobs,
            submitted: 0,
            collected: 0,
            failure: None,
            #[cfg(feature = "profiling")]
            direct,
            #[cfg(feature = "profiling")]
            started: Instant::now(),
        })
    }

    async fn execute<P: BufferPool + 'static>(
        &mut self,
        pool: &ThreadPool,
        packing: &Arc<Packing<P>>,
        stop: &AtomicBool,
    ) -> RuntimeOrFatalResult<()> {
        while self.collected < self.groups.len() {
            if self.collected == self.submitted {
                if self.failure.is_some() {
                    break;
                }
                if let Err(error) = observe_stop(stop) {
                    self.record_failure(error);
                    break;
                }
                self.submit_jobs(pool, packing);
            }
            self.collect_next().await;
        }
        self.failure.take().map_or(Ok(()), Err)
    }

    fn submit_jobs<P: BufferPool + 'static>(
        &mut self,
        pool: &ThreadPool,
        packing: &Arc<Packing<P>>,
    ) {
        let end = self.groups.len().min(self.submitted + self.jobs.len());
        for group in self.submitted..end {
            let slot = group % self.jobs.len();
            self.jobs[slot] = Some(pool.submit_async(pack_parent(
                packing.clone(),
                self.children.clone(),
                self.groups[group].clone(),
            )));
            self.submitted += 1;
        }
    }

    async fn collect_next(&mut self) {
        let group = self.collected;
        let slot = group % self.jobs.len();
        let job = self.jobs[slot]
            .as_ref()
            .unwrap_or_else(|| unreachable!("submitted parent owns a completion slot"));
        // Pool reservation accepts the job; its supervised completion remains
        // authoritative during poison/shutdown. Keep the slot until this wait
        // is ready so cancelling the borrowed observer cannot lose its result.
        let result = job.wait_take_result().await;
        self.jobs[slot] = None;
        self.collected += 1;
        let result = result
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .and_then(|r| r)
            .attach_with(|| format!("operation=hot_packed_build, phase=parent, group={group}"));
        match result {
            Ok(parent) => self.parents.push_reserved(parent),
            Err(error) => self.record_failure(error),
        }
    }

    async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        while self.collected < self.submitted {
            self.collect_next().await;
        }
        self.failure.take().map_or(Ok(()), Err)
    }

    fn record_failure(&mut self, error: RuntimeOrFatalError) {
        self.failure = Some(match self.failure.take() {
            Some(old) => old.merge_cleanup(error),
            None => error,
        });
    }
}

struct Packing<P: 'static> {
    producer: PageProducer<P>,
    runs: Arc<SortedHotRuns>,
    ts: TrxID,
}

// A fixed allocation retains lookahead; consuming a prefix only advances head.
// Initialized slots are reused after wraparound, without moving live entries.
struct LeafWindow {
    entries: BudgetedVec<HotEntryRef>,
    head: usize,
    len: usize,
    capacity: usize,
}

impl LeafWindow {
    fn new(budget: &MemoryBudget, capacity: usize) -> RuntimeOrFatalResult<Self> {
        let mut entries = BudgetedVec::new(budget);
        entries
            .ensure_capacity(capacity, "packing window")
            .change_context(RuntimeError::IndexAccess)?;
        Ok(Self {
            entries,
            head: 0,
            len: 0,
            capacity,
        })
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    #[inline]
    fn get(&self, index: usize) -> Option<HotEntryRef> {
        (index < self.len).then(|| self.entries[(self.head + index) % self.capacity])
    }

    #[inline]
    fn push(&mut self, entry: HotEntryRef) {
        assert!(
            self.len < self.capacity,
            "hot leaf window exceeds its admitted capacity"
        );
        let index = (self.head + self.len) % self.capacity;
        if index == self.entries.len() {
            self.entries.push_reserved(entry);
        } else {
            self.entries[index] = entry;
        }
        self.len += 1;
    }

    #[inline]
    fn consume(&mut self, count: usize) {
        assert!(
            count <= self.len,
            "hot leaf window consumption exceeds retained entries"
        );
        if count != 0 {
            self.head = (self.head + count) % self.capacity;
            self.len -= count;
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.entries.clear();
        self.head = 0;
        self.len = 0;
    }
}

struct PackedLeafConsumer<P: 'static> {
    packing: Arc<Packing<P>>,
    stop: Arc<AtomicBool>,
    unique: bool,
}

impl<P: BufferPool + 'static> HotPartitionConsumer for PackedLeafConsumer<P> {
    type Output = BudgetedVec<ChildDescriptor>;

    async fn consume(
        &self,
        stream: PartitionMergeStream,
    ) -> RuntimeOrFatalResult<CompletedPartition<Self::Output>> {
        if self.unique {
            self.pack::<BTreeU64>(stream, BTreeU64::from).await
        } else {
            self.pack::<BTreeByte>(stream, |_| BTREE_BYTE_ZERO).await
        }
    }
}

impl<P: BufferPool + 'static> PackedLeafConsumer<P> {
    async fn pack<V: BTreeValue + Copy + Send + Sync>(
        &self,
        mut stream: PartitionMergeStream,
        value: fn(RowID) -> V,
    ) -> RuntimeOrFatalResult<CompletedPartition<BudgetedVec<ChildDescriptor>>> {
        // Retain three maximal pages plus lookahead so the final two pages
        // remain editable. Specialize the conservative bound for the value size.
        let window_capacity = (3 * max_node_slots::<V>() + 1).min(stream.remaining_entries());
        let mut window = LeafWindow::new(&self.packing.runs.budget, window_capacity)?;
        let mut entries = BudgetedVec::new(&self.packing.runs.budget);
        let mut leaves = BudgetedVec::new(&self.packing.runs.budget);
        let (left, upper) = stream.neighbors();
        let mut lower = None;
        let mut first = true;
        let mut inhibited = false;
        while let Some(batch) = stream.next_batch()? {
            observe_stop(&self.stop)?;
            if batch.construction_inhibited() {
                inhibited = true;
                window.clear();
            }
            if !inhibited {
                for index in 0..batch.ranks().len() {
                    let (coordinate, _) = batch
                        .entry(index)
                        .unwrap_or_else(|| unreachable!("bounded batch index"));
                    if first {
                        lower = left.map(|_| coordinate);
                        first = false;
                    }
                    window.push(coordinate);
                    if window.is_full() {
                        let count = self
                            .leaf::<V>(&window, lower, upper, value, &mut entries, &mut leaves)
                            .await?;
                        lower = window.get(count);
                        window.consume(count);
                    }
                    if index % 1024 == 1023 {
                        yield_now().await;
                    }
                }
            }
            yield_now().await;
        }
        while window.len() != 0 {
            observe_stop(&self.stop)?;
            let count = self
                .leaf::<V>(&window, lower, upper, value, &mut entries, &mut leaves)
                .await?;
            lower = window.get(count);
            window.consume(count);
            yield_now().await;
        }
        stream.finish(leaves)
    }

    async fn leaf<'a, V: BTreeValue + Copy + Send + Sync>(
        &'a self,
        window: &LeafWindow,
        lower: Option<HotEntryRef>,
        upper: Option<HotEntryRef>,
        value: fn(RowID) -> V,
        entries: &mut BudgetedVec<PackedNodeEntry<'a, V>>,
        leaves: &mut BudgetedVec<ChildDescriptor>,
    ) -> RuntimeOrFatalResult<usize> {
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let mut count = plan_candidates(
            entries,
            window.len(),
            PackedNodePlanParams {
                lower_fence: fence(&self.packing.runs, lower),
                upper_fence: upper.map(|r| fence(&self.packing.runs, Some(r))),
                min_slots: 1,
            },
            "packing entries",
            |index| {
                let coordinate = window
                    .get(index)
                    .unwrap_or_else(|| unreachable!("bounded leaf candidate index"));
                let entry = coordinate.resolve(&self.packing.runs);
                PackedNodeEntry {
                    key: entry.key.as_bytes(),
                    value: value(entry.row_id),
                }
            },
        )?;
        // A singleton final tail is legal, but repair it whenever the two final
        // candidates can share their entries under the newly proposed fences.
        if count + 1 == window.len() && count > 2 {
            let cut = count - 1;
            if fits::<V>(
                fence(&self.packing.runs, lower),
                entries[cut].key,
                &entries[..cut],
            ) && fits::<V>(
                entries[cut].key,
                fence(&self.packing.runs, upper),
                &entries[cut..],
            ) {
                count = cut;
            }
        }
        let high = window.get(count).or(upper);
        if let Some(high) = high
            && entries[count - 1].key >= fence(&self.packing.runs, Some(high))
        {
            return Err(execution_error(
                "hot leaf contains a duplicate or excludes its last key",
            ));
        }
        leaves
            .reserve_one("leaf descriptors")
            .change_context(RuntimeError::IndexAccess)?;
        #[cfg(feature = "profiling")]
        let allocation_started = Instant::now();
        let mut page = self.packing.producer.allocate(0).await?;
        #[cfg(feature = "profiling")]
        let packing_started = Instant::now();
        pack_fixed_entries(
            page.page_mut(),
            KnownFenceNodeParams {
                height: 0,
                ts: self.packing.ts,
                lower_fence: fence(&self.packing.runs, lower),
                lower_fence_value: BTreeU64::INVALID_VALUE,
                upper_fence: high.map(|r| fence(&self.packing.runs, Some(r))),
                hints_enabled: true,
            },
            &entries[..count],
        );
        leaves.push_reserved(ChildDescriptor {
            page_id: page.page_id(),
            height: 0,
            lower,
            upper: high,
            #[cfg(feature = "profiling")]
            measurement: PageMeasurement {
                planning: allocation_started.duration_since(started).as_nanos() as u64,
                allocation: packing_started
                    .duration_since(allocation_started)
                    .as_nanos() as u64,
                packing: packing_started.elapsed().as_nanos() as u64,
                occupied: page.page().effective_space(),
            },
        });
        page.set_dirty();
        Ok(count)
    }
}

// Even zero-byte key suffixes need a slot and an encoded value. Ignoring the
// header and fences gives a cheap upper bound independent of compression.
const fn max_node_slots<V: BTreeValue>() -> usize {
    BTREE_NODE_USABLE_SIZE / (size_of::<BTreeSlot>() + V::ENCODED_LEN)
}

fn plan_candidates<'a, V: BTreeValue + Copy>(
    entries: &mut BudgetedVec<PackedNodeEntry<'a, V>>,
    available: usize,
    params: PackedNodePlanParams<'a>,
    purpose: &'static str,
    mut entry: impl FnMut(usize) -> PackedNodeEntry<'a, V>,
) -> RuntimeOrFatalResult<usize> {
    // One extra candidate supplies the upper fence even for a maximal node.
    let limit = available.min(max_node_slots::<V>() + 1);
    let mut count = limit.min(INITIAL_CANDIDATES);
    entries.clear();
    loop {
        entries
            .ensure_capacity(count, purpose)
            .change_context(RuntimeError::IndexAccess)?;
        for index in entries.len()..count {
            entries.push_reserved(entry(index));
        }
        let plan = try_plan_sibling_node(params, entries);
        if count < limit {
            let reaches_end = plan.is_some_and(|plan| plan.packed + 1 >= count);
            // The existing planner can stop at overflow once the prefix is inline.
            // An outlined prefix may still shrink and free space at a later fence.
            let prefix_can_shrink =
                PackedNodeSpace::with_fences(params.lower_fence, entries[count - 1].key)
                    .is_none_or(|space| !space.prefix_is_inline());
            if reaches_end || prefix_can_shrink {
                count = (count * 2).min(limit);
                continue;
            }
        }
        return plan
            .map(|plan| plan.packed)
            .ok_or_else(|| execution_error("hot node key/fences cannot fit a page"));
    }
}

#[inline]
fn fence(runs: &SortedHotRuns, reference: Option<HotEntryRef>) -> &[u8] {
    reference.map_or(&[], |r| r.resolve(runs).key.as_bytes())
}

fn fits<V: BTreeValue + Copy>(
    lower: &[u8],
    upper: &[u8],
    entries: &[PackedNodeEntry<'_, V>],
) -> bool {
    let Some(mut space) = PackedNodeSpace::with_fences(lower, upper) else {
        return false;
    };
    for entry in entries {
        if space
            .add_entry::<V>(entry.key)
            .is_none_or(|size| size > BTREE_NODE_USABLE_SIZE)
        {
            return false;
        }
    }
    space.total_space() <= BTREE_NODE_USABLE_SIZE
}

async fn root_fits(
    runs: &SortedHotRuns,
    children: &[ChildDescriptor],
    #[cfg(feature = "profiling")] profile: &mut ParentPlanningProfile,
) -> bool {
    let Some(mut space) = PackedNodeSpace::with_fences(&[], &[]) else {
        return false;
    };
    for (index, child) in children.iter().enumerate().skip(1) {
        if space
            .add_entry::<BTreeU64>(fence(runs, child.lower))
            .is_none_or(|bytes| bytes > BTREE_NODE_USABLE_SIZE)
        {
            return false;
        }
        if index % 1024 == 0 {
            #[cfg(feature = "profiling")]
            profile.record(Instant::now());
            yield_now().await;
            #[cfg(feature = "profiling")]
            profile.resume(Instant::now());
        }
    }
    true
}

async fn plan_groups(
    runs: &SortedHotRuns,
    children: &[ChildDescriptor],
    #[cfg(feature = "profiling")] profile: &mut ParentPlanningProfile,
) -> RuntimeOrFatalResult<BudgetedVec<Range<usize>>> {
    let mut groups = BudgetedVec::new(&runs.budget);
    let mut entries = BudgetedVec::new(&runs.budget);
    let mut start = 0;
    while start < children.len() {
        let end = children.len().min(start + PARENT_WINDOW);
        let header = usize::from(children[start].lower.is_none());
        let packed = plan_candidates(
            &mut entries,
            end - start - header,
            PackedNodePlanParams {
                lower_fence: fence(runs, children[start].lower),
                upper_fence: children[end - 1].upper.map(|r| fence(runs, Some(r))),
                min_slots: 1,
            },
            "parent planning window",
            |index| {
                let child = children[start + header + index];
                PackedNodeEntry {
                    key: fence(runs, child.lower),
                    value: BTreeU64::from(child.page_id),
                }
            },
        )?;
        let mut count = packed + header;
        if start + count + 1 == children.len() && count > 2 {
            let cut = start + count - 1;
            if branch_fits(runs, &children[start..cut]) && branch_fits(runs, &children[cut..]) {
                count -= 1;
            }
        }
        groups
            .push(start..start + count, "parent group plans")
            .change_context(RuntimeError::IndexAccess)?;
        start += count;
        #[cfg(feature = "profiling")]
        profile.record(Instant::now());
        yield_now().await;
        #[cfg(feature = "profiling")]
        profile.resume(Instant::now());
    }
    Ok(groups)
}

fn branch_fits(runs: &SortedHotRuns, children: &[ChildDescriptor]) -> bool {
    let first = children[0];
    let Some(mut space) = PackedNodeSpace::with_fences(
        fence(runs, first.lower),
        fence(runs, children[children.len() - 1].upper),
    ) else {
        return false;
    };
    for child in &children[usize::from(first.lower.is_none())..] {
        if space
            .add_entry::<BTreeU64>(fence(runs, child.lower))
            .is_none_or(|s| s > BTREE_NODE_USABLE_SIZE)
        {
            return false;
        }
    }
    space.total_space() <= BTREE_NODE_USABLE_SIZE
}

async fn pack_parent<P: BufferPool + 'static>(
    packing: Arc<Packing<P>>,
    children: Arc<BudgetedVec<ChildDescriptor>>,
    range: Range<usize>,
) -> RuntimeOrFatalResult<ChildDescriptor> {
    let children = &children[range];
    let Packing { producer, runs, ts } = &*packing;
    #[cfg(feature = "profiling")]
    let started = Instant::now();
    let first = children[0];
    let height = first
        .height
        .checked_add(1)
        .ok_or_else(|| execution_error("packed tree height overflow"))?;
    let last = children[children.len() - 1];
    let header = usize::from(first.lower.is_none());
    let mut entries = BudgetedVec::new(&runs.budget);
    entries
        .ensure_capacity(children.len() - header, "parent packing entries")
        .change_context(RuntimeError::IndexAccess)?;
    for (index, child) in children.iter().enumerate() {
        if child.height != first.height || (index > 0 && children[index - 1].upper != child.lower) {
            return Err(execution_error(
                "parent children have unequal heights or nonadjacent fences",
            ));
        }
        if index >= header {
            entries.push_reserved(PackedNodeEntry {
                key: fence(runs, child.lower),
                value: BTreeU64::from(child.page_id),
            });
        }
    }
    if !branch_fits(runs, children) {
        return Err(execution_error(
            "planned branch exceeds exact page capacity",
        ));
    }
    #[cfg(feature = "profiling")]
    let allocation_started = Instant::now();
    let mut page = producer.allocate(height).await?;
    #[cfg(feature = "profiling")]
    let packing_started = Instant::now();
    pack_fixed_entries(
        page.page_mut(),
        KnownFenceNodeParams {
            height,
            ts: *ts,
            lower_fence: fence(runs, first.lower),
            upper_fence: last.upper.map(|r| fence(runs, Some(r))),
            lower_fence_value: if header == 1 {
                BTreeU64::from(first.page_id)
            } else {
                BTreeU64::INVALID_VALUE
            },
            hints_enabled: true,
        },
        &entries,
    );
    let result = ChildDescriptor {
        page_id: page.page_id(),
        height,
        lower: first.lower,
        upper: last.upper,
        #[cfg(feature = "profiling")]
        measurement: PageMeasurement {
            planning: allocation_started.duration_since(started).as_nanos() as u64,
            allocation: packing_started
                .duration_since(allocation_started)
                .as_nanos() as u64,
            packing: packing_started.elapsed().as_nanos() as u64,
            occupied: page.page().effective_space(),
        },
    };
    page.set_dirty();
    #[cfg(test)]
    {
        use super::page_cleanup::test_packed;
        test_packed(producer, height).await?;
    }
    Ok(result)
}

#[cfg(feature = "profiling")]
fn collect_level(measurements: &mut HotPackedMeasurements, children: &[ChildDescriptor]) {
    let Some(first) = children.first() else {
        return;
    };
    let mut level = HotPackedLevel {
        height: first.height,
        pages: children.len(),
        ..Default::default()
    };
    for child in children {
        let m = child.measurement;
        level.occupied_bytes += m.occupied;
        level.allocation_nanos += m.allocation;
        level.packing_nanos += m.packing;
        if first.height == 0 {
            measurements.leaf_planning_nanos += m.planning;
        } else {
            measurements.max_job_nanos = measurements
                .max_job_nanos
                .max(m.planning + m.allocation + m.packing);
        }
        measurements.max_sync_nanos = measurements.max_sync_nanos.max(m.planning).max(m.packing);
    }
    measurements.levels.push(level);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::minimum_fixed_pool_bytes;
    use crate::buffer::{FixedBufferPool, PoolRole};
    use crate::component::{ComponentRegistry, RegistryBuilder};
    use crate::conf::ThreadPoolConfig;
    use crate::id::RowID;
    use crate::index::BTreeKey;
    use crate::index::btree::BTree;
    use crate::index::build::merge::{test_prepare_packed, test_runs};
    use crate::index::build::{DuplicateCheck, MemoryBudget, budget};
    use crate::quiescent::QuiescentBox;
    use crate::runtime::thread_pool::ThreadPoolWorkers;
    use crate::value::ValKind;
    use futures::future::{Either, select};
    use std::collections::BTreeMap;

    struct PoolScope(ComponentRegistry);

    impl Drop for PoolScope {
        fn drop(&mut self) {
            assert!(!self.0.shutdown_all().is_degraded());
        }
    }

    /// Verify that the original bootstrap root now holds the expected live entries.
    pub(crate) async fn assert_recovery_root(
        pool: &EvictableBufferPool,
        guard: &PoolGuard,
        root: PageID,
        entries: usize,
    ) {
        let page = pool
            .get_page::<BTreeNode>(guard, root, LatchFallbackMode::Shared)
            .await
            .unwrap()
            .lock_shared_async()
            .await
            .unwrap();
        assert_eq!(page.page().ts(), crate::trx::MIN_SNAPSHOT_TS);
        assert_eq!(page.page().count(), entries);
    }

    /// Pause a recovery producer after recording an allocated leaf or parent page.
    pub(crate) fn gate_recovery_allocation<P: BufferPool>(
        cleanup: &StagedPageCleanup<P>,
        height: u16,
    ) -> (flume::Receiver<()>, flume::Sender<()>) {
        use super::super::page_cleanup::{TestFault, TestPoint, test_gate};
        test_gate(cleanup, TestPoint::Allocated(height), 1, TestFault::None)
    }

    /// Inject a recovery cleanup invariant panic after one successful reclamation.
    pub(crate) fn panic_recovery_cleanup<P: BufferPool>(
        cleanup: &StagedPageCleanup<P>,
    ) -> flume::Receiver<()> {
        use super::super::page_cleanup::{TestFault, TestPoint, test_gate};
        let (entered, release) = test_gate(cleanup, TestPoint::Reclaim, 2, TestFault::Panic);
        release.send(()).unwrap();
        entered
    }

    async fn workers(
        count: usize,
    ) -> (
        PoolScope,
        QuiescentGuard<ThreadPool>,
        QuiescentGuard<EnginePoisoner>,
    ) {
        let mut builder = RegistryBuilder::new();
        builder.build::<EnginePoisoner>(()).await.unwrap();
        builder
            .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(count))
            .await
            .unwrap();
        builder.build::<ThreadPoolWorkers>(()).await.unwrap();
        let registry = builder.finish();
        let pool = registry.dependency::<ThreadPool>();
        let poisoner = registry.dependency::<EnginePoisoner>();
        (PoolScope(registry), pool, poisoner)
    }

    fn pages(size: usize) -> QuiescentBox<FixedBufferPool> {
        QuiescentBox::new(FixedBufferPool::with_capacity(PoolRole::Index, size).unwrap())
    }

    fn key(index: usize, width: usize, prefix: usize) -> BTreeKey {
        let mut bytes = vec![b'p'; prefix + width];
        bytes[prefix..prefix + 8].copy_from_slice(&(index as u64).to_be_bytes());
        for (offset, byte) in bytes[prefix + 8..].iter_mut().enumerate() {
            *byte = (index.wrapping_mul(31).wrapping_add(offset)) as u8;
        }
        BTreeKey::from(bytes.as_slice())
    }

    fn input(
        count: usize,
        width: usize,
        prefix: usize,
        policy: DuplicateCheck,
    ) -> Arc<SortedHotRuns> {
        let groups = if count == 0 {
            vec![]
        } else {
            (0..4)
                .map(|group| {
                    (group..count)
                        .step_by(4)
                        .map(|i| key(i, width, prefix))
                        .collect()
                })
                .collect()
        };
        test_runs(groups, policy, MemoryBudget::new(256 * 1024 * 1024))
    }

    fn duplicate_input() -> Arc<SortedHotRuns> {
        test_runs(
            vec![
                (0..3000).map(|i| key(i, 256, 0)).collect(),
                vec![key(2500, 256, 0), key(2700, 256, 0)],
            ],
            DuplicateCheck::Collect,
            MemoryBudget::new(16 * 1024 * 1024),
        )
    }

    fn expect_complete<T>(outcome: HotPackedOutcome<T>) -> T {
        match outcome {
            HotPackedOutcome::Complete(value) => value,
            HotPackedOutcome::Duplicate(conflict) => panic!("unexpected duplicate: {conflict:?}"),
        }
    }

    fn assert_duplicate<T>(outcome: HotPackedOutcome<T>) {
        let HotPackedOutcome::Duplicate(conflict) = outcome else {
            panic!("duplicate authorized construction");
        };
        assert_eq!(
            conflict,
            HotDuplicate {
                right_rank: 2501,
                rows: [RowID::new(999_997_500), RowID::new(999_900_000)],
            }
        );
    }

    async fn staging(
        pool: &QuiescentBox<FixedBufferPool>,
        unique: bool,
    ) -> StagingMemIndex<FixedBufferPool> {
        StagingMemIndex::new(
            pool.guard(),
            pool.create_base_guard(),
            vec![ValType::new(ValKind::VarByte, false)],
            unique,
            TrxID::new(7),
        )
        .await
        .unwrap()
    }

    async fn verify(
        tree: &BTree,
        guard: &PoolGuard,
        oracle: &BTreeMap<BTreeKey, RowID>,
        unique: bool,
    ) -> Vec<PageID> {
        let mut actual = BTreeMap::new();
        let mut ids = Vec::new();
        for height in 0..=tree.height() {
            let mut cursor = tree.cursor(guard, height);
            cursor.seek(&[]).await.unwrap();
            let mut previous: Option<Vec<u8>> = None;
            while let Some(page) = cursor.next().await.unwrap() {
                let node = page.page();
                assert_eq!(node.height(), height);
                assert!(node.ts() >= TrxID::new(7));
                assert_eq!(
                    node.lower_fence_key().as_bytes(),
                    previous.as_deref().unwrap_or(&[])
                );
                if node.count() == 0 && height == 0 {
                    assert!(node.lower_fence_value().is_deleted());
                } else if height == 0 && unique {
                    assert!(node.validate_persisted_layout::<BTreeU64>());
                } else if height == 0 {
                    assert!(node.validate_persisted_layout::<BTreeByte>());
                } else {
                    assert!(node.validate_persisted_layout::<BTreeU64>());
                    assert_eq!(node.lower_fence_value().is_deleted(), previous.is_some());
                }
                if height == 0 {
                    for i in 0..node.count() {
                        let key = node.key(i);
                        assert!(node.within_boundary(&key));
                        let row = if unique {
                            node.value::<BTreeU64>(i).to_row_id()
                        } else {
                            assert_eq!(node.value::<BTreeByte>(i), BTREE_BYTE_ZERO);
                            oracle[&key]
                        };
                        assert!(actual.insert(key, row).is_none());
                    }
                }
                previous = Some(node.upper_fence_key().as_bytes().to_vec());
                ids.push(page.page_id());
            }
            assert!(
                previous.as_ref().is_some_and(Vec::is_empty),
                "height={height}, actual_entries={}, expected_entries={}, last_fence_length={}",
                actual.len(),
                oracle.len(),
                previous.as_ref().map_or(0, Vec::len)
            );
        }
        assert_eq!(&actual, oracle);
        ids
    }

    fn oracle(runs: &SortedHotRuns) -> BTreeMap<BTreeKey, RowID> {
        runs.runs()
            .iter()
            .flat_map(|run| {
                run.entries()
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.row_id))
            })
            .collect()
    }

    fn assert_candidate_plans<V: BTreeValue + Copy>(keys: &[BTreeKey], value: V) {
        let budget = MemoryBudget::new(1024 * 1024);
        let mut entries = BudgetedVec::new(&budget);
        for (start, count) in [
            (1, 1),
            (1, 63),
            (1, 64),
            (1, 65),
            (1, 127),
            (1, 128),
            (1, 129),
            (1, keys.len() - 2),
            (64, keys.len() - 65),
            (keys.len() - 2, 1),
        ] {
            for open in [false, true] {
                let remaining = &keys[start..start + count];
                let params = PackedNodePlanParams {
                    lower_fence: if open { &[] } else { remaining[0].as_bytes() },
                    upper_fence: (!open).then(|| keys[start + count].as_bytes()),
                    min_slots: 1,
                };
                let full: Vec<_> = remaining
                    .iter()
                    .map(|key| PackedNodeEntry {
                        key: key.as_bytes(),
                        value,
                    })
                    .collect();
                let expected = try_plan_sibling_node(params, &full).map(|plan| plan.packed);
                let actual = plan_candidates(
                    &mut entries,
                    full.len(),
                    params,
                    "packing entries",
                    |index| full[index],
                );
                assert_eq!(
                    actual.ok(),
                    expected,
                    "start={start}, count={count}, open={open}"
                );
                assert!(entries.capacity() <= max_node_slots::<V>() + 1);
            }
        }
    }

    /// Purpose: Protect retained leaf order and allocation reuse across circular-window wraparound and reset.
    /// Expected: Consumed and refilled windows match a queue oracle without moving or reallocating backing storage.
    #[test]
    fn packed_leaf_window_wraparound() {
        use std::collections::VecDeque;
        let runs = input(100, 8, 0, DuplicateCheck::Skip);
        for capacity in [0, 1, 7, 64] {
            let budget = MemoryBudget::new(4096);
            let mut window = LeafWindow::new(&budget, capacity).unwrap();
            let allocation = window.entries.as_ptr();
            let mut expected = VecDeque::new();
            for round in 0..100 {
                while !window.is_full() {
                    let entry = HotEntryRef::new(&runs, round % 4, (round + window.len()) % 25);
                    window.push(entry);
                    expected.push_back(entry);
                }
                for (index, &entry) in expected.iter().enumerate() {
                    assert_eq!(window.get(index), Some(entry));
                }
                assert_eq!(window.get(expected.len()), None);
                assert_eq!(window.entries.as_ptr(), allocation);
                assert_eq!(budget.used(), capacity * size_of::<HotEntryRef>());
                let count = (round % 5 + 1).min(expected.len());
                let retained = window.entries.to_vec();
                window.consume(count);
                assert_eq!(&*window.entries, retained.as_slice());
                expected.drain(..count);
                assert_eq!(window.len(), expected.len());
                if round % 11 == 10 {
                    window.clear();
                    expected.clear();
                }
            }
            drop(window);
            assert_eq!(budget.used(), 0);
        }
    }

    /// Purpose: Protect bounded planning against changing widths, candidate boundaries and open or compressed fences.
    /// Expected: Both value specializations select the same prefix as full-window planning within a single-page scratch bound.
    #[test]
    fn packed_candidate_plans_match_full_window() {
        use rand::{RngExt, SeedableRng};
        use rand_chacha::ChaCha8Rng;
        for (count, width, prefix) in [(20_000, 8, 0), (500, 8192, 0), (15_000, 8, 512)] {
            let keys: Vec<_> = (0..count).map(|i| key(i, width, prefix)).collect();
            assert_candidate_plans(&keys, BTreeU64::from(0));
            assert_candidate_plans(&keys, BTREE_BYTE_ZERO);
        }
        let mut rng = ChaCha8Rng::seed_from_u64(317_001);
        let keys: Vec<_> = (0..3000)
            .map(|i| key(i, if rng.random_ratio(1, 8) { 2048 } else { 8 }, 0))
            .collect();
        assert_candidate_plans(&keys, BTreeU64::from(0));
        assert_candidate_plans(&keys, BTREE_BYTE_ZERO);
    }

    /// Purpose: Protect planning when a later shorter prefix frees enough space after an earlier overflow.
    /// Expected: Geometric probing passes the apparent interior split and keeps the later fitting prefix.
    #[test]
    fn packed_candidate_prefix_shrink() {
        use crate::index::btree::BTreeHeader;
        let mut keys: Vec<_> = (0..63u32)
            .map(|i| {
                let mut bytes = vec![b'p'; 16];
                bytes.push(b'a');
                bytes.extend_from_slice(&i.to_be_bytes()[1..]);
                bytes
            })
            .collect();
        // At the long fence, the first 63 byte-valued entries overflow by three
        // bytes. The next fence makes the 17-byte prefix inline: 64 entries fit.
        let wide_len = BTREE_NODE_USABLE_SIZE + 3 - size_of::<BTreeHeader>() - 63 * 9;
        let mut wide = vec![b'p'; 16];
        wide.extend_from_slice(b"a\xff");
        wide.resize(wide_len, 0);
        keys.push(wide);
        for suffix in *b"bcd" {
            let mut bytes = vec![b'p'; 16];
            bytes.push(suffix);
            keys.push(bytes);
        }
        let mut upper = vec![b'p'; 16];
        upper.extend_from_slice(&[b'z'; 20]);
        let full: Vec<_> = keys
            .iter()
            .map(|key| PackedNodeEntry {
                key,
                value: BTREE_BYTE_ZERO,
            })
            .collect();
        let params = PackedNodePlanParams {
            lower_fence: &keys[0],
            upper_fence: Some(&upper),
            min_slots: 1,
        };
        assert_eq!(
            try_plan_sibling_node(params, &full[..64]).unwrap().packed,
            62
        );
        assert_eq!(try_plan_sibling_node(params, &full).unwrap().packed, 64);
        assert!(!fits::<BTreeByte>(&keys[0], &keys[63], &full[..63]));
        assert!(fits::<BTreeByte>(&keys[0], &keys[64], &full[..64]));
        let mut entries = BudgetedVec::new(&MemoryBudget::new(4096));
        assert_eq!(
            plan_candidates(&mut entries, full.len(), params, "packing entries", |i| {
                full[i]
            })
            .unwrap(),
            64
        );
    }

    /// Purpose: Protect bounded work and retained scratch allocation across repeated wide-key node plans.
    /// Expected: Each plan resolves only its small candidate prefix, and reused capacity succeeds even when new admission is rejected.
    #[test]
    fn packed_candidate_buffer_reuse() {
        let keys: Vec<_> = (0..500).map(|i| key(i, 2048, 0)).collect();
        let budget = MemoryBudget::new(64 * 1024);
        let mut entries = BudgetedVec::new(&budget);
        let params = PackedNodePlanParams {
            lower_fence: &[],
            upper_fence: None,
            min_slots: 1,
        };
        let mut resolved = 0;
        let mut entry = |i: usize| {
            resolved += 1;
            PackedNodeEntry {
                key: keys[i].as_bytes(),
                value: BTreeU64::from(0),
            }
        };
        let first = plan_candidates(
            &mut entries,
            keys.len(),
            params,
            "packing entries",
            &mut entry,
        )
        .unwrap();
        let allocation = entries.as_ptr();
        budget::fail_at(&budget, "packing entries");
        let second = plan_candidates(
            &mut entries,
            keys.len(),
            params,
            "packing entries",
            &mut entry,
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(resolved, 128);
        assert_eq!(entries.as_ptr(), allocation);
        assert_eq!(
            budget.used(),
            64 * size_of::<PackedNodeEntry<'_, BTreeU64>>()
        );
        // A later narrow-key page needs more candidates. Failed growth must
        // preserve the original allocation and its admission for cleanup.
        let narrow: Vec<_> = (0..500).map(|i| key(i, 8, 0)).collect();
        assert!(
            plan_candidates(&mut entries, narrow.len(), params, "packing entries", |i| {
                PackedNodeEntry {
                    key: narrow[i].as_bytes(),
                    value: BTreeU64::from(0),
                }
            },)
            .is_err()
        );
        assert_eq!(entries.as_ptr(), allocation);
        assert_eq!(
            budget.used(),
            64 * size_of::<PackedNodeEntry<'_, BTreeU64>>()
        );
        drop(entries);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Protect streaming packed contents, fences, value specialization and fixed-root ownership across input shapes.
    /// Expected: Checked and trusted builds match the sorted oracle and destroying installed trees reclaims every page.
    #[test]
    fn packed_contents_and_fixed_root() {
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(16 * 1024 * 1024);
            for (count, width, prefix, unique, partitions, batch) in [
                (0, 8, 0, true, 0, 11),
                (1, 8, 0, true, 1, 11),
                (32, 8, 0, false, 2, 7),
                (128, 8, 0, true, 4, 19),
                (600, 256, 0, true, 2, 31),
                (64, 64, 800, false, 2, 23),
                // One entry beyond the packing window forces wraparound within a batch.
                (3 * max_node_slots::<BTreeU64>() + 2, 8, 0, true, 1, 32_768),
            ] {
                for policy in [DuplicateCheck::Collect, DuplicateCheck::Skip] {
                    let runs = input(count, width, prefix, policy);
                    let expected = oracle(&runs);
                    let plan =
                        test_prepare_packed(runs, workers.clone(), 2, partitions, batch).await;
                    let mut staging = staging(&pool, unique).await;
                    let root_id = staging.check_empty().await.unwrap().page_id();
                    let (mut build, mut cleanup) =
                        staging.start_build(plan, workers.clone(), poisoner.clone());
                    let mut ready = expect_complete(build.execute().await.unwrap());
                    assert_eq!(ready.assembly.completion.entries(), count);
                    ready.install().await.unwrap();
                    cleanup.run().await.unwrap();
                    #[cfg(feature = "profiling")]
                    {
                        let sample = &ready.assembly.measurements;
                        assert_eq!(
                            sample.levels.iter().map(|l| l.pages).sum::<usize>()
                                + usize::from(count == 0),
                            pool.allocated()
                        );
                        for level in &sample.levels {
                            assert!(level.occupied_bytes <= level.pages * BTREE_NODE_USABLE_SIZE);
                        }
                        assert_eq!(
                            sample.levels.last().map(|l| l.height),
                            ready.assembly.root.map(|r| r.height)
                        );
                        assert!(
                            sample.scratch_peak_bytes
                                >= ready.assembly.merge.scratch_peak_bytes as usize
                        );
                    }
                    assert!(
                        ready.install().await.is_err(),
                        "installation evidence is move-once"
                    );
                    drop(ready);
                    drop(build);
                    let index = staging.finish();
                    if count > 1 {
                        assert!(
                            index.tree().height() >= 1,
                            "case={count}/{width}/{partitions}/{batch}"
                        );
                    }
                    let guard = pool.create_base_guard();
                    let ids = verify(index.tree(), &guard, &expected, unique).await;
                    assert!(ids.contains(&root_id));
                    assert_eq!(pool.allocated(), ids.len());
                    index.destroy(&guard).await.unwrap();
                    assert_eq!(pool.allocated(), 0);
                }
            }
        });
    }

    /// Purpose: Protect global parent planning and arbitrary-depth destruction with wide separators.
    /// Expected: Multiple serial upper levels preserve every entry and reclaim non-leftmost branches completely.
    #[test]
    fn packed_deep_tree_and_reclamation() {
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(4).await;
            let pool = pages(128 * 1024 * 1024);
            let runs = input(1800, 8192, 0, DuplicateCheck::Collect);
            let expected = oracle(&runs);
            let plan = test_prepare_packed(runs, workers.clone(), 4, 8, 137).await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            let mut ready = expect_complete(build.execute().await.unwrap());
            assert!(ready.assembly.root.unwrap().height >= 3);
            ready.install().await.unwrap();
            cleanup.run().await.unwrap();
            drop(ready);
            drop(build);
            let index = staging.finish();
            let guard = pool.create_base_guard();
            let ids = verify(index.tree(), &guard, &expected, true).await;
            assert_eq!(pool.allocated(), ids.len());
            index.destroy(&guard).await.unwrap();
            assert_eq!(pool.allocated(), 0);
            for id in ids {
                assert!(!pool.is_allocated(id), "leaked page {id:?}");
            }
        });
    }

    /// Purpose: Protect ordinary failure cleanup at each charged construction boundary.
    /// Expected: Scratch rejection leaves the destination empty and reclaims all detached allocations.
    #[test]
    fn packed_scratch_failures_reclaim() {
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(64 * 1024 * 1024);
            for purpose in [
                "packing window",
                "packing entries",
                "staged page tracking",
                "leaf descriptors",
                "child descriptors",
                "parent planning window",
                "parent group plans",
                "direct parent descriptors",
                "parent job ledger",
                "parent packing entries",
                "upper descriptors",
            ] {
                let runs = input(350, 8192, 0, DuplicateCheck::Collect);
                let budget = runs.budget.clone();
                let plan = test_prepare_packed(runs, workers.clone(), 2, 4, 17).await;
                budget::fail_at(&budget, purpose);
                let mut staging = staging(&pool, true).await;
                let (mut build, mut cleanup) =
                    staging.start_build(plan, workers.clone(), poisoner.clone());
                assert!(
                    build.execute().await.is_err(),
                    "missing rejection: {purpose}"
                );
                cleanup.run().await.unwrap();
                drop(build);
                assert_eq!(pool.allocated(), 1, "{purpose}");
                drop(staging.check_empty().await.unwrap());
                staging.destroy().await.unwrap();
                assert_eq!(pool.allocated(), 0);
            }
        });
    }

    /// Purpose: Protect explicit caller rejection and abandonment of fully assembled private trees.
    /// Expected: Both paths defer reclamation to the caller, which reclaims every detached page without changing the empty root.
    #[test]
    fn packed_ready_abort_and_drop() {
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(1).await;
            let pool = pages(32 * 1024 * 1024);
            for abandon in [false, true] {
                let runs = input(500, 256, 0, DuplicateCheck::Collect);
                let plan = test_prepare_packed(runs, workers.clone(), 1, 4, 33).await;
                let mut staging = staging(&pool, true).await;
                let (mut build, mut cleanup) =
                    staging.start_build(plan, workers.clone(), poisoner.clone());
                let mut ready = expect_complete(build.execute().await.unwrap());
                let allocated = pool.allocated();
                let mut reclaim = Box::pin(cleanup.run());
                assert!(futures::poll!(reclaim.as_mut()).is_pending());
                drop(reclaim);
                assert_eq!(pool.allocated(), allocated);
                if !abandon {
                    ready.abort().unwrap();
                    assert!(ready.install().await.is_err());
                }
                drop(ready);
                drop(build);
                assert!(pool.allocated() > 1, "dropping owners must not run cleanup");
                cleanup.run().await.unwrap();
                cleanup.run().await.unwrap();
                assert_eq!(pool.allocated(), 1);
                drop(staging.check_empty().await.unwrap());
                staging.destroy().await.unwrap();
            }
        });
    }

    /// Purpose: Protect caller-neutral duplicate evidence at the packed-build boundary.
    /// Expected: The earliest duplicate returns its rank and RowIDs, and caller-driven cleanup reclaims all detached pages.
    #[test]
    fn packed_duplicates_are_outcomes() {
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(32 * 1024 * 1024);
            let plan = test_prepare_packed(duplicate_input(), workers.clone(), 2, 4, 23).await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            assert_duplicate(build.execute().await.unwrap());
            assert!(pool.allocated() > 1);
            cleanup.run().await.unwrap();
            assert_eq!(pool.allocated(), 1);
            drop(build);
            assert!(poisoner.poison_error().is_none());
            staging.destroy().await.unwrap();
        });
    }

    /// Purpose: Protect caller-owned cleanup resumption after partial reclamation of a duplicate build.
    /// Expected: Cancelling the cleanup future preserves remaining page IDs, and resumption reclaims them exactly once.
    #[test]
    fn packed_duplicate_cleanup_cancellation() {
        use super::super::page_cleanup::{TestFault, TestPoint, test_gate, test_remaining};
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(32 * 1024 * 1024);
            let plan = test_prepare_packed(duplicate_input(), workers.clone(), 2, 4, 23).await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            assert_duplicate(build.execute().await.unwrap());
            drop(build);
            let allocated = test_remaining(&cleanup);
            assert!(allocated.len() > 1);
            let (entered, release) = test_gate(&cleanup, TestPoint::Reclaim, 2, TestFault::None);
            let mut reclaim = Box::pin(cleanup.run());
            let waiting = entered.recv_async();
            futures::pin_mut!(waiting);
            assert!(matches!(
                select(reclaim.as_mut(), waiting).await,
                Either::Right((Ok(()), _))
            ));
            drop(reclaim);
            drop(release);
            assert!(!pool.is_allocated(allocated[0]));
            assert_eq!(test_remaining(&cleanup), allocated[1..]);
            assert_eq!(pool.allocated(), allocated.len());
            cleanup.run().await.unwrap();
            cleanup.run().await.unwrap();
            assert_eq!(pool.allocated(), 1);
            assert!(allocated.iter().all(|&id| !pool.is_allocated(id)));
            assert!(poisoner.poison_error().is_none());
            let root = staging.check_empty().await.unwrap();
            assert!(root.page().lower_fence_key().is_empty());
            assert!(root.page().upper_fence_key().is_empty());
            assert!(root.page().lower_fence_value().is_deleted());
            drop(root);
            staging.destroy().await.unwrap();
            assert_eq!(pool.allocated(), 0);
        });
    }

    /// Purpose: Protect cleanup failure handling after the caller receives duplicate evidence.
    /// Expected: A reopen error returns Fatal, poisons the engine, and preserves unreclaimed pages without retrying cleanup.
    #[test]
    fn packed_duplicate_cleanup_failure_is_fatal() {
        use super::super::page_cleanup::{
            TestFault, TestPoint, test_gate, test_recover, test_remaining,
        };
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(32 * 1024 * 1024);
            let plan = test_prepare_packed(duplicate_input(), workers.clone(), 2, 4, 23).await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            let (entered, release) = test_gate(&cleanup, TestPoint::Reclaim, 1, TestFault::Runtime);
            assert_duplicate(build.execute().await.unwrap());
            drop(build);
            release.send(()).unwrap();
            let failure = cleanup.run().await.unwrap_err();
            assert_eq!(failure.current_context(), &FatalError::PurgeDeallocate);
            entered.recv_async().await.unwrap();
            let retained = test_remaining(&cleanup);
            assert!(!retained.is_empty());
            assert_eq!(pool.allocated(), retained.len() + 1);
            assert!(retained.iter().all(|&id| pool.is_allocated(id)));
            assert!(poisoner.poison_error().is_some());
            assert!(cleanup.run().await.is_err());
            assert_eq!(test_remaining(&cleanup), retained);
            test_recover(&cleanup).await;
            assert_eq!(pool.allocated(), 1);
            staging.destroy().await.unwrap();
            assert_eq!(pool.allocated(), 0);
        });
    }

    /// Purpose: Protect cancellation after partial leaf, direct-parent, serial-upper and root construction.
    /// Expected: Resumption preserves contents without orphan pages, while settlement and abandonment retain in-flight producers through reclamation.
    #[test]
    fn packed_cancel_execute_settle_and_abandon() {
        use super::super::page_cleanup::{TestFault, TestPoint, test_gate, test_remaining};

        #[derive(Clone, Copy, Debug)]
        enum Stage {
            Leaf,
            DirectParent,
            UpperParent,
            Root,
        }

        #[derive(Clone, Copy, Debug)]
        enum Action {
            Resume,
            Settle,
            Abandon,
        }

        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(16 * 1024 * 1024);
            for stage in [
                Stage::Leaf,
                Stage::DirectParent,
                Stage::UpperParent,
                Stage::Root,
            ] {
                let (count, width, height, ordinal, collected) = match stage {
                    Stage::Leaf => (16, 256, 0, 1, 0),
                    Stage::DirectParent => (128, 8192, 1, 3, 2),
                    Stage::UpperParent => (384, 8192, 2, 2, 1),
                    Stage::Root => (16, 8, 1, 1, 0),
                };
                for action in [Action::Resume, Action::Settle, Action::Abandon] {
                    let runs = input(count, width, 0, DuplicateCheck::Collect);
                    let expected = oracle(&runs);
                    let plan = test_prepare_packed(runs, workers.clone(), 2, 4, 27).await;
                    let mut staging = staging(&pool, true).await;
                    let (mut build, mut cleanup) =
                        staging.start_build(plan, workers.clone(), poisoner.clone());
                    let (entered, release) = test_gate(
                        &cleanup,
                        TestPoint::Allocated(height),
                        ordinal,
                        TestFault::None,
                    );
                    let mut execute = Box::pin(build.execute());
                    let waiting = entered.recv_async();
                    futures::pin_mut!(waiting);
                    match select(execute.as_mut(), waiting).await {
                        Either::Right((Ok(()), _)) => (),
                        _ => {
                            panic!("allocation gate must precede completion: {stage:?}, {action:?}")
                        }
                    }
                    drop(execute);
                    let allocated = test_remaining(&cleanup);
                    assert!(!allocated.is_empty(), "{stage:?}, {action:?}");
                    if height != 0 {
                        let level = build.state.parent_level.as_ref().unwrap();
                        assert!(level.collected >= collected, "{stage:?}, {action:?}");
                        assert_eq!(level.children[0].height + 1, height);
                        assert_eq!(
                            level.jobs.len(),
                            if matches!(stage, Stage::DirectParent) {
                                2
                            } else {
                                1
                            }
                        );
                        if matches!(stage, Stage::Root) {
                            assert_eq!(level.groups.len(), 1);
                        }
                    }
                    match action {
                        Action::Resume => {
                            release.send(()).unwrap();
                            let mut ready = expect_complete(build.execute().await.unwrap());
                            ready.install().await.unwrap();
                            cleanup.run().await.unwrap();
                            drop(ready);
                            drop(build);
                            let index = staging.finish();
                            let guard = pool.create_base_guard();
                            let reachable = verify(index.tree(), &guard, &expected, true).await;
                            assert_eq!(
                                pool.allocated(),
                                reachable.len(),
                                "orphan page after {stage:?} resumption"
                            );
                            index.destroy(&guard).await.unwrap();
                        }
                        Action::Settle => {
                            let mut settle = Box::pin(build.settle());
                            assert!(futures::poll!(settle.as_mut()).is_pending());
                            drop(settle);
                            release.send(()).unwrap();
                            assert!(build.settle().await.is_err());
                            drop(build);
                            cleanup.run().await.unwrap();
                            assert_eq!(pool.allocated(), 1, "{stage:?}");
                            staging.destroy().await.unwrap();
                        }
                        Action::Abandon => {
                            drop(build);
                            let mut reclaim = Box::pin(cleanup.run());
                            assert!(futures::poll!(reclaim.as_mut()).is_pending());
                            assert!(allocated.iter().all(|&id| pool.is_allocated(id)));
                            release.send(()).unwrap();
                            reclaim.await.unwrap();
                            assert_eq!(pool.allocated(), 1, "{stage:?}");
                            staging.destroy().await.unwrap();
                        }
                    }
                    assert_eq!(pool.allocated(), 0, "{stage:?}, {action:?}");
                    for page_id in allocated {
                        assert!(
                            !pool.is_allocated(page_id),
                            "{stage:?}, {action:?}: {page_id:?}"
                        );
                    }
                }
            }
        });
    }

    /// Purpose: Protect root installation cancellation before ownership transfer.
    /// Expected: Cleanup waits through a cancelled install, the fixed root remains empty, and resumption installs exactly once.
    #[test]
    fn packed_cancel_install() {
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(1).await;
            let pool = pages(16 * 1024 * 1024);
            let plan = test_prepare_packed(
                input(1000, 256, 0, DuplicateCheck::Skip),
                workers.clone(),
                1,
                4,
                77,
            )
            .await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            let mut ready = expect_complete(build.execute().await.unwrap());
            let root = ready.staging.check_empty().await.unwrap();
            let allocated = pool.allocated();
            let mut install = Box::pin(ready.install());
            assert!(futures::poll!(install.as_mut()).is_pending());
            let mut reclaim = Box::pin(cleanup.run());
            assert!(futures::poll!(reclaim.as_mut()).is_pending());
            assert_eq!(pool.allocated(), allocated);
            drop(install);
            assert!(futures::poll!(reclaim.as_mut()).is_pending());
            assert_eq!(pool.allocated(), allocated);
            assert_eq!(root.page().count(), 0);
            drop(root);
            ready.install().await.unwrap();
            reclaim.await.unwrap();
            drop(ready);
            drop(build);
            let index = staging.finish();
            let guard = pool.create_base_guard();
            index.destroy(&guard).await.unwrap();
            assert_eq!(pool.allocated(), 0);
        });
    }

    /// Purpose: Distinguish typed reopen errors from invariant panics after partial reclamation.
    /// Expected: Reopen errors cache Fatal without retry; panics escape unchanged without poisoning, and both preserve the exact remaining page IDs.
    #[test]
    fn packed_cleanup_errors_and_panics_preserve_progress() {
        use super::super::page_cleanup::{
            TestFault, TestPoint, test_gate, test_recover, test_remaining,
        };
        smol::block_on(async {
            for fault in [TestFault::Runtime, TestFault::Panic] {
                let (_scope, workers, poisoner) = workers(1).await;
                let pool = pages(16 * 1024 * 1024);
                let plan = test_prepare_packed(
                    input(1500, 256, 0, DuplicateCheck::Skip),
                    workers.clone(),
                    1,
                    4,
                    77,
                )
                .await;
                let mut staging = staging(&pool, true).await;
                let (mut build, mut cleanup) =
                    staging.start_build(plan, workers.clone(), poisoner.clone());
                let mut ready = expect_complete(build.execute().await.unwrap());
                let all = test_remaining(&cleanup);
                let (entered, release) = test_gate(&cleanup, TestPoint::Reclaim, 2, fault);
                ready.abort().unwrap();
                release.send(()).unwrap();
                let outcome = AssertUnwindSafe(cleanup.run()).catch_unwind().await;
                match fault {
                    TestFault::Runtime => {
                        let failure = outcome.unwrap().unwrap_err();
                        assert_eq!(failure.current_context(), &FatalError::PurgeDeallocate);
                        assert!(poisoner.poison_error().is_some());
                        // A cached typed failure must not restart reclamation.
                        assert!(cleanup.run().await.is_err());
                    }
                    TestFault::Panic => {
                        let payload = outcome.unwrap_err();
                        assert_eq!(
                            payload.downcast_ref::<&str>(),
                            Some(&"injected staged panic")
                        );
                        assert!(poisoner.poison_error().is_none());
                    }
                    TestFault::None => unreachable!(),
                }
                entered.recv_async().await.unwrap();
                assert!(!pool.is_allocated(all[0]));
                let remaining = test_remaining(&cleanup);
                assert_eq!(remaining, all[1..]);
                assert_eq!(pool.allocated(), remaining.len() + 1);
                // This injected fault precedes pool access, so test teardown can
                // reclaim safely. Production must abandon cleanup after a panic.
                test_recover(&cleanup).await;
                drop(ready);
                drop(build);
                staging.destroy().await.unwrap();
                assert_eq!(pool.allocated(), 0);
            }
        });
    }

    /// Purpose: Protect leaf and parent construction when a worker panic accompanies an ordinary allocation-stage failure.
    /// Expected: Accepted jobs drain, Fatal takes precedence, and ordinary detached pages are still fully reclaimed.
    #[test]
    fn packed_later_panic_overrides_runtime_failure() {
        use super::super::page_cleanup::{TestFault, TestPoint, test_gate};
        smol::block_on(async {
            for (height, count, width) in [(0, 3000, 256), (1, 500, 8192)] {
                let (_scope, workers, poisoner) = workers(2).await;
                let pool = pages(64 * 1024 * 1024);
                let plan = test_prepare_packed(
                    input(count, width, 0, DuplicateCheck::Collect),
                    workers.clone(),
                    2,
                    4,
                    77,
                )
                .await;
                let mut staging = staging(&pool, true).await;
                let (mut build, mut cleanup) =
                    staging.start_build(plan, workers.clone(), poisoner.clone());
                let (first, release_first) = test_gate(
                    &cleanup,
                    TestPoint::Allocated(height),
                    1,
                    TestFault::Runtime,
                );
                let (second, release_second) =
                    test_gate(&cleanup, TestPoint::Allocated(height), 2, TestFault::Panic);
                let control = async {
                    first.recv_async().await.unwrap();
                    second.recv_async().await.unwrap();
                    release_first.send(()).unwrap();
                    release_second.send(()).unwrap();
                };
                let (result, ()) = futures::join!(build.execute(), control);
                assert!(matches!(result, Err(RuntimeOrFatalError::Fatal(_))));
                drop(result);
                drop(build);
                assert!(cleanup.run().await.is_err());
                assert_eq!(pool.allocated(), 1);
                staging.destroy().await.unwrap();
                assert_eq!(pool.allocated(), 0);
            }
        });
    }

    /// Purpose: Protect synchronous parent-planning measurements across yields and final partial intervals.
    /// Expected: Maxima exclude suspended time, wall time accumulates whole attempts, and larger existing page measurements are retained.
    #[cfg(feature = "profiling")]
    #[test]
    fn packed_parent_planning_intervals() {
        use std::time::Duration;
        let origin = Instant::now();
        let at = |n| origin + Duration::from_nanos(n);
        for (yields, completed, longest) in [
            (&[][..], 13, 13),
            (&[(10, 100), (130, 200)][..], 205, 30),
            (&[(10, 100), (130, 200)][..], 240, 40),
        ] {
            for previous_max in [0, 50] {
                let mut measurements = HotPackedMeasurements {
                    parent_planning_nanos: 17,
                    max_sync_nanos: previous_max,
                    ..Default::default()
                };
                let mut profile = ParentPlanningProfile::new(at(0));
                for &(before_yield, resumed) in yields {
                    profile.record(at(before_yield));
                    profile.resume(at(resumed));
                }
                profile.finish(at(completed), &mut measurements);
                assert_eq!(measurements.parent_planning_nanos, 17 + completed);
                assert_eq!(measurements.max_sync_nanos, previous_max.max(longest));
            }
        }
    }

    /// Purpose: Protect exact open-root fanout and grouping across prefix compression loss and singleton tails.
    /// Expected: Root fit matches layout byte limits and global parent groups cover adjacent children with strict progress.
    #[test]
    fn packed_root_fit_and_parent_groups() {
        smol::block_on(async {
            for (width, capacity) in [(4, 4089), (8, 2726), (64, 818), (256, 241)] {
                let keys = (0..capacity + 1)
                    .map(|index| {
                        if width == 4 {
                            BTreeKey::from((index as u32).to_be_bytes().as_slice())
                        } else {
                            key(index, width, 0)
                        }
                    })
                    .collect();
                let runs = test_runs(
                    vec![keys],
                    DuplicateCheck::Skip,
                    MemoryBudget::new(8 * 1024 * 1024),
                );
                let descriptors = |count| {
                    (0..count)
                        .map(|index| ChildDescriptor {
                            page_id: PageID::new(index as u64),
                            height: 0,
                            lower: (index != 0).then(|| HotEntryRef::new(&runs, 0, index)),
                            upper: (index + 1 < count)
                                .then(|| HotEntryRef::new(&runs, 0, index + 1)),
                            #[cfg(feature = "profiling")]
                            measurement: PageMeasurement::default(),
                        })
                        .collect::<Vec<_>>()
                };
                assert!(
                    root_fits(
                        &runs,
                        &descriptors(capacity),
                        #[cfg(feature = "profiling")]
                        &mut ParentPlanningProfile::new(Instant::now()),
                    )
                    .await,
                    "width={width}"
                );
                let overflow = descriptors(capacity + 1);
                assert!(
                    !root_fits(
                        &runs,
                        &overflow,
                        #[cfg(feature = "profiling")]
                        &mut ParentPlanningProfile::new(Instant::now()),
                    )
                    .await,
                    "width={width}"
                );
                let groups = plan_groups(
                    &runs,
                    &overflow,
                    #[cfg(feature = "profiling")]
                    &mut ParentPlanningProfile::new(Instant::now()),
                )
                .await
                .unwrap();
                assert_eq!(groups.first().unwrap().start, 0);
                assert_eq!(groups.last().unwrap().end, overflow.len());
                assert!(groups.len() < overflow.len());
                for pair in groups.windows(2) {
                    assert_eq!(pair[0].end, pair[1].start);
                }
                for group in groups.iter() {
                    assert!(group.len() >= 2, "singleton {group:?} width={width}");
                    assert!(branch_fits(&runs, &overflow[group.clone()]));
                }
            }
            let runs = test_runs(
                vec![(0..600).map(|i| key(i, 8, 512)).collect()],
                DuplicateCheck::Skip,
                MemoryBudget::new(2 * 1024 * 1024),
            );
            let children: Vec<_> = (0..600)
                .map(|i| ChildDescriptor {
                    page_id: PageID::new(i as u64),
                    height: 0,
                    lower: (i != 0).then(|| HotEntryRef::new(&runs, 0, i)),
                    upper: (i + 1 < 600).then(|| HotEntryRef::new(&runs, 0, i + 1)),
                    #[cfg(feature = "profiling")]
                    measurement: PageMeasurement::default(),
                })
                .collect();
            assert!(
                branch_fits(&runs, &children[1..599]),
                "finite fences compress the common prefix"
            );
            assert!(
                !root_fits(
                    &runs,
                    &children,
                    #[cfg(feature = "profiling")]
                    &mut ParentPlanningProfile::new(Instant::now()),
                )
                .await,
                "open root must account for complete separators"
            );
        });
    }

    /// Purpose: Protect normal mutations and structural maintenance after private packed installation.
    /// Expected: Online root/internal splits, full and partial internal sibling merges preserve the key oracle and exact reclamation.
    #[test]
    fn packed_online_splits_and_internal_merges() {
        use crate::index::btree::{BTreeCompactConfig, test_take_merge_observations};
        const INITIAL_ENTRIES: usize = 64;
        const FINAL_ENTRIES: usize = 512;

        fn mutation_key(index: usize) -> BTreeKey {
            let base = key(index % INITIAL_ENTRIES, 8192, 0);
            if index < INITIAL_ENTRIES {
                return base;
            }
            let mut bytes = base.as_bytes().to_vec();
            bytes.extend_from_slice(&(index as u64).to_be_bytes());
            BTreeKey::from(bytes.as_slice())
        }
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(16 * 1024 * 1024);
            let runs = input(INITIAL_ENTRIES, 8192, 0, DuplicateCheck::Collect);
            let mut expected = oracle(&runs);
            let plan = test_prepare_packed(runs, workers.clone(), 2, 4, 23).await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            let mut ready = expect_complete(build.execute().await.unwrap());
            ready.install().await.unwrap();
            cleanup.run().await.unwrap();
            drop(ready);
            drop(build);
            let index = staging.finish();
            let guard = pool.create_base_guard();
            let tree = index.tree();
            let initial_height = tree.height();
            assert!(initial_height >= 2);
            for index in INITIAL_ENTRIES..FINAL_ENTRIES {
                let key = mutation_key(index);
                let row = RowID::new(index as u64);
                assert!(
                    tree.insert(&guard, &key, BTreeU64::from(row), false, TrxID::new(9))
                        .await
                        .unwrap()
                        .is_ok()
                );
                expected.insert(key, row);
            }
            assert!(
                tree.height() > initial_height,
                "online additions must force a root split"
            );
            for index in (0..FINAL_ENTRIES).step_by(11) {
                let key = mutation_key(index);
                let replacement = RowID::new(index as u64 + 2_000_000);
                tree.update(
                    &guard,
                    &key,
                    BTreeU64::from(expected[&key]),
                    BTreeU64::from(replacement),
                    TrxID::new(10),
                )
                .await
                .unwrap();
                expected.insert(key, replacement);
            }
            // Sparse lower keys beside dense upper keys exercise both full and
            // partial internal sibling merges.
            for index in (0..FINAL_ENTRIES)
                .filter(|i| i % INITIAL_ENTRIES < INITIAL_ENTRIES / 2 && i % 4 != 0)
            {
                let key = mutation_key(index);
                let old = expected.remove(&key).unwrap();
                assert!(
                    tree.delete(&guard, &key, BTreeU64::from(old), true, TrxID::new(11))
                        .await
                        .unwrap()
                        .is_ok()
                );
            }
            test_take_merge_observations();
            for page in tree
                .compact_all::<BTreeU64>(&guard, BTreeCompactConfig::default())
                .await
                .unwrap()
            {
                pool.deallocate_page(page);
            }
            let observations = test_take_merge_observations();
            for full in [true, false] {
                assert!(
                    observations
                        .iter()
                        .any(|&(height, is_full, nonleft)| height >= 1
                            && is_full == full
                            && nonleft),
                    "missing internal full={full}: {observations:?}"
                );
            }
            for (key, row) in &expected {
                assert_eq!(
                    tree.lookup_optimistic::<BTreeU64>(&guard, key)
                        .await
                        .unwrap(),
                    Some(BTreeU64::from(*row))
                );
            }
            verify(tree, &guard, &expected, true).await;
            index.destroy(&guard).await.unwrap();
            assert_eq!(pool.allocated(), 0);
        });
    }

    /// Purpose: Protect page-pool exhaustion and poisoned build admission before detached construction.
    /// Expected: Both failures preserve the empty root, and caller-driven cleanup reclaims detached pages even under poison.
    #[test]
    fn packed_page_exhaustion_and_rejected_admission() {
        smol::block_on(async {
            for reject in [false, true] {
                let (_scope, workers, poisoner) = workers(1).await;
                let pool = pages(minimum_fixed_pool_bytes() * 4);
                let plan = test_prepare_packed(
                    input(1500, 256, 0, DuplicateCheck::Skip),
                    workers.clone(),
                    1,
                    4,
                    77,
                )
                .await;
                let mut staging = staging(&pool, true).await;
                if reject {
                    poisoner.poison(Report::new(FatalError::StorageIo));
                }
                let (mut build, mut cleanup) =
                    staging.start_build(plan, workers.clone(), poisoner.clone());
                assert!(build.execute().await.is_err());
                drop(build);
                assert_eq!(cleanup.run().await.is_err(), reject);
                assert_eq!(pool.allocated(), 1);
                drop(staging.check_empty().await.unwrap());
                staging.destroy().await.unwrap();
            }
        });
    }

    /// Purpose: Protect caller-driven cleanup independently of worker-pool availability and before construction starts.
    /// Expected: Cleanup waits for the owner decision, remains usable after pool shutdown or poison, and reclaims every detached page.
    #[test]
    fn packed_cleanup_without_pool_admission() {
        smol::block_on(async {
            for construct in [false, true] {
                for poison in [false, true] {
                    let (scope, workers, poisoner) = workers(1).await;
                    let pool = pages(16 * 1024 * 1024);
                    let plan = test_prepare_packed(
                        input(500, 256, 0, DuplicateCheck::Skip),
                        workers.clone(),
                        1,
                        4,
                        77,
                    )
                    .await;
                    let mut staging = staging(&pool, true).await;
                    let (mut build, mut cleanup) =
                        staging.start_build(plan, workers.clone(), poisoner.clone());
                    let mut reclaim = Box::pin(cleanup.run());
                    assert!(futures::poll!(reclaim.as_mut()).is_pending());
                    drop(reclaim);
                    if construct {
                        let ready = expect_complete(build.execute().await.unwrap());
                        drop(ready);
                        assert!(pool.allocated() > 1);
                    }
                    drop(build);
                    if poison {
                        poisoner.poison(Report::new(FatalError::StorageIo));
                    }
                    assert!(!scope.0.shutdown_all().is_degraded());
                    assert_eq!(cleanup.run().await.is_err(), poison);
                    assert_eq!(pool.allocated(), 1);
                    assert_eq!(cleanup.run().await.is_err(), poison);
                    staging.destroy().await.unwrap();
                    assert_eq!(pool.allocated(), 0);
                }
            }
        });
    }

    /// Purpose: Protect staging across real index-pool eviction and reopen for installation and abort.
    /// Expected: Dirty staged pages survive eviction, installed lookups agree with the oracle, and both paths return all allocations.
    #[test]
    fn packed_evicted_pages_install_and_abort() {
        use super::super::page_cleanup::test_remaining;
        use crate::Engine;
        use crate::buffer::test_evict_existing_page;
        use crate::table::tests::lightweight_test_engine_config;
        smol::block_on(async {
            let temp = tempfile::TempDir::new().unwrap();
            let engine = Engine::bootstrap(lightweight_test_engine_config(
                temp.path().to_path_buf(),
                "packed-evict",
            ))
            .await
            .unwrap();
            let core = &engine.inner().core;
            let pool = core.pools.index.clone();
            let baseline = pool.allocated();
            for install in [false, true] {
                let runs = input(600, 256, 0, DuplicateCheck::Collect);
                let expected = oracle(&runs);
                let plan = test_prepare_packed(runs, core.thread_pool.clone(), 1, 4, 71).await;
                let mut staging = StagingMemIndex::new(
                    pool.clone(),
                    pool.create_base_guard(),
                    vec![ValType::new(ValKind::VarByte, false)],
                    true,
                    TrxID::new(7),
                )
                .await
                .unwrap();
                let (mut build, mut cleanup) =
                    staging.start_build(plan, core.thread_pool.clone(), core.poisoner.clone());
                let mut ready = expect_complete(build.execute().await.unwrap());
                let ids = test_remaining(&cleanup);
                for &id in &ids {
                    test_evict_existing_page(pool.clone(), id).await;
                }
                if install {
                    ready.install().await.unwrap();
                } else {
                    ready.abort().unwrap();
                }
                cleanup.run().await.unwrap();
                drop(ready);
                drop(build);
                if install {
                    let index = staging.finish();
                    let guard = pool.create_base_guard();
                    for (key, row) in &expected {
                        assert_eq!(
                            index
                                .tree()
                                .lookup_optimistic::<BTreeU64>(&guard, key)
                                .await
                                .unwrap(),
                            Some(BTreeU64::from(*row))
                        );
                    }
                    index.destroy(&guard).await.unwrap();
                } else {
                    staging.destroy().await.unwrap();
                }
                assert_eq!(pool.allocated(), baseline);
                for id in ids {
                    assert!(!pool.is_allocated(id));
                }
            }
        });
    }

    /// Purpose: Protect deterministic global topology and outstanding-job bounds under reversed direct-parent completion.
    /// Expected: A completed later parent cannot admit more jobs until the blocked earlier result is collected, and final contents agree.
    #[test]
    fn packed_parent_completion_order_and_bound() {
        use super::super::page_cleanup::{TestFault, TestPoint, test_gate};
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(2).await;
            let pool = pages(64 * 1024 * 1024);
            let runs = input(500, 8192, 0, DuplicateCheck::Skip);
            let expected = oracle(&runs);
            let plan = test_prepare_packed(runs, workers.clone(), 2, 4, 73).await;
            let mut staging = staging(&pool, true).await;
            let (mut build, mut cleanup) =
                staging.start_build(plan, workers.clone(), poisoner.clone());
            let (first, release) = test_gate(&cleanup, TestPoint::Allocated(1), 1, TestFault::None);
            let (later, continue_later) =
                test_gate(&cleanup, TestPoint::Packed(1), 1, TestFault::None);
            let (third, continue_third) =
                test_gate(&cleanup, TestPoint::Allocated(1), 3, TestFault::None);
            let control = async {
                first.recv_async().await.unwrap();
                later.recv_async().await.unwrap();
                assert!(
                    third.try_recv().is_err(),
                    "submitted-but-uncollected parent count exceeded two"
                );
                continue_later.send(()).unwrap();
                continue_third.send(()).unwrap();
                release.send(()).unwrap();
            };
            let (result, ()) = futures::join!(build.execute(), control);
            let mut ready = expect_complete(result.unwrap());
            ready.install().await.unwrap();
            cleanup.run().await.unwrap();
            drop(ready);
            drop(build);
            let index = staging.finish();
            let guard = pool.create_base_guard();
            verify(index.tree(), &guard, &expected, true).await;
            index.destroy(&guard).await.unwrap();
            assert_eq!(pool.allocated(), 0);
        });
    }

    /// Purpose: Protect encoded nullable/composite keys and repeated non-unique logical keys with distinct physical RowIDs.
    /// Expected: Packing preserves complete physical keys and active leaf values through the direct single-run path.
    #[test]
    fn packed_composite_null_and_nonunique_identity() {
        use crate::index::BTreeKeyEncoder;
        use crate::value::Val;
        smol::block_on(async {
            let (_scope, workers, poisoner) = workers(1).await;
            let pool = pages(16 * 1024 * 1024);
            for unique in [false, true] {
                let mut types = vec![
                    ValType::new(ValKind::VarByte, true),
                    ValType::new(ValKind::I32, false),
                ];
                if !unique {
                    types.push(ValType::new(ValKind::U64, false));
                }
                let encoder = BTreeKeyEncoder::new(types);
                let keys: Vec<_> = (0..1500)
                    .map(|i| {
                        let mut values = vec![
                            if i % 3 == 0 {
                                Val::Null
                            } else {
                                Val::from("composite")
                            },
                            Val::from(if unique { i } else { i % 5 }),
                        ];
                        if !unique {
                            values.push(Val::from(RowID::new(1_000_000_000 - i as u64)));
                        }
                        encoder.encode(&values)
                    })
                    .collect();
                let runs = test_runs(
                    vec![keys],
                    DuplicateCheck::Collect,
                    MemoryBudget::new(4 * 1024 * 1024),
                );
                let expected = oracle(&runs);
                let plan = test_prepare_packed(runs, workers.clone(), 1, 1, 37).await;
                let mut staging = staging(&pool, unique).await;
                let (mut build, mut cleanup) =
                    staging.start_build(plan, workers.clone(), poisoner.clone());
                let mut ready = expect_complete(build.execute().await.unwrap());
                ready.install().await.unwrap();
                cleanup.run().await.unwrap();
                drop(ready);
                drop(build);
                let index = staging.finish();
                let guard = pool.create_base_guard();
                verify(index.tree(), &guard, &expected, unique).await;
                if !unique {
                    let mut cursor = index.tree().cursor(&guard, 0);
                    cursor.seek(&[]).await.unwrap();
                    while let Some(page) = cursor.next().await.unwrap() {
                        for i in 0..page.page().count() {
                            assert_eq!(
                                page.page()
                                    .unpack_value::<BTreeU64>(page.page().slot(i))
                                    .to_row_id(),
                                expected[&page.page().key(i)]
                            );
                        }
                    }
                }
                index.destroy(&guard).await.unwrap();
                assert_eq!(pool.allocated(), 0);
            }
        });
    }
}
