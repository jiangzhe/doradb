//! Retained boundary/consumer ledgers and the private streaming handoff.
use super::co_rank::{self, HotMergeCut};
use super::loser_tree::LoserTree;
use super::{BudgetedVec, DuplicateCheck, HotRunEntry, LocalDuplicates, SortedHotRuns};
use crate::completion::Completion;
use crate::error::{MultiDomainResultExt, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult};
use crate::id::RowID;
#[cfg(feature = "profiling")]
use crate::profiling::{HotMergeMeasurements, HotMergeWorkerProfile};
use crate::quiescent::QuiescentGuard;
use crate::runtime::thread_pool::ThreadPool;
use error_stack::{Report, ResultExt};
use std::cmp::Ordering;
use std::future::Future;
#[cfg(feature = "profiling")]
use std::mem::size_of;
use std::mem::take;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
#[cfg(feature = "profiling")]
use std::time::Instant;

const DEFAULT_BATCH_ENTRIES: usize = 32_768;

/// Coordinates established against one retained immutable run owner.
/// Retained references must be resolved with that same owner, not another plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HotEntryRef {
    run: usize,
    position: usize,
}

impl HotEntryRef {
    /// Establish coordinate bounds once before retaining a reference.
    #[inline]
    pub(super) fn new(runs: &SortedHotRuns, run: usize, position: usize) -> Self {
        assert!(
            runs.entry(run, position).is_some(),
            "hot merge coordinate outside run: run={run}, position={position}"
        );
        Self { run, position }
    }

    /// Compare established coordinates using physical key and provenance.
    #[inline]
    pub(super) fn compare(self, runs: &SortedHotRuns, other: Self) -> Ordering {
        runs.compare((self.run, self.position), (other.run, other.position))
            .unwrap_or_else(|| unreachable!("hot merge references belong to retained runs"))
    }

    #[inline]
    fn resolve(self, runs: &SortedHotRuns) -> &HotRunEntry {
        &runs.runs()[self.run].entries()[self.position]
    }
}

/// Caller-neutral earliest conflict, identified by the right entry's global rank.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HotDuplicate {
    /// Global output rank of the second equal key.
    pub(crate) right_rank: usize,
    /// Conflicting rows in output order.
    pub(crate) rows: [RowID; 2],
}

/// Ordered immutable cuts, moved here only after the complete boundary barrier.
struct HotMergeBoundaries {
    cuts: Vec<HotMergeCut>,
}

/// A prepared plan conveys ownership and fences, but no completed-validation authority.
pub(crate) struct PreparedHotMerge {
    runs: Arc<SortedHotRuns>,
    boundaries: HotMergeBoundaries,
    entries: usize,
    workers: usize,
    batch_entries: usize,
    boundary_duplicate: Option<HotDuplicate>,
    inhibited: AtomicBool,
    #[cfg(feature = "profiling")]
    measurements: HotMergeMeasurements,
}

impl PreparedHotMerge {
    #[inline]
    fn partitions(&self) -> usize {
        self.boundaries.cuts.len().saturating_sub(1)
    }

    #[inline]
    fn range(&self, partition: usize) -> Range<usize> {
        self.boundaries.cuts[partition].rank..self.boundaries.cuts[partition + 1].rank
    }
}

type CutCompletion = Arc<Completion<RuntimeOrFatalResult<HotMergeCut>>>;

/// Retains accepted cut jobs when the borrowed preparation future is cancelled.
pub(crate) struct HotMergePreparation {
    runs: Arc<SortedHotRuns>,
    pool: QuiescentGuard<ThreadPool>,
    workers: usize,
    batch_entries: usize,
    entries: usize,
    partitions: usize,
    stop: Arc<AtomicBool>,
    jobs: Vec<Option<CutCompletion>>,
    cuts: Vec<HotMergeCut>,
    submitted: usize,
    collected: usize,
    failure: Option<RuntimeOrFatalError>,
    finished: bool,
    #[cfg(feature = "profiling")]
    started: Option<Instant>,
    #[cfg(test)]
    hook: tests::CutHook,
}

impl HotMergePreparation {
    /// Use the production four-leaf batch minimum and independently sized partitions.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production callers migrate in RFC 0032 phases 4 and 5"
        )
    )]
    pub(crate) fn new(
        runs: Arc<SortedHotRuns>,
        pool: QuiescentGuard<ThreadPool>,
        workers: usize,
    ) -> RuntimeOrFatalResult<Self> {
        let entries = input_entries(&runs)?;
        let partitions = if runs.runs().len() <= 1 {
            usize::from(entries != 0)
        } else {
            entries
                .min(workers.saturating_mul(4))
                .min(entries.div_ceil(65_536).max(1))
        };
        Self::with_sizing(runs, pool, workers, partitions, DEFAULT_BATCH_ENTRIES)
    }

    fn with_sizing(
        runs: Arc<SortedHotRuns>,
        pool: QuiescentGuard<ThreadPool>,
        workers: usize,
        partitions: usize,
        batch_entries: usize,
    ) -> RuntimeOrFatalResult<Self> {
        let entries = input_entries(&runs)?;
        assert!(
            workers > 0 && workers <= pool.worker_threads() && batch_entries > 0,
            "hot merge requires positive sizing within the pool worker budget"
        );
        assert!(
            (entries == 0 && partitions == 0)
                || (entries != 0 && (1..=entries).contains(&partitions)),
            "hot merge partition count outside input rank range"
        );
        assert!(
            runs.runs().len() != 1 || partitions == 1,
            "hot merge single run must use direct bypass"
        );
        for run in runs.runs() {
            assert!(!run.entries().is_empty(), "hot merge retained an empty run");
            assert!(
                matches!(
                    (runs.duplicates, run.duplicates),
                    (DuplicateCheck::Skip, LocalDuplicates::Unchecked)
                        | (DuplicateCheck::Collect, LocalDuplicates::Checked { .. })
                ),
                "hot merge source policy and local evidence disagree: group={}",
                run.group_id
            );
        }
        Ok(Self {
            runs,
            pool,
            workers,
            batch_entries,
            entries,
            partitions,
            stop: Arc::new(AtomicBool::new(false)),
            jobs: (0..partitions.saturating_sub(1)).map(|_| None).collect(),
            cuts: Vec::with_capacity(partitions + 1),
            submitted: 0,
            collected: 0,
            failure: None,
            finished: false,
            #[cfg(feature = "profiling")]
            started: None,
            #[cfg(test)]
            hook: tests::CutHook::default(),
        })
    }

    /// Complete all cuts before exposing a plan; resumable after observer cancellation.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "production callers migrate in RFC 0032 phases 4 and 5"
        )
    )]
    pub(crate) async fn execute(&mut self) -> RuntimeOrFatalResult<Arc<PreparedHotMerge>> {
        assert!(
            !self.finished,
            "hot merge preparation reused after settlement"
        );
        #[cfg(feature = "profiling")]
        self.started.get_or_insert_with(Instant::now);
        if self.cuts.is_empty() && self.entries != 0 {
            match co_rank::endpoint(&self.runs, 0) {
                Ok(cut) => self.cuts.push(cut),
                Err(error) => self.record_failure(error),
            }
        }
        while self.failure.is_none() && self.collected < self.jobs.len() {
            while self.submitted < self.jobs.len() && self.submitted - self.collected < self.workers
            {
                let cut = self.submitted + 1;
                let rank =
                    ((cut as u128 * self.entries as u128) / self.partitions as u128) as usize;
                let runs = self.runs.clone();
                let stop = self.stop.clone();
                #[cfg(test)]
                let hook = self.hook.clone();
                self.jobs[self.submitted] = Some(self.pool.submit_async(async move {
                    #[cfg(test)]
                    hook.before(cut).await?;
                    co_rank::co_rank(&runs, rank, &stop)
                }));
                self.submitted += 1;
            }
            self.collect_next().await;
        }
        if self.failure.is_some() {
            self.settle().await?;
            unreachable!("failed boundary settlement returns its failure");
        }
        if self.entries != 0 {
            match co_rank::endpoint(&self.runs, self.entries) {
                Ok(cut) => self.cuts.push(cut),
                Err(error) => {
                    self.record_failure(error);
                    self.settle().await?;
                }
            }
        }
        co_rank::verify(&self.runs, &self.cuts)?;
        let mut conflict = None;
        #[cfg(feature = "profiling")]
        let mut comparisons = 0u64;
        if self.runs.duplicates == DuplicateCheck::Collect {
            for cut in &self.cuts {
                if let (Some(left), Some(right)) = (cut.left, cut.right)
                    && !proven_distinct(&self.runs, left, right)
                {
                    #[cfg(feature = "profiling")]
                    {
                        comparisons += 1;
                    }
                    if left.resolve(&self.runs).key == right.resolve(&self.runs).key {
                        conflict =
                            earliest(conflict, Some(duplicate(&self.runs, left, right, cut.rank)));
                    }
                }
            }
        }
        let locally_inhibited = self.runs.runs().iter().any(|run| {
            matches!(
                run.duplicates,
                LocalDuplicates::Checked {
                    first_duplicate_position: Some(_)
                }
            )
        });
        #[cfg(feature = "profiling")]
        let measurements = HotMergeMeasurements {
            entries: self.entries as u64,
            runs: self.runs.runs().len() as u64,
            workers: self.workers as u64,
            partitions: self.partitions as u64,
            batch_entries: self.batch_entries as u64,
            checked: self.runs.duplicates == DuplicateCheck::Collect,
            boundary_wall_nanos: self.started.map_or(0, |s| s.elapsed().as_nanos() as u64),
            cut_worker_nanos: self.cuts.iter().map(|c| c.elapsed_nanos).sum(),
            max_cut_nanos: self.cuts.iter().map(|c| c.elapsed_nanos).max().unwrap_or(0),
            boundary_bytes: self
                .cuts
                .iter()
                .map(|c| c.positions.capacity() * size_of::<usize>())
                .sum::<usize>() as u64,
            duplicate_comparisons: comparisons,
            validation_bytes: validation_bytes(self.workers.min(self.partitions), self.partitions)
                as u64,
            ..Default::default()
        };
        self.finished = true;
        Ok(Arc::new(PreparedHotMerge {
            runs: self.runs.clone(),
            boundaries: HotMergeBoundaries {
                cuts: take(&mut self.cuts),
            },
            entries: self.entries,
            workers: self.workers,
            batch_entries: self.batch_entries,
            boundary_duplicate: conflict,
            inhibited: AtomicBool::new(conflict.is_some() || locally_inhibited),
            #[cfg(feature = "profiling")]
            measurements,
        }))
    }

    /// Stop admission and drain accepted cut jobs. Pool reservation is acceptance;
    /// supervised children publish move-once completion after dropping captures.
    /// Poison/shutdown still drain accepted jobs. This retained owner owns cleanup,
    /// and a cancelled wait leaves its ledger slot intact for a later settlement.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.stop.store(true, AtomicOrdering::Release);
        while self.collected < self.submitted {
            self.collect_next().await;
        }
        self.cuts.clear();
        self.finished = true;
        match self.failure.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn collect_next(&mut self) {
        let cut = self.collected;
        let completion = self.jobs[cut]
            .as_ref()
            .unwrap_or_else(|| unreachable!("submitted co-rank owns a completion slot"));
        let result = completion.wait_take_result().await;
        self.jobs[cut] = None;
        self.collected += 1;
        let result = result
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .and_then(|r| r)
            .attach_with(|| format!("operation=hot_index_merge, phase=boundary, cut={}", cut + 1));
        match result {
            Ok(cut) => self.cuts.push(cut),
            Err(error) => self.record_failure(error),
        }
    }

    fn record_failure(&mut self, error: RuntimeOrFatalError) {
        self.stop.store(true, AtomicOrdering::Release);
        self.failure = Some(match self.failure.take() {
            Some(old) => old.merge_cleanup(error),
            None => error,
        });
    }
}

impl Drop for HotMergePreparation {
    fn drop(&mut self) {
        self.stop.store(true, AtomicOrdering::Release);
    }
}

#[derive(Default)]
struct ValidationState {
    previous: Option<HotEntryRef>,
    conflict: Option<HotDuplicate>,
    #[cfg(any(test, feature = "profiling"))]
    comparisons: u64,
}

enum BatchEntries<'a> {
    Direct {
        start: usize,
        entries: &'a [HotRunEntry],
    },
    Merged(&'a [HotEntryRef]),
}

/// Borrowed bounded output; the next pull requires release of this view.
pub(crate) struct HotBatch<'a> {
    runs: &'a SortedHotRuns,
    data: BatchEntries<'a>,
    ranks: Range<usize>,
    inhibited: bool,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "phase 3 consumes the streaming handoff")
)]
impl HotBatch<'_> {
    /// Global output ranks in this batch.
    #[inline]
    pub(crate) fn ranks(&self) -> Range<usize> {
        self.ranks.clone()
    }

    /// Whether duplicate evidence prohibits further private construction.
    #[inline]
    pub(crate) fn construction_inhibited(&self) -> bool {
        self.inhibited
    }

    /// Borrow an entry and its stable coordinates without copying its key.
    #[inline]
    pub(crate) fn entry(&self, index: usize) -> Option<(HotEntryRef, &HotRunEntry)> {
        match self.data {
            BatchEntries::Direct { start, entries } => entries.get(index).map(|entry| {
                (
                    HotEntryRef {
                        run: 0,
                        position: start + index,
                    },
                    entry,
                )
            }),
            BatchEntries::Merged(entries) => entries
                .get(index)
                .map(|&entry| (entry, entry.resolve(self.runs))),
        }
    }
}

/// A single partition's resumable merge and validation state.
pub(crate) struct PartitionMergeStream {
    plan: Arc<PreparedHotMerge>,
    partition: usize,
    stop: Arc<AtomicBool>,
    next: usize,
    end: usize,
    tree: Option<LoserTree>,
    buffer: BudgetedVec<HotEntryRef>,
    validation: ValidationState,
    fill: fn(&mut Self, usize),
    #[cfg(feature = "profiling")]
    profile: HotMergeWorkerProfile,
}

impl PartitionMergeStream {
    /// Borrow retained coordinates for a packing consumer's bounded lookahead.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 3 retains packing candidates")
    )]
    #[inline]
    pub(crate) fn entry(&self, reference: HotEntryRef) -> Option<&HotRunEntry> {
        self.plan.runs.entry(reference.run, reference.position)
    }

    /// Entries immediately before and after this partition; absent global ends
    /// denote open fences. Resolve these through the retained stream owner.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 3 plans partition fences")
    )]
    #[inline]
    pub(crate) fn neighbors(&self) -> (Option<HotEntryRef>, Option<HotEntryRef>) {
        (
            self.plan.boundaries.cuts[self.partition].left,
            self.plan.boundaries.cuts[self.partition + 1].right,
        )
    }

    fn new(
        plan: Arc<PreparedHotMerge>,
        partition: usize,
        stop: Arc<AtomicBool>,
    ) -> RuntimeOrFatalResult<Self> {
        observe_stop(&stop)?;
        let range = plan.range(partition);
        let mut buffer = BudgetedVec::new(&plan.runs.budget);
        let tree = if plan.runs.single_run().is_some() {
            None
        } else {
            buffer
                .ensure_capacity(plan.batch_entries.min(range.len()), "merge batch")
                .change_context(RuntimeError::IndexAccess)?;
            Some(
                LoserTree::new(
                    &plan.runs,
                    &plan.boundaries.cuts[partition].positions,
                    &plan.boundaries.cuts[partition + 1].positions,
                    &plan.runs.budget,
                )
                .change_context(RuntimeError::IndexAccess)?,
            )
        };
        let conflict = if tree.is_none() {
            match plan.runs.runs()[0].duplicates {
                LocalDuplicates::Checked {
                    first_duplicate_position: Some(position),
                } => {
                    assert!(
                        position > 0 && position < range.end,
                        "hot merge invalid single-run duplicate position={position}"
                    );
                    Some(duplicate(
                        &plan.runs,
                        HotEntryRef::new(&plan.runs, 0, position - 1),
                        HotEntryRef::new(&plan.runs, 0, position),
                        position,
                    ))
                }
                _ => None,
            }
        } else {
            None
        };
        let fill = if plan.runs.duplicates == DuplicateCheck::Collect {
            Self::fill::<true>
        } else {
            Self::fill::<false>
        };
        #[cfg(feature = "profiling")]
        let profile = HotMergeWorkerProfile {
            reference_bytes: (buffer.capacity() * size_of::<HotEntryRef>()) as u64,
            ..Default::default()
        };
        Ok(Self {
            plan,
            partition,
            stop,
            next: range.start,
            end: range.end,
            tree,
            buffer,
            validation: ValidationState {
                conflict,
                ..Default::default()
            },
            fill,
            #[cfg(feature = "profiling")]
            profile,
        })
    }

    /// Pull one full batch or the partition's final tail; cancellation is an error.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 3 consumes the streaming handoff")
    )]
    pub(crate) fn next_batch(&mut self) -> RuntimeOrFatalResult<Option<HotBatch<'_>>> {
        observe_stop(&self.stop)?;
        if self.next == self.end {
            return Ok(None);
        }
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        let start = self.next;
        let count = self.plan.batch_entries.min(self.end - start);
        if self.tree.is_some() {
            (self.fill)(self, count);
        } else {
            self.next += count;
        }
        #[cfg(feature = "profiling")]
        {
            let nanos = started.elapsed().as_nanos() as u64;
            self.profile.first_batch.get_or_insert_with(Instant::now);
            self.profile.merge_check_nanos += nanos;
            self.profile.max_batch_nanos = self.profile.max_batch_nanos.max(nanos);
        }
        let data = if self.tree.is_some() {
            BatchEntries::Merged(&self.buffer)
        } else {
            BatchEntries::Direct {
                start,
                entries: &self.plan.runs.runs()[0].entries()[start..self.next],
            }
        };
        // Read after fused checking: the discovering batch is already inhibited.
        Ok(Some(HotBatch {
            runs: &self.plan.runs,
            data,
            ranks: start..self.next,
            inhibited: self.plan.inhibited.load(AtomicOrdering::Acquire),
        }))
    }

    fn fill<const CHECK: bool>(&mut self, count: usize) {
        self.buffer.clear();
        let tree = self
            .tree
            .as_mut()
            .unwrap_or_else(|| unreachable!("multi-run pull owns a loser tree"));
        for _ in 0..count {
            let entry = tree
                .pop(&self.plan.runs)
                .unwrap_or_else(|| unreachable!("verified co-ranks supply every assigned entry"));
            if CHECK && self.validation.conflict.is_none() {
                if let Some(previous) = self.validation.previous
                    && !proven_distinct(&self.plan.runs, previous, entry)
                {
                    #[cfg(any(test, feature = "profiling"))]
                    {
                        self.validation.comparisons += 1;
                    }
                    if previous.resolve(&self.plan.runs).key == entry.resolve(&self.plan.runs).key {
                        self.validation.conflict =
                            Some(duplicate(&self.plan.runs, previous, entry, self.next));
                        self.plan.inhibited.store(true, AtomicOrdering::Release);
                    }
                }
                self.validation.previous = Some(entry);
            }
            // Admission was completed before the first advancement. Capacity is
            // retained across pulls and count never exceeds that admitted bound.
            self.buffer.push(entry, "merge batch").unwrap_or_else(|_| {
                unreachable!("hot merge batch was fully admitted before advancement")
            });
            self.next += 1;
        }
    }

    /// Convert an exhausted successful consumer into a plan-bound completion.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 3 consumes the streaming handoff")
    )]
    pub(crate) fn finish<T: Send + 'static>(
        self,
        output: T,
    ) -> RuntimeOrFatalResult<CompletedPartition<T>> {
        observe_stop(&self.stop)?;
        if self.next != self.end {
            return Err(execution_error(
                "partition consumer returned before exhausting its stream",
            ));
        }
        #[cfg(feature = "profiling")]
        let profile = HotMergeWorkerProfile {
            duplicate_comparisons: self.validation.comparisons,
            ..self.profile
        };
        Ok(CompletedPartition {
            plan: self.plan,
            partition: self.partition,
            entries: self.next,
            conflict: self.validation.conflict,
            output,
            #[cfg(feature = "profiling")]
            profile,
        })
    }
}

/// Private evidence minted only by exhaustive successful stream consumption.
pub(crate) struct CompletedPartition<T> {
    plan: Arc<PreparedHotMerge>,
    partition: usize,
    entries: usize,
    conflict: Option<HotDuplicate>,
    output: T,
    #[cfg(feature = "profiling")]
    profile: HotMergeWorkerProfile,
}

/// Narrow engine-internal hook for the future private packing consumer.
pub(crate) trait HotPartitionConsumer: Send + Sync + 'static {
    /// Per-partition ownership returned after successful consumption.
    type Output: Send + 'static;

    /// Consume the owned stream in this same finite ThreadPool job. Yield
    /// between synchronous pulls; awaiting consumer work also releases the worker.
    fn consume(
        &self,
        stream: PartitionMergeStream,
    ) -> impl Future<Output = RuntimeOrFatalResult<CompletedPartition<Self::Output>>> + Send;
}

/// Successful hot distinctness authority; cold/hot checks require separate evidence.
pub(crate) struct HotMergeCompletion {
    plan: Arc<PreparedHotMerge>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "phase 3 consumes the streaming handoff")
)]
impl HotMergeCompletion {
    /// Number of entries whose successful consumption this evidence certifies.
    #[inline]
    pub(crate) fn entries(&self) -> usize {
        self.plan.entries
    }

    /// Distinguish checked hot keys from the source's trusted contract.
    #[inline]
    pub(crate) fn checked(&self) -> bool {
        self.plan.runs.duplicates == DuplicateCheck::Collect
    }
}

/// Settled consumer outputs and either distinctness authority or deterministic conflict.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "phase 3 consumes the streaming handoff")
)]
pub(crate) struct HotMergeOutcome<T> {
    /// Results in planned partition order, independent of execution order.
    pub(crate) outputs: Vec<T>,
    /// Duplicate evidence never supplies installation authority.
    pub(crate) validation: Result<HotMergeCompletion, HotDuplicate>,
    /// Successful full-consumption sample, including fully checked duplicates.
    #[cfg(feature = "profiling")]
    pub(crate) measurements: HotMergeMeasurements,
}

type PartitionCompletion<T> = Arc<Completion<RuntimeOrFatalResult<CompletedPartition<T>>>>;

/// Retains consumer jobs and output ownership across cancellation of borrowed futures.
pub(crate) struct HotMergeConsumption<C: HotPartitionConsumer> {
    plan: Arc<PreparedHotMerge>,
    pool: QuiescentGuard<ThreadPool>,
    consumer: Arc<C>,
    stop: Arc<AtomicBool>,
    jobs: Vec<Option<PartitionCompletion<C::Output>>>,
    results: Vec<CompletedPartition<C::Output>>,
    submitted: usize,
    collected: usize,
    failure: Option<RuntimeOrFatalError>,
    finished: bool,
    #[cfg(feature = "profiling")]
    started: Option<Instant>,
}

impl<C: HotPartitionConsumer> HotMergeConsumption<C> {
    /// Prepare result slots before submitting any consumer job.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 3 supplies the private packing consumer")
    )]
    pub(crate) fn new(
        plan: Arc<PreparedHotMerge>,
        pool: QuiescentGuard<ThreadPool>,
        consumer: C,
    ) -> Self {
        assert!(
            plan.workers <= pool.worker_threads(),
            "hot merge consumption pool is smaller than prepared worker budget"
        );
        let partitions = plan.partitions();
        Self {
            plan,
            pool,
            consumer: Arc::new(consumer),
            stop: Arc::new(AtomicBool::new(false)),
            jobs: (0..partitions).map(|_| None).collect(),
            results: Vec::with_capacity(partitions),
            submitted: 0,
            collected: 0,
            failure: None,
            finished: false,
            #[cfg(feature = "profiling")]
            started: None,
        }
    }

    /// Consume every required partition, retaining bounded admission through collection.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 3 supplies the private packing consumer")
    )]
    pub(crate) async fn execute(&mut self) -> RuntimeOrFatalResult<HotMergeOutcome<C::Output>> {
        assert!(
            !self.finished,
            "hot merge consumption reused after settlement"
        );
        #[cfg(feature = "profiling")]
        self.started.get_or_insert_with(Instant::now);
        while self.failure.is_none() && self.collected < self.jobs.len() {
            while self.submitted < self.jobs.len()
                && self.submitted - self.collected < self.plan.workers
            {
                let plan = self.plan.clone();
                let stop = self.stop.clone();
                let consumer = self.consumer.clone();
                let partition = self.submitted;
                self.jobs[partition] = Some(self.pool.submit_async(async move {
                    #[cfg(feature = "profiling")]
                    let started = Instant::now();
                    let stream = PartitionMergeStream::new(plan, partition, stop)?;
                    #[cfg(feature = "profiling")]
                    let consumer_started = Instant::now();
                    let result = consumer.consume(stream).await?;
                    #[cfg(feature = "profiling")]
                    let result = CompletedPartition {
                        profile: HotMergeWorkerProfile {
                            job_nanos: started.elapsed().as_nanos() as u64,
                            consumer_nanos: consumer_started.elapsed().as_nanos() as u64
                                - result.profile.merge_check_nanos,
                            ..result.profile
                        },
                        ..result
                    };
                    Ok(result)
                }));
                self.submitted += 1;
            }
            self.collect_next().await;
        }
        if self.failure.is_some() {
            self.settle().await?;
            unreachable!("failed consumption settlement returns its failure");
        }
        if self.results.len() != self.plan.partitions() {
            return Err(execution_error(
                "hot merge is missing partition completions",
            ));
        }
        let mut conflict = self.plan.boundary_duplicate;
        #[cfg(feature = "profiling")]
        let mut measurements = self.plan.measurements;
        #[cfg(feature = "profiling")]
        let mut first_batch: Option<Instant> = None;
        let mut coverage = 0usize;
        for (partition, result) in self.results.iter().enumerate() {
            if !Arc::ptr_eq(&self.plan, &result.plan)
                || result.partition != partition
                || result.entries != self.plan.range(partition).end
            {
                return Err(execution_error(
                    "hot merge completion identity or coverage mismatch",
                ));
            }
            coverage += self.plan.range(partition).len();
            conflict = earliest(conflict, result.conflict);
            #[cfg(feature = "profiling")]
            {
                let profile = result.profile;
                if let Some(first) = profile.first_batch {
                    first_batch = Some(first_batch.map_or(first, |old| old.min(first)));
                }
                measurements.merge_check_nanos += profile.merge_check_nanos;
                measurements.max_batch_nanos =
                    measurements.max_batch_nanos.max(profile.max_batch_nanos);
                measurements.max_job_nanos = measurements.max_job_nanos.max(profile.job_nanos);
                measurements.job_worker_nanos += profile.job_nanos;
                measurements.consumer_worker_nanos += profile.consumer_nanos;
                measurements.duplicate_comparisons += profile.duplicate_comparisons;
                measurements.max_reference_bytes = measurements
                    .max_reference_bytes
                    .max(profile.reference_bytes);
            }
        }
        if coverage != self.plan.entries {
            return Err(execution_error("hot merge total coverage mismatch"));
        }
        #[cfg(feature = "profiling")]
        {
            let started = self
                .started
                .unwrap_or_else(|| unreachable!("merge profiling starts before execution"));
            measurements.consumption_wall_nanos = started.elapsed().as_nanos() as u64;
            measurements.first_batch_nanos = first_batch.map_or(0, |first| {
                measurements.boundary_wall_nanos + first.duration_since(started).as_nanos() as u64
            });
            measurements.active_reference_bytes = measurements.max_reference_bytes
                * self.plan.workers.min(self.plan.partitions()) as u64;
            measurements.scratch_peak_bytes = self.plan.runs.budget.peak() as u64;
        }
        self.finished = true;
        Ok(HotMergeOutcome {
            outputs: take(&mut self.results)
                .into_iter()
                .map(|result| result.output)
                .collect(),
            validation: match conflict {
                Some(conflict) => Err(conflict),
                None => Ok(HotMergeCompletion {
                    plan: self.plan.clone(),
                }),
            },
            #[cfg(feature = "profiling")]
            measurements,
        })
    }

    /// Stop admission and drain accepted jobs before dropping outputs. Pool
    /// reservation accepts work; supervised consumers produce authoritative
    /// move-once results even during poison/shutdown. This retained owner holds
    /// cleanup responsibility and keeps the awaited slot across cancellation.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.stop.store(true, AtomicOrdering::Release);
        while self.collected < self.submitted {
            self.collect_next().await;
        }
        self.results.clear();
        self.finished = true;
        match self.failure.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn collect_next(&mut self) {
        let partition = self.collected;
        let completion = self.jobs[partition]
            .as_ref()
            .unwrap_or_else(|| unreachable!("submitted partition owns a completion slot"));
        let result = completion.wait_take_result().await;
        self.jobs[partition] = None;
        self.collected += 1;
        let result = result
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .and_then(|r| r)
            .attach_with(|| {
                format!("operation=hot_index_merge, phase=consumer, partition={partition}")
            });
        match result {
            Ok(result)
                if Arc::ptr_eq(&self.plan, &result.plan)
                    && result.partition == partition
                    && result.entries == self.plan.range(partition).end =>
            {
                self.results.push(result)
            }
            Ok(_) => self.record_failure(execution_error(
                "foreign, repeated, or incomplete partition completion",
            )),
            Err(error) => self.record_failure(error),
        }
    }

    fn record_failure(&mut self, error: RuntimeOrFatalError) {
        self.stop.store(true, AtomicOrdering::Release);
        self.failure = Some(match self.failure.take() {
            Some(old) => old.merge_cleanup(error),
            None => error,
        });
    }
}

impl<C: HotPartitionConsumer> Drop for HotMergeConsumption<C> {
    fn drop(&mut self) {
        self.stop.store(true, AtomicOrdering::Release);
    }
}

/// Preserve execution-contract failures in the index-access domain.
pub(super) fn execution_error(message: &'static str) -> RuntimeOrFatalError {
    Report::new(RuntimeError::IndexAccess)
        .attach(message)
        .into()
}

/// Treat cooperative stop as incomplete execution, never normal exhaustion.
#[inline]
pub(super) fn observe_stop(stop: &AtomicBool) -> RuntimeOrFatalResult<()> {
    if stop.load(AtomicOrdering::Acquire) {
        Err(execution_error(
            "hot merge execution stopped before completion",
        ))
    } else {
        Ok(())
    }
}

#[cold]
fn duplicate(
    runs: &SortedHotRuns,
    left: HotEntryRef,
    right: HotEntryRef,
    rank: usize,
) -> HotDuplicate {
    HotDuplicate {
        right_rank: rank,
        rows: [left.resolve(runs).row_id, right.resolve(runs).row_id],
    }
}

#[inline]
fn proven_distinct(runs: &SortedHotRuns, left: HotEntryRef, right: HotEntryRef) -> bool {
    left.run == right.run
        && left.position + 1 == right.position
        && runs.runs()[left.run].duplicates
            == LocalDuplicates::Checked {
                first_duplicate_position: None,
            }
}

#[inline]
fn earliest(left: Option<HotDuplicate>, right: Option<HotDuplicate>) -> Option<HotDuplicate> {
    match (left, right) {
        (Some(l), Some(r)) => Some(if l.right_rank <= r.right_rank { l } else { r }),
        (l, r) => l.or(r),
    }
}

fn input_entries(runs: &SortedHotRuns) -> RuntimeOrFatalResult<usize> {
    runs.runs().iter().try_fold(0usize, |n, run| {
        n.checked_add(run.entries().len())
            .ok_or_else(|| execution_error("hot merge entry count overflow"))
    })
}

// Endpoints are borrowed from the boundary table, not duplicated per summary.
#[cfg(feature = "profiling")]
fn validation_bytes(active: usize, partitions: usize) -> usize {
    size_of::<AtomicBool>()
        + size_of::<Option<HotDuplicate>>()
        + active * size_of::<ValidationState>()
        + partitions * size_of::<Option<HotDuplicate>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::{ComponentRegistry, RegistryBuilder};
    use crate::conf::ThreadPoolConfig;
    use crate::index::BTreeKey;
    use crate::index::btree::BTreeValue;
    use crate::index::build::{HotSortedRun, MemoryBudget, MemoryReservation, budget, worker};
    use crate::memcmp::MEM_CMP_KEY_INLINE;
    use crate::poison::EnginePoisoner;
    #[cfg(feature = "profiling")]
    use crate::profiling::{HotBuildMeasurements, HotBuildWorkerProfile};
    use crate::runtime::thread_pool::ThreadPoolWorkers;
    use crate::runtime::yield_now;
    use futures::FutureExt;
    use parking_lot::Mutex;
    use rand::{RngExt, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::collections::BTreeMap;
    use std::mem::size_of;
    use std::ops::Deref;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::ptr;

    #[derive(Clone, Copy, Default)]
    enum Fault {
        #[default]
        None,
        Runtime,
        Panic,
    }

    #[derive(Clone)]
    struct Gate {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        fault: Fault,
    }

    /// Per-cut gates and call accounting, compiled only for deterministic lifecycle tests.
    #[derive(Clone, Default)]
    pub(crate) struct CutHook {
        gates: Arc<Mutex<BTreeMap<usize, Gate>>>,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl CutHook {
        fn gate(&self, index: usize, fault: Fault) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered, receive) = flume::bounded(1);
            let (release, wait) = flume::bounded(1);
            self.gates.lock().insert(
                index,
                Gate {
                    entered,
                    release: wait,
                    fault,
                },
            );
            (receive, release)
        }

        /// Wait on a configured semantic gate before applying its injected outcome.
        pub(crate) async fn before(&self, index: usize) -> RuntimeOrFatalResult<()> {
            self.calls.lock().push(index);
            let gate = self.gates.lock().get(&index).cloned();
            if let Some(gate) = gate {
                gate.entered.send(()).unwrap();
                gate.release.recv_async().await.unwrap();
                match gate.fault {
                    Fault::None => {}
                    Fault::Runtime => {
                        return Err(execution_error("injected merge consumer failure"));
                    }
                    Fault::Panic => panic!("injected merge worker panic"),
                }
            }
            Ok(())
        }
    }

    struct PoolScope(ComponentRegistry);

    impl Deref for PoolScope {
        type Target = ComponentRegistry;

        fn deref(&self) -> &ComponentRegistry {
            &self.0
        }
    }

    impl Drop for PoolScope {
        fn drop(&mut self) {
            assert!(!self.0.shutdown_all().is_degraded());
        }
    }

    #[derive(Default)]
    struct Drained {
        count: usize,
        checksum: u64,
        entries: Vec<HotEntryRef>,
        batches: Vec<usize>,
        comparisons: u64,
    }

    #[derive(Default)]
    struct Drain {
        hook: CutHook,
        capture: bool,
    }

    impl HotPartitionConsumer for Drain {
        type Output = Drained;

        async fn consume(
            &self,
            mut stream: PartitionMergeStream,
        ) -> RuntimeOrFatalResult<CompletedPartition<Drained>> {
            self.hook.before(stream.partition).await?;
            let mut output = Drained::default();
            let (left, right) = stream.neighbors();
            for reference in [left, right].into_iter().flatten() {
                assert!(ptr::eq(
                    stream.entry(reference).unwrap(),
                    reference.resolve(&stream.plan.runs)
                ));
            }
            let initial_pointer = stream.buffer.as_ptr().addr();
            let initial_capacity = stream.buffer.capacity();
            while let Some(batch) = stream.next_batch()? {
                if self.capture {
                    output.batches.push(batch.ranks().len());
                }
                for index in 0..batch.ranks().len() {
                    let (coordinate, entry) = batch.entry(index).unwrap();
                    output.checksum = output.checksum.wrapping_add(entry.row_id.as_u64());
                    output.count += 1;
                    if self.capture {
                        output.entries.push(coordinate);
                    }
                }
                assert!(batch.entry(batch.ranks().len()).is_none());
                if stream.validation.conflict.is_some() {
                    assert!(stream.plan.inhibited.load(AtomicOrdering::Acquire));
                }
                assert_eq!(stream.buffer.as_ptr().addr(), initial_pointer);
                assert_eq!(stream.buffer.capacity(), initial_capacity);
                yield_now().await;
            }
            output.comparisons = stream.validation.comparisons;
            stream.finish(output)
        }
    }

    struct EarlyConsumer;

    impl HotPartitionConsumer for EarlyConsumer {
        type Output = ();

        async fn consume(
            &self,
            mut stream: PartitionMergeStream,
        ) -> RuntimeOrFatalResult<CompletedPartition<()>> {
            let _batch = stream.next_batch()?;
            stream.finish(())
        }
    }

    struct ForeignConsumer(Mutex<Option<CompletedPartition<()>>>);

    impl HotPartitionConsumer for ForeignConsumer {
        type Output = ();

        async fn consume(
            &self,
            _stream: PartitionMergeStream,
        ) -> RuntimeOrFatalResult<CompletedPartition<()>> {
            Ok(self.0.lock().take().unwrap())
        }
    }

    struct FailAfterConsumption(Fault);

    impl HotPartitionConsumer for FailAfterConsumption {
        type Output = Drained;

        async fn consume(
            &self,
            stream: PartitionMergeStream,
        ) -> RuntimeOrFatalResult<CompletedPartition<Drained>> {
            let completed = Drain::default().consume(stream).await?;
            assert!(completed.conflict.is_some());
            match self.0 {
                Fault::None => Ok(completed),
                Fault::Runtime => Err(execution_error(
                    "consumer failed after complete duplicate validation",
                )),
                Fault::Panic => panic!("consumer panicked after complete duplicate validation"),
            }
        }
    }

    async fn pool(workers: usize) -> (PoolScope, QuiescentGuard<ThreadPool>) {
        let mut builder = RegistryBuilder::new();
        builder.build::<EnginePoisoner>(()).await.unwrap();
        builder
            .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(workers))
            .await
            .unwrap();
        builder.build::<ThreadPoolWorkers>(()).await.unwrap();
        let registry = builder.finish();
        let pool = registry.dependency::<ThreadPool>();
        (PoolScope(registry), pool)
    }

    fn fixture(groups: Vec<Vec<BTreeKey>>, policy: DuplicateCheck) -> Arc<SortedHotRuns> {
        fixture_in(groups, policy, MemoryBudget::new(usize::MAX))
    }

    fn fixture_in(
        groups: Vec<Vec<BTreeKey>>,
        policy: DuplicateCheck,
        budget: MemoryBudget,
    ) -> Arc<SortedHotRuns> {
        let mut runs = Vec::new();
        for (group, keys) in groups.into_iter().enumerate() {
            if keys.is_empty() {
                continue;
            }
            let mut entries = BudgetedVec::new(&budget);
            entries
                .ensure_capacity(keys.len(), "fixture entries")
                .unwrap();
            let mut payload = MemoryReservation::new(&budget);
            for (position, key) in keys.into_iter().enumerate() {
                if key.as_bytes().len() > MEM_CMP_KEY_INLINE {
                    payload.grow(key.as_bytes().len(), "fixture keys").unwrap();
                }
                entries
                    .push(
                        HotRunEntry {
                            key,
                            row_id: RowID::new(1_000_000_000 - (group * 100_000 + position) as u64),
                        },
                        "fixture entries",
                    )
                    .unwrap();
            }
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            let duplicates = worker::local_duplicates(&entries, policy, &AtomicBool::new(false));
            runs.push(Arc::new(HotSortedRun {
                group_id: group * 3 + 1,
                entries,
                duplicates,
                #[cfg(feature = "profiling")]
                profile: HotBuildWorkerProfile::default(),
                payload,
            }));
        }
        Arc::new(SortedHotRuns {
            runs,
            duplicates: policy,
            budget,
            #[cfg(feature = "profiling")]
            measurements: HotBuildMeasurements::default(),
        })
    }

    fn numbers(groups: &[&[u32]], policy: DuplicateCheck) -> Arc<SortedHotRuns> {
        fixture(
            groups
                .iter()
                .map(|group| group.iter().map(|&n| BTreeKey::from(n)).collect())
                .collect(),
            policy,
        )
    }

    fn oracle(runs: &SortedHotRuns) -> Vec<HotEntryRef> {
        let mut entries: Vec<_> = runs
            .runs()
            .iter()
            .enumerate()
            .flat_map(|(run, source)| {
                (0..source.entries().len()).map(move |position| HotEntryRef { run, position })
            })
            .collect();
        // Deliberately independent of production comparison, co-rank, and loser tree.
        entries.sort_by_key(|entry| {
            (
                entry.resolve(runs).key.as_bytes(),
                runs.runs()[entry.run].group_id,
                entry.position,
            )
        });
        entries
    }

    fn oracle_conflict(runs: &SortedHotRuns, entries: &[HotEntryRef]) -> Option<HotDuplicate> {
        if runs.duplicates == DuplicateCheck::Skip {
            return None;
        }
        entries.windows(2).enumerate().find_map(|(rank, pair)| {
            let left = pair[0].resolve(runs);
            let right = pair[1].resolve(runs);
            (left.key.as_bytes() == right.key.as_bytes()).then_some(HotDuplicate {
                right_rank: rank + 1,
                rows: [left.row_id, right.row_id],
            })
        })
    }

    async fn check_case(
        runs: Arc<SortedHotRuns>,
        pool: &QuiescentGuard<ThreadPool>,
        workers: usize,
        partitions: usize,
        batch: usize,
    ) {
        let expected = oracle(&runs);
        let conflict = oracle_conflict(&runs, &expected);
        let baseline = runs.budget.used();
        let mut prepare = HotMergePreparation::with_sizing(
            runs.clone(),
            pool.clone(),
            workers,
            partitions,
            batch,
        )
        .unwrap();
        let plan = prepare.execute().await.unwrap();
        assert_eq!(prepare.submitted, partitions.saturating_sub(1));
        let mut prefix = vec![0usize; runs.runs().len()];
        let mut previous_rank = 0;
        for cut in &plan.boundaries.cuts {
            for entry in &expected[previous_rank..cut.rank] {
                prefix[entry.run] += 1;
            }
            assert_eq!(&*cut.positions, prefix.as_slice(), "rank={}", cut.rank);
            assert_eq!(cut.left, cut.rank.checked_sub(1).map(|rank| expected[rank]));
            assert_eq!(cut.right, expected.get(cut.rank).copied());
            previous_rank = cut.rank;
        }
        let mut consume = HotMergeConsumption::new(
            plan.clone(),
            pool.clone(),
            Drain {
                capture: true,
                ..Default::default()
            },
        );
        let outcome = consume.execute().await.unwrap();
        assert_eq!(consume.submitted, partitions);
        let actual: Vec<_> = outcome
            .outputs
            .iter()
            .flat_map(|output| output.entries.iter().copied())
            .collect();
        assert_eq!(actual, expected);
        for (partition, output) in outcome.outputs.iter().enumerate() {
            assert_eq!(output.count, plan.range(partition).len());
            let mut remaining = output.count;
            for &count in &output.batches {
                assert_eq!(count, remaining.min(batch));
                remaining -= count;
            }
            assert_eq!(remaining, 0);
        }
        match &outcome.validation {
            Ok(completion) => {
                assert_eq!(conflict, None);
                assert_eq!(completion.entries(), expected.len());
                assert_eq!(
                    completion.checked(),
                    runs.duplicates == DuplicateCheck::Collect
                );
            }
            Err(actual) => assert_eq!(Some(*actual), conflict),
        }
        #[cfg(feature = "profiling")]
        {
            assert_eq!(outcome.measurements.entries, expected.len() as u64);
            assert!(
                outcome.measurements.job_worker_nanos >= outcome.measurements.merge_check_nanos
            );
            assert!(
                outcome.measurements.job_worker_nanos >= outcome.measurements.consumer_worker_nanos
            );
            assert_eq!(outcome.measurements.partitions, partitions as u64);
            if runs.duplicates == DuplicateCheck::Skip || runs.runs().len() <= 1 {
                assert_eq!(outcome.measurements.duplicate_comparisons, 0);
            }
            assert!(
                outcome.measurements.max_reference_bytes
                    <= (batch * size_of::<HotEntryRef>()) as u64
            );
        }
        drop(outcome);
        drop(consume);
        drop(plan);
        drop(prepare);
        assert_eq!(runs.budget.used(), baseline);
    }

    fn assert_four_leaves<V: BTreeValue>(batch: &HotBatch<'_>, value: V) {
        use crate::index::btree::algo::{PackedNodeEntry, PackedNodePlanParams, plan_sibling_node};
        let entries: Vec<_> = (0..batch.ranks().len())
            .map(|index| PackedNodeEntry {
                key: batch.entry(index).unwrap().1.key.as_bytes(),
                value,
            })
            .collect();
        let mut remaining = entries.as_slice();
        for _ in 0..4 {
            let plan = plan_sibling_node(
                PackedNodePlanParams {
                    lower_fence: remaining[0].key,
                    upper_fence: None,
                    min_slots: 1,
                },
                remaining,
            );
            assert!(
                plan.packed < remaining.len(),
                "full production batch must leave a tail after four capacity-limited leaves"
            );
            assert_eq!(plan.upper_fence, Some(remaining[plan.packed].key));
            remaining = &remaining[plan.packed..];
        }
        assert!(!remaining.is_empty());
    }

    /// Purpose: Protect exact prefixes, neighbors, provenance order, and bounded pulls at deterministic edge cases.
    /// Expected: Every cut and emitted coordinate matches an independent full-sort oracle, including empty and direct bypasses.
    #[test]
    fn edge_cases_match_independent_oracle() {
        smol::block_on(async {
            let (_registry, pool) = pool(4).await;
            for policy in [DuplicateCheck::Skip, DuplicateCheck::Collect] {
                for groups in [
                    vec![],
                    vec![vec![4, 2, 2, 1]],
                    vec![vec![1, 4, 7, 10], vec![2, 3, 8, 11], vec![5, 6, 9, 12]],
                    vec![vec![], vec![1], vec![], vec![0, 1, 1, 1, 1, 1, 3, 5, 8]],
                    vec![vec![9; 13], vec![9; 5], vec![9; 7]],
                ] {
                    let refs: Vec<_> = groups.iter().map(Vec::as_slice).collect();
                    let runs = numbers(&refs, policy);
                    let n = input_entries(&runs).unwrap();
                    let q_values = if n == 0 {
                        vec![0]
                    } else if runs.runs().len() == 1 {
                        vec![1]
                    } else {
                        vec![1, 2, n]
                    };
                    for q in q_values {
                        for b in [1, 3, 8, DEFAULT_BATCH_ENTRIES] {
                            check_case(runs.clone(), &pool, 2, q, b).await;
                        }
                    }
                }
            }
        });
    }

    /// Purpose: Exercise clamped steps, skew, key widths and independent K/P/Q/B using reproducible varied fixtures.
    /// Expected: Every rank cut and merged stream agrees with full sorting, with deterministic earliest duplicate rows.
    #[test]
    fn seeded_runs_and_every_rank_match_oracle() {
        smol::block_on(async {
            let (_registry, pool) = pool(4).await;
            let mut rng = ChaCha8Rng::seed_from_u64(0x0003_16c0_2026);
            for case in 0..120 {
                let k = rng.random_range(0..12);
                let wide = case % 3 == 0;
                let groups = (0..k)
                    .map(|_| {
                        (0..rng.random_range(0..80))
                            .map(|_| {
                                let n: u32 = rng.random_range(0..150);
                                if wide {
                                    let mut bytes = vec![42; 96];
                                    bytes.extend_from_slice(&n.to_be_bytes());
                                    BTreeKey::from(bytes.as_slice())
                                } else {
                                    BTreeKey::from(n)
                                }
                            })
                            .collect()
                    })
                    .collect();
                let policy = if case % 2 == 0 {
                    DuplicateCheck::Collect
                } else {
                    DuplicateCheck::Skip
                };
                let runs = fixture(groups, policy);
                let expected = oracle(&runs);
                let stop = AtomicBool::new(false);
                let mut counts = vec![0usize; runs.runs().len()];
                for rank in 0..=expected.len() {
                    let cut = co_rank::co_rank(&runs, rank, &stop).unwrap();
                    assert_eq!(
                        &*cut.positions,
                        counts.as_slice(),
                        "case={case}, rank={rank}"
                    );
                    if let Some(entry) = expected.get(rank) {
                        counts[entry.run] += 1;
                    }
                }
                let q = if expected.is_empty() {
                    0
                } else if runs.runs().len() == 1 {
                    1
                } else {
                    rng.random_range(1..=expected.len().min(16))
                };
                check_case(
                    runs,
                    &pool,
                    rng.random_range(1..=4),
                    q,
                    rng.random_range(1..40),
                )
                .await;
            }
        });
    }

    /// Purpose: Verify validation work suppression without changing full consumption or earliest conflict selection.
    /// Expected: Skip/direct streams compare no keys, disjoint clean runs reuse local proof, and each conflicting partition stops comparing after its first conflict.
    #[test]
    fn validation_reuses_proof_and_stops_only_local_comparisons() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            type Case<'a> = (&'a [&'a [u32]], usize, Option<usize>);
            let cases: &[Case<'_>] = &[
                (&[&[0, 2, 4, 6], &[1, 3, 5, 7]], 7, None),
                (&[&[0, 1, 2, 3], &[4, 5, 6, 7]], 1, None),
                (&[&[0, 0, 2, 4], &[1, 3, 5, 6]], 1, Some(1)),
                (&[&[0, 1, 2, 3, 4, 5], &[5, 6]], 1, Some(6)),
            ];
            for &(groups, comparisons, conflict_rank) in cases {
                for policy in [DuplicateCheck::Collect, DuplicateCheck::Skip] {
                    let runs = numbers(groups, policy);
                    let mut preparation =
                        HotMergePreparation::with_sizing(runs.clone(), pool.clone(), 2, 1, 2)
                            .unwrap();
                    let plan = preparation.execute().await.unwrap();
                    let mut stream =
                        PartitionMergeStream::new(plan, 0, Arc::new(AtomicBool::new(false)))
                            .unwrap();
                    let mut count = 0;
                    while let Some(batch) = stream.next_batch().unwrap() {
                        count += batch.ranks().len();
                        if policy == DuplicateCheck::Collect
                            && conflict_rank.is_some_and(|rank| rank < count)
                        {
                            assert!(batch.construction_inhibited());
                        }
                    }
                    let expected_calls = if policy == DuplicateCheck::Skip {
                        0
                    } else {
                        comparisons
                    };
                    assert_eq!(
                        stream.validation.comparisons, expected_calls as u64,
                        "groups={groups:?}"
                    );
                    assert_eq!(count, input_entries(&runs).unwrap());
                    assert_eq!(
                        stream.validation.conflict.map(|c| c.right_rank),
                        if policy == DuplicateCheck::Skip {
                            None
                        } else {
                            conflict_rank
                        }
                    );
                }
            }
            // A recorded local conflict cannot shortcut multi-run global ranking.
            check_case(
                numbers(&[&[1, 3, 3, 5], &[0, 1, 2, 4]], DuplicateCheck::Collect),
                &pool,
                2,
                4,
                1,
            )
            .await;
            // Cut neighbors in the same clean run also reuse proof.
            let runs = numbers(
                &[&[0, 1, 2, 3, 4, 5], &[6, 7, 8, 9]],
                DuplicateCheck::Collect,
            );
            let mut preparation =
                HotMergePreparation::with_sizing(runs, pool.clone(), 2, 4, 1).unwrap();
            let plan = preparation.execute().await.unwrap();
            let mut consumption = HotMergeConsumption::new(plan, pool, Drain::default());
            let result = consumption.execute().await.unwrap();
            #[cfg(feature = "profiling")]
            assert_eq!(result.measurements.duplicate_comparisons, 1);
            assert!(result.validation.is_ok());
        });
    }

    /// Purpose: Exercise independent cut jobs and a cancelled borrowed preparation future behind a delayed first cut.
    /// Expected: Later cuts finish without predecessor progress, credit includes uncollected results, and resumption computes each cut once.
    #[test]
    fn boundary_barrier_is_independent_bounded_and_resumable() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let runs = numbers(
                &[&[0, 2, 4, 6, 8], &[1, 3, 5, 7, 9]],
                DuplicateCheck::Collect,
            );
            let mut preparation = HotMergePreparation::with_sizing(runs, pool, 2, 5, 2).unwrap();
            let (entered, release) = preparation.hook.gate(1, Fault::None);
            assert!(preparation.execute().now_or_never().is_none());
            entered.recv_async().await.unwrap();
            while !preparation.jobs[1].as_ref().unwrap().is_completed() {
                yield_now().await;
            }
            assert!(preparation.execute().now_or_never().is_none());
            assert_eq!((preparation.submitted, preparation.collected), (2, 0));
            assert_eq!(preparation.cuts.len(), 1);
            release.send(()).unwrap();
            let plan = preparation.execute().await.unwrap();
            assert_eq!(plan.partitions(), 5);
            let mut calls = preparation.hook.calls.lock().clone();
            calls.sort_unstable();
            assert_eq!(calls, vec![1, 2, 3, 4]);
        });
    }

    /// Purpose: Keep higher-rank duplicates and completed results from cancelling or over-admitting required partition work.
    /// Expected: The delayed lower-rank conflict wins after all partitions finish, and a paused consumer retains exactly the worker credit.
    #[test]
    fn duplicate_order_and_consumer_backpressure_survive_cancellation() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let runs = numbers(
                &[&[0, 1, 1, 3, 4, 6], &[2, 3, 5, 7, 8, 9]],
                DuplicateCheck::Collect,
            );
            let expected = oracle_conflict(&runs, &oracle(&runs)).unwrap();
            let mut preparation =
                HotMergePreparation::with_sizing(runs, pool.clone(), 2, 4, 1).unwrap();
            let plan = preparation.execute().await.unwrap();
            let drain = Drain {
                capture: true,
                ..Default::default()
            };
            let hook = drain.hook.clone();
            let (entered, release) = hook.gate(0, Fault::None);
            let mut consumption = HotMergeConsumption::new(plan, pool, drain);
            assert!(consumption.execute().now_or_never().is_none());
            entered.recv_async().await.unwrap();
            while !consumption.jobs[1].as_ref().unwrap().is_completed() {
                yield_now().await;
            }
            assert_eq!((consumption.submitted, consumption.collected), (2, 0));
            assert_eq!(hook.calls.lock().len(), 2);
            assert!(consumption.execute().now_or_never().is_none());
            release.send(()).unwrap();
            let outcome = consumption.execute().await.unwrap();
            assert_eq!(outcome.validation.err(), Some(expected));
            assert_eq!(outcome.outputs.iter().map(|o| o.count).sum::<usize>(), 12);
            assert_eq!(hook.calls.lock().len(), 4);
        });
    }

    /// Purpose: Preserve cleanup and Fatal precedence after an earlier ordinary failure in either stage.
    /// Expected: Admission stops, settlement waits for accepted siblings, and a later supervised panic remains Fatal despite duplicate evidence.
    #[test]
    fn failures_drain_siblings_and_preserve_fatal_precedence() {
        smol::block_on(async {
            for boundary in [true, false] {
                for sibling_panic in [false, true] {
                    let (_registry, pool) = pool(2).await;
                    let runs = numbers(
                        &[&[0, 0, 1, 3, 4, 5], &[0, 2, 3, 6, 7, 8]],
                        DuplicateCheck::Collect,
                    );
                    let budget = runs.budget.clone();
                    let mut preparation =
                        HotMergePreparation::with_sizing(runs, pool.clone(), 2, 4, 1).unwrap();
                    let hook = if boundary {
                        preparation.hook.clone()
                    } else {
                        CutHook::default()
                    };
                    let first = usize::from(boundary);
                    let (entered1, release1) = hook.gate(first, Fault::Runtime);
                    let (entered2, release2) = hook.gate(
                        first + 1,
                        if sibling_panic {
                            Fault::Panic
                        } else {
                            Fault::None
                        },
                    );
                    if boundary {
                        assert!(preparation.execute().now_or_never().is_none());
                        entered1.recv_async().await.unwrap();
                        entered2.recv_async().await.unwrap();
                        release1.send(()).unwrap();
                        while !preparation.jobs[0].as_ref().unwrap().is_completed() {
                            yield_now().await;
                        }
                        assert!(preparation.execute().now_or_never().is_none());
                        assert_eq!(preparation.submitted, 2);
                        release2.send(()).unwrap();
                        let error = preparation.settle().await.err().unwrap();
                        assert_eq!(
                            matches!(error, RuntimeOrFatalError::Fatal(_)),
                            sibling_panic
                        );
                        assert_eq!(preparation.submitted, preparation.collected);
                    } else {
                        let plan = preparation.execute().await.unwrap();
                        let mut consumption = HotMergeConsumption::new(
                            plan,
                            pool,
                            Drain {
                                hook,
                                capture: false,
                            },
                        );
                        assert!(consumption.execute().now_or_never().is_none());
                        entered1.recv_async().await.unwrap();
                        entered2.recv_async().await.unwrap();
                        release1.send(()).unwrap();
                        while !consumption.jobs[0].as_ref().unwrap().is_completed() {
                            yield_now().await;
                        }
                        assert!(consumption.execute().now_or_never().is_none());
                        assert_eq!(consumption.submitted, 2);
                        release2.send(()).unwrap();
                        let error = consumption.settle().await.err().unwrap();
                        assert_eq!(
                            matches!(error, RuntimeOrFatalError::Fatal(_)),
                            sibling_panic
                        );
                        assert_eq!(consumption.submitted, consumption.collected);
                    }
                    drop(preparation);
                    assert_eq!(budget.used(), 0);
                }
            }
        });
    }

    /// Purpose: Reject partial, stopped, foreign-plan, and wrong-partition consumer authority.
    /// Expected: Only fully exhausted records for the expected plan and partition can certify coverage.
    #[test]
    fn completion_authority_rejects_incomplete_or_foreign_results() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let runs = numbers(&[&[0, 2, 4, 6], &[1, 3, 5, 7]], DuplicateCheck::Collect);
            let mut prepare =
                HotMergePreparation::with_sizing(runs.clone(), pool.clone(), 2, 2, 1).unwrap();
            let plan = prepare.execute().await.unwrap();
            let stream =
                PartitionMergeStream::new(plan.clone(), 0, Arc::new(AtomicBool::new(false)))
                    .unwrap();
            assert!(stream.finish(()).is_err());
            let mut consume = HotMergeConsumption::new(plan.clone(), pool.clone(), EarlyConsumer);
            assert!(consume.execute().await.is_err());
            for foreign_plan in [true, false] {
                let mut other =
                    HotMergePreparation::with_sizing(runs.clone(), pool.clone(), 2, 2, 1).unwrap();
                let owner = if foreign_plan {
                    other.execute().await.unwrap()
                } else {
                    plan.clone()
                };
                let mut stream = PartitionMergeStream::new(
                    owner,
                    usize::from(!foreign_plan),
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
                while stream.next_batch().unwrap().is_some() {}
                let completed = stream.finish(()).unwrap();
                let mut consumption = HotMergeConsumption::new(
                    plan.clone(),
                    pool.clone(),
                    ForeignConsumer(Mutex::new(Some(completed))),
                );
                // Feed a real supervised completion into the coordinator's retained slot.
                let consumer = consumption.consumer.clone();
                consumption.jobs[0] =
                    Some(pool.submit_async(async move { Ok(consumer.0.lock().take().unwrap()) }));
                consumption.submitted = 1;
                consumption.collect_next().await;
                assert!(consumption.failure.is_some());
                assert!(consumption.settle().await.is_err());
            }
            let stop = Arc::new(AtomicBool::new(false));
            let mut stream = PartitionMergeStream::new(plan, 0, stop.clone()).unwrap();
            while stream.next_batch().unwrap().is_some() {}
            stop.store(true, AtomicOrdering::Release);
            assert!(stream.next_batch().is_err());
            assert!(stream.finish(()).is_err());
        });
    }

    /// Purpose: Keep accepted cut and consumer captures alive after their coordinator is abandoned.
    /// Expected: Stop is observed after gate release, authoritative completion releases input owners, and all admitted scratch returns to zero.
    #[test]
    fn abandoned_owners_retain_captures_until_supervised_settlement() {
        smol::block_on(async {
            for boundary in [true, false] {
                let (_registry, pool) = pool(2).await;
                let runs = numbers(&[&[0, 2, 4, 6], &[1, 3, 5, 7]], DuplicateCheck::Collect);
                let weak = Arc::downgrade(&runs);
                let budget = runs.budget.clone();
                let mut prepare =
                    HotMergePreparation::with_sizing(runs, pool.clone(), 2, 4, 1).unwrap();
                if boundary {
                    let (entered, release) = prepare.hook.gate(1, Fault::None);
                    assert!(prepare.execute().now_or_never().is_none());
                    entered.recv_async().await.unwrap();
                    let jobs: Vec<_> = prepare.jobs.iter().flatten().cloned().collect();
                    drop(prepare);
                    assert!(weak.upgrade().is_some());
                    release.send(()).unwrap();
                    for job in jobs {
                        drop(job.wait_take_result().await.unwrap());
                    }
                } else {
                    let plan = prepare.execute().await.unwrap();
                    drop(prepare);
                    let drain = Drain::default();
                    let (entered, release) = drain.hook.gate(0, Fault::None);
                    let mut consume = HotMergeConsumption::new(plan, pool, drain);
                    assert!(consume.execute().now_or_never().is_none());
                    entered.recv_async().await.unwrap();
                    let jobs: Vec<_> = consume.jobs.iter().flatten().cloned().collect();
                    drop(consume);
                    assert!(weak.upgrade().is_some());
                    release.send(()).unwrap();
                    for job in jobs {
                        drop(job.wait_take_result().await.unwrap());
                    }
                }
                assert!(weak.upgrade().is_none());
                assert_eq!(budget.used(), 0);
            }
        });
    }

    /// Purpose: Enforce typed admission failures at each substantial kernel allocation and poisoned pool admission.
    /// Expected: Failed boundaries never prepare a plan, failed consumers settle accepted siblings, and resource or Fatal context survives cleanup.
    #[test]
    fn allocation_and_poison_failures_never_certify_partial_work() {
        use crate::error::{FatalError, ResourceError};
        smol::block_on(async {
            for purpose in [
                "merge boundary positions",
                "merge batch",
                "merge cursors",
                "merge loser tree",
                "poison",
            ] {
                let (registry, pool) = pool(2).await;
                let runs = numbers(&[&[0, 2, 4, 6], &[1, 3, 5, 7]], DuplicateCheck::Collect);
                let budget = runs.budget.clone();
                let mut prepare =
                    HotMergePreparation::with_sizing(runs, pool.clone(), 2, 4, 2).unwrap();
                let error = if purpose == "merge boundary positions" {
                    budget::fail_at(&budget, purpose);
                    prepare.execute().await.err().unwrap()
                } else {
                    let plan = prepare.execute().await.unwrap();
                    if purpose == "poison" {
                        registry
                            .dependency::<EnginePoisoner>()
                            .poison(Report::new(FatalError::RedoWrite));
                    } else {
                        budget::fail_at(&budget, purpose);
                    }
                    let mut consume = HotMergeConsumption::new(plan, pool, Drain::default());
                    let error = consume.execute().await.err().unwrap();
                    assert_eq!(consume.submitted, consume.collected);
                    error
                };
                match error {
                    RuntimeOrFatalError::Runtime(report) => {
                        assert_ne!(purpose, "poison");
                        assert!(report.contains::<ResourceError>());
                    }
                    RuntimeOrFatalError::Fatal(_) => assert_eq!(purpose, "poison"),
                }
                drop(prepare);
                assert_eq!(budget.used(), 0, "allocation={purpose}");
            }
        });
    }

    /// Purpose: Bound output scratch independently of input length and verify the production batch reservation without shrinking.
    /// Expected: Fixed K/P/Q/B has fixed incremental capacity, short partitions reserve only their length, and impossible growth preserves the original reservation.
    #[test]
    fn memory_is_bounded_and_growth_admission_is_transactional() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let mut capacities = Vec::new();
            for n in [70_000usize, 140_000] {
                let runs = fixture(
                    (0..2)
                        .map(|run| {
                            (0..n / 2)
                                .map(|position| BTreeKey::from((position * 2 + run) as u32))
                                .collect()
                        })
                        .collect(),
                    DuplicateCheck::Collect,
                );
                let baseline = runs.budget.used();
                let mut prepare = HotMergePreparation::with_sizing(
                    runs.clone(),
                    pool.clone(),
                    2,
                    2,
                    DEFAULT_BATCH_ENTRIES,
                )
                .unwrap();
                let plan = prepare.execute().await.unwrap();
                let mut left =
                    PartitionMergeStream::new(plan.clone(), 0, Arc::new(AtomicBool::new(false)))
                        .unwrap();
                let right =
                    PartitionMergeStream::new(plan.clone(), 1, Arc::new(AtomicBool::new(false)))
                        .unwrap();
                assert_eq!(left.buffer.capacity(), DEFAULT_BATCH_ENTRIES);
                assert_eq!(right.buffer.capacity(), DEFAULT_BATCH_ENTRIES);
                capacities.push(runs.budget.used() - baseline);
                assert_eq!(
                    left.next_batch().unwrap().unwrap().ranks().len(),
                    DEFAULT_BATCH_ENTRIES
                );
                #[cfg(feature = "profiling")]
                assert_eq!(
                    plan.measurements.validation_bytes,
                    validation_bytes(2, 2) as u64
                );
                drop(left);
                drop(right);
                drop(plan);
                drop(prepare);
                assert_eq!(runs.budget.used(), baseline);
            }
            assert_eq!(capacities[0], capacities[1]);
            let runs = numbers(&[&[1, 3], &[2, 4]], DuplicateCheck::Skip);
            let mut prepare = HotMergePreparation::new(runs, pool.clone(), 2).unwrap();
            let plan = prepare.execute().await.unwrap();
            let stream =
                PartitionMergeStream::new(plan, 0, Arc::new(AtomicBool::new(false))).unwrap();
            assert_eq!(stream.buffer.capacity(), 4);
            let n = DEFAULT_BATCH_ENTRIES;
            let budget = MemoryBudget::new(n * size_of::<HotRunEntry>() + 32 * 1024);
            let runs = fixture_in(
                (0..2)
                    .map(|r| {
                        (0..n / 2)
                            .map(|i| BTreeKey::from((i * 2 + r) as u32))
                            .collect()
                    })
                    .collect(),
                DuplicateCheck::Skip,
                budget.clone(),
            );
            let baseline = budget.used();
            let mut prepare = HotMergePreparation::new(runs, pool, 2).unwrap();
            let plan = prepare.execute().await.unwrap();
            let failed =
                PartitionMergeStream::new(plan.clone(), 0, Arc::new(AtomicBool::new(false)));
            assert!(
                failed.is_err(),
                "default batch admission must not shrink to fit spare scratch"
            );
            drop(failed);
            drop(plan);
            drop(prepare);
            assert!(baseline > 0);
            assert_eq!(budget.used(), 0);
            let budget = MemoryBudget::new(80);
            let mut values = BudgetedVec::<u64>::new(&budget);
            values.ensure_capacity(4, "test buffer").unwrap();
            values.push(19, "test buffer").unwrap();
            assert_eq!(budget.used(), 32);
            assert!(values.ensure_capacity(8, "test buffer").is_err()); // old + replacement = 96
            assert!(values.ensure_capacity(usize::MAX, "test buffer").is_err());
            assert_eq!(&*values, &[19]);
            assert_eq!(budget.used(), 32);
            values.clear();
            assert_eq!(budget.used(), 32);
            drop(values);
            assert_eq!(budget.used(), 0);
        });
    }

    /// Purpose: Validate four-leaf production pulls against real fence-aware page packing for dense and wide physical keys.
    /// Expected: Direct and merged full batches supply four capacity-limited leaves plus a tail, while direct batches borrow source entries with no reference allocation.
    #[test]
    fn default_batches_supply_four_leaf_pages_with_tail() {
        use crate::index::btree::{BTREE_BYTE_ZERO, BTreeU64};
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            for shape in 0..3 {
                for k in [1, 2] {
                    use crate::index::BTreeKeyEncoder;
                    use crate::value::{Val, ValKind, ValType};
                    let types = match shape {
                        0 => vec![ValType::new(ValKind::U32, false)],
                        1 => vec![
                            ValType::new(ValKind::VarByte, false),
                            ValType::new(ValKind::U64, false),
                        ],
                        _ => vec![
                            ValType::new(ValKind::U32, false),
                            ValType::new(ValKind::VarByte, false),
                        ],
                    };
                    let encoder = BTreeKeyEncoder::new(types);
                    let groups = (0..k)
                        .map(|run| {
                            (0..DEFAULT_BATCH_ENTRIES / k)
                                .map(|position| {
                                    let n = (position * k + run) as u32;
                                    let values = match shape {
                                        0 => vec![Val::from(n)],
                                        1 => vec![
                                            Val::from(vec![7u8; 128].as_slice()),
                                            Val::from(u64::from(n)),
                                        ],
                                        _ => vec![
                                            Val::from(n),
                                            Val::from(vec![(n % 251) as u8; 192].as_slice()),
                                        ],
                                    };
                                    encoder.encode(&values)
                                })
                                .collect()
                        })
                        .collect();
                    let runs = fixture(groups, DuplicateCheck::Collect);
                    let mut prepare =
                        HotMergePreparation::new(runs.clone(), pool.clone(), 2).unwrap();
                    let plan = prepare.execute().await.unwrap();
                    assert_eq!(plan.partitions(), 1);
                    let mut stream =
                        PartitionMergeStream::new(plan, 0, Arc::new(AtomicBool::new(false)))
                            .unwrap();
                    if k == 1 {
                        assert_eq!(stream.buffer.capacity(), 0);
                    }
                    let batch = stream.next_batch().unwrap().unwrap();
                    assert_eq!(batch.ranks().len(), DEFAULT_BATCH_ENTRIES);
                    if k == 1 {
                        assert!(ptr::eq(
                            batch.entry(0).unwrap().1,
                            &runs.single_run().unwrap()[0]
                        ));
                    }
                    if shape == 1 {
                        assert_four_leaves(&batch, BTREE_BYTE_ZERO);
                    } else {
                        assert_four_leaves(&batch, BTreeU64::from(0u64));
                    }
                    assert!(stream.next_batch().unwrap().is_none());
                    assert_eq!(
                        stream.validation.comparisons,
                        if k == 1 {
                            0
                        } else {
                            (DEFAULT_BATCH_ENTRIES - 1) as u64
                        }
                    );
                }
            }
        });
    }

    /// Purpose: Preserve physical-key semantics for nullable composites and non-unique logical ties.
    /// Expected: Unique equal encodings conflict deterministically, while appended RowIDs distinguish non-unique keys unless the entire encoded key repeats.
    #[test]
    fn encoded_composites_nulls_and_nonunique_keys_keep_physical_identity() {
        use crate::index::BTreeKeyEncoder;
        use crate::value::{Val, ValKind, ValType};
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let composite = BTreeKeyEncoder::new([
                ValType::new(ValKind::I32, true),
                ValType::new(ValKind::VarByte, false),
            ]);
            let key = |value| composite.encode(&[value, Val::from(vec![17u8; 128].as_slice())]);
            let runs = fixture(
                vec![
                    vec![key(Val::Null), key(Val::from(3i32))],
                    vec![key(Val::Null), key(Val::from(2i32))],
                ],
                DuplicateCheck::Collect,
            );
            check_case(runs, &pool, 2, 3, 1).await;
            let nonunique = BTreeKeyEncoder::new([
                ValType::new(ValKind::I32, false),
                ValType::new(ValKind::U64, false),
            ]);
            for exact_duplicate in [false, true] {
                let keys: Vec<_> = (0..8u64)
                    .map(|row| {
                        nonunique.encode(&[
                            Val::from(7i32),
                            Val::from(if exact_duplicate { row / 2 } else { row }),
                        ])
                    })
                    .collect();
                let runs = fixture(
                    vec![
                        keys.iter().step_by(2).cloned().collect(),
                        keys.iter().skip(1).step_by(2).cloned().collect(),
                    ],
                    DuplicateCheck::Collect,
                );
                assert_eq!(
                    oracle_conflict(&runs, &oracle(&runs)).is_some(),
                    exact_duplicate
                );
                check_case(runs, &pool, 2, 4, 1).await;
            }
        });
    }

    /// Purpose: Prevent missing local evidence from silently downgrading source-selected validation.
    /// Expected: Unchecked pairs never qualify for proof reuse, and a checked source with unchecked local evidence violates the construction contract.
    #[test]
    fn unchecked_local_evidence_cannot_downgrade_validation() {
        smol::block_on(async {
            let (_registry, pool) = pool(1).await;
            let mut runs = numbers(&[&[0, 1]], DuplicateCheck::Skip);
            assert!(!proven_distinct(
                &runs,
                HotEntryRef::new(&runs, 0, 0),
                HotEntryRef::new(&runs, 0, 1)
            ));
            Arc::get_mut(&mut runs).unwrap().duplicates = DuplicateCheck::Collect;
            let rejected =
                catch_unwind(AssertUnwindSafe(|| HotMergePreparation::new(runs, pool, 1)));
            assert!(rejected.is_err());
        });
    }

    /// Purpose: Reject a cut allocation after siblings have already been accepted and one has completed.
    /// Expected: The failed barrier drains every accepted cut and returns no prepared plan, releasing all boundary reservations.
    #[test]
    fn interior_cut_failure_drains_completed_and_pending_siblings() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let runs = numbers(&[&[0, 2, 4, 6], &[1, 3, 5, 7]], DuplicateCheck::Collect);
            let budget = runs.budget.clone();
            let baseline = budget.used();
            let mut prepare = HotMergePreparation::with_sizing(runs, pool, 2, 4, 1).unwrap();
            let (entered, release) = prepare.hook.gate(1, Fault::None);
            assert!(prepare.execute().now_or_never().is_none());
            entered.recv_async().await.unwrap();
            while !prepare.jobs[1].as_ref().unwrap().is_completed() {
                yield_now().await;
            }
            budget::fail_at(&budget, "merge boundary positions");
            release.send(()).unwrap();
            assert!(prepare.execute().await.is_err());
            assert_eq!((prepare.submitted, prepare.collected), (2, 2));
            assert!(prepare.cuts.is_empty());
            assert_eq!(budget.used(), baseline);
            drop(prepare);
            assert_eq!(budget.used(), 0);
        });
    }

    /// Purpose: Keep completed duplicate summaries from masking a later consumer failure.
    /// Expected: Fully drained duplicates produce no success authority when the consumer returns Runtime or panics afterward.
    #[test]
    fn late_consumer_failures_outrank_completed_duplicate_summaries() {
        smol::block_on(async {
            for fault in [Fault::Runtime, Fault::Panic] {
                let (_registry, pool) = pool(2).await;
                let runs = numbers(&[&[0, 0, 2], &[1, 3, 4]], DuplicateCheck::Collect);
                let budget = runs.budget.clone();
                let mut prepare =
                    HotMergePreparation::with_sizing(runs, pool.clone(), 2, 1, 2).unwrap();
                let plan = prepare.execute().await.unwrap();
                let mut consumption =
                    HotMergeConsumption::new(plan, pool, FailAfterConsumption(fault));
                let error = consumption.execute().await.err().unwrap();
                assert_eq!(
                    matches!(error, RuntimeOrFatalError::Fatal(_)),
                    matches!(fault, Fault::Panic)
                );
                assert_eq!(consumption.collected, 1);
                assert!(consumption.results.is_empty());
                drop(consumption);
                drop(prepare);
                assert_eq!(budget.used(), 0);
            }
        });
    }

    /// Purpose: Reject replay of a completed partition in a different result slot.
    /// Expected: A valid first record cannot authorize accepting the same partition again, and settlement discards all partial results.
    #[test]
    fn repeated_partition_completion_cannot_cover_a_missing_partition() {
        smol::block_on(async {
            let (_registry, pool) = pool(2).await;
            let runs = numbers(&[&[0, 2], &[1, 3]], DuplicateCheck::Collect);
            let mut prepare =
                HotMergePreparation::with_sizing(runs, pool.clone(), 2, 2, 1).unwrap();
            let plan = prepare.execute().await.unwrap();
            let mut consumption = HotMergeConsumption::new(
                plan.clone(),
                pool.clone(),
                ForeignConsumer(Mutex::new(None)),
            );
            for slot in 0..2 {
                let mut stream =
                    PartitionMergeStream::new(plan.clone(), 0, Arc::new(AtomicBool::new(false)))
                        .unwrap();
                while stream.next_batch().unwrap().is_some() {}
                let record = stream.finish(()).unwrap();
                consumption.jobs[slot] = Some(pool.submit_async(async move { Ok(record) }));
            }
            consumption.submitted = 2;
            consumption.collect_next().await;
            assert_eq!(consumption.results.len(), 1);
            consumption.collect_next().await;
            assert!(consumption.failure.is_some());
            assert!(consumption.settle().await.is_err());
            assert!(consumption.results.is_empty());
        });
    }
}
