//! Complete-input hot/cold uniqueness validation against a sealed private tree.
//!
//! Owns strategy selection, jobs, conflict evidence and coverage verification.
//! Cold traversal lives in `cold_reader` and resource lifetimes in `sealed_cold`.
//! Hot-driven checks share packing jobs; cold-driven jobs settle before packing.
use super::cold_reader::{
    ColdBatch, ColdRange, ColdReadAdmission, ColdReader, LOOKUP_BATCH_ENTRIES, plan_ranges,
};
use super::merge::{HotBatch, MergeCompletion, PreparedMerge, execution_error};
use super::sealed_cold::{ColdBuildIdentity, ColdReadAuthority};
use super::{BuildOutcome, SortedRuns};
use crate::completion::Completion;
use crate::error::{ResourceError, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult};
use crate::id::RowID;
#[cfg(feature = "profiling")]
use crate::index::disk_tree::DISK_TREE_BLOCK_SIZE;
#[cfg(feature = "profiling")]
use crate::profiling::{ColdHotMeasurements, clock::Instant};
use crate::quiescent::QuiescentGuard;
use crate::runtime::{thread_pool::ThreadPool, yield_now};
use std::sync::Arc;
#[cfg(feature = "profiling")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

#[cfg(test)]
pub(super) use tests::forced_selection as test_selection;
#[cfg(test)]
pub(crate) use tests::with_cold as test_with_cold;

// Release probes measured about 0.2 us per persisted tree level, versus
// 0.06–0.15 us for one resident run search. A height term captures the
// meaningful disk slope without fitting run-length/cache-specific parameters.
const DISK_LOOKUP_COST: u128 = 40;
const RUN_LOOKUP_COST: u128 = 10;
const SCAN_COST: u128 = 1;
const COMPARE_COST: u128 = 2;
// Even dozens of distributed probes can validate many cold leaf pages.
const MIN_VALIDATION_ENTRIES: usize = 32;

/// Deterministic validation direction. Declaration order also defines score ties.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ValidationMethod {
    Empty,
    Ordered,
    Forward,
    Reverse,
}

/// Cold authority and admitted sizing selected before preparing hot merge cuts.
pub(super) struct CrossTierSelection {
    cold: ColdReadAuthority,
    method: ValidationMethod,
    /// Common effective worker ceiling for all three methods.
    pub(super) workers: usize,
    /// Hot-rank partition count fixed once during merge preparation.
    pub(super) partitions: usize,
    ranges: usize,
    admission: Option<ColdReadAdmission>,
}

impl CrossTierSelection {
    /// Select using complete counts and reduce concurrency before any submission.
    pub(super) fn select(
        cold: &ColdReadAuthority,
        runs: &SortedRuns,
        workers: usize,
    ) -> RuntimeOrFatalResult<Self> {
        let hot = runs
            .runs()
            .iter()
            .try_fold(0usize, |sum, run| sum.checked_add(run.entries().len()))
            .ok_or_else(|| execution_error("cross-tier hot input count overflow"))?;
        let default_partitions = hot.min(workers.saturating_mul(4)).min(hot.div_ceil(65_536));
        if hot == 0 || cold.entries() == 0 {
            return Ok(Self {
                cold: cold.clone(),
                method: ValidationMethod::Empty,
                workers,
                partitions: default_partitions,
                ranges: 0,
                admission: None,
            });
        }
        for effective in (1..=workers).rev() {
            let partitions = hot
                .min(effective.saturating_mul(4))
                .min(hot.div_ceil(MIN_VALIDATION_ENTRIES));
            let ranges = cold
                .entries()
                .min(effective.saturating_mul(4))
                .min(cold.entries().div_ceil(MIN_VALIDATION_ENTRIES))
                .max(1);
            let method = select_method(
                hot,
                cold.entries(),
                runs.runs().len(),
                partitions,
                cold.height(),
            );
            // Controlled benchmark builds can pin this private constant at build
            // time by editing the selector; there is no public runtime switch.
            match ColdReadAdmission::new(
                cold,
                effective,
                ranges,
                ReverseProbe::scratch_bytes(runs.runs().len())?,
            ) {
                Ok(admission) => {
                    return Ok(Self {
                        cold: cold.clone(),
                        method,
                        workers: effective,
                        partitions,
                        ranges,
                        admission: Some(admission),
                    });
                }
                Err(RuntimeOrFatalError::Runtime(error))
                    if effective > 1 && error.contains::<ResourceError>() => {}
                Err(error) => return Err(error),
            }
        }
        Err(execution_error("cross-tier validation requires a worker"))
    }
}

/// Immutable binding between a sealed root and one exact prepared hot plan.
pub(super) struct CrossTierPlan {
    hot: Arc<PreparedMerge>,
    selection: CrossTierSelection,
    #[cfg(feature = "profiling")]
    active: AtomicU64,
    #[cfg(feature = "profiling")]
    peak: AtomicU64,
    #[cfg(test)]
    hooks: tests::Hooks,
}

impl CrossTierPlan {
    /// Bind evidence only after the complete hot boundary barrier has succeeded.
    pub(super) fn bind(
        hot: Arc<PreparedMerge>,
        selection: CrossTierSelection,
    ) -> RuntimeOrFatalResult<Arc<Self>> {
        if selection.workers < hot.workers() {
            return Err(execution_error(
                "prepared hot concurrency exceeds admitted readers",
            ));
        }
        Ok(Arc::new(Self {
            hot,
            selection,
            #[cfg(feature = "profiling")]
            active: AtomicU64::new(0),
            #[cfg(feature = "profiling")]
            peak: AtomicU64::new(0),
            #[cfg(test)]
            hooks: tests::Hooks::default(),
        }))
    }

    /// Expected cold binding copied into compact ready-tree evidence.
    #[inline]
    pub(super) fn cold_identity(&self) -> &ColdBuildIdentity {
        self.selection.cold.identity()
    }

    fn reader(&self) -> ColdReader {
        ColdReader::new(
            self.selection.cold.clone(),
            self.selection
                .admission
                .as_ref()
                .unwrap_or_else(|| unreachable!("nonempty validation admits readers"))
                .clone(),
        )
    }

    /// Reverse work is driven by cold intervals, never hot partition summaries.
    #[inline]
    pub(super) fn is_reverse(&self) -> bool {
        self.selection.method == ValidationMethod::Reverse
    }

    fn summary(&self, part: usize) -> CrossTierSummary {
        CrossTierSummary {
            hot: self.hot.identity(),
            cold: self.cold_identity().clone(),
            method: self.selection.method,
            partition: part,
            consumed: 0,
            conflict: None,
            #[cfg(feature = "profiling")]
            measurements: ColdHotMeasurements::default(),
            #[cfg(feature = "profiling")]
            span: None,
        }
    }

    fn check_summary(
        &self,
        summary: &CrossTierSummary,
        partition: usize,
    ) -> RuntimeOrFatalResult<()> {
        if !Arc::ptr_eq(&summary.hot, &self.hot.identity())
            || !summary.cold.matches(self.cold_identity())
            || summary.method != self.selection.method
            || summary.partition != partition
        {
            return Err(execution_error("cross-tier summary binding mismatch"));
        }
        Ok(())
    }
}

/// Direction-specific exhaustive coverage; cold counts never stand in for hot counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrossTierCoverage {
    HotPartitions { partitions: usize, entries: usize },
    ColdRanges { ranges: usize, entries: usize },
    EmptyTiers,
}

/// Deterministic conflict, ordered by the first matching global hot rank.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CrossTierDuplicate {
    /// Global hot rank identifying the conflict.
    pub(crate) hot_rank: usize,
    /// Participating hot row selected by the merge's provenance order.
    pub(crate) hot_row: RowID,
    /// Participating live cold row.
    pub(crate) cold_row: RowID,
}

/// Compact summary minted only after production partition consumption.
pub(super) struct CrossTierSummary {
    hot: Arc<()>,
    cold: ColdBuildIdentity,
    method: ValidationMethod,
    partition: usize,
    consumed: usize,
    conflict: Option<CrossTierDuplicate>,
    #[cfg(feature = "profiling")]
    measurements: ColdHotMeasurements,
    #[cfg(feature = "profiling")]
    span: Option<(Instant, Instant)>,
}

/// Successful compact evidence bound to both inputs and the caller's contract.
pub(super) struct CrossTierCompletion {
    hot: Arc<()>,
    cold: Option<ColdBuildIdentity>,
    hot_entries: usize,
    cold_entries: usize,
    method: Option<ValidationMethod>,
    coverage: Option<CrossTierCoverage>,
    #[cfg(feature = "profiling")]
    measurements: ColdHotMeasurements,
}

impl CrossTierCompletion {
    /// Check the caller's actual sealed artifact before private installation.
    pub(super) fn validate_cold(
        &self,
        binding: Option<(&ColdBuildIdentity, usize)>,
    ) -> RuntimeOrFatalResult<()> {
        match (&self.cold, binding) {
            (None, None) => Ok(()),
            (Some(actual), Some((expected, entries)))
                if actual.matches(expected) && self.cold_entries == entries =>
            {
                Ok(())
            }
            _ => Err(execution_error(
                "ready tree does not validate CREATE's sealed cold artifact",
            )),
        }
    }

    /// Reject equal-sized evidence from another root, generation or hot invocation.
    pub(super) fn validate(
        &self,
        hot: &MergeCompletion,
        cold: Option<&ColdBuildIdentity>,
    ) -> RuntimeOrFatalResult<()> {
        let cold_matches = match (&self.cold, cold) {
            (None, None) => true,
            (Some(left), Some(right)) => left.matches(right),
            _ => false,
        };
        let coverage_matches = match (self.method, self.coverage) {
            (None, None) => cold.is_none(),
            (Some(ValidationMethod::Empty), Some(CrossTierCoverage::EmptyTiers)) => {
                cold.is_some() && (self.hot_entries == 0 || self.cold_entries == 0)
            }
            (
                Some(ValidationMethod::Forward | ValidationMethod::Ordered),
                Some(CrossTierCoverage::HotPartitions { entries, .. }),
            ) => cold.is_some() && entries == self.hot_entries,
            (
                Some(ValidationMethod::Reverse),
                Some(CrossTierCoverage::ColdRanges { entries, .. }),
            ) => cold.is_some() && entries == self.cold_entries,
            _ => false,
        };
        if !Arc::ptr_eq(&self.hot, &hot.identity())
            || self.hot_entries != hot.entries()
            || !cold_matches
            || !coverage_matches
        {
            return Err(execution_error(
                "ready tree cross-tier identity or coverage mismatch",
            ));
        }
        Ok(())
    }

    /// Successful-publication profiling is collected by the owning CREATE operation.
    #[cfg(feature = "profiling")]
    pub(super) fn measurements(&self) -> ColdHotMeasurements {
        self.measurements
    }
}

#[cfg(feature = "profiling")]
struct ActiveReader(Arc<CrossTierPlan>);

#[cfg(feature = "profiling")]
impl ActiveReader {
    fn new(plan: Arc<CrossTierPlan>) -> Self {
        let active = plan.active.fetch_add(1, AtomicOrdering::AcqRel) + 1;
        plan.peak.fetch_max(active, AtomicOrdering::Relaxed);
        Self(plan)
    }
}

#[cfg(feature = "profiling")]
impl Drop for ActiveReader {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, AtomicOrdering::AcqRel);
    }
}

/// Partition-local persisted validation retained across borrowed hot batches.
pub(super) struct CrossTierCursor {
    plan: Arc<CrossTierPlan>,
    reader: Option<ColdReader>,
    summary: CrossTierSummary,
    #[cfg(feature = "profiling")]
    _active: Option<ActiveReader>,
}

impl CrossTierCursor {
    /// Reverse uses its separate range ledger; empty tiers perform no reads.
    pub(super) fn new(plan: Arc<CrossTierPlan>, partition: usize) -> Option<Self> {
        if plan.is_reverse() {
            return None;
        }
        let nonempty = plan.selection.method != ValidationMethod::Empty;
        Some(Self {
            reader: nonempty.then(|| plan.reader()),
            summary: plan.summary(partition),
            #[cfg(feature = "profiling")]
            _active: nonempty.then(|| ActiveReader::new(plan.clone())),
            plan,
        })
    }

    /// Inspect every assigned hot rank, including after a discovered conflict.
    /// The merge still owns the borrowed batch and checks hot distinctness.
    pub(super) async fn validate(
        &mut self,
        batch: &HotBatch<'_>,
        stop: &AtomicBool,
    ) -> RuntimeOrFatalResult<BuildOutcome<()>> {
        let range = self.plan.hot.range(self.summary.partition);
        let ranks = batch.ranks();
        if ranks.start != range.start + self.summary.consumed || ranks.end > range.end {
            return Err(execution_error("cross-tier batch rank mismatch"));
        }
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        #[cfg(test)]
        if self.summary.consumed == 0 {
            self.plan.hooks.before(self.summary.partition).await?;
        }
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        if let Some(reader) = &mut self.reader {
            let forward = self.plan.selection.method == ValidationMethod::Forward;
            let mut keys: [&[u8]; LOOKUP_BATCH_ENTRIES] = [&[]; LOOKUP_BATCH_ENTRIES];
            let mut hits = [None; LOOKUP_BATCH_ENTRIES];
            for start in (0..ranks.len()).step_by(LOOKUP_BATCH_ENTRIES) {
                let count = LOOKUP_BATCH_ENTRIES.min(ranks.len() - start);
                if forward {
                    for (offset, key) in keys[..count].iter_mut().enumerate() {
                        *key = batch
                            .entry(start + offset)
                            .unwrap_or_else(|| unreachable!("bounded hot batch"))
                            .1
                            .key
                            .as_bytes();
                    }
                    if let BuildOutcome::Stopped = reader
                        .lookup_batch(&keys[..count], &mut hits[..count], stop)
                        .await?
                    {
                        return Ok(BuildOutcome::Stopped);
                    }
                    #[cfg(feature = "profiling")]
                    {
                        self.summary.measurements.probes += count as u64;
                        self.summary.measurements.batches += 1;
                    }
                }
                for (offset, hit) in hits[..count].iter().enumerate() {
                    let (_, hot) = batch
                        .entry(start + offset)
                        .unwrap_or_else(|| unreachable!("bounded hot batch"));
                    let found = if forward {
                        *hit
                    } else {
                        reader.seek_match(hot.key.as_bytes()).await?
                    };
                    if let Some(cold_row) = found {
                        earliest(
                            &mut self.summary.conflict,
                            Some(CrossTierDuplicate {
                                hot_rank: ranks.start + start + offset,
                                hot_row: hot.row_id,
                                cold_row,
                            }),
                        );
                        batch.inhibit_construction();
                    }
                }
                // Forward lookup already yields after each lookup batch.
                if !forward && (start + count).is_multiple_of(256) {
                    yield_now().await;
                }
            }
        }
        #[cfg(feature = "profiling")]
        {
            let end = Instant::now();
            self.summary.measurements.worker_nanos += (end - started).as_nanos() as u64;
            extend_span(&mut self.summary.span, Some((started, end)));
        }
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        self.summary.consumed += ranks.len();
        Ok(BuildOutcome::Complete(()))
    }

    /// Transfer compact evidence after the stream's final consumption check.
    pub(super) fn finish(self) -> CrossTierSummary {
        #[cfg(test)]
        self.plan.hooks.after(self.summary.partition);
        let summary = self.summary;
        #[cfg(feature = "profiling")]
        let summary = {
            let mut summary = summary;
            if let Some(reader) = &self.reader {
                summary.measurements.comparisons = reader.comparisons;
                summary.measurements.scanned = reader.scanned;
                summary.measurements.node_reads = reader.node_reads;
            }
            summary
        };
        summary
    }
}

struct ReverseSummary {
    summary: CrossTierSummary,
    lower: Option<Arc<Vec<u8>>>,
    upper: Option<Arc<Vec<u8>>>,
    exhausted: bool,
}

type RangeResult = RuntimeOrFatalResult<BuildOutcome<Vec<ColdRange>>>;
type RangeJob = Arc<Completion<RuntimeOrFatalResult<BuildOutcome<ReverseSummary>>>>;

/// Retained planner and cold-range job ledger, independent of borrowed execution.
pub(super) struct ReverseValidation {
    pool: QuiescentGuard<ThreadPool>,
    stop: Arc<AtomicBool>,
    planner: Option<Arc<Completion<RangeResult>>>,
    ranges: Option<Arc<Vec<ColdRange>>>,
    jobs: Vec<Option<RangeJob>>,
    outputs: Vec<ReverseSummary>,
    submitted: usize,
    collected: usize,
    failure: Option<RuntimeOrFatalError>,
    // Admission outlives the range keys, summaries and accepted-job ledgers.
    plan: Arc<CrossTierPlan>,
    #[cfg(feature = "profiling")]
    started: Option<Instant>,
    #[cfg(feature = "profiling")]
    wall_nanos: u64,
}

impl ReverseValidation {
    /// Allocate ledgers from the plan's pre-admitted job allowance.
    pub(super) fn new(
        plan: Arc<CrossTierPlan>,
        pool: QuiescentGuard<ThreadPool>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            jobs: (0..plan.selection.workers).map(|_| None).collect(),
            outputs: Vec::with_capacity(plan.selection.ranges),
            plan,
            pool,
            stop,
            planner: None,
            ranges: None,
            submitted: 0,
            collected: 0,
            failure: None,
            #[cfg(feature = "profiling")]
            started: None,
            #[cfg(feature = "profiling")]
            wall_nanos: 0,
        }
    }

    /// Pool reservation accepts each finite job. Completion slots are the
    /// authoritative result through poison/shutdown; the parent retains this
    /// ledger, root leases and admission until every accepted read settles.
    pub(super) async fn execute(&mut self) -> RuntimeOrFatalResult<BuildOutcome<()>> {
        #[cfg(feature = "profiling")]
        self.started.get_or_insert_with(Instant::now);
        if self.ranges.is_none() && !self.stop.load(AtomicOrdering::Acquire) {
            if self.planner.is_none() {
                self.planner = Some(self.pool.submit_async(plan_ranges(
                    self.plan.selection.cold.clone(),
                    self.plan.selection.admission.as_ref().unwrap().clone(),
                    self.plan.selection.ranges,
                    self.stop.clone(),
                )));
            }
            self.collect_planner().await;
        }
        while self.failure.is_none()
            && !self.stop.load(AtomicOrdering::Acquire)
            && self.collected < self.ranges.as_ref().map_or(0, |ranges| ranges.len())
        {
            let ranges = self.ranges.as_ref().unwrap();
            while self.submitted < ranges.len() && self.submitted - self.collected < self.jobs.len()
            {
                let part = self.submitted;
                let slot = part % self.jobs.len();
                self.jobs[slot] = Some(self.pool.submit_async(run_reverse(
                    self.plan.clone(),
                    ranges.clone(),
                    part,
                    self.stop.clone(),
                )));
                self.submitted += 1;
            }
            self.collect_next().await;
        }
        if self.failure.is_some() || self.stop.load(AtomicOrdering::Acquire) {
            self.settle().await?;
            return Ok(BuildOutcome::Stopped);
        }
        if self
            .outputs
            .iter()
            .any(|summary| summary.summary.conflict.is_some())
        {
            self.plan.hot.inhibit_construction();
        }
        #[cfg(feature = "profiling")]
        {
            self.wall_nanos = self
                .started
                .map_or(0, |started| started.elapsed().as_nanos() as u64);
        }
        Ok(BuildOutcome::Complete(()))
    }

    async fn collect_planner(&mut self) {
        let result = self.planner.as_ref().unwrap().wait_take_result().await;
        self.planner = None;
        match result
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .and_then(|r| r)
        {
            Ok(BuildOutcome::Complete(ranges)) => self.ranges = Some(Arc::new(ranges)),
            Ok(BuildOutcome::Stopped) => debug_assert!(
                self.stop.load(AtomicOrdering::Acquire),
                "cold range planner returned Stopped without a stop request"
            ),
            Err(error) => self.record(error),
        }
    }

    async fn collect_next(&mut self) {
        let slot = self.collected % self.jobs.len();
        let result = self.jobs[slot].as_ref().unwrap().wait_take_result().await;
        self.jobs[slot] = None;
        self.collected += 1;
        match result
            .map_err(|e| e.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .and_then(|r| r)
        {
            Ok(BuildOutcome::Complete(summary)) => self.outputs.push(summary),
            Ok(BuildOutcome::Stopped) => debug_assert!(
                self.stop.load(AtomicOrdering::Acquire),
                "reverse validation worker returned Stopped without a stop request"
            ),
            Err(error) => self.record(error),
        }
    }

    fn record(&mut self, error: RuntimeOrFatalError) {
        self.stop.store(true, AtomicOrdering::Release);
        self.failure = Some(match self.failure.take() {
            Some(old) => old.merge_cleanup(error),
            None => error,
        });
    }

    /// Resumable obligation drain. Cancellation keeps each slot until ready;
    /// reader leases release only after supervised async jobs finish their IO.
    pub(super) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.stop.store(true, AtomicOrdering::Release);
        if self.planner.is_some() {
            self.collect_planner().await;
        }
        while self.collected < self.submitted {
            self.collect_next().await;
        }
        self.failure.take().map_or(Ok(()), Err)
    }

    fn verify(
        &self,
        plan: &Arc<CrossTierPlan>,
    ) -> RuntimeOrFatalResult<Option<CrossTierDuplicate>> {
        let ranges = self
            .ranges
            .as_ref()
            .ok_or_else(|| execution_error("reverse ranges missing"))?;
        if self.stop.load(AtomicOrdering::Acquire)
            || !Arc::ptr_eq(&self.plan, plan)
            || self.planner.is_some()
            || self.failure.is_some()
            || self.outputs.len() != ranges.len()
            || self.collected != ranges.len()
            || self.submitted != ranges.len()
        {
            return Err(execution_error(
                "reverse coverage ledger incomplete or foreign",
            ));
        }
        let mut entries = 0usize;
        let mut conflict = None;
        for (part, (range, output)) in ranges.iter().zip(&self.outputs).enumerate() {
            plan.check_summary(&output.summary, part)?;
            if !output.exhausted
                || output.lower != range.lower
                || output.upper != range.upper
                || (part == 0 && range.lower.is_some())
                || (part + 1 == ranges.len() && range.upper.is_some())
                || (part > 0 && (range.lower.is_none() || ranges[part - 1].upper != range.lower))
                || (part + 1 < ranges.len() && range.upper.is_none())
                || matches!((&range.lower, &range.upper), (Some(lower), Some(upper)) if lower >= upper)
            {
                return Err(execution_error("reverse interval coverage mismatch"));
            }
            entries = entries
                .checked_add(output.summary.consumed)
                .ok_or_else(|| execution_error("reverse count overflow"))?;
            earliest(&mut conflict, output.summary.conflict);
        }
        if entries != plan.selection.cold.entries() {
            return Err(execution_error(
                "reverse total differs from sealed cold count",
            ));
        }
        Ok(conflict)
    }
}

#[derive(Clone, Copy, Default)]
struct HotProbe {
    rank: usize,
    first: Option<(usize, usize)>,
}

struct ReverseProbe {
    positions: Vec<usize>,
    matches: Vec<HotProbe>,
    first: bool,
    #[cfg(feature = "profiling")]
    max_sync_nanos: u64,
}

impl ReverseProbe {
    fn scratch_bytes(runs: usize) -> RuntimeOrFatalResult<usize> {
        runs.checked_mul(size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(LOOKUP_BATCH_ENTRIES * size_of::<HotProbe>()))
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .ok_or_else(|| execution_error("reverse lookup admission size overflow"))
    }

    fn new(runs: usize) -> Self {
        Self {
            positions: vec![0; runs],
            matches: vec![HotProbe::default(); LOOKUP_BATCH_ENTRIES],
            first: true,
            #[cfg(feature = "profiling")]
            max_sync_nanos: 0,
        }
    }

    /// Finish one borrowed batch between cooperative stop checks, yielding
    /// between pairs of runs to bound each synchronous interval to 256 probes.
    async fn probe(
        &mut self,
        runs: &SortedRuns,
        batch: &ColdBatch<'_>,
        stop: &AtomicBool,
    ) -> RuntimeOrFatalResult<BuildOutcome<u64>> {
        assert!(
            batch.len() <= LOOKUP_BATCH_ENTRIES,
            "reverse lookup batch capacity"
        );
        self.matches[..batch.len()].fill(HotProbe::default());
        let mut comparisons = 0;
        #[cfg(feature = "profiling")]
        let mut started = Instant::now();
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        for (run_index, run) in runs.runs().iter().enumerate() {
            for (index, found) in self.matches[..batch.len()].iter_mut().enumerate() {
                let entries = run.entries();
                let cold_entry = batch.entry(index)?;
                let cold_key = cold_entry.key;
                let position = hot_lower_bound(
                    entries.len(),
                    self.positions[run_index],
                    self.first && index == 0,
                    |position| {
                        comparisons += 1;
                        cold_key.cmp_bytes(entries[position].key.as_bytes()).is_gt()
                    },
                );
                self.positions[run_index] = position;
                found.rank += position;
                if let Some(entry) = entries.get(position) {
                    comparisons += 1;
                    if cold_key.cmp_bytes(entry.key.as_bytes()).is_eq() {
                        let candidate = (run_index, position);
                        if found.first.is_none_or(|old| {
                            runs.compare(candidate, old)
                                .is_some_and(|order| order.is_lt())
                        }) {
                            found.first = Some(candidate);
                        }
                    }
                }
            }
            if run_index % 2 == 1 && run_index + 1 < runs.runs().len() {
                #[cfg(feature = "profiling")]
                {
                    self.max_sync_nanos =
                        self.max_sync_nanos.max(started.elapsed().as_nanos() as u64);
                }
                yield_now().await;
                #[cfg(feature = "profiling")]
                {
                    started = Instant::now();
                }
            }
        }
        self.first = false;
        #[cfg(feature = "profiling")]
        {
            self.max_sync_nanos = self.max_sync_nanos.max(started.elapsed().as_nanos() as u64);
        }
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        Ok(BuildOutcome::Complete(comparisons))
    }
}

/// Join exact summaries before selecting any duplicate diagnostics.
pub(super) fn complete<'a>(
    plan: &Arc<PreparedMerge>,
    cross: Option<&Arc<CrossTierPlan>>,
    summaries: impl Iterator<Item = Option<&'a CrossTierSummary>>,
    reverse: Option<&ReverseValidation>,
) -> RuntimeOrFatalResult<Result<CrossTierCompletion, CrossTierDuplicate>> {
    if reverse.is_some() != cross.is_some_and(|cross| cross.is_reverse()) {
        return Err(execution_error(
            "cross-tier reverse authority direction mismatch",
        ));
    }
    let mut count = 0;
    let mut conflict = None;
    #[cfg(feature = "profiling")]
    let mut measurements = ColdHotMeasurements::default();
    #[cfg(feature = "profiling")]
    let mut span = None;
    for (partition, summary) in summaries.enumerate() {
        if partition >= plan.partitions() {
            return Err(execution_error("unexpected cross-tier hot partition"));
        }
        match (cross, summary) {
            (None, None) => (),
            (Some(cross), None) if cross.is_reverse() => (),
            (Some(cross), Some(summary)) if !cross.is_reverse() => {
                cross.check_summary(summary, partition)?;
                if summary.consumed != plan.range(partition).len() {
                    return Err(execution_error("incomplete cross-tier hot coverage"));
                }
                earliest(&mut conflict, summary.conflict);
                #[cfg(feature = "profiling")]
                {
                    measurements.add(summary.measurements);
                    extend_span(&mut span, summary.span);
                }
            }
            _ => return Err(execution_error("cross-tier coverage direction mismatch")),
        }
        count += 1;
    }
    if count != plan.partitions() {
        return Err(execution_error("missing cross-tier hot partitions"));
    }
    let coverage = if let Some(cross) = cross {
        if !Arc::ptr_eq(&cross.hot, plan) {
            return Err(execution_error("foreign cross-tier prepared input"));
        }
        match cross.selection.method {
            ValidationMethod::Empty => Some(CrossTierCoverage::EmptyTiers),
            ValidationMethod::Reverse => {
                let reverse =
                    reverse.ok_or_else(|| execution_error("missing reverse completion"))?;
                let result = reverse.verify(cross)?;
                earliest(&mut conflict, result);
                #[cfg(feature = "profiling")]
                for summary in &reverse.outputs {
                    measurements.add(summary.summary.measurements);
                    extend_span(&mut span, summary.summary.span);
                }
                Some(CrossTierCoverage::ColdRanges {
                    ranges: reverse.outputs.len(),
                    entries: cross.selection.cold.entries(),
                })
            }
            _ => Some(CrossTierCoverage::HotPartitions {
                partitions: count,
                entries: plan.entries(),
            }),
        }
    } else {
        None
    };
    #[cfg(feature = "profiling")]
    if let Some(cross) = cross {
        measurements.hot_entries = plan.entries() as u64;
        measurements.cold_entries = cross.selection.cold.entries() as u64;
        measurements.wall_nanos = reverse.map_or_else(
            || span.map_or(0, |(start, end)| (end - start).as_nanos() as u64),
            |reverse| reverse.wall_nanos,
        );
        measurements.workers = if cross.selection.method == ValidationMethod::Empty {
            0
        } else {
            cross.selection.workers as u64
        };
        measurements.jobs = if cross.is_reverse() {
            reverse.map_or(0, |r| r.outputs.len())
        } else if cross.selection.method == ValidationMethod::Empty {
            0
        } else {
            count
        } as u64;
        measurements.peak = cross.peak.load(AtomicOrdering::Acquire);
        measurements.scratch_bytes =
            cross.selection.admission.as_ref().map_or(0, |a| a.bytes()) as u64;
        measurements.pool_pin_bytes = measurements.peak * DISK_TREE_BLOCK_SIZE as u64;
        match cross.selection.method {
            ValidationMethod::Forward => measurements.forward = 1,
            ValidationMethod::Ordered => measurements.ordered = 1,
            ValidationMethod::Reverse => measurements.reverse = 1,
            ValidationMethod::Empty => measurements.empty = 1,
        }
    }
    Ok(match conflict {
        Some(conflict) => Err(conflict),
        None => Ok(CrossTierCompletion {
            hot: plan.identity(),
            cold: cross.map(|cross| cross.cold_identity().clone()),
            hot_entries: plan.entries(),
            cold_entries: cross.map_or(0, |cross| cross.selection.cold.entries()),
            method: cross.map(|cross| cross.selection.method),
            coverage,
            #[cfg(feature = "profiling")]
            measurements,
        }),
    })
}

fn select_method(
    hot: usize,
    cold: usize,
    runs: usize,
    partitions: usize,
    height: u16,
) -> ValidationMethod {
    if hot == 0 || cold == 0 {
        return ValidationMethod::Empty;
    }
    let (h, c, r, p) = (hot as u128, cold as u128, runs as u128, partitions as u128);
    let disk_lookup = DISK_LOOKUP_COST * (u128::from(height) + 1);
    let forward = disk_lookup.saturating_mul(h);
    let reverse = SCAN_COST
        .saturating_mul(c)
        .saturating_add(RUN_LOOKUP_COST.saturating_mul(c).saturating_mul(r));
    let ordered = disk_lookup
        .saturating_mul(p)
        .saturating_add(SCAN_COST.saturating_mul(c))
        .saturating_add(COMPARE_COST.saturating_mul(h.saturating_add(c)));
    [
        (ordered, ValidationMethod::Ordered),
        (forward, ValidationMethod::Forward),
        (reverse, ValidationMethod::Reverse),
    ]
    .into_iter()
    .min()
    .unwrap_or_else(|| unreachable!("three validation methods"))
    .1
}

fn earliest(target: &mut Option<CrossTierDuplicate>, candidate: Option<CrossTierDuplicate>) {
    if let Some(candidate) = candidate
        && target.is_none_or(|old| candidate.hot_rank < old.hot_rank)
    {
        *target = Some(candidate);
    }
}

// The predicate is entry < query. Monotonic queries never retreat from start;
// galloping bounds the work across sparse gaps without scanning intervening keys.
fn hot_lower_bound(
    len: usize,
    start: usize,
    first: bool,
    mut less: impl FnMut(usize) -> bool,
) -> usize {
    let (mut low, mut high) = (start, len);
    if !first {
        if start == len || !less(start) {
            return start;
        }
        low = start + 1;
        let mut step = 1usize;
        loop {
            let bound = start.saturating_add(step).min(len);
            if bound == len || !less(bound) {
                high = bound;
                break;
            }
            low = bound + 1;
            step = step.saturating_mul(2);
        }
    }
    while low < high {
        let mid = low + (high - low) / 2;
        if less(mid) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

async fn run_reverse(
    plan: Arc<CrossTierPlan>,
    ranges: Arc<Vec<ColdRange>>,
    part: usize,
    stop: Arc<AtomicBool>,
) -> RuntimeOrFatalResult<BuildOutcome<ReverseSummary>> {
    #[cfg(feature = "profiling")]
    let (_active, started) = (ActiveReader::new(plan.clone()), Instant::now());
    #[cfg(test)]
    plan.hooks.before(part).await?;
    let range = &ranges[part];
    let mut reader = plan.reader();
    reader
        .seek(range.lower.as_ref().map_or(&[], |key| key.as_slice()))
        .await?;
    let mut probe = ReverseProbe::new(plan.hot.runs().runs().len());
    let mut summary = plan.summary(part);
    while let Some(batch) = reader.next_batch().await? {
        if stop.load(AtomicOrdering::Acquire) {
            return Ok(BuildOutcome::Stopped);
        }
        let available = batch.len();
        let batch = batch.before(range.upper.as_ref().map(|key| key.as_slice()))?;
        if batch.len() == 0 {
            break;
        }
        if let Some(lower) = &range.lower
            && batch.compare_key(0, lower)?.is_lt()
        {
            return Err(execution_error("reverse cursor precedes assigned range"));
        }
        let BuildOutcome::Complete(comparisons) =
            probe.probe(plan.hot.runs(), &batch, &stop).await?
        else {
            return Ok(BuildOutcome::Stopped);
        };
        #[cfg(not(feature = "profiling"))]
        let _ = comparisons;
        #[cfg(feature = "profiling")]
        {
            summary.measurements.comparisons += comparisons;
            summary.measurements.probes += (batch.len() * plan.hot.runs().runs().len()) as u64;
            summary.measurements.batches += 1;
        }
        for (index, found) in probe.matches[..batch.len()].iter().enumerate() {
            if let Some((run, position)) = found.first {
                earliest(
                    &mut summary.conflict,
                    Some(CrossTierDuplicate {
                        hot_rank: found.rank,
                        hot_row: plan.hot.runs().entry(run, position).unwrap().row_id,
                        cold_row: batch.row_id(index)?,
                    }),
                );
            }
        }
        summary.consumed += batch.len();
        if batch.len() < available {
            break;
        }
        yield_now().await;
    }
    #[cfg(feature = "profiling")]
    {
        summary.measurements.scanned = summary.consumed as u64;
        summary.measurements.comparisons += reader.comparisons;
        summary.measurements.node_reads = reader.node_reads;
        summary.measurements.max_sync_nanos = probe.max_sync_nanos;
        summary.measurements.worker_nanos = started.elapsed().as_nanos() as u64;
        summary.span = Some((started, Instant::now()));
    }
    #[cfg(test)]
    plan.hooks.after(part);
    Ok(BuildOutcome::Complete(ReverseSummary {
        summary,
        lower: range.lower.clone(),
        upper: range.upper.clone(),
        exhausted: true,
    }))
}

#[cfg(feature = "profiling")]
fn extend_span(span: &mut Option<(Instant, Instant)>, next: Option<(Instant, Instant)>) {
    if let Some((start, end)) = next {
        *span = Some(span.map_or((start, end), |(first, last)| {
            (first.min(start), last.max(end))
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::super::cold_reader::{scalar_lookup, test_collect};
    use super::super::complete_work;
    use super::*;
    use crate::buffer::{global_readonly_pool_scope, table_readonly_pool};
    use crate::catalog::{
        IndexID, IndexRef, IndexSlot, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
        StorageIndexKey, StorageIndexSpec, TableMetadata,
    };
    use crate::component::{ComponentRegistry, RegistryBuilder};
    use crate::conf::{ColdIndexBuildConfig, ThreadPoolConfig};
    use crate::error::{DataIntegrityError, ResourceError};
    use crate::file::{build_test_fs, table_file::MutableTableFile};
    use crate::id::{TableID, TrxID};
    use crate::index::build::disk_builder::{
        ColdBuildPolicy, ColdBuildProgress, DiskBuildAdmission, DiskBulkBuild,
    };
    use crate::index::build::fail_build_budget;
    use crate::index::build::merge::{
        CompletedPartition, MergeConsumption, MergePreparation, PartitionConsumer,
        PartitionMergeStream, test_runs,
    };
    use crate::index::build::{
        BuildEntries, DuplicateCheck, IndexBuildEntry, MemoryBudget, SortedRun,
    };
    use crate::index::{BTreeKey, SecondaryDiskTreeRuntime};
    use crate::memcmp::{MEM_CMP_KEY_INLINE, track_heap_drop};
    use crate::poison::EnginePoisoner;
    #[cfg(feature = "profiling")]
    use crate::profiling::HotExtractionMeasurements;
    use crate::runtime::thread_pool::ThreadPoolWorkers;
    use crate::value::ValKind;
    use error_stack::Report;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    #[derive(Default)]
    pub(super) struct Hooks {
        gate: Mutex<Option<flume::Receiver<()>>>,
        entered: Mutex<Option<flume::Sender<usize>>>,
        finished: Mutex<Option<flume::Sender<usize>>>,
        fail: AtomicBool,
    }

    impl Hooks {
        pub(super) async fn before(&self, part: usize) -> RuntimeOrFatalResult<()> {
            if let Some(entered) = &*self.entered.lock() {
                entered.send(part).unwrap();
            }
            if part == 0 {
                let gate = self.gate.lock().take();
                if let Some(gate) = gate {
                    gate.recv_async().await.unwrap();
                }
                if self.fail.load(AtomicOrdering::Acquire) {
                    return Err(Report::new(DataIntegrityError::InvalidPayload)
                        .change_context(RuntimeError::IndexAccess)
                        .into());
                }
            }
            Ok(())
        }

        pub(super) fn after(&self, part: usize) {
            if let Some(finished) = &*self.finished.lock() {
                finished.send(part).unwrap();
            }
        }
    }

    struct PoolScope(ComponentRegistry);

    impl Drop for PoolScope {
        fn drop(&mut self) {
            assert!(!self.0.shutdown_all().is_degraded());
        }
    }

    struct Check(Option<Arc<CrossTierPlan>>);

    impl PartitionConsumer for Check {
        type Output = Option<CrossTierSummary>;

        async fn consume(
            &self,
            mut stream: PartitionMergeStream,
        ) -> RuntimeOrFatalResult<BuildOutcome<CompletedPartition<Self::Output>>> {
            let (_, part) = stream.identity();
            let mut cursor = self
                .0
                .as_ref()
                .and_then(|cross| CrossTierCursor::new(cross.clone(), part));
            loop {
                let batch = match stream.next_batch()? {
                    BuildOutcome::Complete(Some(batch)) => batch,
                    BuildOutcome::Complete(None) => break,
                    BuildOutcome::Stopped => return Ok(BuildOutcome::Stopped),
                };
                if let Some(cursor) = &mut cursor
                    && let BuildOutcome::Stopped =
                        cursor.validate(&batch, &AtomicBool::new(false)).await?
                {
                    return Ok(BuildOutcome::Stopped);
                }
                yield_now().await;
            }
            stream.finish(cursor.map(CrossTierCursor::finish))
        }
    }

    /// Pin the internal policy while keeping production admission and binding.
    pub(crate) fn forced_selection(
        cold: &ColdReadAuthority,
        plan: &PreparedMerge,
        method: ValidationMethod,
    ) -> CrossTierSelection {
        let mut selection = CrossTierSelection::select(cold, plan.runs(), plan.workers()).unwrap();
        selection.method = method;
        selection
    }

    /// Exercise actual persisted construction and keep its filesystem and pool live.
    pub(crate) async fn with_cold<T>(
        entries: Vec<IndexBuildEntry>,
        action: impl AsyncFnOnce(ColdReadAuthority) -> T,
    ) -> T {
        with_runs(entries, 1, action).await
    }

    async fn pool() -> PoolScope {
        let mut builder = RegistryBuilder::new();
        builder.build::<EnginePoisoner>(()).await.unwrap();
        builder
            .build::<ThreadPool>(ThreadPoolConfig::default().worker_threads(2))
            .await
            .unwrap();
        builder.build::<ThreadPoolWorkers>(()).await.unwrap();
        PoolScope(builder.finish())
    }

    async fn with_runs<T>(
        entries: Vec<IndexBuildEntry>,
        groups: usize,
        action: impl AsyncFnOnce(ColdReadAuthority) -> T,
    ) -> T {
        let scope = pool().await;
        let metadata = Arc::new(
            TableMetadata::try_new(
                vec![StorageColumnSpec::new(
                    ValKind::VarByte,
                    StorageColumnFlags::empty(),
                )],
                vec![StorageIndexSpec::new(
                    vec![StorageIndexKey::new(0)],
                    StorageIndexFlags::UK,
                )],
            )
            .unwrap(),
        );
        let (_temp, fs) = build_test_fs();
        let id = TableID::new(501);
        let table = fs.create_table_file(id, metadata.clone(), false).unwrap();
        let (table, old) = table.commit(TrxID::new(1), false).await.unwrap();
        drop(old);
        let global = global_readonly_pool_scope(32 * 1024 * 1024);
        let disk_pool = table_readonly_pool(&global, id, &table);
        let guard = disk_pool.create_base_guard();
        let file = MutableTableFile::fork(
            &table,
            fs.background_writes(),
            disk_pool.global_pool().clone(),
            guard.clone(),
        );
        let admission = DiskBuildAdmission::new(
            ColdBuildPolicy::new(ColdIndexBuildConfig::default(), 2).unwrap(),
            true,
        )
        .unwrap();
        let budget = admission.budget.clone();
        let mut inputs: Vec<_> = (0..groups).map(|_| BuildEntries::new(&budget)).collect();
        for (ordinal, entry) in entries.into_iter().enumerate() {
            let input = &mut inputs[ordinal % groups];
            if entry.key.as_bytes().len() > MEM_CMP_KEY_INLINE {
                input
                    .payload
                    .grow(entry.key.as_bytes().len(), "test cold key")
                    .unwrap();
            }
            input.entries.push(entry, "test cold input").unwrap();
        }
        let runs: Vec<_> = inputs
            .into_iter()
            .enumerate()
            .filter_map(|(group, input)| {
                if input.entries.is_empty() {
                    return None;
                }
                let run = complete_work(SortedRun::finish_group(
                    input.entries,
                    input.payload,
                    DuplicateCheck::Collect,
                    group,
                    &AtomicBool::new(false),
                ));
                Some(Arc::new(run))
            })
            .collect();
        let weak: Vec<_> = runs.iter().map(Arc::downgrade).collect();
        let heap: Vec<_> = runs
            .iter()
            .flat_map(|run| run.entries())
            .filter(|entry| entry.key.as_bytes().len() > MEM_CMP_KEY_INLINE)
            .map(|entry| track_heap_drop(&entry.key))
            .collect();
        let runs = Arc::new(SortedRuns {
            _run_admission: None,
            runs,
            duplicates: DuplicateCheck::Collect,
            budget: budget.clone(),
            #[cfg(feature = "profiling")]
            measurements: HotExtractionMeasurements::default(),
        });
        let mut cold = ColdBuildProgress::default();
        let build = cold.start(
            DiskBulkBuild::from_runs(
                file,
                runs,
                admission,
                scope.0.dependency::<ThreadPool>(),
                scope.0.dependency::<EnginePoisoner>(),
                TrxID::new(2),
            )
            .unwrap(),
        );
        let completed = build.build().await.unwrap();
        let runtime = SecondaryDiskTreeRuntime::new(
            IndexSlot::new(0),
            metadata,
            table,
            disk_pool.global_pool().clone(),
        )
        .unwrap();
        let sealed = cold
            .seal(
                completed,
                id,
                IndexRef::new(IndexID::new(0), IndexSlot::new(0)),
                runtime,
                guard,
            )
            .unwrap();
        assert!(
            heap.iter()
                .all(|dropped| dropped.load(AtomicOrdering::Acquire)),
            "outlined allocations must be freed independently of budget counters"
        );
        assert!(
            weak.iter().all(|run| run.upgrade().is_none()),
            "sealing must release every cold run owner"
        );
        assert_eq!(
            budget.used(),
            0,
            "construction ledgers must drop before hot admission"
        );
        let result = action(sealed.unique_reader().unwrap()).await;
        drop(cold);
        assert_eq!(budget.used(), 0, "all validation admission released");
        result
    }

    // Lower bounds count strictly smaller keys in every complete run. Their sum is
    // the first global hot rank, even when equal keys span several hot partitions.
    fn probe_hot_runs(runs: &SortedRuns, key: &[u8]) -> (Option<(usize, RowID)>, u64) {
        let mut rank = 0;
        let mut first: Option<(usize, usize)> = None;
        let mut comparisons = 0;
        for (run_index, run) in runs.runs().iter().enumerate() {
            let position = run.entries().partition_point(|entry| {
                comparisons += 1;
                entry.key.as_bytes() < key
            });
            rank += position;
            if let Some(entry) = run.entries().get(position) {
                comparisons += 1;
                if entry.key.as_bytes() == key {
                    let candidate = (run_index, position);
                    if first.is_none_or(|old| {
                        runs.compare(candidate, old)
                            .is_some_and(|order| order.is_lt())
                    }) {
                        first = Some(candidate);
                    }
                }
            }
        }
        (
            first.map(|(run, position)| (rank, runs.entry(run, position).unwrap().row_id)),
            comparisons,
        )
    }

    fn entries(values: impl IntoIterator<Item = u32>, width: usize) -> Vec<IndexBuildEntry> {
        values
            .into_iter()
            .map(|value| IndexBuildEntry {
                key: key(value, width),
                row_id: RowID::new(u64::from(value) + 100_000),
            })
            .collect()
    }

    fn key(value: u32, width: usize) -> BTreeKey {
        let mut bytes = value.to_be_bytes().to_vec();
        bytes.resize(width.max(4), 7);
        BTreeKey::from(bytes.as_slice())
    }

    async fn parallel_validation(
        cold: &ColdReadAuthority,
        pool: &QuiescentGuard<ThreadPool>,
        method: ValidationMethod,
        budget: MemoryBudget,
    ) -> (MergePreparation, Arc<CrossTierPlan>) {
        let runs = test_runs(
            vec![(0..8192).map(|n| key(100_001 + n, 32)).collect()],
            DuplicateCheck::Collect,
            budget,
        );
        let mut selection = CrossTierSelection::select(cold, &runs, 2).unwrap();
        selection.method = method;
        assert!(selection.partitions > 1 && selection.ranges > 1);
        let mut preparation =
            MergePreparation::with_sizing(runs, pool.clone(), 2, selection.partitions, 128)
                .unwrap();
        let plan = preparation.execute().await.unwrap();
        (preparation, CrossTierPlan::bind(plan, selection).unwrap())
    }

    async fn check_method(
        cold: ColdReadAuthority,
        groups: Vec<Vec<BTreeKey>>,
        method: ValidationMethod,
        cold_oracle: &BTreeMap<Vec<u8>, RowID>,
    ) {
        let scope = pool().await;
        let pool = scope.0.dependency::<ThreadPool>();
        let runs = test_runs(
            groups,
            DuplicateCheck::Collect,
            MemoryBudget::new(32 * 1024 * 1024),
        );
        let mut oracle: Vec<_> = runs
            .runs()
            .iter()
            .enumerate()
            .flat_map(|(run, entries)| (0..entries.entries().len()).map(move |pos| (run, pos)))
            .collect();
        oracle.sort_by_key(|&(run, pos)| {
            (
                runs.entry(run, pos).unwrap().key.as_bytes(),
                runs.runs()[run].group_id,
                pos,
            )
        });
        let mut selection = CrossTierSelection::select(&cold, &runs, 2).unwrap();
        if selection.method != ValidationMethod::Empty {
            selection.method = method;
        }
        // Exercise cuts below the production tiny-work threshold as well.
        let parts = oracle.len().min(7);
        // Larger fixtures cross the lookup chunk boundary within a hot batch;
        // tiny fixtures retain coverage of partially filled merge batches.
        let batch_entries = if oracle.len() > LOOKUP_BATCH_ENTRIES {
            LOOKUP_BATCH_ENTRIES + 1
        } else {
            7
        };
        let mut prepare =
            MergePreparation::with_sizing(runs.clone(), pool.clone(), 2, parts, batch_entries)
                .unwrap();
        let plan = prepare.execute().await.unwrap();
        let cross = CrossTierPlan::bind(plan.clone(), selection).unwrap();
        let expected = oracle.iter().enumerate().find_map(|(rank, &(run, pos))| {
            let entry = runs.entry(run, pos).unwrap();
            cold_oracle
                .get(entry.key.as_bytes())
                .map(|&cold_row| CrossTierDuplicate {
                    hot_rank: rank,
                    hot_row: entry.row_id,
                    cold_row,
                })
        });
        let mut reverse = cross.is_reverse().then(|| {
            ReverseValidation::new(
                cross.clone(),
                pool.clone(),
                Arc::new(AtomicBool::new(false)),
            )
        });
        if let Some(reverse) = &mut reverse {
            complete_work(reverse.execute().await.unwrap());
        }
        let mut merge = MergeConsumption::new(plan.clone(), pool, Check(Some(cross.clone())));
        let result = merge.execute().await.unwrap();
        let completed = complete(
            &plan,
            Some(&cross),
            result.outputs.iter().map(Option::as_ref),
            reverse.as_ref(),
        )
        .unwrap();
        assert_eq!(
            completed.as_ref().err().copied(),
            expected,
            "method={method:?}"
        );
        if let (Ok(completed), Ok(hot)) = (completed, result.validation) {
            completed.validate(&hot, Some(cold.identity())).unwrap();
        }
        assert!(
            complete(
                &plan,
                Some(&cross),
                result.outputs.iter().skip(1).map(Option::as_ref),
                reverse.as_ref()
            )
            .is_err()
                || plan.partitions() == 0
        );
    }

    /// Purpose: Preserve absolute ranks and provenance while reusing hot-run positions across borrowed leaf batches.
    /// Expected: Full and clipped ranges, uneven runs, duplicate hot keys and sparse gaps match an independently sorted hot oracle.
    #[test]
    fn reverse_batches_preserve_ranks_and_provenance() {
        smol::block_on(async {
            for width in [32, 512] {
                let values: Vec<_> = (0..2048).step_by(2).chain([10_000, 50_000]).collect();
                with_cold(entries(values.clone(), width), async |cold| {
                    let runs = test_runs(
                        vec![
                            (0..2048).map(|n| key(n, width)).collect(),
                            (0..512)
                                .flat_map(|n| [key(n * 4, width), key(n * 4, width)])
                                .collect(),
                            [127, 128, 129, 10_000, 49_999]
                                .into_iter()
                                .map(|n| key(n, width))
                                .collect(),
                        ],
                        DuplicateCheck::Collect,
                        MemoryBudget::new(16 * 1024 * 1024),
                    );
                    let mut oracle: Vec<_> = runs
                        .runs()
                        .iter()
                        .flat_map(|run| {
                            run.entries()
                                .iter()
                                .enumerate()
                                .map(move |(position, entry)| {
                                    (
                                        entry.key.as_bytes().to_vec(),
                                        run.group_id,
                                        position,
                                        entry.row_id,
                                    )
                                })
                        })
                        .collect();
                    oracle.sort_by(|a, b| (&a.0, a.1, a.2).cmp(&(&b.0, b.1, b.2)));
                    let admission = ColdReadAdmission::new(
                        &cold,
                        1,
                        1,
                        ReverseProbe::scratch_bytes(runs.runs().len()).unwrap(),
                    )
                    .unwrap();
                    for (lower, upper) in [(0, 253), (0, 255), (0, 257), (127, 50_001), (0, 50_001)]
                    {
                        let mut reader = ColdReader::new(cold.clone(), admission.clone());
                        reader.seek(key(lower, width).as_bytes()).await.unwrap();
                        let upper_key = key(upper, width);
                        let mut probe = ReverseProbe::new(runs.runs().len());
                        let mut seen = 0;
                        let expected_keys: Vec<_> = values
                            .iter()
                            .copied()
                            .filter(|&n| n >= lower && n < upper)
                            .collect();
                        while let Some(batch) = reader.next_batch().await.unwrap() {
                            let batch = batch.before(Some(upper_key.as_bytes())).unwrap();
                            if batch.len() == 0 {
                                break;
                            }
                            complete_work(
                                probe
                                    .probe(&runs, &batch, &AtomicBool::new(false))
                                    .await
                                    .unwrap(),
                            );
                            for (index, found) in probe.matches[..batch.len()].iter().enumerate() {
                                let query = key(expected_keys[seen], width);
                                assert!(
                                    batch.compare_key(index, query.as_bytes()).unwrap().is_eq()
                                );
                                let rank = oracle
                                    .partition_point(|entry| entry.0.as_slice() < query.as_bytes());
                                assert_eq!(
                                    found.rank, rank,
                                    "width={width}, lower={lower}, seen={seen}"
                                );
                                let row = oracle
                                    .get(rank)
                                    .filter(|entry| entry.0 == query.as_bytes())
                                    .map(|entry| entry.3);
                                assert_eq!(
                                    found.first.map(|(run, position)| runs
                                        .entry(run, position)
                                        .unwrap()
                                        .row_id),
                                    row
                                );
                                seen += 1;
                            }
                        }
                        assert_eq!(seen, expected_keys.len());
                    }
                    let mut reader = ColdReader::new(cold, admission);
                    reader.seek(&[]).await.unwrap();
                    let batch = reader.next_batch().await.unwrap().unwrap();
                    assert_eq!(
                        ReverseProbe::new(runs.runs().len())
                            .probe(&runs, &batch, &AtomicBool::new(true))
                            .await
                            .unwrap(),
                        BuildOutcome::Stopped
                    );
                })
                .await;
            }
        });
    }

    /// Purpose: Bound monotonic resident searches across dense neighborhoods and distant gaps.
    /// Expected: Galloping returns independent lower bounds, uses fewer comparisons for nearby keys, and scratch arithmetic rejects overflow.
    #[test]
    fn monotonic_hot_search_reuses_positions() {
        let entries: Vec<_> = (0..65_536u64).map(|n| n * 2).collect();
        let queries: Vec<_> = (0..4096)
            .chain([65_534, 65_535, 65_536, 131_070, 131_071, u64::MAX])
            .collect();
        let mut position = 0;
        let mut batch_comparisons = 0;
        let mut scalar_comparisons = 0;
        for (index, query) in queries.iter().enumerate() {
            position = hot_lower_bound(entries.len(), position, index == 0, |pos| {
                batch_comparisons += 1;
                entries[pos] < *query
            });
            let expected = entries.partition_point(|entry| {
                scalar_comparisons += 1;
                entry < query
            });
            assert_eq!(position, expected, "query={query}");
        }
        assert!(
            batch_comparisons < scalar_comparisons / 2,
            "batch={batch_comparisons}, scalar={scalar_comparisons}"
        );
        assert!(ReverseProbe::scratch_bytes(usize::MAX).is_err());
        assert_eq!(hot_lower_bound(0, 0, true, |_| unreachable!()), 0);
    }

    /// Purpose: Compare every validation direction with independent membership and merge-rank oracles.
    /// Expected: Empty tiers, wide keys, interleaving, equal hot keys and distant ranges produce the same first conflict and exact coverage.
    #[test]
    fn methods_match_complete_membership_oracle() {
        smol::block_on(async {
            for (cold, groups, width) in [
                (vec![], vec![vec![0, 2], vec![1, 3]], 4),
                (vec![1, 2], vec![], 128),
                (vec![1, 3, 8], vec![vec![0, 2, 4], vec![6, 10]], 128),
                (vec![2, 7], vec![vec![1, 2, 2, 2, 9], vec![2, 2, 2, 7]], 256),
                (
                    (0..6000).step_by(2).collect(),
                    vec![vec![1, 5998], vec![3, 5999]],
                    32,
                ),
                (
                    vec![123],
                    vec![(0..2048).collect(), (2048..4096).collect()],
                    4,
                ),
            ] {
                let cold_oracle: BTreeMap<_, _> = cold
                    .iter()
                    .map(|&value| {
                        (
                            key(value, width).as_bytes().to_vec(),
                            RowID::new(u64::from(value) + 100_000),
                        )
                    })
                    .collect();
                with_cold(entries(cold, width), async |cold| {
                    for method in [
                        ValidationMethod::Forward,
                        ValidationMethod::Ordered,
                        ValidationMethod::Reverse,
                    ] {
                        check_method(
                            cold.clone(),
                            groups
                                .iter()
                                .map(|group| group.iter().map(|&value| key(value, width)).collect())
                                .collect(),
                            method,
                            &cold_oracle,
                        )
                        .await;
                    }
                })
                .await;
            }
        });
    }

    /// Purpose: Distinguish intentionally skipped validation from required validation with either or both tiers empty.
    /// Expected: Empty-tier evidence retains the exact cold identity without reader admission; skipped evidence cannot authorize a required cold binding.
    #[test]
    fn required_empty_validation_keeps_binding_without_admission() {
        smol::block_on(async {
            let scope = pool().await;
            let pool = scope.0.dependency::<ThreadPool>();
            for (cold_values, hot_values) in
                [(vec![], vec![1, 3]), (vec![0, 2], vec![]), (vec![], vec![])]
            {
                with_cold(entries(cold_values, 64), async |cold| {
                    // Required-empty validation must succeed even when admitting
                    // persisted readers would fail. Skipped validation also has no IO.
                    fail_build_budget(cold.budget(), "cross-tier readers and jobs");
                    for required in [false, true] {
                        let runs = test_runs(
                            vec![hot_values.iter().map(|&value| key(value, 64)).collect()],
                            DuplicateCheck::Collect,
                            MemoryBudget::new(4 * 1024 * 1024),
                        );
                        let selection =
                            required.then(|| CrossTierSelection::select(&cold, &runs, 2).unwrap());
                        if let Some(selection) = &selection {
                            assert_eq!(selection.method, ValidationMethod::Empty);
                            assert!(selection.admission.is_none());
                            assert!(selection.cold.identity().matches(cold.identity()));
                        }
                        let mut preparation = if let Some(selection) = &selection {
                            MergePreparation::with_sizing(
                                runs,
                                pool.clone(),
                                selection.workers,
                                selection.partitions,
                                128,
                            )
                            .unwrap()
                        } else {
                            MergePreparation::new(runs, pool.clone(), 2).unwrap()
                        };
                        let plan = preparation.execute().await.unwrap();
                        let cross = selection
                            .map(|selection| CrossTierPlan::bind(plan.clone(), selection).unwrap());
                        let mut merge =
                            MergeConsumption::new(plan.clone(), pool.clone(), Check(cross.clone()));
                        let result = merge.execute().await.unwrap();
                        let completion = complete(
                            &plan,
                            cross.as_ref(),
                            result.outputs.iter().map(Option::as_ref),
                            None,
                        )
                        .unwrap()
                        .unwrap();
                        let hot = result.validation.unwrap();
                        let binding = Some((cold.identity(), cold.entries()));
                        if required {
                            assert_eq!(completion.method, Some(ValidationMethod::Empty));
                            assert_eq!(completion.coverage, Some(CrossTierCoverage::EmptyTiers));
                            assert!(
                                cross
                                    .as_ref()
                                    .unwrap()
                                    .cold_identity()
                                    .matches(cold.identity())
                            );
                            completion.validate(&hot, Some(cold.identity())).unwrap();
                            completion.validate_cold(binding).unwrap();
                            assert!(completion.validate_cold(None).is_err());
                        } else {
                            assert!(completion.method.is_none() && completion.coverage.is_none());
                            completion.validate(&hot, None).unwrap();
                            completion.validate_cold(None).unwrap();
                            assert!(completion.validate(&hot, Some(cold.identity())).is_err());
                            assert!(completion.validate_cold(binding).is_err());
                        }
                        assert_eq!(
                            cold.budget().used(),
                            0,
                            "empty or skipped validation admits no reader"
                        );
                    }
                })
                .await;
            }
        });
    }

    /// Purpose: Keep the selector deterministic across ratio extremes, ties and maximal counts.
    /// Expected: Empty inputs skip IO; anchor ratios choose the intended direction and arithmetic never wraps.
    #[test]
    fn selector_ratios_and_overflow() {
        assert_eq!(select_method(0, 42, 1, 0, 0), ValidationMethod::Empty);
        assert_eq!(select_method(42, 0, 1, 1, 0), ValidationMethod::Empty);
        assert_eq!(
            select_method(10, 1_000_000, 1, 1, 0),
            ValidationMethod::Forward
        );
        assert_eq!(
            select_method(1_000_000, 10, 1, 8, 0),
            ValidationMethod::Reverse
        );
        assert_eq!(
            select_method(100_000, 100_000, 4, 8, 0),
            ValidationMethod::Ordered
        );
        assert_eq!(select_method(2, 12, 1, 1, 0), ValidationMethod::Ordered);
        assert_eq!(select_method(11, 40, 1, 11, 0), ValidationMethod::Forward);
        assert_eq!(select_method(500, 9500, 2, 8, 0), ValidationMethod::Forward);
        assert_eq!(select_method(500, 9500, 2, 8, 1), ValidationMethod::Ordered);
        assert_eq!(select_method(300, 9700, 2, 8, 1), ValidationMethod::Forward);
        assert_eq!(
            select_method(usize::MAX, usize::MAX, usize::MAX, usize::MAX, 0),
            ValidationMethod::Forward
        );
    }

    /// Purpose: Drain reverse-validation workers after cancellation while preserving a late source failure.
    /// Expected: Stop-only settlement succeeds, a gated integrity failure retains its cause, and stopped validation never certifies coverage.
    #[test]
    fn reverse_cancellation_preserves_source_failures() {
        use futures::FutureExt;
        use futures::future::{Either, select};
        smol::block_on(async {
            with_cold(entries((0..8192).step_by(2), 32), async |cold| {
                for fail in [false, true] {
                    let scope = pool().await;
                    let pool = scope.0.dependency::<ThreadPool>();
                    let budget = MemoryBudget::new(16 * 1024 * 1024);
                    let (preparation, cross) = parallel_validation(
                        &cold,
                        &pool,
                        ValidationMethod::Reverse,
                        budget.clone(),
                    )
                    .await;
                    let (entered, entering) = flume::unbounded();
                    let (release, gate) = flume::bounded(1);
                    *cross.hooks.entered.lock() = Some(entered);
                    *cross.hooks.gate.lock() = Some(gate);
                    cross.hooks.fail.store(fail, AtomicOrdering::Release);
                    let mut reverse = ReverseValidation::new(
                        cross.clone(),
                        pool,
                        Arc::new(AtomicBool::new(false)),
                    );
                    let mut execute = Box::pin(reverse.execute());
                    let admitted = Box::pin(async {
                        let mut parts = [
                            entering.recv_async().await.unwrap(),
                            entering.recv_async().await.unwrap(),
                        ];
                        parts.sort_unstable();
                        assert_eq!(parts, [0, 1]);
                    });
                    assert!(matches!(
                        select(execute.as_mut(), admitted).await,
                        Either::Right(((), _))
                    ));
                    drop(execute);
                    assert!(reverse.settle().now_or_never().is_none());
                    release.send(()).unwrap();
                    match reverse.settle().await {
                        Ok(()) => assert!(!fail),
                        Err(RuntimeOrFatalError::Runtime(error)) if fail => {
                            assert!(error.contains::<DataIntegrityError>());
                            assert!(!format!("{error:?}").contains("secondary cleanup failure"));
                        }
                        result => panic!("unexpected reverse settlement: {result:?}"),
                    }
                    assert_eq!((reverse.submitted, reverse.collected), (2, 2));
                    assert!(reverse.jobs.iter().all(Option::is_none));
                    assert!(reverse.verify(&cross).is_err());
                    drop(reverse);
                    drop(cross);
                    drop(preparation);
                    assert_eq!(budget.used(), 0);
                }
            })
            .await;
        });
    }

    /// Purpose: Keep all methods parallel below the construction partition target, including cold keys outside the hot interval.
    /// Expected: A second job completes while the first is gated, accepted work survives observer loss, and integrity errors retain their typed cause after drain.
    #[test]
    fn validation_jobs_progress_and_settle_independently() {
        use futures::FutureExt;
        smol::block_on(async {
            with_cold(entries((0..8192).step_by(2), 32), async |cold| {
                for method in [
                    ValidationMethod::Forward,
                    ValidationMethod::Ordered,
                    ValidationMethod::Reverse,
                ] {
                    for fail in [false, true] {
                        let scope = pool().await;
                        let pool = scope.0.dependency::<ThreadPool>();
                        let (_preparation, cross) = parallel_validation(
                            &cold,
                            &pool,
                            method,
                            MemoryBudget::new(16 * 1024 * 1024),
                        )
                        .await;
                        let plan = cross.hot.clone();
                        let (entered, entering) = flume::unbounded();
                        let (finished, finishing) = flume::unbounded();
                        let (release, gate) = flume::bounded(1);
                        *cross.hooks.entered.lock() = Some(entered);
                        *cross.hooks.finished.lock() = Some(finished);
                        *cross.hooks.gate.lock() = Some(gate);
                        cross.hooks.fail.store(fail, AtomicOrdering::Release);
                        let inspect = async {
                            let mut entered = vec![
                                entering.recv_async().await.unwrap(),
                                entering.recv_async().await.unwrap(),
                            ];
                            entered.sort_unstable();
                            assert_eq!(entered, [0, 1], "{method:?}");
                            assert_eq!(
                                finishing.recv_async().await.unwrap(),
                                1,
                                "first job remains paused"
                            );
                            release.send(()).unwrap();
                        };
                        if method == ValidationMethod::Reverse {
                            let mut reverse = ReverseValidation::new(
                                cross.clone(),
                                pool.clone(),
                                Arc::new(AtomicBool::new(false)),
                            );
                            assert!(reverse.execute().now_or_never().is_none());
                            let (result, ()) = futures::join!(reverse.execute(), inspect);
                            if fail {
                                let RuntimeOrFatalError::Runtime(error) = result.unwrap_err()
                                else {
                                    panic!("integrity error changed domain")
                                };
                                assert!(error.contains::<DataIntegrityError>());
                            } else {
                                complete_work(result.unwrap());
                                assert_eq!(reverse.verify(&cross).unwrap(), None);
                            }
                            assert!(reverse.jobs.iter().all(Option::is_none));
                        } else {
                            let mut merge = MergeConsumption::new(
                                plan.clone(),
                                pool,
                                Check(Some(cross.clone())),
                            );
                            assert!(merge.execute().now_or_never().is_none());
                            let (result, ()) = futures::join!(merge.execute(), inspect);
                            if fail {
                                let RuntimeOrFatalError::Runtime(error) = result.err().unwrap()
                                else {
                                    panic!("integrity error changed domain")
                                };
                                assert!(error.contains::<DataIntegrityError>());
                            } else {
                                let result = result.unwrap();
                                assert!(
                                    complete(
                                        &plan,
                                        Some(&cross),
                                        result.outputs.iter().map(Option::as_ref),
                                        None
                                    )
                                    .unwrap()
                                    .is_ok()
                                );
                            }
                        }
                    }
                }
            })
            .await;
        });
    }

    /// Purpose: Prevent forged reverse coverage and resource failures from authorizing a ready tree.
    /// Expected: Missing, foreign, repeated, partial and overlapping intervals fail before duplicates; bounded admission failure releases its reservation.
    #[test]
    fn reverse_evidence_and_reader_admission_are_checked() {
        smol::block_on(async {
            with_cold(entries(0..4096, 128), async |cold| {
                let scope = pool().await;
                let pool = scope.0.dependency::<ThreadPool>();
                let runs = test_runs(
                    vec![vec![key(7, 128), key(100_000, 128)]],
                    DuplicateCheck::Collect,
                    MemoryBudget::new(4 * 1024 * 1024),
                );
                let mut selection = CrossTierSelection::select(&cold, &runs, 2).unwrap();
                selection.method = ValidationMethod::Reverse;
                let mut prepare =
                    MergePreparation::with_sizing(runs, pool.clone(), 2, 2, 1).unwrap();
                let plan = prepare.execute().await.unwrap();
                let cross = CrossTierPlan::bind(plan, selection).unwrap();
                let mut reverse =
                    ReverseValidation::new(cross.clone(), pool, Arc::new(AtomicBool::new(false)));
                complete_work(reverse.execute().await.unwrap());
                assert_eq!(reverse.verify(&cross).unwrap().unwrap().hot_rank, 0);
                let saved = reverse.outputs[0].summary.hot.clone();
                reverse.outputs[0].summary.hot = Arc::new(());
                assert!(reverse.verify(&cross).is_err());
                reverse.outputs[0].summary.hot = saved;
                reverse.outputs[0].exhausted = false;
                assert!(reverse.verify(&cross).is_err());
                reverse.outputs[0].exhausted = true;
                reverse.outputs[0].summary.consumed += 1;
                assert!(reverse.verify(&cross).is_err());
                reverse.outputs[0].summary.consumed -= 1;
                let end = reverse.outputs[0].upper.take();
                assert!(reverse.verify(&cross).is_err());
                reverse.outputs[0].upper = end;
                reverse.outputs.swap(0, 1);
                assert!(reverse.verify(&cross).is_err());
                reverse.outputs.swap(0, 1);
                let last = reverse.outputs.pop().unwrap();
                assert!(reverse.verify(&cross).is_err());
                reverse.outputs.push(last);
                assert!(reverse.verify(&cross).unwrap().is_some());
                drop(reverse);
                drop(cross);
                assert_eq!(cold.budget().used(), 0);
                fail_build_budget(cold.budget(), "cross-tier readers and jobs");
                let runs = test_runs(
                    vec![vec![key(8, 128)]],
                    DuplicateCheck::Collect,
                    MemoryBudget::new(4096),
                );
                let error = CrossTierSelection::select(&cold, &runs, 2).err().unwrap();
                let RuntimeOrFatalError::Runtime(error) = error else {
                    panic!("resource cause changed domain")
                };
                assert!(error.contains::<ResourceError>());
                assert_eq!(cold.budget().used(), 0);
            })
            .await;
        });
    }

    /// Purpose: Seal multiple outlined-key runs without retaining a flattened cold representation.
    /// Expected: Every run and original key allocation is gone before reads begin, while the completed private root preserves exact contents.
    #[test]
    fn seal_releases_multiple_resident_runs() {
        smol::block_on(async {
            with_runs(entries(0..4096, 512), 4, async |cold| {
                let admission = ColdReadAdmission::new(&cold, 1, 1, 0).unwrap();
                let mut reader = ColdReader::new(cold, admission);
                reader.seek(&[]).await.unwrap();
                let actual = test_collect(&mut reader).await;
                let expected: Vec<_> = (0..4096)
                    .map(|value| {
                        (
                            key(value, 512).as_bytes().to_vec(),
                            RowID::new(u64::from(value) + 100_000),
                        )
                    })
                    .collect();
                assert_eq!(actual, expected);
            })
            .await;
        });
    }

    /// Purpose: Reject hot coverage and installation evidence from equal-sized foreign inputs or the wrong direction.
    /// Expected: Every method binds both identities and distinct hot/cold counts; missing, repeated and partial hot summaries fail before installation.
    #[test]
    fn hot_summaries_and_installation_bind_both_inputs() {
        smol::block_on(async {
            with_cold(entries([0, 2, 4], 64), async |cold| {
                with_cold(entries([0, 2, 4], 64), async |foreign| {
                    let scope = pool().await;
                    let pool = scope.0.dependency::<ThreadPool>();
                    for method in [
                        ValidationMethod::Forward,
                        ValidationMethod::Ordered,
                        ValidationMethod::Reverse,
                    ] {
                        let runs = test_runs(
                            vec![vec![key(1, 64), key(3, 64)], vec![key(5, 64), key(7, 64)]],
                            DuplicateCheck::Collect,
                            MemoryBudget::new(4 * 1024 * 1024),
                        );
                        let mut selection = CrossTierSelection::select(&cold, &runs, 2).unwrap();
                        selection.method = method;
                        let mut preparation =
                            MergePreparation::with_sizing(runs, pool.clone(), 2, 2, 1).unwrap();
                        let plan = preparation.execute().await.unwrap();
                        let cross = CrossTierPlan::bind(plan.clone(), selection).unwrap();
                        let mut reverse = cross.is_reverse().then(|| {
                            ReverseValidation::new(
                                cross.clone(),
                                pool.clone(),
                                Arc::new(AtomicBool::new(false)),
                            )
                        });
                        if let Some(reverse) = &mut reverse {
                            complete_work(reverse.execute().await.unwrap());
                        }
                        let mut merge = MergeConsumption::new(
                            plan.clone(),
                            pool.clone(),
                            Check(Some(cross.clone())),
                        );
                        let mut result = merge.execute().await.unwrap();
                        let mut completion = complete(
                            &plan,
                            Some(&cross),
                            result.outputs.iter().map(Option::as_ref),
                            reverse.as_ref(),
                        )
                        .unwrap()
                        .ok()
                        .unwrap();
                        let hot = result.validation.unwrap();
                        completion.validate(&hot, Some(cold.identity())).unwrap();
                        completion
                            .validate_cold(Some((cold.identity(), 3)))
                            .unwrap();
                        assert!(
                            completion
                                .validate_cold(Some((cold.identity(), 4)))
                                .is_err()
                        );
                        assert!(
                            completion
                                .validate_cold(Some((foreign.identity(), 3)))
                                .is_err()
                        );
                        assert!(completion.validate(&hot, Some(foreign.identity())).is_err());
                        assert!(completion.validate_cold(None).is_err());
                        completion.method = Some(ValidationMethod::Empty);
                        assert!(completion.validate(&hot, Some(cold.identity())).is_err());
                        completion.method = Some(method);
                        completion.validate(&hot, Some(cold.identity())).unwrap();
                        if !cross.is_reverse() {
                            result.outputs[0].as_mut().unwrap().consumed -= 1;
                            assert!(
                                complete(
                                    &plan,
                                    Some(&cross),
                                    result.outputs.iter().map(Option::as_ref),
                                    None
                                )
                                .is_err()
                            );
                            result.outputs[0].as_mut().unwrap().consumed += 1;
                            result.outputs[0].as_mut().unwrap().partition = 1;
                            assert!(
                                complete(
                                    &plan,
                                    Some(&cross),
                                    result.outputs.iter().map(Option::as_ref),
                                    None
                                )
                                .is_err()
                            );
                            result.outputs[0].as_mut().unwrap().partition = 0;
                            result.outputs[0].as_mut().unwrap().cold = foreign.identity().clone();
                            assert!(
                                complete(
                                    &plan,
                                    Some(&cross),
                                    result.outputs.iter().map(Option::as_ref),
                                    None
                                )
                                .is_err()
                            );
                            result.outputs[0].as_mut().unwrap().cold = cold.identity().clone();
                            result.outputs[0].as_mut().unwrap().method = ValidationMethod::Reverse;
                            assert!(
                                complete(
                                    &plan,
                                    Some(&cross),
                                    result.outputs.iter().map(Option::as_ref),
                                    None
                                )
                                .is_err()
                            );
                        }
                    }
                })
                .await;
            })
            .await;
        });
    }

    /// Purpose: Reduce reader concurrency to fit the cold budget and reject a hot plan exceeding that admitted ceiling.
    /// Expected: One reader fits without exceeding the budget; rejected binding releases its entire reservation without reselection.
    #[test]
    fn reader_admission_reduces_concurrency_before_submission() {
        smol::block_on(async {
            with_cold(entries(0..4096, 64), async |cold| {
                let one =
                    ColdReadAdmission::new(&cold, 1, 4, ReverseProbe::scratch_bytes(1).unwrap())
                        .unwrap();
                let bytes = cold.budget().used();
                drop(one);
                let occupied = cold
                    .budget()
                    .reserve(
                        ColdIndexBuildConfig::default().max_scratch_bytes - bytes,
                        "other retained scratch",
                    )
                    .unwrap();
                let runs = test_runs(
                    vec![(8192..16_384).map(|value| key(value, 64)).collect()],
                    DuplicateCheck::Collect,
                    MemoryBudget::new(16 * 1024 * 1024),
                );
                let selection = CrossTierSelection::select(&cold, &runs, 2).unwrap();
                assert_eq!(selection.workers, 1);
                assert_eq!(
                    cold.budget().used(),
                    ColdIndexBuildConfig::default().max_scratch_bytes
                );
                let scope = pool().await;
                let pool = scope.0.dependency::<ThreadPool>();
                let mut preparation =
                    MergePreparation::with_sizing(runs, pool, 2, selection.partitions, 128)
                        .unwrap();
                let plan = preparation.execute().await.unwrap();
                assert_eq!(plan.workers(), 2);
                let error = CrossTierPlan::bind(plan, selection).err().unwrap();
                assert!(matches!(error, RuntimeOrFatalError::Runtime(_)));
                assert_eq!(
                    cold.budget().used(),
                    occupied.bytes(),
                    "rejected binding releases reader admission"
                );
                drop(occupied);
                assert_eq!(cold.budget().used(), 0);
            })
            .await;
        });
    }

    /// Purpose: Compare scalar and batched lookup work on clustered and sparse sorted keys in release builds.
    /// Expected: Both paths match independent hit/rank totals for each key width and hot-run count; timings remain diagnostic only.
    #[test]
    #[ignore = "explicit release batching comparison"]
    fn batched_validation_microbench() {
        use super::super::cold_reader::scalar_next;
        use std::hint::black_box;
        use std::time::Instant;
        smol::block_on(async {
            println!(
                "direction,shape,width,runs,method,repeat,nanos,comparisons,node_reads,batches"
            );
            for width in [8, 512] {
                for sparse in [false, true] {
                    let shape = if sparse { "sparse" } else { "clustered" };
                    let values: Vec<u32> = (0..2048)
                        .map(|n| if sparse { (n / 2) * 64 + n % 2 } else { n })
                        .collect();
                    let keys: Vec<_> = values.iter().map(|&value| key(value, width)).collect();
                    let expected_sum: usize = values
                        .iter()
                        .filter(|value| **value % 2 == 0)
                        .map(|value| (*value / 2) as usize)
                        .sum();
                    with_cold(entries((0..32_768).map(|n| n * 2), width), async |cold| {
                        let admission = ColdReadAdmission::new(&cold, 1, 1, 0).unwrap();
                        for repeat in 0..8 {
                            for batched in if repeat % 2 == 0 { [false, true] } else { [true, false] } {
                                let mut reader = ColdReader::new(cold.clone(), admission.clone());
                                let mut hits = 0;
                                let mut batches = 0;
                                let stop = AtomicBool::new(false);
                                let start = Instant::now();
                                if batched {
                                    let mut borrowed = [&[][..]; LOOKUP_BATCH_ENTRIES];
                                    let mut output = [None; LOOKUP_BATCH_ENTRIES];
                                    for chunk in keys.chunks(LOOKUP_BATCH_ENTRIES) {
                                        for (target, key) in borrowed.iter_mut().zip(chunk) { *target = key.as_bytes(); }
                                        complete_work(reader.lookup_batch(&borrowed[..chunk.len()], &mut output[..chunk.len()], &stop).await.unwrap());
                                        hits += black_box(&output[..chunk.len()]).iter().filter(|row| row.is_some()).count();
                                        batches += 1;
                                    }
                                } else {
                                    for key in &keys {
                                        hits += usize::from(black_box(scalar_lookup(&mut reader, key.as_bytes()).await.unwrap()).is_some());
                                    }
                                }
                                let elapsed = start.elapsed().as_nanos();
                                assert_eq!(hits, 1024);
                                println!("forward,{shape},{width},0,{},{repeat},{elapsed},{},{},{batches}", if batched {"batched"} else {"scalar"}, reader.comparisons, reader.node_reads);
                            }
                        }
                    }).await;
                    with_cold(entries(values.clone(), width), async |cold| {
                        for groups in [1, 4, 16] {
                            let runs = test_runs((0..groups).map(|group| (group..32_768).step_by(groups as usize).map(|n| key(n * 2, width)).collect()).collect(), DuplicateCheck::Collect, MemoryBudget::new(128 * 1024 * 1024));
                            let admission = ColdReadAdmission::new(&cold, 1, 1, ReverseProbe::scratch_bytes(runs.runs().len()).unwrap()).unwrap();
                            for repeat in 0..8 {
                                for batched in if repeat % 2 == 0 { [false, true] } else { [true, false] } {
                                    let mut reader = ColdReader::new(cold.clone(), admission.clone());
                                    reader.seek(&[]).await.unwrap();
                                    let mut probe = ReverseProbe::new(runs.runs().len());
                                    let (mut hits, mut ranks, mut comparisons, mut batches) = (0, 0, 0, 0);
                                    let start = Instant::now();
                                    if batched {
                                        while let Some(batch) = reader.next_batch().await.unwrap() {
                                            comparisons += complete_work(probe.probe(&runs, &batch, &AtomicBool::new(false)).await.unwrap());
                                            for found in black_box(&probe.matches[..batch.len()]) {
                                                if found.first.is_some() { hits += 1; ranks += found.rank; }
                                            }
                                            batches += 1;
                                        }
                                    } else {
                                        while let Some((key, _)) = scalar_next(&mut reader).await.unwrap() {
                                            let (found, work) = black_box(probe_hot_runs(&runs, &key));
                                            comparisons += work;
                                            if let Some((rank, _)) = found { hits += 1; ranks += rank; }
                                        }
                                    }
                                    let elapsed = start.elapsed().as_nanos();
                                    assert_eq!(hits, 1024);
                                    assert_eq!(ranks, expected_sum);
                                    println!("reverse,{shape},{width},{groups},{},{repeat},{elapsed},{comparisons},{},{batches}", if batched {"batched"} else {"scalar"}, reader.node_reads);
                                }
                            }
                        }
                    }).await;
                }
            }
        });
    }

    /// Purpose: Calibrate the production persisted-probe, ordered-cursor and complete-run search costs in release builds.
    /// Expected: Consumed results match independent hit counts; timings are reported without assertions and fixture construction stays outside timing.
    #[test]
    #[ignore = "explicit release calibration"]
    fn validation_microbench() {
        use std::hint::black_box;
        use std::time::Instant;
        smol::block_on(async {
            println!("path,entries,width,runs,height,operations,nanos,comparisons");
            for width in [8, 512] {
                for count in [64u32, 4096, 32_768] {
                    with_cold(entries((0..count).map(|n| n * 2), width), async |cold| {
                        let admission = ColdReadAdmission::new(&cold, 1, 1, 0).unwrap();
                        let mut reader = ColdReader::new(cold.clone(), admission);
                        let probes: Vec<_> = (0..2048)
                            .map(|n| key((n * 31) % (count * 2), width))
                            .collect();
                        // Warm the specific workload outside timing; constrained-cache
                        // public experiments separately account for pool misses.
                        for probe in &probes {
                            black_box(scalar_lookup(&mut reader, probe.as_bytes()).await.unwrap());
                        }
                        let before = reader.comparisons;
                        let start = Instant::now();
                        let mut hits = 0;
                        for probe in &probes {
                            hits += usize::from(
                                black_box(
                                    scalar_lookup(&mut reader, black_box(probe.as_bytes()))
                                        .await
                                        .unwrap(),
                                )
                                .is_some(),
                            );
                        }
                        let elapsed = start.elapsed().as_nanos();
                        assert_eq!(hits, 1024);
                        println!(
                            "disk,{count},{width},0,{},{},{elapsed},{}",
                            cold.height(),
                            probes.len(),
                            reader.comparisons - before
                        );
                        let ordered: Vec<_> = (0..count * 2).map(|n| key(n, width)).collect();
                        let before = reader.comparisons;
                        let start = Instant::now();
                        let mut hits = 0;
                        for probe in &ordered {
                            hits += usize::from(
                                black_box(
                                    reader
                                        .seek_match(black_box(probe.as_bytes()))
                                        .await
                                        .unwrap(),
                                )
                                .is_some(),
                            );
                        }
                        let elapsed = start.elapsed().as_nanos();
                        assert_eq!(hits, count as usize);
                        println!(
                            "ordered,{count},{width},0,{},{},{elapsed},{}",
                            cold.height(),
                            ordered.len(),
                            reader.comparisons - before
                        );
                        for groups in [1, 4, 16] {
                            let runs = test_runs(
                                (0..groups)
                                    .map(|group| {
                                        (group..count)
                                            .step_by(groups as usize)
                                            .map(|n| key(n * 2, width))
                                            .collect()
                                    })
                                    .collect(),
                                DuplicateCheck::Collect,
                                MemoryBudget::new(128 * 1024 * 1024),
                            );
                            let start = Instant::now();
                            let mut hits = 0;
                            let mut comparisons = 0;
                            for probe in &probes {
                                let (found, work) =
                                    black_box(probe_hot_runs(&runs, black_box(probe.as_bytes())));
                                hits += usize::from(found.is_some());
                                comparisons += work;
                            }
                            let elapsed = start.elapsed().as_nanos();
                            assert_eq!(hits, 1024);
                            println!(
                                "resident,{count},{width},{groups},0,{},{elapsed},{comparisons}",
                                probes.len()
                            );
                        }
                    })
                    .await;
                }
            }
        });
    }
}
