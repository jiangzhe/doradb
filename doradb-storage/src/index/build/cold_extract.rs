//! Bounded cold jobs whose accepted completions survive cancellation of execution borrows.
use super::cold_source::{ColdBuildSource, ColdWorkspace};
use super::local_sort::LocalSortJobs;
use super::merge::execution_error;
use super::{BudgetedVec, BuildOutcome, SortedRun, SortedRuns};
#[cfg(feature = "profiling")]
use crate::completion::Completion;
use crate::error::{
    MultiDomainResultExt, QuadError, QuadResult, RuntimeError, RuntimeOrFatalError,
    RuntimeOrFatalResult,
};
use crate::index::column_block_index::{ColdPartition, ColumnBlockRangeCursor, ColumnSourcePlan};
#[cfg(feature = "profiling")]
use crate::profiling::{ColdExtractionMeasurements, ColdRunMeasurements, clock::Instant};
use crate::quiescent::QuiescentGuard;
use crate::runtime::thread_pool::ThreadPool;
use error_stack::{Report, ResultExt};
use std::mem::replace;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
pub(crate) use tests::gate_cold_extraction;

#[cfg(feature = "profiling")]
struct WorkerProfile {
    sample: ColdRunMeasurements,
    started: Instant,
    sort_started: Instant,
    finished: Instant,
}

#[cfg(feature = "profiling")]
struct ActiveJob(Arc<ColdBuildSource>);

#[cfg(feature = "profiling")]
impl Drop for ActiveJob {
    fn drop(&mut self) {
        self.0.profile.jobs.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ColdSourcePlan {
    identity: Arc<()>,
    source: Arc<ColdBuildSource>,
    ranges: ColumnSourcePlan,
    #[cfg(feature = "profiling")]
    requested_partitions: usize,
}

struct ColdResult {
    identity: Arc<()>,
    partition: ColdPartition,
    exhausted: bool,
    run: Arc<SortedRun>,
    workspace: ColdWorkspace,
    #[cfg(feature = "profiling")]
    profile: WorkerProfile,
}

/// Retains every accepted extraction result until exact coverage or settled failure.
pub(crate) struct ColdLocalSort {
    plan: Arc<ColdSourcePlan>,
    pool: QuiescentGuard<ThreadPool>,
    workers: usize,
    workspaces: BudgetedVec<Option<ColdWorkspace>>,
    jobs: LocalSortJobs<QuadResult<BuildOutcome<ColdResult>>>,
    runs: BudgetedVec<Arc<SortedRun>>,
    stop: Arc<AtomicBool>,
    failure: Option<QuadError>,
    finished: bool,
    #[cfg(feature = "profiling")]
    started: Instant,
    #[cfg(feature = "profiling")]
    planned: Instant,
    #[cfg(feature = "profiling")]
    configured_workers: usize,
    #[cfg(feature = "profiling")]
    samples: BudgetedVec<WorkerProfile>,
}

impl ColdLocalSort {
    /// Plan admitted ranges and ledgers before submitting any work.
    pub(crate) async fn new(
        source: Arc<ColdBuildSource>,
        pool: QuiescentGuard<ThreadPool>,
        workers: usize,
    ) -> QuadResult<Self> {
        #[cfg(feature = "profiling")]
        let started = Instant::now();
        #[cfg(feature = "profiling")]
        let configured_workers = workers;
        let mut workspaces = BudgetedVec::new(&source.budget);
        // A failed additional minimum workspace lowers W before P and the cuts
        // are fixed. Resident input growth never waits for another run to retire.
        if !source.is_empty() {
            for _ in 0..workers {
                if let Err(error) = workspaces.reserve_one("cold workspace owners") {
                    if workspaces.is_empty() {
                        return Err(error.change_context(RuntimeError::IndexAccess).into());
                    }
                    break;
                }
                match source.workspace() {
                    Ok(workspace) => workspaces.push_reserved(Some(workspace)),
                    Err(error) if workspaces.is_empty() => return Err(error),
                    Err(QuadError::Runtime(_)) => break,
                    Err(error) => return Err(error),
                }
            }
        }
        let workers = workspaces.len().max(1);
        // Use the same partition policy at every worker count. A lone worker
        // processes its ranges serially; small sources may still yield one run.
        let requested = workers.checked_mul(4).ok_or_else(|| {
            Report::new(RuntimeError::IndexAccess).attach("cold source partition count overflow")
        })?;
        let ranges = source
            .column_index()
            .plan_build_ranges(requested, &source.budget)
            .await?;
        assert!(
            ranges.partitions.len() <= ranges.entries
                && (ranges.height == 0 || ranges.entries / 8 >= requested),
            "cold planner returned invalid selected-level evidence: height={}, entries={}, requested={requested}",
            ranges.height,
            ranges.entries
        );
        let jobs = LocalSortJobs::new(
            &source.budget,
            ranges.partitions.len(),
            workers,
            "cold completion ledger",
        )
        .change_context(RuntimeError::IndexAccess)?;
        let mut runs = BudgetedVec::new(&source.budget);
        runs.ensure_capacity(ranges.partitions.len(), "cold run owners")
            .change_context(RuntimeError::IndexAccess)?;
        #[cfg(feature = "profiling")]
        let mut samples = BudgetedVec::new(&source.budget);
        #[cfg(feature = "profiling")]
        samples
            .ensure_capacity(ranges.partitions.len(), "cold profiling ledger")
            .change_context(RuntimeError::IndexAccess)?;
        Ok(Self {
            #[cfg(feature = "profiling")]
            started,
            #[cfg(feature = "profiling")]
            planned: Instant::now(),
            #[cfg(feature = "profiling")]
            configured_workers,
            #[cfg(feature = "profiling")]
            samples,
            plan: Arc::new(ColdSourcePlan {
                identity: Arc::new(()),
                source,
                ranges,
                #[cfg(feature = "profiling")]
                requested_partitions: requested,
            }),
            pool,
            workers,
            workspaces,
            jobs,
            runs,
            stop: Arc::new(AtomicBool::new(false)),
            failure: None,
            finished: false,
        })
    }

    /// Extract complete resident runs; every empty range still requires a terminal certificate.
    pub(crate) async fn execute(&mut self) -> QuadResult<SortedRuns> {
        assert!(!self.finished, "cold extraction reused after settlement");
        while self.jobs.collected < self.jobs.len() && self.failure.is_none() {
            while self.jobs.can_submit() && !self.stop.load(Ordering::Acquire) {
                let partition = self.jobs.submitted;
                let plan = self.plan.clone();
                let stop = self.stop.clone();
                let workspace = self.workspaces[partition % self.workers]
                    .take()
                    .unwrap_or_else(|| unreachable!("cold worker slot released before reuse"));
                self.jobs.submit(&self.pool, async move {
                    let result = extract(plan, stop.clone(), partition, workspace).await;
                    if result.is_err() {
                        stop.store(true, Ordering::Release);
                    }
                    result
                });
            }
            if self.stop.load(Ordering::Acquire) {
                break;
            }
            self.collect_next().await;
        }
        if self.failure.is_some() || self.stop.load(Ordering::Acquire) {
            self.drain().await;
            return Err(self.failure.take().unwrap_or_else(|| {
                execution_error("cold extraction stopped without an execution failure").into()
            }));
        }
        self.finished = true;
        let runs = replace(&mut self.runs, BudgetedVec::new(&self.plan.source.budget));
        Ok(SortedRuns::from_admitted(
            runs,
            self.plan.source.budget.clone(),
            self.plan.source.duplicates(),
        ))
    }

    /// Bound known source control objects separately from admitted bulk storage.
    /// Executor internals, allocator metadata and pool-owned pages are excluded.
    #[cfg(feature = "profiling")]
    pub(crate) fn fixed_overhead_bytes(&self) -> u64 {
        (size_of::<Self>()
            + size_of::<ColdSourcePlan>()
            + size_of::<ColdBuildSource>()
            + self.workers * size_of::<Completion<QuadResult<BuildOutcome<ColdResult>>>>()
            + self.jobs.len()
                * (size_of::<SortedRun>()
                    + size_of::<ColdRunMeasurements>()
                    + 4 * size_of::<usize>())) as u64
    }

    /// Snapshot completed source work for publication only after CREATE succeeds.
    #[cfg(feature = "profiling")]
    pub(crate) fn measurements(&self) -> ColdExtractionMeasurements {
        let source = &self.plan.source.profile;
        let samples = &self.samples;
        let span = |start: Option<Instant>, end: Option<Instant>| {
            start.zip(end).map_or(0, |(start, end)| {
                end.duration_since(start).as_nanos() as u64
            })
        };
        ColdExtractionMeasurements {
            runs: samples.iter().map(|sample| sample.sample).collect(),
            source_height: u64::from(self.plan.ranges.height),
            source_entries: self.plan.ranges.entries as u64,
            requested_partitions: self.plan.requested_partitions as u64,
            partitions: self.jobs.len() as u64,
            nonempty_runs: samples
                .iter()
                .filter(|sample| sample.sample.live_rows != 0)
                .count() as u64,
            configured_workers: self.configured_workers as u64,
            workers: self.workers as u64,
            concurrency_peak: source.job_peak.load(Ordering::Relaxed) as u64,
            planning_nanos: self.planned.duration_since(self.started).as_nanos() as u64,
            preparation_nanos: self.started.elapsed().as_nanos() as u64,
            extraction_worker_nanos: samples
                .iter()
                .map(|sample| sample.sample.extraction_nanos)
                .sum(),
            sort_worker_nanos: samples.iter().map(|sample| sample.sample.sort_nanos).sum(),
            extraction_wall_nanos: span(
                samples.iter().map(|sample| sample.started).min(),
                samples.iter().map(|sample| sample.sort_started).max(),
            ),
            sort_wall_nanos: span(
                samples.iter().map(|sample| sample.sort_started).min(),
                samples.iter().map(|sample| sample.finished).max(),
            ),
            descriptors: samples.iter().map(|sample| sample.sample.descriptors).sum(),
            physical_rows: samples
                .iter()
                .map(|sample| sample.sample.physical_rows)
                .sum(),
            live_rows: samples.iter().map(|sample| sample.sample.live_rows).sum(),
            encoded_bytes: samples
                .iter()
                .map(|sample| sample.sample.encoded_bytes)
                .sum(),
            node_reads: source.node_reads.load(Ordering::Relaxed) as u64,
            lwc_reads: source.lwc_reads.load(Ordering::Relaxed) as u64,
            descriptor_peak: source.descriptor_peak.load(Ordering::Relaxed) as u64,
        }
    }

    /// Stop admission and settle accepted work before releasing the private fork.
    /// Pool reservation is acceptance. Supervised workers produce move-once completions;
    /// poison/shutdown leave those completions authoritative while storage remains live.
    /// This owner retains the awaited slot across cancellation, and owns stop/drain cleanup.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.drain().await;
        match self.failure.take() {
            None => Ok(()),
            Some(QuadError::Runtime(error)) => Err(RuntimeOrFatalError::Runtime(error)),
            Some(QuadError::Fatal(error)) => Err(RuntimeOrFatalError::Fatal(error)),
            // Forward execution returns Operation unchanged. Only abandoned
            // execution can encounter an operation failure at the cleanup owner.
            Some(QuadError::Operation(error)) => {
                Err(error.change_context(RuntimeError::IndexAccess).into())
            }
            Some(QuadError::Lifecycle(error)) => {
                Err(error.change_context(RuntimeError::IndexAccess).into())
            }
        }
    }

    async fn drain(&mut self) {
        self.stop.store(true, Ordering::Release);
        while self.jobs.collected < self.jobs.submitted {
            self.collect_next().await;
        }
        self.runs.clear();
        self.finished = true;
    }

    async fn collect_next(&mut self) {
        let id = self.jobs.collected;
        let result = self.jobs.collect().await;
        let result = result.map_err(|bridge| bridge.into_quad(RuntimeError::IndexAccess))
            .and_then(|result| result)
            .attach_with(|| format!("operation=create_index, phase=cold_extract, table_id={}, partition={id}, range={:?}, build_ts={}", self.plan.source.table.table_id(), self.plan.ranges.partitions[id].range, self.plan.source.build_ts));
        match result {
            Ok(BuildOutcome::Complete(result)) => {
                #[cfg(feature = "profiling")]
                self.samples.push_reserved(result.profile);
                self.workspaces[id % self.workers] = Some(result.workspace);
                if !Arc::ptr_eq(&result.identity, &self.plan.identity)
                    || result.partition != self.plan.ranges.partitions[id]
                    || !result.exhausted
                    || result.run.group_id != id
                {
                    self.fail(
                        Report::new(RuntimeError::IndexAccess)
                            .attach("cold source completion identity or coverage mismatch")
                            .into(),
                    );
                } else if !result.run.entries().is_empty() && self.failure.is_none() {
                    self.runs.push_reserved(result.run);
                }
            }
            Ok(BuildOutcome::Stopped) => debug_assert!(
                self.stop.load(Ordering::Acquire),
                "cold extraction returned Stopped without a stop request"
            ),
            Err(error) => self.fail(error),
        }
    }

    fn fail(&mut self, error: QuadError) {
        self.stop.store(true, Ordering::Release);
        self.failure = Some(match self.failure.take() {
            None => error,
            Some(old) => match (old, error) {
                (QuadError::Fatal(old), error) => QuadError::Fatal(
                    old.attach(format!("secondary cold worker failure: {error:?}")),
                ),
                (old, QuadError::Fatal(error)) => {
                    QuadError::Fatal(error.attach(format!("earlier cold worker failure: {old:?}")))
                }
                (QuadError::Runtime(old), error) => QuadError::Runtime(
                    old.attach(format!("secondary cold worker failure: {error:?}")),
                ),
                (old, QuadError::Runtime(error)) => QuadError::Runtime(
                    error.attach(format!("earlier cold worker failure: {old:?}")),
                ),
                (QuadError::Operation(old), error) => QuadError::Operation(
                    old.attach(format!("secondary cold worker failure: {error:?}")),
                ),
                (QuadError::Lifecycle(old), error) => QuadError::Lifecycle(
                    old.attach(format!("secondary cold worker failure: {error:?}")),
                ),
            },
        });
    }
}

impl Drop for ColdLocalSort {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Accepted closures retain source authority even if their observer drops.
    }
}

async fn extract(
    plan: Arc<ColdSourcePlan>,
    stop: Arc<AtomicBool>,
    id: usize,
    mut workspace: ColdWorkspace,
) -> QuadResult<BuildOutcome<ColdResult>> {
    #[cfg(feature = "profiling")]
    let started = Instant::now();
    #[cfg(feature = "profiling")]
    let _active = {
        let jobs = plan.source.profile.jobs.fetch_add(1, Ordering::Relaxed) + 1;
        plan.source
            .profile
            .job_peak
            .fetch_max(jobs, Ordering::Relaxed);
        ActiveJob(plan.source.clone())
    };
    #[cfg(feature = "profiling")]
    {
        workspace.sample = ColdRunMeasurements::default();
    }
    #[cfg(test)]
    plan.source.test.before(id).await?;
    let partition = &plan.ranges.partitions[id];
    let mut cursor = ColumnBlockRangeCursor::new(
        &plan.source.column_index(),
        partition.range.clone(),
        workspace
            .scan
            .take()
            .unwrap_or_else(|| unreachable!("cold worker owns scan workspace")),
    );
    let BuildOutcome::Complete(entries) = plan
        .source
        .extract(&mut cursor, &mut workspace, &stop)
        .await?
    else {
        return Ok(BuildOutcome::Stopped);
    };
    if stop.load(Ordering::Acquire) {
        return Ok(BuildOutcome::Stopped);
    }
    #[cfg(feature = "profiling")]
    let sort_started = Instant::now();
    let BuildOutcome::Complete(run) =
        SortedRun::finish_group(entries, plan.source.duplicates(), id, &stop)
    else {
        return Ok(BuildOutcome::Stopped);
    };
    let run = Arc::new(run);
    #[cfg(feature = "profiling")]
    let profile = {
        let finished = Instant::now();
        WorkerProfile {
            sample: ColdRunMeasurements {
                partition: id as u64,
                start_row_id: partition.range.start.as_u64(),
                end_row_id: partition.range.end.as_u64(),
                retained_bytes: run.retained_bytes(),
                extraction_nanos: sort_started.duration_since(started).as_nanos() as u64,
                sort_nanos: finished.duration_since(sort_started).as_nanos() as u64,
                elapsed_nanos: finished.duration_since(started).as_nanos() as u64,
                ..workspace.sample
            },
            started,
            sort_started,
            finished,
        }
    };
    let exhausted = cursor.exhausted();
    workspace.scan = Some(cursor.into_workspace());
    Ok(BuildOutcome::Complete(ColdResult {
        identity: plan.identity.clone(),
        partition: partition.clone(),
        exhausted,
        run,
        workspace,
        #[cfg(feature = "profiling")]
        profile,
    }))
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::complete_work;
    use super::*;
    use crate::Engine;
    use crate::catalog::index::IndexDdlGateScope;
    use crate::catalog::{StorageIndexFlags, StorageIndexKey, StorageIndexSpec, tests::table2};
    use crate::completion::Completion;
    use crate::error::{DataIntegrityError, OperationError};
    use crate::id::{RowID, TrxID};
    use crate::index::build::{MemoryBudget, fail_build_budget_after, payload_accounting};
    use crate::index::secondary_index_encoder;
    use crate::runtime::yield_now;
    use crate::session::tests::{SessionTestExt, assert_checkpoint_published};
    use crate::table::tests::{
        assert_freeze_created, insert_one_row, lightweight_test_engine,
        table_for_internal_assertion,
    };
    use crate::value::Val;
    use futures::FutureExt;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[derive(Clone, Copy)]
    enum Fault {
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

    /// Accepted cold-worker scheduling and failure controls.
    #[derive(Default)]
    pub(crate) struct Hooks(Mutex<BTreeMap<usize, Gate>>);

    impl Hooks {
        fn gate(&self, partition: usize, fault: Fault) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (arrive, entered) = flume::unbounded();
            let (release, wait) = flume::unbounded();
            self.0.lock().insert(
                partition,
                Gate {
                    entered: arrive,
                    release: wait,
                    fault,
                },
            );
            (entered, release)
        }

        /// Wait at the requested partition boundary before applying an injected fault.
        pub(crate) async fn before(&self, partition: usize) -> QuadResult<()> {
            let gate = self.0.lock().get(&partition).cloned();
            if let Some(gate) = gate {
                gate.entered.send(()).unwrap();
                gate.release.recv_async().await.unwrap();
                match gate.fault {
                    Fault::None => (),
                    Fault::Runtime => {
                        return Err(Report::new(DataIntegrityError::InvalidPayload)
                            .attach("injected cold extraction failure")
                            .change_context(RuntimeError::IndexAccess)
                            .into());
                    }
                    Fault::Panic => panic!("injected accepted cold worker panic"),
                }
            }
            Ok(())
        }
    }

    struct Fixture {
        source: Arc<ColdBuildSource>,
        expected: Vec<(Vec<u8>, RowID)>,
        engine: Engine,
        _temp: TempDir,
    }

    impl Fixture {
        async fn new(unique: bool) -> Self {
            Self::with_groups(unique, 4).await
        }

        async fn with_groups(unique: bool, groups: i32) -> Self {
            let temp = TempDir::new().unwrap();
            let engine = lightweight_test_engine(&temp, "parallel_cold_extract").await;
            let table_id = table2(&engine).await;
            let table = table_for_internal_assertion(&engine, table_id);
            let mut session = engine.new_session().unwrap();
            let mut values = Vec::new();
            for group in 0..groups {
                for id in group * 8..group * 8 + 8 {
                    let row = vec![Val::from(id), Val::from("outlined".repeat(32).as_str())];
                    let row_id = insert_one_row(table_id, &mut session, row.clone()).await;
                    values.push((row, row_id));
                }
                assert_freeze_created(session.freeze_table(table_id, usize::MAX).await.unwrap());
                assert_checkpoint_published(&mut session, table_id).await;
            }
            let gates = Arc::new(
                IndexDdlGateScope::acquire(table.clone(), engine.inner().core.catalog.clone())
                    .await
                    .unwrap(),
            );
            let plan = table
                .finalize_create_index(StorageIndexSpec::new(
                    vec![StorageIndexKey::new(1), StorageIndexKey::new(0)],
                    if unique {
                        StorageIndexFlags::UK
                    } else {
                        StorageIndexFlags::empty()
                    },
                ))
                .unwrap();
            let encoder = secondary_index_encoder(
                plan.old_layout().metadata(),
                plan.new_index_spec(),
                !unique,
            );
            let mut expected: Vec<_> = values
                .into_iter()
                .map(|(row, row_id)| {
                    let mut key = vec![row[1].clone(), row[0].clone()];
                    if !unique {
                        key.push(Val::from(row_id));
                    }
                    (encoder.encode(&key).as_bytes().to_vec(), row_id)
                })
                .collect();
            expected.sort_unstable();
            let source = Arc::new(ColdBuildSource::capture(
                &plan,
                session.pool_guards(),
                gates,
                TrxID::new(1000),
                MemoryBudget::new(16 * 1024 * 1024),
            ));
            drop(session);
            Self {
                source,
                expected,
                engine,
                _temp: temp,
            }
        }

        async fn sort(&self, workers: usize) -> ColdLocalSort {
            ColdLocalSort::new(
                self.source.clone(),
                self.engine.inner().core.thread_pool.clone(),
                workers,
            )
            .await
            .unwrap()
        }
    }

    /// Hold an accepted source worker until the lifecycle test releases it.
    pub(crate) fn gate_cold_extraction(
        source: &ColdBuildSource,
        partition: usize,
    ) -> (flume::Receiver<()>, flume::Sender<()>) {
        source.test.gate(partition, Fault::None)
    }

    fn contents(runs: &SortedRuns) -> Vec<(Vec<u8>, RowID)> {
        for run in runs.runs() {
            assert!(
                run.entries()
                    .windows(2)
                    .all(|pair| pair[0].key <= pair[1].key),
                "source partition {} must already be locally sorted",
                run.group_id
            );
        }
        let mut actual: Vec<_> = runs
            .runs()
            .iter()
            .flat_map(|run| {
                run.entries()
                    .iter()
                    .map(|entry| (entry.key.as_bytes().to_vec(), entry.row_id))
            })
            .collect();
        actual.sort_unstable();
        actual
    }

    /// Purpose: Preserve common range planning with one worker across empty, single-run and multi-run sources.
    /// Expected: Serial execution survives a cancelled borrow, preserves exact contents and coverage in both key modes, and releases all admission after empty or deleted input.
    #[test]
    fn cold_single_worker_ranges_and_empty_sources() {
        smol::block_on(async {
            for unique in [false, true] {
                for groups in [0, 1, 4] {
                    let fixture = Fixture::with_groups(unique, groups).await;
                    assert!(fixture.engine.inner().core.thread_pool.worker_threads() > 1);
                    let mut sort = fixture.sort(1).await;
                    assert_eq!(sort.jobs.len(), groups as usize);
                    if groups != 0 {
                        let ranges = &sort.plan.ranges.partitions;
                        assert_eq!(ranges[0].range.start, RowID::new(0));
                        assert!(
                            ranges.last().unwrap().range.end
                                > fixture.expected.iter().map(|row| row.1).max().unwrap()
                        );
                        let (entered, release) = fixture.source.test.gate(0, Fault::None);
                        assert!(sort.execute().now_or_never().is_none());
                        entered.recv_async().await.unwrap();
                        assert_eq!((sort.jobs.submitted, sort.jobs.collected), (1, 0));
                        release.send(()).unwrap();
                    }
                    let runs = sort.execute().await.unwrap();
                    assert_eq!(runs.runs().len(), groups as usize);
                    assert_eq!(contents(&runs), fixture.expected);
                    assert_eq!(sort.jobs.collected, sort.jobs.len());
                    #[cfg(feature = "profiling")]
                    {
                        let profile = sort.measurements();
                        assert_eq!(profile.requested_partitions, 4);
                        assert_eq!(profile.partitions, groups as u64);
                        assert_eq!(profile.nonempty_runs, groups as u64);
                        assert_eq!(profile.descriptors, groups as u64);
                        assert_eq!(profile.live_rows, fixture.expected.len() as u64);
                        assert_eq!(profile.concurrency_peak, u64::from(groups != 0));
                    }
                    drop(runs);
                    drop(sort);
                    assert_eq!(fixture.source.budget.used(), 0);
                    fixture.source.test.0.lock().clear();
                    for (_, row_id) in &fixture.expected {
                        fixture
                            .source
                            .table
                            .deletion_buffer()
                            .put_committed(*row_id, TrxID::new(1))
                            .unwrap();
                    }
                    let mut sort = fixture.sort(1).await;
                    let runs = sort.execute().await.unwrap();
                    assert!(runs.runs().is_empty());
                    assert_eq!(runs.duplicates, fixture.source.duplicates());
                    assert_eq!(sort.jobs.collected, groups as usize);
                    drop(runs);
                    drop(sort);
                    assert_eq!(fixture.source.budget.used(), 0);
                }
            }
        });
    }

    /// Purpose: Retain the only accepted cold worker through cancellation of its settlement borrow.
    /// Expected: Drain completes after the gate opens, preserves runtime or supervised fatal errors, and releases every source allocation.
    #[test]
    fn cold_single_worker_cancelled_settlement() {
        smol::block_on(async {
            for fault in [Fault::None, Fault::Runtime, Fault::Panic] {
                let fixture = Fixture::new(true).await;
                let (entered, release) = fixture.source.test.gate(0, fault);
                let mut sort = fixture.sort(1).await;
                assert!(sort.execute().now_or_never().is_none());
                entered.recv_async().await.unwrap();
                assert!(sort.settle().now_or_never().is_none());
                assert_eq!((sort.jobs.submitted, sort.jobs.collected), (1, 0));
                release.send(()).unwrap();
                match (fault, sort.settle().await) {
                    (Fault::None, Ok(())) => (),
                    (Fault::Runtime, Err(RuntimeOrFatalError::Runtime(error))) => {
                        assert!(error.contains::<DataIntegrityError>());
                    }
                    (Fault::Panic, Err(RuntimeOrFatalError::Fatal(_))) => (),
                    (_, result) => panic!("unexpected single-worker settlement: {result:?}"),
                }
                assert_eq!(sort.jobs.collected, 1);
                assert!(sort.runs.is_empty());
                drop(sort);
                assert_eq!(fixture.source.budget.used(), 0);
            }
        });
    }

    /// Purpose: Retain parallel cold extraction across a cancelled execution borrow and reversed completion order.
    /// Expected: Two gated jobs coexist, the worker bound holds, resumed execution covers every source range and both physical key modes match an independent value oracle.
    #[test]
    fn cold_parallel_cancellation_and_reverse_completion() {
        smol::block_on(async {
            for unique in [true, false] {
                let fixture = Fixture::new(unique).await;
                let (first, release_first) = fixture.source.test.gate(0, Fault::None);
                let (second, release_second) = fixture.source.test.gate(1, Fault::None);
                let mut sort = fixture.sort(2).await;
                assert!(sort.execute().now_or_never().is_none());
                first.recv_async().await.unwrap();
                second.recv_async().await.unwrap();
                assert_eq!(sort.jobs.submitted, 2);
                assert_eq!(sort.jobs.collected, 0);
                release_second.send(()).unwrap();
                while !sort.jobs.slots[1].as_ref().unwrap().is_completed() {
                    yield_now().await;
                }
                assert!(!sort.jobs.slots[0].as_ref().unwrap().is_completed());
                release_first.send(()).unwrap();
                let runs = sort.execute().await.unwrap();
                assert_eq!(sort.jobs.collected, sort.plan.ranges.partitions.len());
                assert!(runs.runs().len() > 1);
                assert!(
                    runs.runs()
                        .windows(2)
                        .all(|pair| pair[0].group_id < pair[1].group_id)
                );
                assert_eq!(contents(&runs), fixture.expected);
                #[cfg(feature = "profiling")]
                {
                    let profile = sort.measurements();
                    assert_eq!(profile.concurrency_peak, 2);
                    assert_eq!(profile.live_rows, 32);
                    assert_eq!(profile.runs.len(), sort.jobs.collected);
                    assert_eq!(profile.descriptors, 4);
                    assert_eq!(profile.physical_rows, 32);
                    assert_eq!(profile.lwc_reads, 4);
                    assert_eq!(
                        profile.encoded_bytes,
                        fixture
                            .expected
                            .iter()
                            .map(|(key, _)| key.len() as u64)
                            .sum::<u64>()
                    );
                    assert!(profile.descriptor_peak <= 64);
                    for (id, run) in profile.runs.iter().enumerate() {
                        assert_eq!(run.partition, id as u64);
                        assert_eq!(
                            (run.descriptors, run.physical_rows, run.live_rows),
                            (1, 8, 8)
                        );
                        assert!(run.retained_bytes >= run.encoded_bytes);
                        assert_eq!(run.elapsed_nanos, run.extraction_nanos + run.sort_nanos);
                    }
                }
                drop(runs);
                drop(sort);
                assert_eq!(fixture.source.budget.used(), 0);
            }
        });
    }

    /// Purpose: Drain accepted cold siblings after failure, including cancellation of the settlement borrow.
    /// Expected: Later partitions are never admitted, all source resources remain owned until completion, and a later supervised panic outranks the original runtime failure.
    #[test]
    fn cold_failure_drain_and_fatal_precedence() {
        smol::block_on(async {
            for fault in [Fault::None, Fault::Panic] {
                let fixture = Fixture::new(true).await;
                let (first, release_first) = fixture.source.test.gate(0, Fault::Runtime);
                let (second, release_second) = fixture.source.test.gate(1, fault);
                let mut sort = fixture.sort(2).await;
                assert!(sort.execute().now_or_never().is_none());
                first.recv_async().await.unwrap();
                second.recv_async().await.unwrap();
                release_first.send(()).unwrap();
                sort.collect_next().await;
                assert!(sort.failure.is_some());
                assert!(sort.settle().now_or_never().is_none());
                assert_eq!(sort.jobs.submitted, 2);
                release_second.send(()).unwrap();
                let error = sort.settle().await.unwrap_err();
                assert_eq!(
                    matches!(error, RuntimeOrFatalError::Fatal(_)),
                    matches!(fault, Fault::Panic)
                );
                assert!(format!("{error:?}").contains("injected cold extraction failure"));
                drop(sort);
                assert_eq!(fixture.source.budget.used(), 0);
            }
        });
    }

    /// Purpose: Observe a later cold worker failure while the first accepted worker remains gated.
    /// Expected: Resumed execution drains the original accepted jobs without dispatching new ranges or replacing the worker error with missing-workspace failure.
    #[test]
    fn cold_later_failure_stops_admission_before_ordered_collection() {
        smol::block_on(async {
            let fixture = Fixture::new(true).await;
            let (first, release_first) = fixture.source.test.gate(0, Fault::None);
            let (second, release_second) = fixture.source.test.gate(1, Fault::Runtime);
            let mut sort = fixture.sort(2).await;
            assert!(sort.execute().now_or_never().is_none());
            first.recv_async().await.unwrap();
            second.recv_async().await.unwrap();
            release_second.send(()).unwrap();
            while !sort.jobs.slots[1].as_ref().unwrap().is_completed() {
                yield_now().await;
            }
            assert!(sort.stop.load(Ordering::Acquire));
            assert!(sort.execute().now_or_never().is_none());
            assert_eq!(sort.jobs.submitted, 2);
            release_first.send(()).unwrap();
            let error = sort.execute().await.err().unwrap();
            assert!(matches!(&error, QuadError::Runtime(_)), "{error:?}");
            assert!(format!("{error:?}").contains("injected cold extraction failure"));
            assert_eq!(sort.jobs.submitted, 2);
            assert_eq!(sort.jobs.collected, 2);
            assert!(sort.runs.is_empty());
            drop(sort);
            assert_eq!(fixture.source.budget.used(), 0);
        });
    }

    /// Purpose: Certify cold ranges that emit no live rows and reject forged terminal source coverage.
    /// Expected: Empty partitions count as completed coverage; foreign plans, wrong ranges, wrong IDs and premature exhaustion cannot enter resident merge input.
    #[test]
    fn cold_empty_ranges_and_terminal_certificates() {
        smol::block_on(async {
            let fixture = Fixture::new(false).await;
            for (_, row_id) in &fixture.expected {
                fixture
                    .source
                    .table
                    .deletion_buffer()
                    .put_committed(*row_id, TrxID::new(1))
                    .unwrap();
            }
            let mut sort = fixture.sort(2).await;
            let runs = sort.execute().await.unwrap();
            assert!(runs.runs().is_empty());
            assert_eq!(sort.jobs.collected, sort.jobs.len());
            assert_eq!(runs.duplicates, super::super::DuplicateCheck::Skip);
            drop(runs);
            drop(sort);
            for invalid in ["foreign", "id", "range", "incomplete"] {
                let mut sort = fixture.sort(2).await;
                let workspace = sort.workspaces[0].take().unwrap();
                let mut result = complete_work(
                    extract(sort.plan.clone(), sort.stop.clone(), 0, workspace)
                        .await
                        .unwrap(),
                );
                match invalid {
                    "foreign" => result.identity = Arc::new(()),
                    "id" => result.partition.id += 1,
                    "range" => result.partition.range.start = result.partition.range.start + 1,
                    "incomplete" => result.exhausted = false,
                    _ => unreachable!(),
                }
                let completion = Arc::new(Completion::new());
                completion.complete(Ok(Ok(BuildOutcome::Complete(result))));
                sort.jobs.slots[0] = Some(completion);
                sort.jobs.submitted = 1;
                sort.collect_next().await;
                assert!(sort.settle().await.is_err(), "{invalid}");
                assert!(sort.runs.is_empty(), "{invalid}");
                drop(sort);
                assert_eq!(fixture.source.budget.used(), 0, "{invalid}");
            }
        });
    }

    /// Purpose: Preserve native cold deletion-marker failures through asynchronous extraction transport.
    /// Expected: An uncommitted reference returns WriteConflict, a committed reference filters exactly its row, and both paths release all admitted source storage.
    #[test]
    fn cold_marker_classification_survives_transport() {
        use crate::trx::tests::shared_trx_status;
        use crate::trx::{MAX_SNAPSHOT_TS, MIN_ACTIVE_TRX_ID};
        smol::block_on(async {
            let fixture = Fixture::new(true).await;
            let row_id = fixture.expected[0].1;
            let status = Arc::new(shared_trx_status(MIN_ACTIVE_TRX_ID + 12));
            let deletes = fixture.source.table.deletion_buffer();
            deletes
                .put_ref(row_id, status.clone(), MAX_SNAPSHOT_TS)
                .unwrap();
            let mut sort = fixture.sort(1).await;
            let error = sort.execute().await.err().unwrap();
            assert!(
                matches!(&error, QuadError::Operation(report) if report.current_context() == &OperationError::WriteConflict),
                "{error:?}"
            );
            drop(sort);
            assert_eq!(fixture.source.budget.used(), 0);
            assert!(deletes.remove_ref_if_owned(row_id, &status));
            let committed = Arc::new(shared_trx_status(TrxID::new(1)));
            deletes
                .put_ref(row_id, committed.clone(), MAX_SNAPSHOT_TS)
                .unwrap();
            let mut sort = fixture.sort(1).await;
            let runs = sort.execute().await.unwrap();
            assert_eq!(contents(&runs), fixture.expected[1..]);
            drop(runs);
            drop(sort);
            assert_eq!(fixture.source.budget.used(), 0);
            assert!(
                matches!(deletes.get(row_id), Some(crate::table::DeleteMarker::Ref(status)) if Arc::ptr_eq(&status, &committed))
            );
        });
    }

    /// Purpose: Reduce cold extraction concurrency when only one minimum reusable workspace is admitted.
    /// Expected: Partition planning uses the reduced allowance, extraction still matches the full source oracle, and no workspace is charged twice or retained after settlement.
    #[test]
    fn cold_workspace_admission_reduces_workers_before_planning() {
        smol::block_on(async {
            let fixture = Fixture::new(false).await;
            fail_build_budget_after(&fixture.source.budget, "cold identity and deletions", 1);
            let mut sort = fixture.sort(2).await;
            assert_eq!(sort.workers, 1);
            assert_eq!(sort.jobs.len(), 4);
            #[cfg(feature = "profiling")]
            {
                assert_eq!(sort.measurements().configured_workers, 2);
                assert_eq!(sort.measurements().requested_partitions, 4);
            }
            let runs = sort.execute().await.unwrap();
            assert_eq!(runs.runs().len(), 4);
            assert_eq!(contents(&runs), fixture.expected);
            drop(runs);
            drop(sort);
            assert_eq!(fixture.source.budget.used(), 0);
        });
    }

    /// Purpose: Extract outlined cold values when scratch fits retained output but not an extra decoded row payload.
    /// Expected: Both key modes preserve exact contents at the budget limit and release all retained charges on drop.
    #[test]
    fn cold_row_temporaries_do_not_consume_retained_budget() {
        use crate::index::build::IndexBuildEntry;
        smol::block_on(async {
            const LIMIT: usize = 1024 * 1024;
            for unique in [false, true] {
                let mut fixture = Fixture::with_groups(unique, 1).await;
                Arc::get_mut(&mut fixture.source).unwrap().budget = MemoryBudget::new(LIMIT);
                let source = &fixture.source;
                let budget = &source.budget;
                let mut workspace = source.workspace().unwrap();
                // Eight rows fill the initial entry buffer without replacement.
                assert_eq!(fixture.expected.len(), 8);
                let payload_bytes = fixture
                    .expected
                    .iter()
                    .map(|(key, _)| key.len())
                    .sum::<usize>();
                let retained_bytes = 8 * size_of::<IndexBuildEntry>() + payload_bytes;
                let occupied = budget
                    .reserve(
                        LIMIT - budget.used() - retained_bytes,
                        "test retained headroom",
                    )
                    .unwrap();
                let baseline = budget.used();
                let end = fixture.expected.iter().map(|(_, row)| *row).max().unwrap() + 1;
                let mut cursor = ColumnBlockRangeCursor::new(
                    &source.column_index(),
                    RowID::new(0)..end,
                    workspace.scan.take().unwrap(),
                );
                let rows = complete_work(
                    source
                        .extract(&mut cursor, &mut workspace, &AtomicBool::new(false))
                        .await
                        .unwrap(),
                );
                assert!(cursor.exhausted());
                let mut actual: Vec<_> = rows
                    .iter()
                    .map(|entry| (entry.key.as_bytes().to_vec(), entry.row_id))
                    .collect();
                actual.sort_unstable();
                assert_eq!(actual, fixture.expected, "unique={unique}");
                assert_eq!(payload_accounting(&rows).1, payload_bytes);
                assert_eq!(budget.used(), LIMIT);
                drop(rows);
                assert_eq!(budget.used(), baseline);
                drop(cursor);
                drop(workspace);
                drop(occupied);
                assert_eq!(budget.used(), 0);
            }
        });
    }

    /// Purpose: Validate LWC binding and physical row counts before filtering live or wholly deleted cold ranges.
    /// Expected: Checksummed but mismatched source blocks retain InvalidPayload and specific diagnostics without producing merge input or retaining admission.
    #[test]
    fn cold_lwc_shape_is_checked_before_deletion_filtering() {
        use crate::file::block_integrity::BLOCK_INTEGRITY_HEADER_SIZE;
        use crate::file::cow_file::tests::rewrite_page_with_checksum;
        use crate::table::tests::corrupt_lwc_block_binding_value;
        smol::block_on(async {
            for all_deleted in [false, true] {
                for corruption in ["binding", "row count"] {
                    let fixture = Fixture::new(true).await;
                    let table = &fixture.source.table;
                    let entries = fixture
                        .source
                        .column_index()
                        .collect_scan_entries()
                        .await
                        .unwrap();
                    let entry = &entries[0];
                    if all_deleted {
                        for (_, row_id) in &fixture.expected {
                            table
                                .deletion_buffer()
                                .put_committed(*row_id, TrxID::new(1))
                                .unwrap();
                        }
                    }
                    let path = fixture
                        .engine
                        .inner()
                        .table_fs
                        .user_table_file_path(table.table_id());
                    if corruption == "binding" {
                        corrupt_lwc_block_binding_value(path, entry.block_id);
                    } else {
                        rewrite_page_with_checksum(path, entry.block_id, |page| {
                            let offset = BLOCK_INTEGRITY_HEADER_SIZE + 8;
                            page[offset..offset + 2]
                                .copy_from_slice(&(entry.row_count - 1).to_le_bytes());
                        });
                    }
                    table.disk_pool().invalidate_block(
                        fixture.engine.inner().core.pools.pool_guards().disk_guard(),
                        table.file().sparse_file().file_id(),
                        entry.block_id,
                    );
                    let mut sort = fixture.sort(1).await;
                    let QuadError::Runtime(error) = sort.execute().await.err().unwrap() else {
                        panic!("corrupt source must retain a Runtime integration context");
                    };
                    assert_eq!(
                        error.downcast_ref::<DataIntegrityError>(),
                        Some(&DataIntegrityError::InvalidPayload)
                    );
                    let diagnostic = format!("{error:?}");
                    assert!(
                        diagnostic.contains(corruption) && diagnostic.contains("mismatch"),
                        "{corruption}, deleted={all_deleted}: {diagnostic}"
                    );
                    drop(sort);
                    assert_eq!(fixture.source.budget.used(), 0);
                }
            }
        });
    }
}
