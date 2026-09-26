//! Finite hot-row extraction, without index-page construction or publication.
//!
//! Factories in catalog/recovery establish source stability. A retained local-sort
//! coordinator owns every accepted completion independently of its borrowed execution future.
mod budget;
mod source;
mod worker;

pub(crate) use budget::{BudgetedVec, MemoryBudget, MemoryReservation};
pub(crate) use source::{HotBuildCapture, HotBuildSource};

use crate::completion::Completion;
use crate::conf::HotIndexBuildConfig;
use crate::error::ConfigResult;
use crate::error::{MultiDomainResultExt, RuntimeError, RuntimeOrFatalError, RuntimeOrFatalResult};
use crate::id::RowID;
use crate::index::BTreeKey;
#[cfg(feature = "profiling")]
use crate::profiling::{HotBuildMeasurements, HotBuildProfile, HotBuildWorkerProfile};
use crate::quiescent::QuiescentGuard;
use crate::runtime::thread_pool::ThreadPool;
use std::cmp::Ordering;
use std::mem::take;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

/// Validated immutable limits used identically during bootstrap and runtime.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HotBuildPolicy {
    /// Maximum simultaneously admitted bulk scratch capacity.
    pub(crate) max_scratch_bytes: usize,
    /// Maximum submitted jobs whose results remain uncollected.
    pub(crate) max_workers: usize,
    /// Soft physical-page target before the run cap binds.
    pub(crate) target_pages_per_run: usize,
    run_cap: usize,
}

impl HotBuildPolicy {
    /// Normalize public configuration before any filesystem effects.
    pub(crate) fn new(mut config: HotIndexBuildConfig, pool_workers: usize) -> ConfigResult<Self> {
        config.validate(pool_workers)?;
        let workers = config.max_workers.unwrap_or(pool_workers);
        Ok(Self {
            max_scratch_bytes: config.max_scratch_bytes,
            max_workers: workers,
            target_pages_per_run: config.target_pages_per_run,
            run_cap: workers * 4,
        })
    }

    /// Plan balanced contiguous page ranges, assigning extra pages to earlier groups.
    pub(crate) fn plan_groups(self, total_pages: usize) -> Vec<Range<usize>> {
        let group_count = total_pages
            .div_ceil(self.target_pages_per_run)
            .min(self.run_cap);
        if group_count == 0 {
            return Vec::new();
        }
        let pages_per_group = total_pages / group_count;
        let extra_pages = total_pages % group_count;
        let mut ranges = Vec::with_capacity(group_count);
        let mut start = 0;
        for group in 0..group_count {
            let mut count = pages_per_group;
            if group < extra_pages {
                count += 1;
            }
            let end = start + count;
            ranges.push(start..end);
            start = end;
        }
        ranges
    }
}

/// Caller-owned duplicate validation policy, independent of global sizing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DuplicateCheck {
    Skip,
    Collect,
}

/// Local evidence only; multiple checked runs can still share a key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalDuplicates {
    Unchecked,
    Checked {
        first_duplicate_position: Option<usize>,
    },
}

/// One owned encoded entry; unique keys have no separate RowID sort tie-breaker.
#[derive(Debug)]
pub(crate) struct HotRunEntry {
    /// Owned physical encoded key.
    pub(crate) key: BTreeKey,
    /// Logical identity of this live row.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 2 consumes extracted RowIDs")
    )]
    pub(crate) row_id: RowID,
}

/// Immutable sorted entries and their allocation-lifetime reservations.
pub(crate) struct HotSortedRun {
    /// Original deterministic group identity, independent of completion order.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 2 consumes retained run metadata")
    )]
    pub(crate) group_id: usize,
    entries: BudgetedVec<HotRunEntry>,
    /// Local evidence selected by the invocation duplicate policy.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 2 consumes retained run metadata")
    )]
    pub(crate) duplicates: LocalDuplicates,
    #[cfg(feature = "profiling")]
    profile: HotBuildWorkerProfile,
    // Entries (including keys) are destroyed before their payload admission.
    payload: MemoryReservation,
}

impl HotSortedRun {
    /// Borrow sorted owned entries without cloning keys or building references.
    pub(crate) fn entries(&self) -> &[HotRunEntry] {
        &self.entries
    }
}

/// Shared immutable run owners in planned order with checked coordinate access.
pub(crate) struct SortedHotRuns {
    runs: Vec<Arc<HotSortedRun>>,
    /// Completed build counts, durations, and scratch high-water.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 2 consumes retained run metadata")
    )]
    #[cfg(feature = "profiling")]
    pub(crate) measurements: HotBuildMeasurements,
    /// Shared admission retained for run ownership and downstream phases.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "phase 2 consumes retained run metadata")
    )]
    pub(crate) budget: MemoryBudget,
}

impl SortedHotRuns {
    /// Borrow the retained nonempty run owners.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "immutable run interface reserved for RFC 0032 phase 2"
        )
    )]
    pub(crate) fn runs(&self) -> &[Arc<HotSortedRun>] {
        &self.runs
    }

    /// Borrow an entry only when both coordinates are in bounds.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "immutable run interface reserved for RFC 0032 phase 2"
        )
    )]
    pub(crate) fn entry(&self, run: usize, position: usize) -> Option<&HotRunEntry> {
        self.runs.get(run)?.entries.get(position)
    }

    /// Borrow the direct one-run view used by the next phase's bypass.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "immutable run interface reserved for RFC 0032 phase 2"
        )
    )]
    pub(crate) fn single_run(&self) -> Option<&[HotRunEntry]> {
        if self.runs.len() == 1 {
            Some(self.runs[0].entries())
        } else {
            None
        }
    }

    /// Compare physical keys, then deterministic original group and position.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "immutable run interface reserved for RFC 0032 phase 2"
        )
    )]
    pub(crate) fn compare(&self, left: (usize, usize), right: (usize, usize)) -> Option<Ordering> {
        let l = self.entry(left.0, left.1)?;
        let r = self.entry(right.0, right.1)?;
        Some(l.key.cmp(&r.key).then_with(|| {
            (self.runs[left.0].group_id, left.1).cmp(&(self.runs[right.0].group_id, right.1))
        }))
    }
}

type JobResult = RuntimeOrFatalResult<Option<Arc<HotSortedRun>>>;
type JobCompletion = Arc<Completion<JobResult>>;

/// Planned page range and its completion retained until collection.
struct HotBuildJob {
    pages: Range<usize>,
    completion: Option<JobCompletion>,
}

/// Parallel page-group extraction and local sorting into owned runs.
///
/// Dropping the borrowed `execute()` future retains all accepted work in this owner.
pub(crate) struct HotLocalSort {
    source: Arc<HotBuildSource>,
    pool: QuiescentGuard<ThreadPool>,
    max_workers: usize,
    stop: Arc<AtomicBool>,
    jobs: Vec<HotBuildJob>,
    runs: Vec<Arc<HotSortedRun>>,
    submitted: usize,
    collected: usize,
    failure: Option<RuntimeOrFatalError>,
    #[cfg(feature = "profiling")]
    profile: HotBuildProfile,
    finished: bool,
}

impl HotLocalSort {
    /// Plan page groups and prepare bounded bookkeeping before accepting any job.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "phase 1 primitive; production callers migrate in RFC 0032 phases 4 and 5"
        )
    )]
    pub(crate) fn new(
        source: HotBuildSource,
        pool: QuiescentGuard<ThreadPool>,
        policy: HotBuildPolicy,
    ) -> Self {
        let jobs: Vec<_> = policy
            .plan_groups(source.pages.len())
            .into_iter()
            .map(|pages| HotBuildJob {
                pages,
                completion: None,
            })
            .collect();
        let groups = jobs.len();
        let runs = Vec::with_capacity(groups);
        #[cfg(feature = "profiling")]
        let profile = HotBuildProfile::new(HotBuildMeasurements {
            capture_elapsed_nanos: source.capture_elapsed_nanos,
            source_pages: source.pages.len() as u64,
            workers: policy.max_workers as u64,
            page_target: policy.target_pages_per_run as u64,
            planned_groups: groups as u64,
            ..Default::default()
        });
        Self {
            source: Arc::new(source),
            pool,
            max_workers: policy.max_workers,
            stop: Arc::new(AtomicBool::new(false)),
            jobs,
            runs,
            submitted: 0,
            collected: 0,
            failure: None,
            #[cfg(feature = "profiling")]
            profile,
            finished: false,
        }
    }

    /// Extract and locally sort page groups through a borrowed future.
    /// Cancellation leaves all records in this owner.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "phase 1 primitive; production callers migrate in RFC 0032 phases 4 and 5"
        )
    )]
    pub(crate) async fn execute(&mut self) -> RuntimeOrFatalResult<SortedHotRuns> {
        assert!(
            !self.finished,
            "hot local sort executed after terminal settlement"
        );
        #[cfg(feature = "profiling")]
        self.profile.start();
        while self.collected < self.jobs.len() {
            while self.failure.is_none()
                && self.submitted < self.jobs.len()
                && self.submitted - self.collected < self.max_workers
            {
                let group = self.submitted;
                let job = worker::submit(
                    &self.pool,
                    self.source.clone(),
                    self.stop.clone(),
                    group,
                    self.jobs[group].pages.clone(),
                );
                self.jobs[group].completion = Some(job);
                self.submitted += 1;
            }
            if self.failure.is_some() {
                break;
            }
            self.collect_next().await;
        }
        if self.failure.is_some() {
            self.settle().await?;
            unreachable!("failed hot local sort settlement returns its failure");
        }
        self.finished = true;
        let runs = take(&mut self.runs);
        #[cfg(feature = "profiling")]
        let measurements = self.profile.finish(self.source.budget.peak(), runs.len());
        #[cfg(feature = "profiling")]
        self.source.profiler.record(measurements);
        Ok(SortedHotRuns {
            runs,
            #[cfg(feature = "profiling")]
            measurements,
            budget: self.source.budget.clone(),
        })
    }

    /// Stop admission and drain every accepted child, preserving later Fatal precedence.
    ///
    /// Pool reservation is the acceptance edge. Each supervised child produces
    /// its authoritative move-once completion after dropping page/resource captures.
    /// Poison and shutdown do not replace this wait: pool teardown drains accepted
    /// jobs with storage still live. The retained sort owns cancellation cleanup;
    /// dropping this borrowed future leaves its next completion in the ledger.
    pub(crate) async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        self.stop.store(true, AtomicOrdering::Release);
        while self.collected < self.submitted {
            self.collect_next().await;
        }
        self.runs.clear();
        self.finished = true;
        match self.failure.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn collect_next(&mut self) {
        let group = self.collected;
        let job = self.jobs[group]
            .completion
            .as_ref()
            .unwrap_or_else(|| unreachable!("submitted hot-build job has a ledger slot"));
        // Do not take the slot until the completion wait is ready: this is the
        // cancellation-safe ownership boundary described by settle().
        let result = job.wait_take_result().await;
        self.jobs[group].completion = None;
        self.collected += 1;
        let result = result
            .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::IndexAccess))
            .and_then(|result| result);
        let result = result.attach_with(|| {
            format!(
                "operation=hot_index_build, phase=collect, table_id={}, index={}, group={group}",
                self.source.table.table_id(),
                self.source.key.index
            )
        });
        match result {
            Ok(Some(run)) => {
                #[cfg(feature = "profiling")]
                self.profile.collect(&run.profile, run.entries.len());
                // All result slots were allocated before submission.
                if run.entries.is_empty() {
                    return;
                }
                self.runs.push(run);
            }
            Ok(None) => {}
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

impl Drop for HotLocalSort {
    fn drop(&mut self) {
        self.stop.store(true, AtomicOrdering::Release);
        // Accepted closures retain source authority and pool/page handles until
        // supervised completion even if a bootstrap owner abandons this ledger.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use crate::buffer::guard::PageGuard;
    use crate::catalog::index::tests::serial_hot_build_test_entries;
    use crate::catalog::index::{IndexDdlGateScope, capture_hot_index_build};
    use crate::catalog::{
        StorageColumnFlags, StorageColumnSpec, StorageIndexFlags, StorageIndexKey,
        StorageIndexSpec, StorageTableSpec,
    };
    use crate::component::panic_payload_description;
    use crate::conf::{EvictableBufferPoolConfig, ThreadPoolConfig};
    use crate::error::{DataIntegrityError, FatalError};
    use crate::error::{ResourceError, RuntimeError};
    use crate::id::{PageID, TrxID};
    use crate::index::BTreeKeyEncoder;
    use crate::log::redo::{RowRedo, RowRedoKind};
    use crate::recovery::{
        OwnedReplayOp, RowReplayState, capture_hot_build_test_source, pack_test_ops,
    };
    use crate::row::ops::UpdateCol;
    use crate::runtime::yield_now;
    use crate::table::tests::lightweight_test_engine_config;
    use crate::table::{CreateIndexPlan, RowPageDescriptor, Table};
    use crate::trx::MIN_SNAPSHOT_TS;
    use crate::value::{Val, ValKind, ValType};
    use error_stack::Report;
    use std::collections::BTreeMap;
    use std::iter::once;
    use std::mem::take;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use tempfile::TempDir;

    struct Fixture {
        table: Arc<Table>,
        descriptors: Vec<RowPageDescriptor>,
        states: Vec<RowReplayState>,
        engine: Engine,
        _temp: TempDir,
    }

    impl Fixture {
        async fn new(pages: usize, rows: usize, wide: bool) -> Self {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                lightweight_test_engine_config(temp.path().to_path_buf(), "hot-build")
                    .thread_pool(ThreadPoolConfig::default().worker_threads(4))
                    .data_buffer(
                        EvictableBufferPoolConfig::default()
                            .max_mem_size(64usize * 1024 * 1024)
                            .max_file_size(256usize * 1024 * 1024),
                    ),
            )
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_id = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::I32, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::NULLABLE),
                    ]),
                    vec![StorageIndexSpec::new(
                        vec![StorageIndexKey::new(0)],
                        StorageIndexFlags::UK,
                    )],
                )
                .await
                .unwrap()
                .table_id();
            drop(session);
            let table = engine.inner().core.catalog().get_table(table_id).unwrap();
            let guards = engine.inner().core.pools.pool_guards();
            let mut descriptors = Vec::new();
            let mut states = Vec::new();
            for page_index in 0..pages {
                // Reverse physical IDs to keep RowID order independent of allocation identity.
                let page_id = PageID::new((100 + pages - page_index) as u64);
                let page = table
                    .row_store
                    .allocate_row_page_at(guards, rows.max(1), page_id)
                    .await
                    .unwrap();
                let start_row_id = page.page().header.start_row_id;
                let descriptor = RowPageDescriptor {
                    page_id,
                    start_row_id,
                    end_row_id: start_row_id + page.page().header.max_row_count as u64,
                };
                drop(page);
                let mut replay = RowReplayState::new(descriptor);
                let ops = (0..rows).filter(|slot| slot % 7 != 0).flat_map(|slot| {
                    let row_id = start_row_id + slot as u64;
                    let value = if wide {
                        format!("{}-{:04}", "wide".repeat(20), slot % 5)
                    } else {
                        format!("key-{}", slot % 5)
                    };
                    let key = (pages * rows - (page_index * rows + slot)) as i32;
                    let insert = OwnedReplayOp {
                        cts: TrxID::new(10),
                        row: RowRedo {
                            row_id,
                            kind: RowRedoKind::Insert(
                                page_id,
                                vec![
                                    Val::from(key),
                                    if slot % 13 == 0 {
                                        Val::Null
                                    } else {
                                        Val::from(value.as_str())
                                    },
                                ],
                            ),
                        },
                    };
                    let delete = (slot % 11 == 0).then_some(OwnedReplayOp {
                        cts: TrxID::new(11),
                        row: RowRedo {
                            row_id,
                            kind: RowRedoKind::Delete(Some(page_id)),
                        },
                    });
                    once(insert).chain(delete)
                });
                table
                    .recover_row_batch(guards, &mut replay, &pack_test_ops(ops), false)
                    .await
                    .unwrap();
                descriptors.push(descriptor);
                states.push(replay);
            }
            Self {
                table,
                descriptors,
                states,
                engine,
                _temp: temp,
            }
        }

        fn policy(&self, workers: usize, target: usize) -> HotBuildPolicy {
            HotBuildPolicy::new(
                HotIndexBuildConfig::default()
                    .max_workers(Some(workers))
                    .target_pages_per_run(target),
                4,
            )
            .unwrap()
        }

        fn plan(&self, columns: &[usize], unique: bool) -> CreateIndexPlan {
            self.table
                .finalize_create_index(StorageIndexSpec::new(
                    columns
                        .iter()
                        .map(|&column| StorageIndexKey::new(column as u16))
                        .collect(),
                    if unique {
                        StorageIndexFlags::UK
                    } else {
                        StorageIndexFlags::empty()
                    },
                ))
                .unwrap()
        }

        async fn source(
            &self,
            plan: &CreateIndexPlan,
            policy: HotBuildPolicy,
            recovery: bool,
        ) -> HotBuildSource {
            if recovery {
                let source = capture_hot_build_test_source(
                    &self.engine,
                    self.table.clone(),
                    plan.new_index_spec(),
                    Some(
                        self.descriptors
                            .iter()
                            .copied()
                            .map(RowReplayState::new)
                            .collect(),
                    ),
                    policy,
                )
                .await
                .unwrap();
                assert_eq!(source.key.build_ts, MIN_SNAPSHOT_TS);
                source
            } else {
                let gates = Arc::new(
                    IndexDdlGateScope::acquire(
                        self.table.clone(),
                        self.engine.inner().core.catalog.clone(),
                    )
                    .await
                    .unwrap(),
                );
                let source = capture_hot_index_build(
                    plan,
                    gates,
                    self.engine.inner().core.pools.pool_guards().clone(),
                    TrxID::new(42),
                    policy,
                    #[cfg(feature = "profiling")]
                    self.engine.inner().core.trx_sys.hot_build_profiler.clone(),
                )
                .await
                .unwrap();
                assert_eq!(source.key.build_ts, TrxID::new(42));
                source
            }
        }

        async fn oracle(&self, plan: &CreateIndexPlan) -> Vec<(BTreeKey, RowID)> {
            let mut entries =
                serial_hot_build_test_entries(plan, self.engine.inner().core.pools.pool_guards())
                    .await;
            entries.sort_unstable();
            entries
        }
    }

    #[derive(Clone, Copy)]
    enum Fault {
        None,
        Runtime,
        Panic,
    }

    #[derive(Clone)]
    struct WorkerGate {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        fault: Fault,
    }

    #[derive(Default)]
    pub(super) struct WorkerHooks {
        enabled: AtomicBool,
        gates: parking_lot::Mutex<BTreeMap<usize, WorkerGate>>,
    }

    impl WorkerHooks {
        fn gate(&self, group: usize, fault: Fault) -> (flume::Receiver<()>, flume::Sender<()>) {
            let (entered, arrival) = flume::unbounded();
            let (release, wait) = flume::unbounded();
            self.gates.lock().insert(
                group,
                WorkerGate {
                    entered,
                    release: wait,
                    fault,
                },
            );
            self.enabled.store(true, AtomicOrdering::Release);
            (arrival, release)
        }

        /// Await the configured entry gate and apply its requested worker failure.
        pub(super) async fn before(&self, group: usize) -> RuntimeOrFatalResult<()> {
            if !self.enabled.load(AtomicOrdering::Acquire) {
                return Ok(());
            }
            let gate = self.gates.lock().get(&group).cloned();
            if let Some(gate) = gate {
                gate.entered.send(()).unwrap();
                gate.release.recv_async().await.unwrap();
                match gate.fault {
                    Fault::None => {}
                    Fault::Runtime => {
                        return Err(Report::new(DataIntegrityError::InvalidPayload)
                            .attach("injected worker failure")
                            .change_context(RuntimeError::IndexAccess)
                            .into());
                    }
                    Fault::Panic => panic!("injected hot-build panic"),
                }
            }
            Ok(())
        }
    }

    fn contents(runs: &SortedHotRuns) -> Vec<(BTreeKey, RowID)> {
        let mut entries: Vec<_> = runs
            .runs()
            .iter()
            .flat_map(|run| {
                run.entries()
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.row_id))
            })
            .collect();
        entries.sort_unstable();
        entries
    }

    /// Purpose: Publish exactly one engine snapshot per successful extraction through either source adapter.
    /// Expected: Public counters retain prior samples, match live rows, and remain unchanged after a failed build.
    #[cfg(feature = "profiling")]
    #[test]
    fn publishes_completed_profiles_through_session() {
        smol::block_on(async {
            let fixture = Fixture::new(1, 10, false).await;
            let mut session = fixture.engine.new_session().unwrap();
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            assert_eq!(session.hot_index_build_stats().unwrap().completed_builds, 0);
            for (index, recovery) in [false, true].into_iter().enumerate() {
                let source = fixture.source(&plan, policy, recovery).await;
                let mut sort =
                    HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
                let runs = sort.execute().await.unwrap();
                let stats = session.hot_index_build_stats().unwrap();
                assert_eq!(stats.completed_builds, (index + 1) as u64);
                assert_eq!(stats.entries, ((index + 1) * 8) as u64);
                assert_eq!(stats.source_pages, (index + 1) as u64);
                assert_eq!(stats.nonempty_runs, (index + 1) as u64);
                assert_eq!(stats.planned_groups, (index + 1) as u64);
                assert!(stats.scratch_peak_bytes >= runs.measurements.scratch_peak_bytes);
                if recovery {
                    assert_eq!(runs.measurements.duplicate_worker_time_nanos, 0);
                    assert_eq!(runs.measurements.duplicate_wall_elapsed_nanos, 0);
                }
            }
            let before = session.hot_index_build_stats().unwrap();
            let source = fixture.source(&plan, policy, false).await;
            budget::fail_at(&source.budget, "run entries");
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            assert!(sort.execute().await.is_err());
            assert_eq!(session.hot_index_build_stats().unwrap(), before);
            session.close().await.unwrap();
            assert!(session.hot_index_build_stats().is_err());
        });
    }

    /// Purpose: Protect balanced grouping and arithmetic at target and cap boundaries.
    /// Expected: Ranges cover each page exactly once, assign extra pages to earlier groups, and never exceed four balanced groups per worker.
    #[test]
    fn grouping_boundaries() {
        for workers in [1, 2, 4] {
            for target in [1, 128, usize::MAX] {
                let policy = HotBuildPolicy::new(
                    HotIndexBuildConfig::default()
                        .max_workers(Some(workers))
                        .target_pages_per_run(target),
                    4,
                )
                .unwrap();
                for pages in [0, 1, 127, 128, 129, 2049, usize::MAX] {
                    let ranges = policy.plan_groups(pages);
                    let groups = ranges.len();
                    assert_eq!(groups, pages.div_ceil(target).min(workers * 4));
                    let mut covered = 0;
                    let mut min = usize::MAX;
                    let mut max = 0;
                    for range in ranges {
                        assert_eq!(range.start, covered);
                        covered = range.end;
                        min = min.min(range.len());
                        max = max.max(range.len());
                    }
                    assert_eq!(covered, pages);
                    assert!(groups == 0 || max - min <= 1);
                }
            }
        }
        let policy = HotBuildPolicy::new(
            HotIndexBuildConfig::default()
                .max_workers(Some(1))
                .target_pages_per_run(4),
            4,
        )
        .unwrap();
        assert_eq!(policy.plan_groups(10), vec![0..4, 4..7, 7..10]);
    }

    /// Purpose: Verify concurrent scratch admission, reusable payload reservations, and allocation-before-release ownership.
    /// Expected: Admission never exceeds the ceiling, rejected overlapping growth preserves contents, and reset/drop release only owned capacity once.
    #[test]
    fn scratch_concurrency_growth_and_drop() {
        let budget = MemoryBudget::new(4096);
        assert_eq!(budget.used(), 0);
        let live = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..4 {
                let budget = budget.clone();
                let live = &live;
                scope.spawn(move || {
                    for _ in 0..100 {
                        if let Ok(reservation) = budget.reserve(1024, "concurrent test") {
                            assert!(live.fetch_add(1024, AtomicOrdering::SeqCst) + 1024 <= 4096);
                            thread::yield_now();
                            live.fetch_sub(1024, AtomicOrdering::SeqCst);
                            drop(reservation);
                        }
                    }
                });
            }
        });
        assert_eq!(budget.used(), 0);
        #[cfg(feature = "profiling")]
        assert!(budget.peak() <= 4096);
        let retained = budget.reserve(512, "retained payload").unwrap();
        let mut payload = MemoryReservation::new(&budget);
        assert_eq!(budget.used(), 512);
        for bytes in [1024, 256] {
            payload.grow(bytes, "reused payload").unwrap();
            assert_eq!(budget.used(), 512 + bytes);
            payload.release_all();
            assert_eq!(budget.used(), 512);
            payload.release_all();
            assert_eq!(budget.used(), 512);
        }
        drop(payload);
        assert_eq!(budget.used(), 512);
        drop(retained);
        assert_eq!(budget.used(), 0);

        let mut values = BudgetedVec::new(&budget);
        values.ensure_capacity(400, "old vector").unwrap();
        values.push(7u64, "initial").unwrap();
        let before = budget.used();
        let error = values.ensure_capacity(401, "overlap").unwrap_err();
        assert_eq!(error.current_context(), &ResourceError::InsufficientMemory);
        assert!(format!("{error:?}").contains("allocation=overlap"));
        assert_eq!(&*values, &[7]);
        assert_eq!(budget.used(), before);
        assert!(values.ensure_capacity(usize::MAX, "overflow").is_err());
        drop(values);
        assert_eq!(budget.used(), 0);

        // A value's destructor is an independent observation that its vector
        // allocation is still live: admission must not have been released yet.
        struct DropProbe {
            budget: MemoryBudget,
            drops: Arc<AtomicUsize>,
            minimum: usize,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                assert!(self.budget.used() >= self.minimum);
                self.drops.fetch_add(1, AtomicOrdering::SeqCst);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let mut observed = BudgetedVec::new(&budget);
        observed.ensure_capacity(2, "observed capacity").unwrap();
        let minimum = 2 * size_of::<DropProbe>();
        for _ in 0..2 {
            observed
                .push(
                    DropProbe {
                        budget: budget.clone(),
                        drops: drops.clone(),
                        minimum,
                    },
                    "reserved probe",
                )
                .unwrap();
        }
        assert_eq!(budget.used(), minimum);
        drop(observed);
        assert_eq!(drops.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(budget.used(), 0);
    }

    /// Purpose: Preserve scalar, nullable, segmented, inline, outlined, and float key encodings.
    /// Expected: Encoding with a precomputed length matches the established path for vector and iterator constructors, including nullable variable prefixes.
    #[test]
    fn encoding_with_len_matches_standard() {
        let fixtures = [
            (
                vec![
                    ValType::new(ValKind::I32, false),
                    ValType::new(ValKind::U64, false),
                ],
                vec![Val::from(-7i32), Val::from(42u64)],
            ),
            (
                vec![ValType::new(ValKind::U64, false); 4],
                vec![Val::from(42u64); 4],
            ),
            (
                vec![ValType::new(ValKind::F64, false)],
                vec![Val::from(-0.0f64)],
            ),
            (
                vec![ValType::new(ValKind::F64, false)],
                vec![Val::from(f64::NAN)],
            ),
            (
                vec![ValType::new(ValKind::F32, false)],
                vec![Val::from(f32::NEG_INFINITY)],
            ),
            (
                vec![
                    ValType::new(ValKind::VarByte, true),
                    ValType::new(ValKind::I32, false),
                ],
                vec![Val::Null, Val::from(-4i32)],
            ),
            (
                vec![
                    ValType::new(ValKind::VarByte, false),
                    ValType::new(ValKind::U64, false),
                ],
                vec![
                    Val::from("long variable prefix crossing segments"),
                    Val::from(14u64),
                ],
            ),
        ];
        for (types, values) in fixtures {
            let ordinary = BTreeKeyEncoder::new(types.clone());
            let from_iter = BTreeKeyEncoder::new(types.iter().copied());
            let len = from_iter.encoded_len(&values).unwrap();
            let expected = ordinary.encode(&values);
            assert_eq!(len, expected.as_bytes().len());
            assert_eq!(from_iter.encode_with_len(&values, len), expected);
        }
        for len in [0, 14, 23, 24, 25, 255] {
            let values = [Val::from(vec![0xab; len])];
            let encoder = BTreeKeyEncoder::new(vec![ValType::new(ValKind::VarByte, true)]);
            let len = encoder.encoded_len(&values).unwrap();
            assert_eq!(
                encoder.encode_with_len(&values, len),
                encoder.encode(&values)
            );
        }
    }

    /// Purpose: Compare both source adapters with serial extraction across sparse, dense, deleted, wide, and empty data.
    /// Expected: Sorted runs retain exactly the live key/RowID multiset, preserve planned ordering, and obey local duplicate policy.
    #[test]
    fn adapters_match_serial_contents() {
        smol::block_on(async {
            for (pages, rows, wide, target) in [
                (0, 0, false, 1),
                (3, 0, false, 1),
                (1, 1, false, 1),
                (1, 25, false, 1),
                (19, 25, false, 1),
                (7, 25, true, 1),
                // Dense pages in one group spanning multiple cooperative batches.
                (33, 300, false, 128),
            ] {
                let fixture = Fixture::new(pages, rows, wide).await;
                assert_eq!(fixture.engine.inner().core.hot_build_policy.max_workers, 4);
                for (columns, unique) in [
                    (&[0][..], true),
                    (&[1][..], true),
                    (&[1][..], false),
                    (&[1, 0][..], false),
                ] {
                    let plan = fixture.plan(columns, unique);
                    let expected = fixture.oracle(&plan).await;
                    for recovery in [false, true] {
                        for workers in [1, 2, 4] {
                            let policy = fixture.policy(workers, target);
                            let source = fixture.source(&plan, policy, recovery).await;
                            let budget = source.budget.clone();
                            let mut sort = HotLocalSort::new(
                                source,
                                fixture.engine.inner().thread_pool.clone(),
                                policy,
                            );
                            let runs = sort.execute().await.unwrap();
                            assert_eq!(
                                contents(&runs),
                                expected,
                                "pages={pages}, wide={wide}, columns={columns:?}, unique={unique}, recovery={recovery}, workers={workers}, target={target}"
                            );
                            #[cfg(feature = "profiling")]
                            assert_eq!(runs.measurements.entries, expected.len() as u64);
                            #[cfg(feature = "profiling")]
                            assert_eq!(runs.measurements.source_pages, pages as u64);
                            #[cfg(feature = "profiling")]
                            assert_eq!(runs.measurements.workers, workers as u64);
                            #[cfg(feature = "profiling")]
                            assert_eq!(runs.measurements.page_target, target as u64);
                            assert_eq!(runs.budget.used(), budget.used());
                            #[cfg(feature = "profiling")]
                            assert_eq!(runs.measurements.planned_groups, sort.jobs.len() as u64);
                            #[cfg(feature = "profiling")]
                            assert_eq!(runs.measurements.nonempty_runs, runs.runs().len() as u64);
                            #[cfg(feature = "profiling")]
                            assert!(
                                runs.measurements.scratch_peak_bytes
                                    <= policy.max_scratch_bytes as u64
                            );
                            assert!(runs.entry(runs.runs().len(), 0).is_none());
                            assert!(runs.compare((usize::MAX, 0), (0, 0)).is_none());
                            for run in runs.runs() {
                                assert!(
                                    run.entries()
                                        .windows(2)
                                        .all(|pair| pair[0].key <= pair[1].key)
                                );
                                let duplicate = run
                                    .entries()
                                    .windows(2)
                                    .position(|pair| pair[0].key == pair[1].key)
                                    .map(|index| index + 1);
                                assert_eq!(
                                    run.duplicates,
                                    if unique && !recovery {
                                        LocalDuplicates::Checked {
                                            first_duplicate_position: duplicate,
                                        }
                                    } else {
                                        LocalDuplicates::Unchecked
                                    }
                                );
                            }
                            assert!(
                                runs.runs()
                                    .windows(2)
                                    .all(|pair| pair[0].group_id < pair[1].group_id)
                            );
                            assert_eq!(runs.single_run().is_some(), runs.runs().len() == 1);
                            drop(sort);
                            let held = runs.runs().first().cloned();
                            drop(runs);
                            if let Some(run) = held {
                                assert!(budget.used() > 0);
                                drop(run);
                            }
                            assert_eq!(budget.used(), 0);
                        }
                    }
                }
            }
        });
    }

    /// Purpose: Preserve accepted children when a borrowed extraction future is dropped.
    /// Expected: Worker admission remains bounded, explicit settlement waits for every gated sibling, and all safe scratch is released.
    #[test]
    fn cancellation_and_bounded_settlement() {
        use futures::FutureExt;
        smol::block_on(async {
            let fixture = Fixture::new(12, 25, false).await;
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            let source = fixture.source(&plan, policy, false).await;
            let (first_entered, first_release) = source.test.gate(0, Fault::None);
            let (second_entered, second_release) = source.test.gate(1, Fault::None);
            let budget = source.budget.clone();
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            assert!(sort.execute().now_or_never().is_none());
            first_entered.recv_async().await.unwrap();
            second_entered.recv_async().await.unwrap();
            assert_eq!(sort.submitted, 2);
            assert_eq!(sort.collected, 0);
            first_release.send(()).unwrap();
            assert!(sort.settle().now_or_never().is_none());
            assert_eq!(sort.submitted, 2);
            second_release.send(()).unwrap();
            sort.settle().await.unwrap();
            assert_eq!(sort.submitted, sort.collected);
            drop(sort);
            assert_eq!(budget.used(), 0);
        });
    }

    /// Purpose: Drain sibling work after an ordinary failure and preserve later panic/Fatal precedence.
    /// Expected: Terminal return waits for both children, stops later admission, and a supervised panic outranks the earlier Runtime error.
    #[test]
    fn failure_settlement_and_later_fatal() {
        use futures::FutureExt;
        smol::block_on(async {
            for second_fault in [Fault::None, Fault::Panic] {
                let fixture = Fixture::new(12, 25, false).await;
                let plan = fixture.plan(&[1], true);
                let policy = fixture.policy(2, 1);
                let source = fixture.source(&plan, policy, true).await;
                let (first_entered, first_release) = source.test.gate(0, Fault::Runtime);
                let (second_entered, second_release) = source.test.gate(1, second_fault);
                let budget = source.budget.clone();
                let mut sort =
                    HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
                assert!(sort.execute().now_or_never().is_none());
                first_entered.recv_async().await.unwrap();
                second_entered.recv_async().await.unwrap();
                first_release.send(()).unwrap();
                sort.collect_next().await;
                assert!(sort.failure.is_some());
                assert!(sort.settle().now_or_never().is_none());
                assert_eq!(sort.submitted, 2);
                second_release.send(()).unwrap();
                let error = sort.settle().await.unwrap_err();
                match second_fault {
                    Fault::Panic => assert!(matches!(error, RuntimeOrFatalError::Fatal(_))),
                    _ => assert!(matches!(error, RuntimeOrFatalError::Runtime(_))),
                }
                assert!(format!("{error:?}").contains("injected worker failure"));
                drop(sort);
                assert_eq!(budget.used(), 0);
            }
        });
    }

    /// Purpose: Check captured-page identity contracts and source range validation.
    /// Expected: Invalid page identities panic, invalid source ranges return errors, and extraction excludes the checkpointed prefix.
    #[test]
    fn captured_source_validation_and_pivots() {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;

        smol::block_on(async {
            let fixture = Fixture::new(3, 25, false).await;
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            let end = fixture.descriptors.last().unwrap().end_row_id;
            for invalid in ["out_of_range", "missing", "reused", "short", "long"] {
                let mut source = fixture.source(&plan, policy, true).await;
                match invalid {
                    "out_of_range" => source.pages[0].page_id = PageID::new(10000),
                    "missing" => source.pages[0].page_id = PageID::new(2),
                    "long" => source.pages[0].end_row_id = source.pages[0].end_row_id + 1,
                    "reused" => source.pages[0].start_row_id = source.pages[0].start_row_id + 1,
                    _ => source.pages[0].end_row_id = source.pages[0].start_row_id + 1,
                }
                let result = AssertUnwindSafe(
                    source
                        .table
                        .row_store
                        .get_captured_row_page_shared(&source.guards, source.pages[0]),
                )
                .catch_unwind()
                .await;
                let Err(payload) = result else {
                    panic!("accepted {invalid} descriptor");
                };
                let message = panic_payload_description(payload.as_ref());
                assert!(
                    message.contains("captured row page"),
                    "{invalid}: {message}"
                );
            }
            for invalid in ["overlap", "duplicate_page", "excess_end"] {
                let mut source = fixture.source(&plan, policy, true).await;
                let budget = source.budget.clone();
                match invalid {
                    "overlap" => source.pages[1].start_row_id = source.pages[0].start_row_id,
                    "duplicate_page" => source.pages[1].page_id = source.pages[0].page_id,
                    _ => source.pages[2].end_row_id = end + 1,
                }
                let error = source.finish_capture(end).unwrap_err();
                assert_invalid_capture(&error, invalid);
                assert!(budget.used() > 0);
                drop(source);
                assert_eq!(budget.used(), 0, "{invalid}");
            }
            let mut source = fixture.source(&plan, policy, true).await;
            source.pages = BudgetedVec::new(&source.budget);
            source.pivot = fixture.descriptors[1].start_row_id;
            source.push_page(fixture.descriptors[0]).unwrap();
            assert!(source.pages.is_empty());
            source.push_page(fixture.descriptors[1]).unwrap();
            assert_eq!(source.pages.len(), 1);
            source.push_page(fixture.descriptors[2]).unwrap();
            let mut straddling = fixture.descriptors[0];
            straddling.end_row_id = source.pivot + 1;
            assert!(source.push_page(straddling).is_err());
            source.finish_capture(end).unwrap();
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            let runs = sort.execute().await.unwrap();
            let start = fixture.descriptors[1].start_row_id;
            let expected: Vec<_> = fixture
                .oracle(&plan)
                .await
                .into_iter()
                .filter(|(_, row)| start <= *row && *row < end)
                .collect();
            assert!(!expected.is_empty());
            assert_eq!(contents(&runs), expected);

            let mut source = fixture.source(&plan, policy, true).await;
            source.pages = BudgetedVec::new(&source.budget);
            source.pivot = end;
            for &page in &fixture.descriptors {
                source.push_page(page).unwrap();
            }
            source.finish_capture(end).unwrap();
            assert!(source.pages.is_empty());
            let budget = source.budget.clone();
            drop(source);
            assert_eq!(budget.used(), 0);
        });
    }

    fn assert_invalid_capture(error: &Report<RuntimeError>, case: &str) {
        assert_eq!(
            error.current_context(),
            &RuntimeError::IndexAccess,
            "{case}: {error:?}"
        );
        assert_eq!(
            error.downcast_ref::<DataIntegrityError>(),
            Some(&DataIntegrityError::InvalidPayload),
            "{case}: {error:?}"
        );
    }

    /// Purpose: Reject incomplete finalized recovery registries independently of live-row occupancy.
    /// Expected: Missing prefixes, interior pages, suffixes, and whole registries fail during capture; complete or genuinely empty coverage succeeds.
    #[test]
    fn recovery_capture_requires_complete_registry() {
        smol::block_on(async {
            for rows in [0, 25] {
                let fixture = Fixture::new(3, rows, false).await;
                let plan = fixture.plan(&[0], true);
                let policy = fixture.policy(2, 1);
                for (case, indices) in [
                    ("complete", Some(&[2, 0, 1][..])),
                    ("missing_first", Some(&[1, 2][..])),
                    ("missing_middle", Some(&[0, 2][..])),
                    ("missing_last", Some(&[0, 1][..])),
                    ("empty", Some(&[][..])),
                    ("absent", None),
                ] {
                    let states = indices.map(|indices| {
                        indices
                            .iter()
                            .map(|&index| RowReplayState::new(fixture.descriptors[index]))
                            .collect()
                    });
                    let result = capture_hot_build_test_source(
                        &fixture.engine,
                        fixture.table.clone(),
                        plan.new_index_spec(),
                        states,
                        policy,
                    )
                    .await;
                    if case == "complete" {
                        let source = result.unwrap();
                        assert_eq!(&*source.pages, &fixture.descriptors);
                        let budget = source.budget.clone();
                        drop(source);
                        assert_eq!(budget.used(), 0);
                    } else {
                        let Err(RuntimeOrFatalError::Runtime(error)) = result else {
                            panic!("expected invalid coverage: case={case}, rows={rows}");
                        };
                        assert_invalid_capture(&error, case);
                        let report = format!("{error:?}");
                        assert!(
                            report.contains("phase=validate_recovery_pages"),
                            "{case}: {report}"
                        );
                        assert!(
                            report.contains(&format!("table_id={}", fixture.table.table_id())),
                            "{case}: {report}"
                        );
                        assert!(
                            report.contains("expected_start=") || report.contains("expected_end="),
                            "{case}: {report}"
                        );
                    }
                }
            }

            let fixture = Fixture::new(0, 0, false).await;
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            let (end, descriptors) = fixture
                .table
                .row_store
                .snapshot_original_row_pages_from(
                    fixture.engine.inner().core.pools.pool_guards(),
                    RowID::new(0),
                )
                .await
                .unwrap();
            assert_eq!(end, RowID::new(0));
            assert!(descriptors.is_empty());
            for states in [None, Some(Vec::new())] {
                let source = capture_hot_build_test_source(
                    &fixture.engine,
                    fixture.table.clone(),
                    plan.new_index_spec(),
                    states,
                    policy,
                )
                .await
                .unwrap();
                assert!(source.pages.is_empty());
                assert_eq!(source.budget.used(), 0);
            }
        });
    }

    /// Purpose: Distinguish unchecked input, first local duplicates, and cross-run-only conflicts.
    /// Expected: Local evidence preserves every entry and total ordering uses run/position rather than unique RowID.
    #[test]
    fn local_duplicates_and_provenance() {
        smol::block_on(async {
            let stop = AtomicBool::new(false);
            assert_eq!(
                worker::local_duplicates(&[], DuplicateCheck::Collect, &stop),
                LocalDuplicates::Checked {
                    first_duplicate_position: None
                }
            );
            let long_unique: Vec<_> = (0..1024u32)
                .map(|value| HotRunEntry {
                    key: BTreeKey::from(value),
                    row_id: RowID::new(u64::from(value)),
                })
                .collect();
            assert_eq!(
                worker::local_duplicates(
                    &long_unique,
                    DuplicateCheck::Collect,
                    &AtomicBool::new(true)
                ),
                LocalDuplicates::Unchecked
            );
            let entries = vec![
                HotRunEntry {
                    key: BTreeKey::from(1u32),
                    row_id: RowID::new(90),
                },
                HotRunEntry {
                    key: BTreeKey::from(1u32),
                    row_id: RowID::new(1),
                },
                HotRunEntry {
                    key: BTreeKey::from(2u32),
                    row_id: RowID::new(3),
                },
            ];
            assert_eq!(
                worker::local_duplicates(&entries, DuplicateCheck::Skip, &stop),
                LocalDuplicates::Unchecked
            );
            assert_eq!(
                worker::local_duplicates(&entries, DuplicateCheck::Collect, &stop),
                LocalDuplicates::Checked {
                    first_duplicate_position: Some(1)
                }
            );
            assert_eq!(
                worker::local_duplicates(&entries[1..], DuplicateCheck::Collect, &stop),
                LocalDuplicates::Checked {
                    first_duplicate_position: None
                }
            );
            let fixture = Fixture::new(3, 2, false).await;
            let plan = fixture.plan(&[1], true);
            let policy = fixture.policy(2, 1);
            let source = fixture.source(&plan, policy, false).await;
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            let mut runs = sort.execute().await.unwrap();
            assert_eq!(runs.runs().len(), 3);
            #[cfg(feature = "profiling")]
            assert_eq!(runs.measurements.entries, 3);
            assert!(runs.runs().iter().all(|run| run.duplicates
                == LocalDuplicates::Checked {
                    first_duplicate_position: None
                }));
            assert_eq!(runs.entry(0, 0).unwrap().key, runs.entry(1, 0).unwrap().key);
            // Reverse RowIDs relative to run order: a RowID tie-breaker would
            // now give the opposite answer for these equal unique keys.
            Arc::get_mut(&mut runs.runs[0]).unwrap().entries[0].row_id = RowID::new(900);
            Arc::get_mut(&mut runs.runs[1]).unwrap().entries[0].row_id = RowID::new(1);
            assert_eq!(runs.compare((0, 0), (1, 0)), Some(Ordering::Less));
            assert_eq!(runs.compare((1, 0), (0, 0)), Some(Ordering::Greater));
            assert_eq!(runs.compare((1, 0), (1, 0)), Some(Ordering::Equal));
        });
    }

    /// Purpose: Keep completed but uncollected work within admission credit and independent of finish order.
    /// Expected: A completed second group cannot admit another job while the first is gated, and final runs follow the original plan.
    #[test]
    fn completed_uncollected_jobs_hold_credit() {
        use futures::FutureExt;
        smol::block_on(async {
            let fixture = Fixture::new(12, 25, false).await;
            let plan = fixture.plan(&[1], true);
            let expected = fixture.oracle(&plan).await;
            let policy = fixture.policy(2, 1);
            let source = fixture.source(&plan, policy, false).await;
            let (entered, release) = source.test.gate(0, Fault::None);
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            assert!(sort.execute().now_or_never().is_none());
            entered.recv_async().await.unwrap();
            // Completion publication is the semantic predicate; yields only let its
            // already accepted producer run. Nextest supplies the hang watchdog.
            while !sort.jobs[1].completion.as_ref().unwrap().is_completed() {
                yield_now().await;
            }
            assert!(sort.execute().now_or_never().is_none());
            assert_eq!((sort.submitted, sort.collected), (2, 0));
            release.send(()).unwrap();
            let runs = sort.execute().await.unwrap();
            assert_eq!(contents(&runs), expected);
            assert_eq!(sort.submitted, sort.jobs.len());
            assert!(runs.runs().iter().any(|run| matches!(
                run.duplicates,
                LocalDuplicates::Checked {
                    first_duplicate_position: Some(_)
                }
            )));
            assert!(
                runs.runs()
                    .windows(2)
                    .all(|pair| pair[0].group_id < pair[1].group_id)
            );
        });
    }

    /// Purpose: Retain source resources after an owner abandons its extraction ledger.
    /// Expected: Accepted children keep the source alive until authoritative completion, then release scratch without further pool admission.
    #[test]
    fn abandoned_owner_retains_source_until_completion() {
        use futures::FutureExt;
        smol::block_on(async {
            let fixture = Fixture::new(12, 25, false).await;
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            let source = fixture.source(&plan, policy, false).await;
            let (entered, release) = source.test.gate(0, Fault::None);
            let budget = source.budget.clone();
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            let weak_source = Arc::downgrade(&sort.source);
            assert!(sort.execute().now_or_never().is_none());
            entered.recv_async().await.unwrap();
            let observers: Vec<_> = sort
                .jobs
                .iter()
                .filter_map(|job| job.completion.clone())
                .collect();
            drop(sort);
            assert!(weak_source.upgrade().is_some());
            release.send(()).unwrap();
            for observer in observers {
                drop(observer.wait_take_result().await.unwrap());
            }
            assert!(weak_source.upgrade().is_none());
            assert_eq!(budget.used(), 0);
        });
    }

    /// Purpose: Preserve typed scratch failures at descriptor, entry, and outlined-key allocation boundaries.
    /// Expected: No failed admission exceeds the cap or loses accepted children, and dropping the settled owner releases every reservation.
    #[test]
    fn scratch_failures_at_allocation_boundaries() {
        smol::block_on(async {
            let fixture = Fixture::new(4, 25, true).await;
            let plan = fixture.plan(&[1, 0], true);
            let policy = fixture.policy(2, 1);
            for purpose in ["page descriptors", "outlined key", "run entries"] {
                let mut source = fixture.source(&plan, policy, true).await;
                let budget = source.budget.clone();
                budget::fail_at(&budget, purpose);
                let error = if purpose == "page descriptors" {
                    source.pages = BudgetedVec::new(&budget);
                    let error = source.push_page(fixture.descriptors[0]).unwrap_err();
                    drop(source);
                    RuntimeOrFatalError::Runtime(error)
                } else {
                    let mut sort = HotLocalSort::new(
                        source,
                        fixture.engine.inner().thread_pool.clone(),
                        policy,
                    );
                    let Err(error) = sort.execute().await else {
                        panic!("admission succeeded at {purpose}")
                    };
                    assert_eq!(sort.submitted, sort.collected, "{purpose}");
                    error
                };
                let RuntimeOrFatalError::Runtime(report) = error else {
                    panic!("scratch failure promoted to Fatal at {purpose}")
                };
                assert_eq!(
                    report.downcast_ref::<ResourceError>(),
                    Some(&ResourceError::InsufficientMemory),
                    "{purpose}"
                );
                let diagnostic = format!("{report:?}");
                assert!(diagnostic.contains(purpose), "{diagnostic}");
                for field in ["requested=", "used=", "limit="] {
                    assert!(diagnostic.contains(field), "{diagnostic}");
                }
                #[cfg(feature = "profiling")]
                assert!(budget.peak() <= policy.max_scratch_bytes);
                assert_eq!(budget.used(), 0, "{purpose}");
            }
        });
    }

    /// Purpose: Allow capture validation and empty extraction when descriptors consume the bulk budget.
    /// Expected: Both adapters complete with no allowance for bookkeeping or worker temporaries, and release all accounted storage.
    #[test]
    fn bulk_budget_excludes_bookkeeping_and_worker_temporaries() {
        smol::block_on(async {
            for pages in [0, 1] {
                let fixture = Fixture::new(pages, 0, true).await;
                let plan = fixture.plan(&[1, 0], false);
                let policy = fixture.policy(2, 1);
                let source = fixture.source(&plan, policy, true).await;
                let descriptor_bytes = source.budget.used();
                drop(source);
                let policy = HotBuildPolicy {
                    max_scratch_bytes: descriptor_bytes.max(1),
                    ..policy
                };
                for recovery in [false, true] {
                    let source = fixture.source(&plan, policy, recovery).await;
                    let budget = source.budget.clone();
                    assert_eq!(budget.used(), descriptor_bytes);
                    let mut sort = HotLocalSort::new(
                        source,
                        fixture.engine.inner().thread_pool.clone(),
                        policy,
                    );
                    let runs = sort.execute().await.unwrap();
                    assert!(runs.runs().is_empty());
                    assert_eq!(budget.used(), descriptor_bytes);
                    #[cfg(feature = "profiling")]
                    assert_eq!(
                        runs.measurements.scratch_peak_bytes,
                        descriptor_bytes as u64
                    );
                    drop(sort);
                    drop(runs);
                    assert_eq!(budget.used(), 0);
                }
            }
        });
    }

    /// Purpose: Preserve ThreadPool's poison contract when extraction admission is rejected.
    /// Expected: Rejected work never enters its worker, returns Fatal, settles its ledger, and releases safe scratch.
    #[test]
    fn poisoned_pool_rejects_without_polling() {
        smol::block_on(async {
            let fixture = Fixture::new(2, 25, false).await;
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            let source = fixture.source(&plan, policy, true).await;
            let (entered, _release) = source.test.gate(0, Fault::None);
            let budget = source.budget.clone();
            let mut sort =
                HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
            fixture.engine.inner().poisoner.poison(
                Report::new(FatalError::ThreadPoolUnavailable)
                    .attach("hot-build admission fixture"),
            );
            let Err(error) = sort.execute().await else {
                panic!("poisoned admission succeeded")
            };
            assert!(matches!(error, RuntimeOrFatalError::Fatal(_)));
            assert!(entered.try_recv().is_err());
            assert_eq!(sort.submitted, sort.collected);
            drop(sort);
            assert_eq!(budget.used(), 0);
        });
    }

    /// Purpose: Extract only latest physical versions after an update and a replacement into a recovery hole.
    /// Expected: Both adapters exclude the deleted old slot, retain the replacement/current key, and preserve original row version maps.
    #[test]
    fn updates_moves_and_original_replay_sidecars() {
        smol::block_on(async {
            let mut fixture = Fixture::new(2, 25, true).await;
            let guards = fixture.engine.inner().core.pools.pool_guards();
            let first = fixture.descriptors[0];
            let second = fixture.descriptors[1];
            let old = first.start_row_id + 1;
            let replacement = second.start_row_id + 7;
            let updated = first.start_row_id + 2;
            let mut maps = Vec::new();
            for page in &fixture.descriptors {
                let guard = fixture
                    .table
                    .row_store
                    .get_captured_row_page_shared(guards, *page)
                    .await
                    .unwrap();
                maps.push(guard.unwrap_vmap() as *const _ as usize);
            }
            let updates = pack_test_ops([
                OwnedReplayOp {
                    cts: TrxID::new(12),
                    row: RowRedo {
                        row_id: old,
                        kind: RowRedoKind::Delete(Some(first.page_id)),
                    },
                },
                OwnedReplayOp {
                    cts: TrxID::new(12),
                    row: RowRedo {
                        row_id: updated,
                        kind: RowRedoKind::Update(
                            first.page_id,
                            vec![UpdateCol {
                                idx: 1,
                                val: Val::from("changed current value"),
                            }],
                        ),
                    },
                },
            ]);
            fixture
                .table
                .recover_row_batch(guards, &mut fixture.states[0], &updates, false)
                .await
                .unwrap();
            let insert = pack_test_ops([OwnedReplayOp {
                cts: TrxID::new(12),
                row: RowRedo {
                    row_id: replacement,
                    kind: RowRedoKind::Insert(
                        second.page_id,
                        vec![Val::from(777i32), Val::from("live replacement")],
                    ),
                },
            }]);
            fixture
                .table
                .recover_row_batch(guards, &mut fixture.states[1], &insert, false)
                .await
                .unwrap();
            let plan = fixture.plan(&[1], false);
            let policy = fixture.policy(2, 1);
            let expected = fixture.oracle(&plan).await;
            for recovery in [false, true] {
                let source = if recovery {
                    capture_hot_build_test_source(
                        &fixture.engine,
                        fixture.table.clone(),
                        plan.new_index_spec(),
                        Some(take(&mut fixture.states)),
                        policy,
                    )
                    .await
                    .unwrap()
                } else {
                    fixture.source(&plan, policy, false).await
                };
                let mut sort =
                    HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
                let runs = sort.execute().await.unwrap();
                let actual = contents(&runs);
                assert_eq!(actual, expected);
                assert!(!actual.iter().any(|(_, id)| *id == old));
                assert_eq!(
                    actual.iter().filter(|(_, id)| *id == replacement).count(),
                    1
                );
                assert_eq!(actual.iter().filter(|(_, id)| *id == updated).count(), 1);
            }
            assert!(fixture.states.is_empty());
            for (page, map) in fixture.descriptors.iter().zip(maps) {
                let guard = fixture
                    .table
                    .row_store
                    .get_captured_row_page_shared(guards, *page)
                    .await
                    .unwrap();
                assert_eq!(guard.unwrap_vmap() as *const _ as usize, map);
            }
        });
    }

    /// Purpose: Preserve a one-run handoff when empty groups surround the only live page.
    /// Expected: Both adapters omit empty runs, keep the original group identity, and expose the exact single entry and caller-selected summary.
    #[test]
    fn empty_groups_leave_one_run() {
        smol::block_on(async {
            let mut fixture = Fixture::new(3, 0, false).await;
            let descriptor = fixture.descriptors[1];
            let batch = pack_test_ops([OwnedReplayOp {
                cts: TrxID::new(12),
                row: RowRedo {
                    row_id: descriptor.start_row_id,
                    kind: RowRedoKind::Insert(descriptor.page_id, vec![Val::from(7i32), Val::Null]),
                },
            }]);
            fixture
                .table
                .recover_row_batch(
                    fixture.engine.inner().core.pools.pool_guards(),
                    &mut fixture.states[1],
                    &batch,
                    false,
                )
                .await
                .unwrap();
            let plan = fixture.plan(&[0], true);
            let policy = fixture.policy(2, 1);
            for recovery in [false, true] {
                let source = fixture.source(&plan, policy, recovery).await;
                let mut sort =
                    HotLocalSort::new(source, fixture.engine.inner().thread_pool.clone(), policy);
                let runs = sort.execute().await.unwrap();
                #[cfg(feature = "profiling")]
                assert_eq!(runs.measurements.planned_groups, 3);
                assert_eq!(runs.runs().len(), 1);
                assert_eq!(runs.runs()[0].group_id, 1);
                let entries = runs.single_run().unwrap();
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].row_id, descriptor.start_row_id);
                assert_eq!(entries[0].key, BTreeKey::from(7i32));
                assert_eq!(
                    runs.runs()[0].duplicates,
                    if recovery {
                        LocalDuplicates::Unchecked
                    } else {
                        LocalDuplicates::Checked {
                            first_duplicate_position: None,
                        }
                    }
                );
            }
        });
    }
}
