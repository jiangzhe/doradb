//! One finite, joined bootstrap task for serial table/index construction.
use super::{RecoveryResources, RowReplayState};
use crate::buffer::{EvictableBufferPool, PoolGuards};
use crate::component::panic_payload_description;
use crate::error::{
    CompletionErrorBridge, CompletionResult, DataIntegrityError, FatalError, MultiDomainResultExt,
    RecoveryDuplicateKey, RuntimeError, RuntimeOrFatalResult,
};
use crate::id::{PageID, TableID};
use crate::index::build::tree_builder::{HotPackedOutcome, StagingMemIndex};
use crate::index::build::{
    DuplicateCheck, HotBuildCapture, HotBuildPolicy, HotBuildTableSource, HotIndexBuild,
    merge_build_result,
};
use crate::map::FastHashMap;
use crate::poison::EnginePoisoner;
#[cfg(feature = "profiling")]
use crate::profiling::{HotIndexBuildProfiler, RecoveryHotIndexMeasurements};
use crate::quiescent::QuiescentGuard;
use crate::runtime::{block_on, thread_pool::ThreadPool};
use crate::table::Table;
use crate::thread::spawn_named;
use crate::trx::MIN_SNAPSHOT_TS;
use error_stack::{Report, ResultExt};
use std::mem;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::thread::{JoinHandle, panicking};
#[cfg(feature = "profiling")]
use std::time::Instant;

#[cfg(test)]
pub(super) use tests::checked_rebuild;

type Histories = FastHashMap<TableID, FastHashMap<PageID, RowReplayState>>;

/// Value-only terminal report; no table or pool guard crosses the channel.
#[derive(Default)]
pub(super) struct RecoveryHotIndexReport {
    /// Validated hot source pages, counted once per table.
    pub(super) pages: u64,
    /// Entries installed and cleaned across all selected indexes.
    pub(super) entries: u64,
    /// Completed index stage measurements without resource owners.
    #[cfg(feature = "profiling")]
    pub(super) measurements: RecoveryHotIndexMeasurements,
}

/// Recovery-local join ownership, never transferred into the component registry.
pub(super) struct RecoveryHotIndexWorker {
    thread: Option<JoinHandle<()>>,
    terminal: flume::Receiver<CompletionResult<RecoveryHotIndexReport>>,
    poisoner: QuiescentGuard<EnginePoisoner>,
}

impl RecoveryHotIndexWorker {
    /// Accept one finite task before any detached index allocation can begin.
    pub(super) fn start(
        resources: &RecoveryResources<'_>,
        tables: Vec<Arc<Table>>,
        histories: Histories,
    ) -> RuntimeOrFatalResult<Option<Self>> {
        Self::start_with_policy(resources, tables, histories, DuplicateCheck::Skip)
    }

    fn start_with_policy(
        resources: &RecoveryResources<'_>,
        mut tables: Vec<Arc<Table>>,
        histories: Histories,
        duplicates: DuplicateCheck,
    ) -> RuntimeOrFatalResult<Option<Self>> {
        tables.sort_unstable_by_key(|table| table.table_id());
        // Even an empty orphan history requires a live table runtime.
        for table_id in histories.keys() {
            if tables
                .binary_search_by_key(table_id, |table| table.table_id())
                .is_err()
            {
                return Err(Report::new(DataIntegrityError::InvalidRootInvariant)
                    .attach(format!(
                        "rebuild hot indexes requires live runtime: table_id={table_id}"
                    ))
                    .change_context(RuntimeError::Recovery)
                    .into());
            }
        }
        if tables.is_empty() {
            return Ok(None);
        }
        let task = RecoveryHotIndexTask {
            tables,
            histories,
            guards: resources.pool_guards.clone(),
            index_pool: resources.pools.index.clone(),
            thread_pool: resources.thread_pool.clone(),
            poisoner: resources.poisoner.clone(),
            policy: resources.hot_build_policy,
            duplicates,
            #[cfg(feature = "profiling")]
            profiler: resources.hot_build_profiler.clone(),
            active: None,
            report: RecoveryHotIndexReport::default(),
            phase: "source_capture",
            current_table: None,
            #[cfg(test)]
            hooks: tests::Hooks::capture(),
        };
        let (sender, terminal) = flume::bounded(1);
        let thread = spawn_named("Recovery-Index", move || {
            // The root future and its settlement never require observer polling.
            // Dropping the observer leaves task admission and cleanup unchanged.
            let result = task
                .run()
                .map_err(CompletionErrorBridge::capture_runtime_or_fatal);
            let _ = sender.try_send(result);
        })?;
        Ok(Some(Self {
            thread: Some(thread),
            terminal,
            poisoner: resources.poisoner.clone(),
        }))
    }

    /// Await the sole terminal result, then join before redo repair or publication.
    /// The task produces progress, the channel result/disconnection is authoritative,
    /// and poison never bypasses this obligation. Drop owns cancellation by joining;
    /// component shutdown cannot start before this recovery-local handle is destroyed.
    pub(super) async fn wait(mut self) -> RuntimeOrFatalResult<RecoveryHotIndexReport> {
        let result = self.terminal.recv_async().await;
        self.join();
        match result {
            Ok(result) => result
                .map_err(|bridge| bridge.into_runtime_or_fatal(RuntimeError::Recovery))
                .attach("operation=recovery_hot_indexes, phase=terminal_result"),
            Err(_) => Err(self
                .poisoner
                .poison_and_get_first(
                    Report::new(FatalError::RecoveryHotIndexPanic)
                        .attach("operation=recovery_hot_indexes, phase=terminal_disconnected"),
                )
                .into_report()
                .into()),
        }
    }

    fn join(&mut self) {
        let Some(thread) = self.thread.take() else {
            return;
        };
        if let Err(payload) = thread.join() {
            self.poisoner.poison(
                Report::new(FatalError::RecoveryHotIndexPanic).attach(format!(
                    "operation=recovery_hot_indexes, phase=join, panic={}",
                    panic_payload_description(payload.as_ref())
                )),
            );
            if panicking() {
                // Preserve an observer's existing unwind without running an
                // arbitrary worker-payload destructor or starting a second panic.
                mem::forget(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

impl Drop for RecoveryHotIndexWorker {
    fn drop(&mut self) {
        // Intentionally synchronous: cancellation waits for accepted work through
        // cleanup, without admitting a new cleanup thread or detaching on timeout.
        self.join();
    }
}

struct RecoveryHotIndexTask {
    tables: Vec<Arc<Table>>,
    histories: Histories,
    guards: PoolGuards,
    index_pool: QuiescentGuard<EvictableBufferPool>,
    thread_pool: QuiescentGuard<ThreadPool>,
    poisoner: QuiescentGuard<EnginePoisoner>,
    policy: HotBuildPolicy,
    duplicates: DuplicateCheck,
    #[cfg(feature = "profiling")]
    profiler: Arc<HotIndexBuildProfiler>,
    // Stage coordinators and cleanup survive an unwind of the borrowed root future.
    active: Option<HotIndexBuild<EvictableBufferPool>>,
    report: RecoveryHotIndexReport,
    phase: &'static str,
    current_table: Option<TableID>,
    #[cfg(test)]
    hooks: tests::Hooks,
}

impl RecoveryHotIndexTask {
    fn run(mut self) -> RuntimeOrFatalResult<RecoveryHotIndexReport> {
        let result = block_on(self.execute()).attach_with(|| self.diagnostic());
        self.phase = "settlement";
        // Cleanup invariant panics must unwind to the joined owner. They are
        // neither converted into typed failures nor followed by another cleanup.
        let cleanup = block_on(self.settle()).attach_with(|| self.diagnostic());
        merge_build_result(result, cleanup)?;
        #[cfg(test)]
        self.hooks.at(tests::Point::Completed);
        Ok(self.report)
    }

    fn diagnostic(&self) -> String {
        let index = self.active.as_ref().map(|active| active.source().key.index);
        let phase = if self.phase == "index_build" {
            self.active
                .as_ref()
                .map_or(self.phase, |active| active.phase())
        } else {
            self.phase
        };
        format!(
            "operation=recovery_hot_indexes, phase={phase}, table_id={:?}, index={index:?}",
            self.current_table
        )
    }

    async fn execute(&mut self) -> RuntimeOrFatalResult<()> {
        for table in self.tables.clone() {
            self.phase = "source_capture";
            self.current_table = Some(table.table_id());
            #[cfg(test)]
            self.hooks.at(tests::Point::Capture(table.table_id()));
            self.poisoner.ensure_healthy()?;
            #[cfg(feature = "profiling")]
            let started = Instant::now();
            let layout = table.layout_snapshot();
            let pivot = table.row_store.blk_idx().pivot_row_id();
            let mut source = HotBuildTableSource::new(
                HotBuildCapture {
                    table: table.clone(),
                    layout: layout.clone(),
                    guards: self.guards.clone(),
                    pivot,
                    ddl: None,
                    #[cfg(feature = "profiling")]
                    profiler: self.profiler.clone(),
                },
                self.policy,
            );
            #[cfg(test)]
            self.hooks.configure_budget(&source.budget);
            // Successful page creation registers each descriptor, and replay
            // drain returns every surviving page's state before this handoff.
            if let Some(pages) = self.histories.remove(&table.table_id()) {
                for replay in pages.into_values() {
                    source.push_page(replay.into_descriptor())?;
                }
            }
            source.finish_capture().attach_with(|| {
                format!(
                    "operation=recovery_hot_indexes, phase=source_coverage, table_id={}",
                    table.table_id()
                )
            })?;
            self.report.pages += source.page_count() as u64;
            #[cfg(feature = "profiling")]
            {
                self.report.measurements.capture_elapsed_nanos +=
                    started.elapsed().as_nanos() as u64;
            }
            let retained = source.budget.used();
            for (_, spec) in layout.metadata().idx.active_indexes() {
                source.budget.reset_peak(retained);
                self.phase = "index_build";
                #[cfg(test)]
                self.hooks
                    .at(tests::Point::Start(table.table_id(), spec.index));
                let selected = Arc::new(source.select(spec, MIN_SNAPSHOT_TS, self.duplicates));
                let mut staging =
                    StagingMemIndex::for_recovery(&selected, self.index_pool.clone())?;
                self.active = Some(HotIndexBuild::new(
                    selected.clone(),
                    self.thread_pool.clone(),
                    self.poisoner.clone(),
                    self.policy,
                ));
                let active = self
                    .active
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("index build just installed"));
                #[cfg(test)]
                self.hooks.configure_build(active);
                let mut ready = match active.build(&mut staging).await? {
                    HotPackedOutcome::Complete(ready) => ready,
                    HotPackedOutcome::Duplicate(conflict) => {
                        return Err(Report::new(
                            DataIntegrityError::UnexpectedRecoveryDuplicateKey,
                        )
                        .attach(RecoveryDuplicateKey {
                            index_slot: spec.index.slot().as_usize(),
                            row_id: conflict.rows[0],
                            deleted: false,
                        })
                        .attach(format!(
                            "index_slot={}, conflicting_rows={:?}",
                            spec.index.slot(),
                            conflict.rows
                        ))
                        .change_context(RuntimeError::Recovery)
                        .into());
                    }
                };
                self.phase = "installation";
                #[cfg(test)]
                self.hooks.at(tests::Point::Install);
                ready.install().await?;
                self.phase = "cleanup";
                active.settle().await?;
                self.report.entries += ready.entries() as u64;
                #[cfg(feature = "profiling")]
                {
                    let (extraction, merge, packed, cleanup) = active.measurements(&ready);
                    self.report
                        .measurements
                        .record(extraction, merge, packed, cleanup);
                }
                drop(ready);
                self.active = None;
                source.budget.reset_peak(retained);
            }
        }
        Ok(())
    }

    async fn settle(&mut self) -> RuntimeOrFatalResult<()> {
        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };
        active.settle().await
    }
}

#[cfg(test)]
mod tests {
    use super::super::RecoveryCoordinator;
    use super::*;
    use crate::HotIndexBuildConfig;
    use crate::catalog::IndexRef;
    use crate::error::{ResourceError, RuntimeOrFatalError};
    use crate::index::build::tree_builder::{
        StagedPageCleanup, gate_recovery_allocation, panic_recovery_cleanup,
    };
    use crate::index::build::{HotBuildTestPoint, observe_hot_build};
    use crate::index::build::{MemoryBudget, fail_build_budget};
    use crate::thread::{SpawnTestEvent, fail_spawn_named, observe_spawn_named};
    use crate::{
        Engine, EngineConfig, StorageColumnFlags, StorageColumnSpec, StorageIndexFlags,
        StorageIndexKey, StorageIndexSpec, StorageTableSpec, Val, ValKind,
    };
    use futures::FutureExt;
    use futures::future::{Either, select};
    use parking_lot::Mutex;
    use std::cell::RefCell;
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Semantic recovery edges for deterministic cancellation and failure tests.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum Point {
        Capture(TableID),
        Start(TableID, IndexRef),
        Build,
        Ready,
        Install,
        Cleanup,
        Completed,
    }

    thread_local! {
        static HOOK: RefCell<Hooks> = RefCell::new(Hooks::default());
    }

    type CleanupHook = Arc<dyn Fn(&StagedPageCleanup<EvictableBufferPool>) + Send + Sync>;

    /// Test-local observer and optional reclamation panic.
    #[derive(Clone, Default)]
    pub(super) struct Hooks {
        observer: Option<Arc<dyn Fn(Point) + Send + Sync>>,
        panic_cleanup: bool,
        cleanup: Option<CleanupHook>,
        budget_failure: Option<&'static str>,
    }

    impl Hooks {
        /// Capture only the calling test thread's installed controls.
        pub(super) fn capture() -> Self {
            HOOK.with(|hook| hook.borrow().clone())
        }

        /// Reject a named scratch reservation through the production admission path.
        pub(super) fn configure_budget(&self, budget: &MemoryBudget) {
            if let Some(purpose) = self.budget_failure {
                fail_build_budget(budget, purpose);
            }
        }

        /// Bridge recovery observations to the caller-neutral build hooks.
        pub(super) fn configure_build(&self, build: &mut HotIndexBuild<EvictableBufferPool>) {
            let stages = self.clone();
            let cleanup = self.clone();
            observe_hot_build(
                build,
                move |point| {
                    stages.at(match point {
                        HotBuildTestPoint::Build => Point::Build,
                        HotBuildTestPoint::Ready => Point::Ready,
                        HotBuildTestPoint::Cleanup => Point::Cleanup,
                    })
                },
                move |pages| cleanup.arm_cleanup(pages),
            );
        }

        /// Arm partial reclamation failure before the first detached allocation.
        pub(super) fn arm_cleanup(
            &self,
            cleanup: &StagedPageCleanup<EvictableBufferPool>,
        ) -> Option<flume::Receiver<()>> {
            if let Some(hook) = &self.cleanup {
                hook(cleanup);
            }
            self.panic_cleanup.then(|| panic_recovery_cleanup(cleanup))
        }

        /// Observe a reached production predicate or inject a polling panic.
        pub(super) fn at(&self, point: Point) {
            if let Some(hook) = &self.observer {
                hook(point);
            }
        }
    }

    struct ResetHook;

    impl Drop for ResetHook {
        fn drop(&mut self) {
            HOOK.with(|hook| *hook.borrow_mut() = Hooks::default());
        }
    }

    /// Run the recovery adapter with explicit checked evidence for corruption tests.
    pub(crate) async fn checked_rebuild(
        recovery: &mut RecoveryCoordinator<'_>,
    ) -> RuntimeOrFatalResult<()> {
        recovery.dispatcher.drain_all().await?;
        let tables = recovery.resources.catalog.snapshot_live_user_tables();
        let histories = mem::take(&mut recovery.dispatcher.page_history);
        if let Some(worker) = RecoveryHotIndexWorker::start_with_policy(
            &recovery.resources,
            tables,
            histories,
            DuplicateCheck::Collect,
        )? {
            worker.wait().await?;
        }
        Ok(())
    }

    fn install(hook: impl Fn(Point) + Send + Sync + 'static) -> ResetHook {
        HOOK.with(|slot| {
            *slot.borrow_mut() = Hooks {
                observer: Some(Arc::new(hook)),
                ..Default::default()
            }
        });
        ResetHook
    }

    async fn fixture(config: EngineConfig, tables: usize) -> Vec<TableID> {
        let engine = Engine::bootstrap(config).await.unwrap();
        let mut session = engine.new_session().unwrap();
        let mut ids = Vec::new();
        for _ in 0..tables {
            let table = session
                .create_table(
                    StorageTableSpec::new(vec![
                        StorageColumnSpec::new(ValKind::U64, StorageColumnFlags::empty()),
                        StorageColumnSpec::new(ValKind::VarByte, StorageColumnFlags::empty()),
                    ]),
                    vec![
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(1), StorageIndexKey::new(0)],
                            StorageIndexFlags::UK,
                        ),
                        StorageIndexSpec::new(
                            vec![StorageIndexKey::new(0), StorageIndexKey::new(1)],
                            StorageIndexFlags::empty(),
                        ),
                    ],
                )
                .await
                .unwrap()
                .table_id();
            let mut trx = session.begin_trx().unwrap();
            for key in 0..400u64 {
                trx.table_insert_mvcc(table, vec![Val::from(key), Val::from(vec![b'x'; 256])])
                    .await
                    .unwrap();
            }
            trx.commit().await.unwrap();
            ids.push(table);
        }
        session.close().await.unwrap();
        drop(session);
        engine.shutdown();
        drop(engine);
        ids
    }

    fn assert_drop_waits(value: impl Send, release: impl FnOnce()) {
        thread::scope(|scope| {
            let (dropping_tx, dropping_rx) = flume::bounded(1);
            let (done_tx, done_rx) = flume::bounded(1);
            let dropper = scope.spawn(move || {
                dropping_tx.send(()).unwrap();
                drop(value);
                done_tx.send(()).unwrap();
            });
            dropping_rx.recv().unwrap();
            assert!(
                done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
                "accepted work was detached"
            );
            release();
            done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            dropper.join().unwrap();
        });
    }

    /// Purpose: Preserve serial table/slot admission, descriptor reuse and fixed roots during recovery.
    /// Expected: Every selected index completes in table/slot order, entries count per index, and trusted comparisons stay zero.
    #[test]
    fn serial_multi_index_recovery_reports_completed_builds() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let config = EngineConfig::default().storage_root(temp.path());
            let ids = fixture(config.clone(), 2).await;
            let events = Arc::new(Mutex::new(Vec::new()));
            let observed = events.clone();
            let _hook = install(move |point| observed.lock().push(point));
            let engine = Engine::bootstrap(config).await.unwrap();
            let report = engine.recovery_report();
            assert_eq!(report.work.index_entries_inserted, 1600);
            let starts: Vec<_> = events
                .lock()
                .iter()
                .filter_map(|point| match point {
                    Point::Start(table, index) => Some((*table, index.slot().as_usize())),
                    _ => None,
                })
                .collect();
            assert_eq!(
                starts,
                vec![(ids[0], 0), (ids[0], 1), (ids[1], 0), (ids[1], 1)]
            );
            let phases: Vec<_> = events
                .lock()
                .iter()
                .filter(|point| matches!(point, Point::Build | Point::Install | Point::Cleanup))
                .copied()
                .collect();
            assert_eq!(
                phases,
                [Point::Build, Point::Install, Point::Cleanup].repeat(4)
            );
            #[cfg(feature = "profiling")]
            {
                assert_eq!(report.hot_indexes.completed_builds, 4);
                assert_eq!(report.hot_indexes.extraction.entries, 1600);
                assert_eq!(
                    report.hot_indexes.extraction.source_pages,
                    2 * report.work.index_rebuild_pages
                );
                assert_eq!(report.hot_indexes.merge.duplicate_comparisons, 0);
                assert!(!report.hot_indexes.merge.checked);
                assert!(report.hot_indexes.leaf_pages > 4);
                assert!(report.hot_indexes.scratch_peak_bytes > 0);
            }
            engine.shutdown();
        });
    }

    /// Purpose: Keep bootstrap cancellation joined through accepted source, build, installation and cleanup work.
    /// Expected: Dropping the observer waits for the gated task, completes teardown, and permits a subsequent bootstrap.
    #[test]
    fn cancelled_bootstrap_joins_recovery_task() {
        for gate in [
            Point::Capture(TableID::new(0)),
            Point::Build,
            Point::Install,
            Point::Cleanup,
            Point::Completed,
        ] {
            smol::block_on(async {
                let temp = TempDir::new().unwrap();
                let config = EngineConfig::default().storage_root(temp.path());
                let tables = fixture(config.clone(), 1).await;
                let gate = if matches!(gate, Point::Capture(_)) {
                    Point::Capture(tables[0])
                } else {
                    gate
                };
                let exits = Arc::new(Mutex::new(Vec::new()));
                let observed_exits = exits.clone();
                let observer = observe_spawn_named(move |event| {
                    if let SpawnTestEvent::Finished(name) = event {
                        observed_exits.lock().push(name);
                    }
                });
                let (entered_tx, entered_rx) = flume::bounded(1);
                let release = Arc::new(Barrier::new(2));
                let released = release.clone();
                let completed = Arc::new(AtomicBool::new(false));
                let observed = completed.clone();
                let once = AtomicBool::new(false);
                let hook = install(move |point| {
                    if point == Point::Completed {
                        observed.store(true, Ordering::Release);
                    }
                    if point == gate && !once.swap(true, Ordering::AcqRel) {
                        entered_tx.send(()).unwrap();
                        released.wait();
                    }
                });
                let bootstrap = Box::pin(Engine::bootstrap(config.clone()));
                let pending = match select(bootstrap, Box::pin(entered_rx.recv_async())).await {
                    Either::Right((Ok(()), pending)) => pending,
                    _ => panic!("bootstrap did not reach {gate:?}"),
                };
                drop(hook);
                assert_drop_waits(pending, || {
                    release.wait();
                });
                assert!(
                    completed.load(Ordering::Acquire),
                    "accepted task must finish after observer cancellation"
                );
                let exits = exits.lock().clone();
                let recovery_exit = exits
                    .iter()
                    .position(|name| name == "Recovery-Index")
                    .unwrap();
                assert_eq!(
                    exits
                        .iter()
                        .filter(|name| *name == "Recovery-Index")
                        .count(),
                    1
                );
                for dependency in ["ThreadPoolWorker-1", "Shared-Pool-Evictor", "IO-Thread"] {
                    assert!(
                        exits.iter().position(|name| name == dependency).unwrap() > recovery_exit,
                        "{gate:?}: {exits:?}"
                    );
                }
                drop(exits);
                drop(observer);
                let recovered = Engine::bootstrap(config).await.unwrap();
                assert_eq!(recovered.recovery_report().work.index_entries_inserted, 800);
                recovered.shutdown();
            });
        }
    }

    /// Purpose: Join bootstrap cancellation while leaf or parent jobs own newly allocated pages.
    /// Expected: The dropping observer waits for accepted producers, then releases storage after their successful installation and cleanup.
    #[test]
    fn cancelled_bootstrap_joins_leaf_and_parent_producers() {
        for height in [0, 1] {
            smol::block_on(async {
                let temp = TempDir::new().unwrap();
                let config = EngineConfig::default().storage_root(temp.path());
                fixture(config.clone(), 1).await;
                let hook = install(|_| {});
                let (gated_tx, gated_rx) = flume::bounded(1);
                let once = AtomicBool::new(false);
                HOOK.with(|hook| {
                    hook.borrow_mut().cleanup = Some(Arc::new(move |cleanup| {
                        if !once.swap(true, Ordering::AcqRel) {
                            let gate = gate_recovery_allocation(cleanup, height);
                            gated_tx.send(gate).unwrap();
                        }
                    }))
                });
                let bootstrap = Box::pin(Engine::bootstrap(config.clone()));
                let (gate, pending) = match select(bootstrap, Box::pin(gated_rx.recv_async())).await
                {
                    Either::Right((Ok(gate), pending)) => (gate, pending),
                    _ => panic!("bootstrap did not arm height {height}"),
                };
                let (entered, release) = gate;
                let pending = match select(pending, Box::pin(entered.recv_async())).await {
                    Either::Right((Ok(()), pending)) => pending,
                    _ => panic!("bootstrap did not reach height {height}"),
                };
                drop(hook);
                assert_drop_waits(pending, || {
                    release.send(()).unwrap();
                });
                let recovered = Engine::bootstrap(config).await.unwrap();
                assert_eq!(recovered.recovery_report().work.index_entries_inserted, 800);
                recovered.shutdown();
            });
        }
    }

    /// Purpose: Reject recovery thread admission before detached construction and supervise coordinator panics.
    /// Expected: Spawn failure stays typed, construction panics become Fatal after settlement, and installation/cleanup panics escape while releasing bootstrap state.
    #[test]
    fn spawn_failure_and_panics_settle_bootstrap() {
        for fault in [
            None,
            Some(Point::Build),
            Some(Point::Ready),
            Some(Point::Install),
            Some(Point::Cleanup),
        ] {
            smol::block_on(async {
                let temp = TempDir::new().unwrap();
                let config = EngineConfig::default().storage_root(temp.path());
                fixture(config.clone(), 1).await;
                let spawn = fault.is_none().then(|| fail_spawn_named("Recovery-Index"));
                let hook = fault.map(|target| {
                    install(move |point| {
                        assert_ne!(point, target, "injected coordinator panic");
                    })
                });
                let outcome = AssertUnwindSafe(Engine::bootstrap(config.clone()))
                    .catch_unwind()
                    .await;
                if matches!(fault, Some(Point::Install | Point::Cleanup)) {
                    let payload = outcome.err().unwrap();
                    assert!(
                        panic_payload_description(payload.as_ref())
                            .contains("injected coordinator panic")
                    );
                } else {
                    let error = outcome.unwrap().err().unwrap();
                    if fault.is_some() {
                        assert_eq!(
                            error.report().downcast_ref::<FatalError>(),
                            Some(&FatalError::ThreadPoolTaskPanic)
                        );
                    } else {
                        assert_eq!(
                            error.report().downcast_ref::<RuntimeError>(),
                            Some(&RuntimeError::BackgroundSpawn)
                        );
                    }
                }
                drop(hook);
                drop(spawn);
                let recovered = Engine::bootstrap(config).await.unwrap();
                assert_eq!(recovered.recovery_report().work.index_entries_inserted, 800);
                recovered.shutdown();
            });
        }
    }

    /// Purpose: Preserve the scratch Resource cause and release failed bootstrap state.
    /// Expected: Descriptor admission failure stays typed and the same storage root reopens with a sufficient budget.
    #[test]
    fn scratch_failure_releases_bootstrap() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let config = EngineConfig::default().storage_root(temp.path());
            fixture(config.clone(), 1).await;
            let limited = config
                .clone()
                .hot_index_build(HotIndexBuildConfig::default().max_scratch_bytes(1));
            let error = Engine::bootstrap(limited).await.err().unwrap();
            assert!(
                error.report().downcast_ref::<ResourceError>().is_some(),
                "{error:?}"
            );
            let recovered = Engine::bootstrap(config).await.unwrap();
            assert_eq!(recovered.recovery_report().work.index_entries_inserted, 800);
            recovered.shutdown();
        });
    }

    /// Purpose: Settle resource failures from every bulk pipeline stage before bootstrap teardown.
    /// Expected: Descriptor, run, merge and staged-page admission failures preserve their Resource cause and permit a clean subsequent reopen.
    #[test]
    fn named_scratch_failures_preserve_resource_causes() {
        for purpose in [
            "page descriptors",
            "run entries",
            "merge boundary positions",
            "staged page tracking",
        ] {
            smol::block_on(async {
                let temp = TempDir::new().unwrap();
                let config = EngineConfig::default().storage_root(temp.path());
                fixture(config.clone(), 1).await;
                let hook = install(|_| {});
                HOOK.with(|hook| hook.borrow_mut().budget_failure = Some(purpose));
                let error = Engine::bootstrap(config.clone()).await.err().unwrap();
                assert!(
                    error.report().downcast_ref::<ResourceError>().is_some(),
                    "{purpose}: {error:?}"
                );
                assert!(
                    format!("{error:?}").contains(purpose),
                    "{purpose}: {error:?}"
                );
                drop(hook);
                let recovered = Engine::bootstrap(config).await.unwrap();
                assert_eq!(recovered.recovery_report().work.index_entries_inserted, 800);
                recovered.shutdown();
            });
        }
    }

    /// Purpose: Propagate a cleanup invariant panic after recovery has partially reclaimed a staged tree.
    /// Expected: Bootstrap resumes the original cleanup panic without retrying reclamation, releases its guards, and permits a subsequent reopen.
    #[test]
    fn cleanup_panic_propagates_through_bootstrap() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let config = EngineConfig::default().storage_root(temp.path());
            fixture(config.clone(), 1).await;
            let installs = AtomicUsize::new(0);
            let hook = install(move |point| {
                if point == Point::Ready && installs.fetch_add(1, Ordering::AcqRel) == 1 {
                    panic!("abort a fully staged recovery tree");
                }
            });
            HOOK.with(|hook| hook.borrow_mut().panic_cleanup = true);
            let payload = AssertUnwindSafe(Engine::bootstrap(config.clone()))
                .catch_unwind()
                .await
                .err()
                .unwrap();
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"injected staged panic")
            );
            drop(hook);
            let recovered = Engine::bootstrap(config).await.unwrap();
            assert_eq!(recovered.recovery_report().work.index_entries_inserted, 800);
            recovered.shutdown();
        });
    }

    /// Purpose: Join a disconnected terminal producer and preserve the first poison reason.
    /// Expected: A clean producer exit returns Fatal; a panicked producer resumes its original panic, and neither replaces prior poison.
    #[test]
    fn terminal_disconnection_joins_and_preserves_poison() {
        for worker_panics in [false, true] {
            smol::block_on(async {
                let temp = TempDir::new().unwrap();
                let engine = Engine::bootstrap(EngineConfig::default().storage_root(temp.path()))
                    .await
                    .unwrap();
                let poisoner = engine.inner().poisoner.clone();
                poisoner.poison(Report::new(FatalError::StorageIo).attach("first cause"));
                let (sender, terminal) = flume::bounded(1);
                let thread = spawn_named("Recovery-Index-test", move || {
                    drop(sender);
                    if worker_panics {
                        panic!("terminal producer lost");
                    }
                })
                .unwrap();
                let worker = RecoveryHotIndexWorker {
                    thread: Some(thread),
                    terminal,
                    poisoner: poisoner.clone(),
                };
                let outcome = AssertUnwindSafe(worker.wait()).catch_unwind().await;
                if worker_panics {
                    let payload = outcome.err().unwrap();
                    assert_eq!(
                        payload.downcast_ref::<&str>(),
                        Some(&"terminal producer lost")
                    );
                } else {
                    let error = outcome.unwrap().err().unwrap();
                    assert!(
                        matches!(error, RuntimeOrFatalError::Fatal(report) if report.current_context() == &FatalError::StorageIo)
                    );
                }
                assert_eq!(
                    poisoner.poison_error().unwrap().current_context(),
                    &FatalError::StorageIo
                );
                drop(poisoner);
                engine.shutdown();
            });
        }
    }

    /// Purpose: Retain one-time join ownership even after a terminal value is buffered.
    /// Expected: Dropping the observer still waits for worker exit and its post-publication work completes once.
    #[test]
    fn buffered_terminal_result_still_requires_join() {
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(temp.path()))
                .await
                .unwrap();
            let (sender, terminal) = flume::bounded(1);
            let (published_tx, published_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let finished = Arc::new(AtomicUsize::new(0));
            let observed = finished.clone();
            let thread = spawn_named("Recovery-Index-test", move || {
                assert!(
                    sender
                        .try_send(Ok(RecoveryHotIndexReport::default()))
                        .is_ok()
                );
                published_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                observed.fetch_add(1, Ordering::AcqRel);
            })
            .unwrap();
            let worker = RecoveryHotIndexWorker {
                thread: Some(thread),
                terminal,
                poisoner: engine.inner().poisoner.clone(),
            };
            published_rx.recv_async().await.unwrap();
            assert_drop_waits(worker, || {
                release_tx.send(()).unwrap();
            });
            assert_eq!(finished.load(Ordering::Acquire), 1);
            engine.shutdown();
        });
    }

    /// Purpose: Suppress a second unwind when bootstrap destruction joins a panicked worker.
    /// Expected: The observer's original panic survives, worker failure poisons admission, and arbitrary panic-payload destruction is avoided.
    #[test]
    fn worker_panic_during_observer_unwind_preserves_original_panic() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("foreign panic payload destructor");
            }
        }
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(temp.path()))
                .await
                .unwrap();
            let (sender, terminal) = flume::bounded(1);
            let thread = spawn_named("Recovery-Index-test", move || {
                drop(sender);
                panic_any(PanicOnDrop);
            })
            .unwrap();
            let worker = RecoveryHotIndexWorker {
                thread: Some(thread),
                terminal,
                poisoner: engine.inner().poisoner.clone(),
            };
            let error = catch_unwind(AssertUnwindSafe(|| {
                let _worker = worker;
                panic!("observer unwind");
            }))
            .unwrap_err();
            assert_eq!(error.downcast_ref::<&str>(), Some(&"observer unwind"));
            assert_eq!(
                engine
                    .inner()
                    .poisoner
                    .poison_error()
                    .unwrap()
                    .current_context(),
                &FatalError::RecoveryHotIndexPanic
            );
            engine.shutdown();
        });
    }
}
