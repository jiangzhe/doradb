use crate::cli::{LockTableArgs, LockTableMode, LockTableScenario, TableLockScope, Workload};
use crate::error::{BenchError, Result};
use crate::manifest::{KeyRange, Manifest};
use crate::workload::util::RandomTableIndexGenerator;
use crate::workload::{CommonConfig, SessionPlan, SessionSummary, WorkloadConfig, WorkloadRunner};
use doradb_storage::id::TableID;
use doradb_storage::{Engine, Session, TableLockMode};
use smol::channel;
use smol::future::or;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Resolved logical table-lock workload configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LockTableConfig {
    common: CommonConfig,
    num: u64,
    scenario: LockTableScenario,
    mode: LockTableMode,
    width: usize,
    scope: TableLockScope,
    unlock: bool,
    random: bool,
    seed: u64,
    table_ids: Arc<[TableID]>,
    loaded_range: KeyRange,
}

impl WorkloadConfig for LockTableConfig {
    type Args = LockTableArgs;

    const WORKLOAD: Workload = Workload::LockTable;

    fn resolve(manifest: &Manifest, args: &Self::Args) -> Result<Self> {
        validate_lock_options(
            args.scenario(),
            args.mode(),
            args.width(),
            args.scope(),
            args.unlock(),
            args.random(),
            args.explicit_seed(),
        )?;
        manifest.validate_workload_compatible(Self::WORKLOAD)?;
        let worker = args.worker();
        let common = CommonConfig::resolve(
            &manifest.defaults,
            worker.thread_override(),
            worker.session_override(),
            None,
            None,
            worker.include_stats(),
        )?;
        if is_contended(args.scenario()) && common.sessions != 1 {
            return Err(BenchError::message(format!(
                "lock-table --scenario {} requires --sessions 1 for deterministic FIFO admission",
                args.scenario()
            )));
        }
        let table_ids = manifest
            .table_ids()
            .into_iter()
            .map(TableID::new)
            .collect::<Vec<_>>();
        if table_ids.is_empty() {
            return Err(BenchError::message(
                "lock-table workload requires at least one prepared table",
            ));
        }
        if matches!(
            args.scenario(),
            LockTableScenario::NestedCovered | LockTableScenario::ScopeClose
        ) && args.width() > table_ids.len()
        {
            return Err(BenchError::message(format!(
                "lock-table --scenario {} --width {} requires at least {} prepared tables",
                args.scenario(),
                args.width(),
                args.width()
            )));
        }
        Ok(Self {
            common,
            num: args.operation_count(),
            scenario: args.scenario(),
            mode: args.mode(),
            width: args.width(),
            scope: args.scope(),
            unlock: args.unlock(),
            random: args.random(),
            seed: args.seed(),
            table_ids: table_ids.into(),
            loaded_range: manifest.allocated_key_range(),
        })
    }

    fn common(&self) -> &CommonConfig {
        &self.common
    }

    fn operation_count(&self) -> u64 {
        self.num
    }

    fn output_loaded_range(&self) -> KeyRange {
        self.loaded_range
    }

    fn random(&self) -> bool {
        self.random
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn lock_scope(&self) -> Option<TableLockScope> {
        Some(self.scope)
    }

    fn unlock(&self) -> Option<bool> {
        Some(self.unlock)
    }

    fn prepared_table_count(&self) -> Option<usize> {
        Some(self.table_ids.len())
    }

    fn lock_scenario(&self) -> Option<LockTableScenario> {
        Some(self.scenario)
    }

    fn lock_mode(&self) -> Option<LockTableMode> {
        Some(self.mode)
    }

    fn lock_width(&self) -> Option<usize> {
        Some(self.width)
    }
}

/// Executes logical table-lock scenarios for one public session.
#[derive(Clone)]
pub(crate) struct LockTableRunner {
    scenario: LockTableScenario,
    mode: TableLockMode,
    width: usize,
    scope: TableLockScope,
    unlock: bool,
    random: bool,
    seed: u64,
    table_ids: Arc<[TableID]>,
}

impl WorkloadRunner for LockTableRunner {
    type Config = LockTableConfig;

    fn new(config: &Self::Config, _table_id: TableID) -> Self {
        Self {
            scenario: config.scenario,
            mode: match config.mode {
                LockTableMode::Shared => TableLockMode::Shared,
                LockTableMode::Exclusive => TableLockMode::Exclusive,
            },
            width: config.width,
            scope: config.scope,
            unlock: config.unlock,
            random: config.random,
            seed: config.seed,
            table_ids: Arc::clone(&config.table_ids),
        }
    }

    async fn run(
        &self,
        engine: &Engine,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        if plan.number == 0 {
            return Ok(SessionSummary::default());
        }
        if self.scenario != LockTableScenario::Basic {
            return self.run_specialized(engine, session, plan).await;
        }
        match (self.scope, self.unlock) {
            (TableLockScope::Session, false) => self.run_session_retained(session, plan).await,
            (TableLockScope::Session, true) => self.run_session_paired(session, plan).await,
            (TableLockScope::Transaction, false) => {
                self.run_transaction_retained(session, plan).await
            }
            (TableLockScope::Transaction, true) => self.run_transaction_paired(session, plan).await,
        }
    }
}

impl LockTableRunner {
    async fn run_specialized(
        &self,
        engine: &Engine,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        match self.scenario {
            LockTableScenario::Basic => unreachable!("basic scenario uses the legacy path"),
            LockTableScenario::NestedCovered => {
                let tables = self.width_tables(plan)?;
                for _ in 0..plan.number {
                    for &table_id in tables {
                        session
                            .lock_table(table_id, TableLockMode::Exclusive)
                            .await?;
                    }
                    let mut trx = session.begin_trx()?;
                    for &table_id in tables {
                        trx.lock_table(table_id, self.mode).await?;
                    }
                    trx.commit().await?;
                    for &table_id in tables.iter().rev() {
                        session.unlock_table(table_id)?;
                    }
                }
            }
            LockTableScenario::Convert => {
                let table_id = self.stable_table(plan)?;
                for _ in 0..plan.number {
                    session.lock_table(table_id, TableLockMode::Shared).await?;
                    session
                        .lock_table(table_id, TableLockMode::Exclusive)
                        .await?;
                    session.unlock_table(table_id)?;
                }
            }
            LockTableScenario::ScopeClose => {
                let tables = self.width_tables(plan)?;
                for _ in 0..plan.number {
                    let mut trx = session.begin_trx()?;
                    for &table_id in tables {
                        trx.lock_table(table_id, self.mode).await?;
                    }
                    trx.commit().await?;
                }
            }
            LockTableScenario::FirstTouch => {
                let table_id = self.stable_table(plan)?;
                for _ in 0..plan.number {
                    let mut trx = session.begin_trx()?;
                    let scan = trx
                        .exec(async |stmt| stmt.table_scan_mvcc(table_id, &[0], |_| true).await)
                        .await;
                    if let Err(err) = scan {
                        trx.rollback().await?;
                        return Err(err.into());
                    }
                    trx.commit().await?;
                }
            }
            LockTableScenario::Enqueue
            | LockTableScenario::CancelHead
            | LockTableScenario::CancelMiddle
            | LockTableScenario::CancelTail
            | LockTableScenario::Promote => {
                let table_id = self.stable_table(plan)?;
                for _ in 0..plan.number {
                    self.run_contended_lifecycle(engine, session, table_id)
                        .await?;
                }
            }
        }
        Ok(completed_summary(plan.number))
    }

    async fn run_contended_lifecycle(
        &self,
        engine: &Engine,
        blocker: &mut Session,
        table_id: TableID,
    ) -> Result<()> {
        let blocker_mode = match self.mode {
            TableLockMode::Shared => TableLockMode::Exclusive,
            TableLockMode::Exclusive => TableLockMode::Shared,
        };
        blocker.lock_table(table_id, blocker_mode).await?;
        let before = blocker.logical_lock_stats()?;
        let mut waiters = Vec::with_capacity(self.width);
        for _ in 0..self.width {
            waiters.push(engine.new_session()?);
        }
        let cancel_index = match self.scenario {
            LockTableScenario::CancelHead => Some(0),
            LockTableScenario::CancelMiddle => Some(self.width / 2),
            LockTableScenario::CancelTail => Some(self.width - 1),
            _ => None,
        };

        let lifecycle = thread::scope(|scope| -> Result<()> {
            let mut workers = Vec::with_capacity(self.width);
            let mut cancellation = Vec::with_capacity(self.width);
            let acquisition_order = Arc::new(Mutex::new(Vec::with_capacity(self.width)));
            for (index, mut waiter) in waiters.into_iter().enumerate() {
                let (cancel_tx, cancel_rx) = channel::bounded(1);
                cancellation.push(cancel_tx);
                let acquisition_order = Arc::clone(&acquisition_order);
                workers.push(scope.spawn(move || {
                    smol::block_on(async move {
                        enum Outcome {
                            Acquired(doradb_storage::Result<()>),
                            Cancelled,
                        }

                        let outcome = or(
                            async {
                                Outcome::Acquired(waiter.lock_table(table_id, self.mode).await)
                            },
                            async {
                                let _ = cancel_rx.recv().await;
                                Outcome::Cancelled
                            },
                        )
                        .await;
                        match outcome {
                            Outcome::Acquired(result) => {
                                result?;
                                acquisition_order
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .push(index);
                                waiter.unlock_table(table_id)?;
                            }
                            Outcome::Cancelled => {}
                        }
                        waiter.close().await?;
                        Ok::<(), BenchError>(())
                    })
                }));
                if let Err(err) = wait_for_counter(
                    || {
                        blocker
                            .logical_lock_stats()
                            .map(|stats| stats.enqueued_waiters)
                    },
                    before.enqueued_waiters + index as u64 + 1,
                    "waiter enqueue",
                ) {
                    cancel_waiter_workers(&cancellation);
                    let _ = blocker.unlock_table(table_id);
                    let _ = join_waiter_workers(workers);
                    return Err(err);
                }
            }

            if self.scenario == LockTableScenario::Enqueue {
                cancel_waiter_workers(&cancellation);
                join_waiter_workers(workers)?;
                blocker.unlock_table(table_id)?;
                let after = blocker.logical_lock_stats()?;
                assert_eq!(
                    after.promoted_waiters, before.promoted_waiters,
                    "enqueue scenario must not promote a waiter"
                );
                return Ok(());
            }

            if let Some(index) = cancel_index {
                if let Err(err) = cancellation[index].try_send(()) {
                    cancel_waiter_workers(&cancellation);
                    let _ = blocker.unlock_table(table_id);
                    let _ = join_waiter_workers(workers);
                    return Err(BenchError::message(format!(
                        "failed to cancel waiter: {err}"
                    )));
                }
                let (baseline, current) = cancellation_counter(self.scenario, before);
                if let Err(err) = wait_for_counter(
                    || blocker.logical_lock_stats().map(current),
                    baseline + 1,
                    "waiter cancellation",
                ) {
                    cancel_waiter_workers(&cancellation);
                    let _ = blocker.unlock_table(table_id);
                    let _ = join_waiter_workers(workers);
                    return Err(err);
                }
            }

            if let Err(err) = blocker.unlock_table(table_id) {
                cancel_waiter_workers(&cancellation);
                let _ = join_waiter_workers(workers);
                return Err(err.into());
            }
            join_waiter_workers(workers)?;
            let expected_promotions = self.width as u64 - u64::from(cancel_index.is_some());
            let after = blocker.logical_lock_stats()?;
            assert_eq!(
                after.promoted_waiters - before.promoted_waiters,
                expected_promotions,
                "promotion count must match the admitted non-cancelled waiters"
            );
            if self.mode == TableLockMode::Exclusive {
                let expected = (0..self.width)
                    .filter(|&index| Some(index) != cancel_index)
                    .collect::<Vec<_>>();
                let actual = acquisition_order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                assert_eq!(
                    *actual, expected,
                    "exclusive waiters must observe physical handoff in FIFO order"
                );
            }
            Ok(())
        });
        if lifecycle.is_err() {
            let _ = blocker.unlock_table(table_id);
        }
        lifecycle
    }

    fn width_tables(&self, plan: &SessionPlan) -> Result<&[TableID]> {
        let start = plan.session_index % self.table_ids.len();
        if start + self.width <= self.table_ids.len() {
            return Ok(&self.table_ids[start..start + self.width]);
        }
        Ok(&self.table_ids[..self.width])
    }

    async fn run_session_retained(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let table_id = self.stable_table(plan)?;
        for _ in 0..plan.number {
            session.lock_table(table_id, self.mode).await?;
        }
        Ok(completed_summary(plan.number))
    }

    async fn run_session_paired(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let stable_table = self.stable_table(plan)?;
        let mut random = self.random_generator(plan)?;
        let mut operations = 0;
        for _ in 0..plan.number {
            let table_id = self.next_table(stable_table, random.as_mut())?;
            session.lock_table(table_id, self.mode).await?;
            session.unlock_table(table_id)?;
            operations += 1;
        }
        Ok(completed_summary(operations))
    }

    async fn run_transaction_retained(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let table_id = self.stable_table(plan)?;
        let mut trx = session.begin_trx()?;
        for _ in 0..plan.number {
            if let Err(err) = trx.lock_table(table_id, self.mode).await {
                trx.rollback().await?;
                return Err(err.into());
            }
        }
        trx.commit().await?;
        Ok(completed_summary(plan.number))
    }

    async fn run_transaction_paired(
        &self,
        session: &mut Session,
        plan: &SessionPlan,
    ) -> Result<SessionSummary> {
        let stable_table = self.stable_table(plan)?;
        let mut random = self.random_generator(plan)?;
        let mut operations = 0;
        for _ in 0..plan.number {
            let table_id = self.next_table(stable_table, random.as_mut())?;
            let mut trx = session.begin_trx()?;
            if let Err(err) = trx.lock_table(table_id, self.mode).await {
                trx.rollback().await?;
                return Err(err.into());
            }
            trx.commit().await?;
            operations += 1;
        }
        Ok(completed_summary(operations))
    }

    fn stable_table(&self, plan: &SessionPlan) -> Result<TableID> {
        let table_count = self.table_ids.len();
        if table_count == 0 {
            return Err(BenchError::message(
                "lock-table workload requires at least one prepared table",
            ));
        }
        let table_index = plan.session_index % table_count;
        self.table_ids
            .get(table_index)
            .copied()
            .ok_or_else(|| BenchError::message("prepared table selection is out of bounds"))
    }

    fn random_generator(&self, plan: &SessionPlan) -> Result<Option<RandomTableIndexGenerator>> {
        self.random
            .then(|| RandomTableIndexGenerator::new(self.seed, self.table_ids.len(), plan))
            .transpose()
    }

    fn next_table(
        &self,
        stable_table: TableID,
        random: Option<&mut RandomTableIndexGenerator>,
    ) -> Result<TableID> {
        let Some(random) = random else {
            return Ok(stable_table);
        };
        self.table_ids
            .get(random.next_index())
            .copied()
            .ok_or_else(|| BenchError::message("random prepared table selection is out of bounds"))
    }
}

fn join_waiter_workers(workers: Vec<thread::ScopedJoinHandle<'_, Result<()>>>) -> Result<()> {
    for worker in workers {
        worker
            .join()
            .map_err(|_| BenchError::message("lock-table waiter worker panicked"))??;
    }
    Ok(())
}

fn cancel_waiter_workers(cancellation: &[channel::Sender<()>]) {
    for cancel in cancellation {
        let _ = cancel.try_send(());
    }
}

fn validate_lock_options(
    scenario: LockTableScenario,
    mode: LockTableMode,
    width: usize,
    scope: TableLockScope,
    unlock: bool,
    random: bool,
    explicit_seed: Option<u64>,
) -> Result<()> {
    if random && !unlock {
        return Err(BenchError::message(
            "lock-table --rand requires paired release with --unlock",
        ));
    }
    if explicit_seed.is_some() && !random {
        return Err(BenchError::message(
            "lock-table --seed requires random selection with --rand",
        ));
    }
    if scenario == LockTableScenario::Basic {
        if width != 1 {
            return Err(BenchError::message(
                "lock-table --width is only valid for specialized scenarios",
            ));
        }
        return Ok(());
    }
    if random || explicit_seed.is_some() || unlock || scope != TableLockScope::Session {
        return Err(BenchError::message(format!(
            "lock-table --scenario {scenario} does not accept --scope, --unlock, --rand, or --seed"
        )));
    }
    if scenario == LockTableScenario::Convert && mode != LockTableMode::Exclusive {
        return Err(BenchError::message(
            "lock-table --scenario convert requires --mode exclusive",
        ));
    }
    if scenario == LockTableScenario::FirstTouch && mode != LockTableMode::Shared {
        return Err(BenchError::message(
            "lock-table --scenario first-touch requires --mode shared",
        ));
    }
    if matches!(
        scenario,
        LockTableScenario::Convert | LockTableScenario::FirstTouch
    ) && width != 1
    {
        return Err(BenchError::message(format!(
            "lock-table --scenario {scenario} requires --width 1"
        )));
    }
    if scenario == LockTableScenario::CancelMiddle && width < 3 {
        return Err(BenchError::message(
            "lock-table --scenario cancel-middle requires --width at least 3",
        ));
    }
    Ok(())
}

fn is_contended(scenario: LockTableScenario) -> bool {
    matches!(
        scenario,
        LockTableScenario::Enqueue
            | LockTableScenario::CancelHead
            | LockTableScenario::CancelMiddle
            | LockTableScenario::CancelTail
            | LockTableScenario::Promote
    )
}

fn wait_for_counter(
    mut load: impl FnMut() -> doradb_storage::Result<u64>,
    target: u64,
    operation: &str,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if load()? >= target {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::message(format!(
                "timed out waiting for deterministic {operation}"
            )));
        }
        thread::yield_now();
    }
}

fn cancellation_counter(
    scenario: LockTableScenario,
    before: doradb_storage::LogicalLockStats,
) -> (u64, fn(doradb_storage::LogicalLockStats) -> u64) {
    match scenario {
        LockTableScenario::CancelHead => (
            before.cancelled_head_waiters,
            |stats: doradb_storage::LogicalLockStats| stats.cancelled_head_waiters,
        ),
        LockTableScenario::CancelMiddle => (
            before.cancelled_middle_waiters,
            |stats: doradb_storage::LogicalLockStats| stats.cancelled_middle_waiters,
        ),
        LockTableScenario::CancelTail => (
            before.cancelled_tail_waiters,
            |stats: doradb_storage::LogicalLockStats| stats.cancelled_tail_waiters,
        ),
        _ => unreachable!("only cancellation scenarios select a cancellation counter"),
    }
}

fn completed_summary(operations: u64) -> SessionSummary {
    SessionSummary {
        operations,
        ..SessionSummary::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, IndexMode, LogSyncMode, WorkloadArgs};
    use crate::manifest::DefaultsManifest;
    use crate::workload::{benchmark_index_specs, benchmark_table_spec};
    use clap::Parser;

    #[test]
    fn lock_config_resolves_manifest_pool_and_controls() {
        let cli = Cli::try_parse_from([
            "doradb-bench",
            "--root",
            "root",
            "run",
            "lock-table",
            "--num",
            "7",
            "--scope",
            "transaction",
            "--unlock",
            "--rand",
            "--seed",
            "11",
            "--threads",
            "2",
            "--sessions",
            "3",
        ])
        .unwrap();
        let Command::Run {
            workload: WorkloadArgs::LockTable(args),
        } = cli.command.unwrap()
        else {
            panic!("expected lock-table workload");
        };
        let defaults = DefaultsManifest::new(1, 1, 128, 1, LogSyncMode::Fdatasync).unwrap();
        let manifest = Manifest::new_with_tables(1, vec![3, 5], IndexMode::None, defaults);
        let config = LockTableConfig::resolve(&manifest, &args).unwrap();

        assert_eq!(config.operation_count(), 7);
        assert_eq!(config.scope, TableLockScope::Transaction);
        assert!(config.unlock);
        assert!(config.random);
        assert_eq!(config.seed, 11);
        assert_eq!(
            &*config.table_ids,
            &[TableID::new(1), TableID::new(3), TableID::new(5)]
        );
        assert_eq!(config.common.threads, 2);
        assert_eq!(config.common.sessions, 3);
        assert_eq!(config.common.log_sync, LogSyncMode::Fdatasync);
        assert_eq!(config.loaded_range, KeyRange { start: 0, len: 0 });
    }

    #[test]
    fn lock_options_repeat_cli_dependency_validation() {
        let validate = |unlock, random, seed| {
            validate_lock_options(
                LockTableScenario::Basic,
                LockTableMode::Shared,
                1,
                TableLockScope::Session,
                unlock,
                random,
                seed,
            )
        };
        assert!(validate(false, true, None).is_err());
        assert!(validate(true, false, Some(1)).is_err());
        assert!(validate(true, true, None).is_ok());
        assert!(validate(true, true, Some(1)).is_ok());
    }

    #[test]
    fn specialized_lock_options_enforce_scenario_contracts() {
        let validate = |scenario, mode, width| {
            validate_lock_options(
                scenario,
                mode,
                width,
                TableLockScope::Session,
                false,
                false,
                None,
            )
        };
        assert!(validate(LockTableScenario::Convert, LockTableMode::Shared, 1).is_err());
        assert!(validate(LockTableScenario::FirstTouch, LockTableMode::Exclusive, 1).is_err());
        assert!(validate(LockTableScenario::CancelMiddle, LockTableMode::Shared, 2).is_err());
        assert!(validate(LockTableScenario::Promote, LockTableMode::Exclusive, 3).is_ok());
    }

    #[test]
    fn specialized_scenarios_complete_deterministic_lifecycles() {
        smol::block_on(async {
            let temp = tempfile::TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                doradb_storage::EngineConfig::default().storage_root(temp.path()),
            )
            .await
            .unwrap();
            let mut session = engine.new_session().unwrap();
            let mut table_ids = Vec::new();
            for _ in 0..3 {
                table_ids.push(
                    session
                        .create_table(
                            benchmark_table_spec(),
                            benchmark_index_specs(IndexMode::None),
                        )
                        .await
                        .unwrap(),
                );
            }
            let plan = SessionPlan {
                session_index: 0,
                key_start: 0,
                number: 1,
            };
            for (scenario, mode, width) in [
                (LockTableScenario::NestedCovered, TableLockMode::Shared, 3),
                (LockTableScenario::Convert, TableLockMode::Exclusive, 1),
                (LockTableScenario::Enqueue, TableLockMode::Exclusive, 3),
                (LockTableScenario::CancelHead, TableLockMode::Exclusive, 3),
                (LockTableScenario::CancelMiddle, TableLockMode::Exclusive, 3),
                (LockTableScenario::CancelTail, TableLockMode::Exclusive, 3),
                (LockTableScenario::Promote, TableLockMode::Shared, 3),
                (LockTableScenario::FirstTouch, TableLockMode::Shared, 1),
                (LockTableScenario::ScopeClose, TableLockMode::Shared, 3),
            ] {
                let runner = LockTableRunner {
                    scenario,
                    mode,
                    width,
                    scope: TableLockScope::Session,
                    unlock: false,
                    random: false,
                    seed: 0,
                    table_ids: table_ids.clone().into(),
                };
                assert_eq!(
                    runner.run(&engine, &mut session, &plan).await.unwrap(),
                    completed_summary(1)
                );
            }
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn stable_table_selection_uses_session_modulo_pool_size() {
        let runner = LockTableRunner {
            scenario: LockTableScenario::Basic,
            mode: TableLockMode::Shared,
            width: 1,
            scope: TableLockScope::Session,
            unlock: false,
            random: false,
            seed: 0,
            table_ids: vec![TableID::new(2), TableID::new(4)].into(),
        };
        for session_index in 0..6 {
            let plan = SessionPlan {
                session_index,
                key_start: session_index as u64,
                number: 1,
            };
            assert_eq!(
                runner.stable_table(&plan).unwrap(),
                runner.table_ids[session_index % 2]
            );
        }
    }

    #[test]
    fn first_touch_reports_direct_transaction_claim_and_terminal_removal() {
        smol::block_on(async {
            let temp = tempfile::TempDir::new().unwrap();
            let engine = Engine::bootstrap(
                doradb_storage::EngineConfig::default().storage_root(temp.path()),
            )
            .await
            .unwrap();
            let mut setup_session = engine.new_session().unwrap();
            let table_id = setup_session
                .create_table(
                    benchmark_table_spec(),
                    benchmark_index_specs(IndexMode::None),
                )
                .await
                .unwrap();
            setup_session.close().await.unwrap();
            let mut session = engine.new_session().unwrap();
            let before = session.logical_lock_stats().unwrap();
            let runner = LockTableRunner {
                scenario: LockTableScenario::FirstTouch,
                mode: TableLockMode::Shared,
                width: 1,
                scope: TableLockScope::Session,
                unlock: false,
                random: false,
                seed: 0,
                table_ids: vec![table_id].into(),
            };
            let plan = SessionPlan {
                session_index: 0,
                key_start: 0,
                number: 1,
            };

            assert_eq!(
                runner.run(&engine, &mut session, &plan).await.unwrap(),
                completed_summary(1)
            );
            session.close().await.unwrap();
            let mut observer = engine.new_session().unwrap();
            let after = observer.logical_lock_stats().unwrap();

            assert_eq!(
                after.owner_local_covered_publications - before.owner_local_covered_publications,
                0
            );
            assert_eq!(
                after.owner_local_mode_preserving_releases
                    - before.owner_local_mode_preserving_releases,
                0
            );
            assert_eq!(
                after.scope_close_claims_visited - before.scope_close_claims_visited,
                1
            );
            assert_eq!(
                after.scope_close_physical_changes - before.scope_close_physical_changes,
                1
            );
            assert_eq!(
                after.immediate_physical_acquisitions - before.immediate_physical_acquisitions,
                1
            );

            observer.close().await.unwrap();
            engine.shutdown();
        });
    }
}
