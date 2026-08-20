use crate::error::{BenchError, Result};
use crate::fixture::{FixturePlanEffect, FixtureRuntimeEffect};
use crate::measurement::{LatencyDistribution, MeasurementClock, WorkloadCounters};
use crate::plan::{LockTableConfig, LockTableMode, LockTableScenario, TableLockScope};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    RandomTableIndexGenerator, merge_measurement, operation_plans, require_table_pool,
    verify_no_effect, verify_samples, verify_simple_counters,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{Engine, Session, TableLockMode};
use smol::channel;
use smol::future::or;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Table-lock session executor.
#[derive(Clone)]
pub(crate) struct LockTableExecutor {
    config: LockTableConfig,
    spec: LockOperationSpec,
}

impl SessionExecutor for LockTableExecutor {
    type Config = SessionExecutorConfig<LockTableConfig>;
    type Outcome = LockSessionOutcome;

    const IDENTITY: &'static str = "lock-table";

    fn new(config: Self::Config) -> Result<Self> {
        let table_ids = require_table_pool(config.binding, Self::IDENTITY)?;
        let resolved = config.resolved;
        Ok(Self {
            config: resolved,
            spec: LockOperationSpec {
                scenario: resolved.scenario,
                mode: resolved.mode,
                width: resolved.width,
                scope: resolved.scope,
                unlock: resolved.unlock,
                random: resolved.random,
                seed: resolved.seed,
                table_ids,
            },
        })
    }

    fn threads(&self) -> usize {
        self.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.config.num, self.config.sessions)
    }

    fn execute<'a>(
        &'a self,
        engine: &'a Engine,
        session: &'a mut Session,
        plan: &'a SessionPlan,
        clock: &'a MeasurementClock,
        sample_latency: bool,
        cancellation: &'a RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send + 'a {
        execute_lock_session(
            engine,
            session,
            &self.spec,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
    }

    fn after_session_close(
        &self,
        outcome: &mut Self::Outcome,
        clock: Option<&MeasurementClock>,
    ) -> Result<()> {
        if let Some(started) = outcome.retained_session_started.take()
            && let Some(clock) = clock
        {
            outcome
                .measurement
                .latency
                .record(clock.raw_delta_nanos(started, clock.raw())?)?;
        }
        Ok(())
    }

    fn finish_run<'a>(
        &'a self,
        engine: &'a Engine,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        verify_lock_release(engine, &self.spec.table_ids)
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_samples(
            Self::IDENTITY,
            &outcome.measurement.latency,
            expected_samples,
        )?;
        verify_simple_counters(
            Self::IDENTITY,
            outcome.measurement.counters,
            self.config.num,
        )?;
        verify_no_effect(planned_effect)
    }
}

/// Session-local table-lock outcome.
pub(crate) struct LockSessionOutcome {
    measurement: SessionMeasurement,
    retained_session_started: Option<u64>,
}

impl SessionOutcome for LockSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
            retained_session_started: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        if other.retained_session_started.is_some() {
            return Err(BenchError::message(
                "table-lock session timing was not completed after close",
            ));
        }
        merge_measurement(&mut self.measurement, other.measurement)
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Bound controls for one table-lock session.
#[derive(Clone)]
struct LockOperationSpec {
    /// Specialized scenario.
    scenario: LockTableScenario,
    /// Requested physical mode.
    mode: LockTableMode,
    /// Scenario width.
    width: usize,
    /// Basic ownership scope.
    scope: TableLockScope,
    /// Whether basic acquisition uses paired release.
    unlock: bool,
    /// Whether paired basic acquisition selects tables randomly.
    random: bool,
    /// Deterministic selection seed.
    seed: u64,
    /// Ordered runtime table pool.
    table_ids: Arc<[TableID]>,
}

/// Result of one declared session's lock lifecycles.
struct LockOperationResult {
    /// Completely settled logical operations.
    operations: u64,
    /// Exact completed lifecycle samples.
    latency: LatencyDistribution,
    /// Retained-session timing start completed by the session owner after close.
    retained_session_started: Option<u64>,
}

async fn execute_lock_session(
    engine: &Engine,
    session: &mut Session,
    spec: &LockOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<LockSessionOutcome> {
    let result =
        run_lock_operations(engine, session, spec, plan, clock, Some(cancellation)).await?;
    Ok(LockSessionOutcome {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: result.operations,
                ..WorkloadCounters::default()
            },
            latency: result.latency,
        },
        retained_session_started: result.retained_session_started,
    })
}

/// Execute one declared session's table-lock lifecycles.
async fn run_lock_operations(
    engine: &Engine,
    session: &mut Session,
    spec: &LockOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<LockOperationResult> {
    let mut result = LockOperationResult {
        operations: 0,
        latency: LatencyDistribution::new()?,
        retained_session_started: None,
    };
    if plan.number == 0 || cancellation.is_some_and(RunCancellation::is_cancelled) {
        return Ok(result);
    }
    let mode = storage_mode(spec.mode);
    if spec.scenario == LockTableScenario::Basic && !spec.unlock {
        let started = clock.map(MeasurementClock::raw);
        match spec.scope {
            TableLockScope::Session => {
                let table_id = stable_table(&spec.table_ids, plan)?;
                for _ in 0..plan.number {
                    session.lock_table(table_id, mode).await?;
                }
                result.retained_session_started = started;
            }
            TableLockScope::Transaction => {
                let table_id = stable_table(&spec.table_ids, plan)?;
                let mut trx = session.begin_trx()?;
                for _ in 0..plan.number {
                    if let Err(error) = trx.lock_table(table_id, mode).await {
                        let primary = BenchError::from(error);
                        let _ = trx.rollback().await;
                        return Err(primary);
                    }
                }
                trx.commit().await?;
                record_latency(&mut result.latency, clock, started)?;
            }
        }
        result.operations = plan.number;
        return Ok(result);
    }

    let mut random = spec
        .random
        .then(|| RandomTableIndexGenerator::new(spec.seed, spec.table_ids.len(), plan))
        .transpose()?;
    let stable = stable_table(&spec.table_ids, plan)?;
    for _ in 0..plan.number {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        if spec.scenario == LockTableScenario::Basic {
            let table_id = random.as_mut().map_or(Ok(stable), |random| {
                spec.table_ids
                    .get(random.next_index())
                    .copied()
                    .ok_or_else(|| BenchError::message("random table selection is out of bounds"))
            })?;
            match spec.scope {
                TableLockScope::Session => {
                    session.lock_table(table_id, mode).await?;
                    session.unlock_table(table_id)?;
                }
                TableLockScope::Transaction => {
                    let mut trx = session.begin_trx()?;
                    if let Err(error) = trx.lock_table(table_id, mode).await {
                        let primary = BenchError::from(error);
                        let _ = trx.rollback().await;
                        return Err(primary);
                    }
                    trx.commit().await?;
                }
            }
        } else {
            run_specialized_lifecycle(engine, session, spec, plan, mode).await?;
        }
        result.operations = result
            .operations
            .checked_add(1)
            .ok_or_else(|| BenchError::message("lock operation counter overflow"))?;
        record_latency(&mut result.latency, clock, started)?;
    }
    Ok(result)
}

async fn verify_lock_release(engine: &Engine, table_ids: &[TableID]) -> Result<()> {
    let mut session = engine.new_session()?;
    let result = async {
        for &table_id in table_ids {
            session
                .lock_table(table_id, TableLockMode::Exclusive)
                .await?;
            session.unlock_table(table_id)?;
        }
        Ok::<(), BenchError>(())
    }
    .await;
    let close_result = session.close().await.map_err(BenchError::from);
    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(error),
    }
}

async fn run_specialized_lifecycle(
    engine: &Engine,
    session: &mut Session,
    spec: &LockOperationSpec,
    plan: &SessionPlan,
    mode: TableLockMode,
) -> Result<()> {
    match spec.scenario {
        LockTableScenario::Basic => unreachable!("basic scenario is dispatched separately"),
        LockTableScenario::NestedCovered => {
            let tables = width_tables(&spec.table_ids, spec.width, plan)?;
            for &table_id in tables {
                session
                    .lock_table(table_id, TableLockMode::Exclusive)
                    .await?;
            }
            let mut trx = session.begin_trx()?;
            for &table_id in tables {
                if let Err(error) = trx.lock_table(table_id, mode).await {
                    let primary = BenchError::from(error);
                    let _ = trx.rollback().await;
                    for &held in tables.iter().rev() {
                        let _ = session.unlock_table(held);
                    }
                    return Err(primary);
                }
            }
            trx.commit().await?;
            for &table_id in tables.iter().rev() {
                session.unlock_table(table_id)?;
            }
        }
        LockTableScenario::Convert => {
            let table_id = stable_table(&spec.table_ids, plan)?;
            session.lock_table(table_id, TableLockMode::Shared).await?;
            if let Err(error) = session.lock_table(table_id, TableLockMode::Exclusive).await {
                let _ = session.unlock_table(table_id);
                return Err(error.into());
            }
            session.unlock_table(table_id)?;
        }
        LockTableScenario::ScopeClose => {
            let tables = width_tables(&spec.table_ids, spec.width, plan)?;
            let mut trx = session.begin_trx()?;
            for &table_id in tables {
                if let Err(error) = trx.lock_table(table_id, mode).await {
                    let primary = BenchError::from(error);
                    let _ = trx.rollback().await;
                    return Err(primary);
                }
            }
            trx.commit().await?;
        }
        LockTableScenario::FirstTouch => {
            let table_id = stable_table(&spec.table_ids, plan)?;
            let mut trx = session.begin_trx()?;
            let scan = trx.table_scan_mvcc(table_id, &[0], |_| true).await;
            if let Err(error) = scan {
                let primary = BenchError::from(error);
                let _ = trx.rollback().await;
                return Err(primary);
            }
            trx.commit().await?;
        }
        LockTableScenario::Enqueue
        | LockTableScenario::CancelHead
        | LockTableScenario::CancelMiddle
        | LockTableScenario::CancelTail
        | LockTableScenario::Promote => {
            run_contended_lifecycle(
                engine,
                session,
                stable_table(&spec.table_ids, plan)?,
                spec.scenario,
                mode,
                spec.width,
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_contended_lifecycle(
    engine: &Engine,
    blocker: &mut Session,
    table_id: TableID,
    scenario: LockTableScenario,
    mode: TableLockMode,
    width: usize,
) -> Result<()> {
    let blocker_mode = match mode {
        TableLockMode::Shared => TableLockMode::Exclusive,
        TableLockMode::Exclusive => TableLockMode::Shared,
    };
    blocker.lock_table(table_id, blocker_mode).await?;
    let before = blocker.logical_lock_stats()?;
    let mut waiters = Vec::with_capacity(width);
    for _ in 0..width {
        match engine.new_session() {
            Ok(waiter) => waiters.push(waiter),
            Err(error) => {
                let _ = blocker.unlock_table(table_id);
                return Err(error.into());
            }
        }
    }
    let cancel_index = match scenario {
        LockTableScenario::CancelHead => Some(0),
        LockTableScenario::CancelMiddle => Some(width / 2),
        LockTableScenario::CancelTail => Some(width - 1),
        _ => None,
    };

    let lifecycle = thread::scope(|scope| -> Result<()> {
        let mut workers = Vec::with_capacity(width);
        let mut cancellation = Vec::with_capacity(width);
        let acquisition_order = Arc::new(Mutex::new(Vec::with_capacity(width)));
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
                        async { Outcome::Acquired(waiter.lock_table(table_id, mode).await) },
                        async {
                            let _ = cancel_rx.recv().await;
                            Outcome::Cancelled
                        },
                    )
                    .await;
                    if let Outcome::Acquired(result) = outcome {
                        result?;
                        acquisition_order
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .push(index);
                        waiter.unlock_table(table_id)?;
                    }
                    waiter.close().await?;
                    Ok::<(), BenchError>(())
                })
            }));
            if let Err(error) = wait_for_counter(
                || {
                    blocker
                        .logical_lock_stats()
                        .map(|stats| stats.enqueued_waiters)
                },
                before.enqueued_waiters + index as u64 + 1,
                "waiter enqueue",
            ) {
                cancel_waiters(&cancellation);
                let _ = blocker.unlock_table(table_id);
                let _ = join_waiters(workers);
                return Err(error);
            }
        }

        if scenario == LockTableScenario::Enqueue {
            cancel_waiters(&cancellation);
            join_waiters(workers)?;
            blocker.unlock_table(table_id)?;
            let after = blocker.logical_lock_stats()?;
            if after.promoted_waiters != before.promoted_waiters {
                return Err(BenchError::message(format!(
                    "enqueue scenario must not promote a waiter: expected promoted waiter count \
                     {}, found {}",
                    before.promoted_waiters, after.promoted_waiters
                )));
            }
            return Ok(());
        }

        if let Some(index) = cancel_index {
            if let Err(error) = cancellation[index].try_send(()) {
                cancel_waiters(&cancellation);
                let _ = blocker.unlock_table(table_id);
                let _ = join_waiters(workers);
                return Err(BenchError::message(format!(
                    "failed to cancel waiter: {error}"
                )));
            }
            let (baseline, current) = cancellation_counter(scenario, before);
            if let Err(error) = wait_for_counter(
                || blocker.logical_lock_stats().map(current),
                baseline + 1,
                "waiter cancellation",
            ) {
                cancel_waiters(&cancellation);
                let _ = blocker.unlock_table(table_id);
                let _ = join_waiters(workers);
                return Err(error);
            }
        }

        if let Err(error) = blocker.unlock_table(table_id) {
            cancel_waiters(&cancellation);
            let _ = join_waiters(workers);
            return Err(error.into());
        }
        join_waiters(workers)?;
        let expected_promotions = width as u64 - u64::from(cancel_index.is_some());
        let after = blocker.logical_lock_stats()?;
        let actual_promotions = after
            .promoted_waiters
            .saturating_sub(before.promoted_waiters);
        if actual_promotions != expected_promotions {
            return Err(BenchError::message(format!(
                "promotion count must match admitted non-cancelled waiters: expected \
                 {expected_promotions}, found {actual_promotions}"
            )));
        }
        if mode == TableLockMode::Exclusive {
            let expected = (0..width)
                .filter(|&index| Some(index) != cancel_index)
                .collect::<Vec<_>>();
            let actual = acquisition_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if actual.as_slice() != expected.as_slice() {
                return Err(BenchError::message(format!(
                    "exclusive waiters must preserve FIFO: expected {expected:?}, found {:?}",
                    actual.as_slice()
                )));
            }
        }
        Ok(())
    });
    if lifecycle.is_err() {
        let _ = blocker.unlock_table(table_id);
    }
    lifecycle
}

fn stable_table(table_ids: &[TableID], plan: &SessionPlan) -> Result<TableID> {
    table_ids
        .get(plan.session_index % table_ids.len())
        .copied()
        .ok_or_else(|| BenchError::message("stable table selection is out of bounds"))
}

fn width_tables<'a>(
    table_ids: &'a [TableID],
    width: usize,
    plan: &SessionPlan,
) -> Result<&'a [TableID]> {
    if width > table_ids.len() {
        return Err(BenchError::message("scenario width exceeds table pool"));
    }
    let start = plan.session_index % table_ids.len();
    if start + width <= table_ids.len() {
        Ok(&table_ids[start..start + width])
    } else {
        Ok(&table_ids[..width])
    }
}

fn storage_mode(mode: LockTableMode) -> TableLockMode {
    match mode {
        LockTableMode::Shared => TableLockMode::Shared,
        LockTableMode::Exclusive => TableLockMode::Exclusive,
    }
}

fn join_waiters(workers: Vec<thread::ScopedJoinHandle<'_, Result<()>>>) -> Result<()> {
    for worker in workers {
        worker
            .join()
            .map_err(|_| BenchError::message("lock-table waiter worker panicked"))??;
    }
    Ok(())
}

fn cancel_waiters(cancellation: &[channel::Sender<()>]) {
    for cancel in cancellation {
        let _ = cancel.try_send(());
    }
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
        LockTableScenario::CancelHead => (before.cancelled_head_waiters, |stats| {
            stats.cancelled_head_waiters
        }),
        LockTableScenario::CancelMiddle => (before.cancelled_middle_waiters, |stats| {
            stats.cancelled_middle_waiters
        }),
        LockTableScenario::CancelTail => (before.cancelled_tail_waiters, |stats| {
            stats.cancelled_tail_waiters
        }),
        _ => unreachable!("only cancellation scenarios select a counter"),
    }
}

fn record_latency(
    latency: &mut LatencyDistribution,
    clock: Option<&MeasurementClock>,
    started: Option<u64>,
) -> Result<()> {
    if let (Some(clock), Some(started)) = (clock, started) {
        latency.record(clock.raw_delta_nanos(started, clock.raw())?)?;
    }
    Ok(())
}
