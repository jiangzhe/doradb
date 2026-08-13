use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, FixtureRuntimeState,
};
use crate::measurement::{
    BenchmarkAccumulator, BenchmarkAggregate, InternalMetric, LatencyDistribution,
    MeasuredRunResult, MeasurementClock, WorkloadCounters, operations_per_second,
};
use crate::output::{capture_internal_stats, plan_internal_metrics};
use crate::plan::{Phase, Plan, ResolvedWorkload, load_plan};
use crate::plan_output::{
    InvocationReport, PreparePhaseResult, render_stdout_summary, write_plan_output,
};
use crate::workload::{
    CreateTableExecutor, IndexDdlExecutor, IndexScanExecutor, IndexStreamExecutor,
    InsertRandExecutor, InsertSeqExecutor, LockTableExecutor, LookupRandExecutor,
    LookupSeqExecutor, RunCancellation, SessionPlan, StmtNoopExecutor, TableDdlExecutor,
    TableScanExecutor, TrxNoopExecutor,
};
use doradb_storage::{Engine, Session};
use easy_parallel::Parallel;
use smol::{Executor, channel};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Resolved plan input and runtime fixture binding used to build one executor.
pub(crate) struct SessionExecutorConfig<C> {
    /// Workload-specific resolved plan configuration.
    pub(crate) resolved: C,
    /// Runtime fixture capability selected by the phase coordinator.
    pub(crate) binding: FixtureBinding,
}

impl<C> SessionExecutorConfig<C> {
    fn new(resolved: C, binding: FixtureBinding) -> Self {
        Self { resolved, binding }
    }
}

/// Common measurement projection produced by every typed session outcome.
pub(crate) struct SessionMeasurement {
    /// Successful logical workload counters.
    pub(crate) counters: WorkloadCounters,
    /// Exact session-local latency distribution.
    pub(crate) latency: LatencyDistribution,
}

/// Merge and project one workload-specific session outcome.
pub(crate) trait SessionOutcome: Send + Sized + 'static {
    /// Construct an empty aggregate outcome.
    fn empty() -> Result<Self>;

    /// Checked merge of one completely joined session outcome.
    fn merge(&mut self, other: Self) -> Result<()>;

    /// Consume the typed outcome after workload verification.
    fn into_measurement(self) -> SessionMeasurement;
}

/// Static workload implementation used by the generic public-session runner.
pub(crate) trait SessionExecutor: Clone + Send + Sync + Sized + 'static {
    /// Constructor input, which related workloads may share.
    type Config;
    /// Typed session result, which related workloads may share.
    type Outcome: SessionOutcome;

    /// Stable workload identity used for dispatch and diagnostics.
    const IDENTITY: &'static str;

    /// Validate the runtime binding and construct the executor.
    fn new(config: Self::Config) -> Result<Self>;

    /// Executor thread count.
    fn threads(&self) -> usize;

    /// Deterministic public-session assignments.
    fn session_plans(&self) -> Result<Vec<SessionPlan>>;

    /// Execute one session assignment without owning session close.
    fn execute<'a>(
        &'a self,
        engine: &'a Engine,
        session: &'a mut Session,
        plan: &'a SessionPlan,
        clock: Option<&'a MeasurementClock>,
        cancellation: &'a RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send + 'a;

    /// Complete workload-specific timing that ends after successful close.
    fn after_session_close(
        &self,
        _outcome: &mut Self::Outcome,
        _clock: Option<&MeasurementClock>,
    ) -> Result<()> {
        Ok(())
    }

    /// Verify workload-specific state after every declared session has joined.
    fn finish_run<'a>(
        &'a self,
        _engine: &'a Engine,
    ) -> impl Future<Output = Result<()>> + Send + 'a {
        async { Ok(()) }
    }

    /// Verify counters, samples, and fixture effects before phase advancement.
    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect>;
}

struct RunOutcome {
    elapsed_nanos: u128,
    counters: WorkloadCounters,
    latency: LatencyDistribution,
    internal_metrics: Vec<InternalMetric>,
    effect: FixtureRuntimeEffect,
}

struct InvocationResults {
    prepare_phases: Vec<PreparePhaseResult>,
    measured_runs: Vec<MeasuredRunResult>,
    aggregate: BenchmarkAggregate,
}

/// Parse and execute one plan against one new invocation-owned storage root.
pub async fn execute_plan(storage_root: PathBuf, plan_source: PathBuf) -> Result<()> {
    let loaded = load_plan(&plan_source, &storage_root)?;
    let clock = MeasurementClock::new();
    prepare_plan_root(&storage_root)?;

    let engine = Engine::bootstrap(loaded.engine_config).await?;
    let operation_result = execute_phases(&engine, &clock, &loaded.plan).await;
    engine.shutdown();
    let results = operation_result?;
    let report = InvocationReport {
        root: storage_root.clone(),
        plan_source: loaded.plan.source.clone(),
        plan: loaded.plan,
        prepare_phases: results.prepare_phases,
        measured_runs: results.measured_runs,
        aggregate: results.aggregate,
    };
    let detailed_result = write_plan_output(&report)?;
    println!("{}", render_stdout_summary(&report, &detailed_result)?);
    Ok(())
}

async fn execute_phases(
    engine: &Engine,
    clock: &MeasurementClock,
    plan: &Plan,
) -> Result<InvocationResults> {
    let mut fixture = FixtureRuntimeState::default();
    let mut prepare_phases = Vec::new();
    let mut measured_runs = Vec::new();
    let mut final_aggregate = None;

    for (phase_offset, phase) in plan.phases.iter().enumerate() {
        let phase_index = phase_offset + 1;
        match phase {
            Phase::Prepare {
                workload,
                fixture_effect,
            } => {
                let binding = fixture.bind(workload.fixture_requirement())?;
                let outcome =
                    dispatch_workload(engine, clock, workload, binding, fixture_effect, false)
                        .await?;
                fixture.apply(outcome.effect)?;
                prepare_phases.push(PreparePhaseResult {
                    phase_index,
                    workload: workload.identity().to_owned(),
                    elapsed_nanos: outcome.elapsed_nanos,
                    counters: outcome.counters,
                    internal_metrics: outcome.internal_metrics,
                });
            }
            Phase::Benchmark {
                measurement,
                workload,
                fixture_effect,
            } => {
                for _ in 0..measurement.warmup_runs {
                    let binding = fixture.bind(workload.fixture_requirement())?;
                    dispatch_workload(engine, clock, workload, binding, fixture_effect, true)
                        .await?;
                }

                let mut aggregate = BenchmarkAccumulator::new()?;
                let mut phase_effect = None;
                for run_index in 1..=measurement.measured_runs.get() {
                    let binding = fixture.bind(workload.fixture_requirement())?;
                    let outcome =
                        dispatch_workload(engine, clock, workload, binding, fixture_effect, true)
                            .await?;
                    let latency = outcome.latency.summary(workload.latency_unit())?;
                    aggregate.add_run(outcome.elapsed_nanos, outcome.counters, &outcome.latency)?;
                    measured_runs.push(MeasuredRunResult {
                        run_index,
                        elapsed_nanos: outcome.elapsed_nanos,
                        counters: outcome.counters,
                        operations_per_second: operations_per_second(
                            outcome.counters.operations,
                            outcome.elapsed_nanos,
                        ),
                        latency,
                        internal_metrics: outcome.internal_metrics,
                    });
                    if !matches!(outcome.effect, FixtureRuntimeEffect::None) {
                        if phase_effect.is_some() {
                            return Err(BenchError::message(format!(
                                "{} produced more than one mutating runtime effect",
                                workload.identity()
                            )));
                        }
                        phase_effect = Some(outcome.effect);
                    }
                }
                fixture.apply(phase_effect.unwrap_or(FixtureRuntimeEffect::None))?;
                final_aggregate = Some(aggregate.finish(workload.latency_unit())?);
            }
        }
    }

    Ok(InvocationResults {
        prepare_phases,
        measured_runs,
        aggregate: final_aggregate
            .ok_or_else(|| BenchError::message("plan completed without a benchmark aggregate"))?,
    })
}

async fn dispatch_workload(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    binding: FixtureBinding,
    planned_effect: &FixturePlanEffect,
    sample_latency: bool,
) -> Result<RunOutcome> {
    match workload {
        ResolvedWorkload::CreateTable(config) => {
            run_executor::<CreateTableExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::StmtNoop(config) => {
            run_executor::<StmtNoopExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::TrxNoop(config) => {
            run_executor::<TrxNoopExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::InsertSeq(config) => {
            run_executor::<InsertSeqExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::InsertRand(config) => {
            run_executor::<InsertRandExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::TableDdl(config) => {
            run_executor::<TableDdlExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::LookupSeq(config) => {
            run_executor::<LookupSeqExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::LookupRand(config) => {
            run_executor::<LookupRandExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::TableScan(config) => {
            run_executor::<TableScanExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::IndexScan(config) => {
            run_executor::<IndexScanExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::IndexStream(config) => {
            run_executor::<IndexStreamExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::IndexDdl(config) => {
            run_executor::<IndexDdlExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::LockTable(config) => {
            run_executor::<LockTableExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding),
                planned_effect,
                sample_latency,
            )
            .await
        }
    }
}

async fn run_executor<E>(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    config: E::Config,
    planned_effect: &FixturePlanEffect,
    sample_latency: bool,
) -> Result<RunOutcome>
where
    E: SessionExecutor,
{
    if E::IDENTITY != workload.identity() {
        return Err(BenchError::message(format!(
            "executor identity {} does not match resolved workload {}",
            E::IDENTITY,
            workload.identity()
        )));
    }
    let executor = E::new(config)?;
    let expected_samples = if sample_latency {
        workload.expected_samples()?
    } else {
        0
    };
    let stats_state = if workload.include_stats() {
        let session = engine.new_session()?;
        match capture_internal_stats(&session) {
            Ok(before) => Some((session, before)),
            Err(error) => return close_stats_session(session, Err(error)).await,
        }
    } else {
        None
    };

    let started = clock.now();
    let run_result =
        run_session_workers(engine, clock, &executor, sample_latency.then_some(clock)).await;
    let stopped = clock.now();
    let elapsed_result = clock.wall_delta_nanos(started, stopped);
    let outcome = match run_result {
        Ok(result) => result,
        Err(error) => {
            if let Some((mut session, _)) = stats_state {
                let _ = session.close().await;
            }
            return Err(error);
        }
    };
    let elapsed_nanos = elapsed_result?;

    let internal_metrics = if let Some((mut session, before)) = stats_state {
        let metrics_result =
            capture_internal_stats(&session).map(|after| plan_internal_metrics(&before, &after));
        let close_result = session.close().await.map_err(BenchError::from);
        match (metrics_result, close_result) {
            (Ok(metrics), Ok(())) => metrics,
            (Err(error), _) | (Ok(_), Err(error)) => return Err(error),
        }
    } else {
        Vec::new()
    };

    let effect = executor.verify_outcome(planned_effect, &outcome, expected_samples)?;
    let measurement = outcome.into_measurement();
    Ok(RunOutcome {
        elapsed_nanos,
        counters: measurement.counters,
        latency: measurement.latency,
        internal_metrics,
        effect,
    })
}

async fn close_stats_session<T>(mut session: Session, result: Result<T>) -> Result<T> {
    let close_result = session.close().await.map_err(BenchError::from);
    match (result, close_result) {
        (Err(error), _) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(error)) => Err(error),
    }
}

async fn run_session_workers<E>(
    engine: &Engine,
    clock: &MeasurementClock,
    executor: &E,
    sample_clock: Option<&MeasurementClock>,
) -> Result<E::Outcome>
where
    E: SessionExecutor,
{
    let plans = executor.session_plans()?;
    let cancellation = Arc::new(RunCancellation::new());
    let task_executor = Executor::new();
    let tasks = plans
        .into_iter()
        .map(|plan| {
            let workload_executor = executor.clone();
            let cancellation = Arc::clone(&cancellation);
            let clock = clock.clone();
            task_executor.spawn(async move {
                let mut session = match engine.new_session() {
                    Ok(session) => session,
                    Err(error) => {
                        cancellation.fail(error.into());
                        return None;
                    }
                };
                let run_result = workload_executor
                    .execute(
                        engine,
                        &mut session,
                        &plan,
                        sample_clock.map(|_| &clock),
                        &cancellation,
                    )
                    .await;
                let mut outcome = match run_result {
                    Ok(outcome) => Some(outcome),
                    Err(error) => {
                        cancellation.fail(error);
                        None
                    }
                };
                match session.close().await {
                    Ok(()) => {
                        if let Some(outcome) = outcome.as_mut()
                            && let Err(error) = workload_executor
                                .after_session_close(outcome, sample_clock.map(|_| &clock))
                        {
                            cancellation.fail(error);
                        }
                    }
                    Err(error) => cancellation.fail(error.into()),
                }
                outcome
            })
        })
        .collect();
    let outcome = drive_session_tasks(&task_executor, executor.threads(), tasks, cancellation)?;
    executor.finish_run(engine).await?;
    Ok(outcome)
}

fn drive_session_tasks<O>(
    executor: &Executor<'_>,
    threads: usize,
    tasks: Vec<smol::Task<Option<O>>>,
    cancellation: Arc<RunCancellation>,
) -> Result<O>
where
    O: SessionOutcome,
{
    let (signal, shutdown) = channel::unbounded::<()>();
    let shutdown_receiver = shutdown.clone();
    let (_workers, result) = Parallel::new()
        .each(0..threads, move |_| {
            let _ = smol::block_on(executor.run(shutdown_receiver.recv()));
        })
        .finish(move || {
            let _signal = signal;
            smol::block_on(collect_session_results(tasks, cancellation))
        });
    result
}

async fn collect_session_results<O>(
    tasks: Vec<smol::Task<Option<O>>>,
    cancellation: Arc<RunCancellation>,
) -> Result<O>
where
    O: SessionOutcome,
{
    let mut outcome = O::empty()?;
    for task in tasks {
        let Some(result) = task.await else {
            continue;
        };
        if let Err(error) = outcome.merge(result) {
            cancellation.fail(error);
        }
    }
    if let Some(error) = cancellation.take_error() {
        Err(error)
    } else {
        Ok(outcome)
    }
}

fn prepare_plan_root(storage_root: &Path) -> Result<()> {
    if storage_root.exists() {
        return Err(BenchError::message(format!(
            "--root {} must not exist for plan execution",
            storage_root.display()
        )));
    }
    fs::create_dir_all(storage_root).map_err(|error| {
        BenchError::message(format!(
            "failed to create storage root {}: {error}",
            storage_root.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_shared_config<A, B>()
    where
        A: SessionExecutor,
        B: SessionExecutor<Config = A::Config>,
    {
    }

    fn assert_shared_outcome<A, B>()
    where
        A: SessionExecutor,
        B: SessionExecutor<Outcome = A::Outcome>,
    {
    }

    #[test]
    fn related_executor_identities_share_associated_types() {
        assert_shared_config::<StmtNoopExecutor, TrxNoopExecutor>();
        assert_shared_outcome::<StmtNoopExecutor, TrxNoopExecutor>();
        assert_shared_config::<InsertSeqExecutor, InsertRandExecutor>();
        assert_shared_outcome::<InsertSeqExecutor, InsertRandExecutor>();
        assert_shared_config::<TableDdlExecutor, IndexDdlExecutor>();
        assert_shared_outcome::<TableDdlExecutor, IndexDdlExecutor>();
        assert_shared_config::<LookupSeqExecutor, LookupRandExecutor>();
        assert_shared_outcome::<LookupSeqExecutor, IndexStreamExecutor>();
    }

    #[test]
    fn executor_identities_match_resolved_workload_names() {
        assert_eq!(
            [
                CreateTableExecutor::IDENTITY,
                StmtNoopExecutor::IDENTITY,
                TrxNoopExecutor::IDENTITY,
                InsertSeqExecutor::IDENTITY,
                InsertRandExecutor::IDENTITY,
                TableDdlExecutor::IDENTITY,
                LookupSeqExecutor::IDENTITY,
                LookupRandExecutor::IDENTITY,
                TableScanExecutor::IDENTITY,
                IndexScanExecutor::IDENTITY,
                IndexStreamExecutor::IDENTITY,
                IndexDdlExecutor::IDENTITY,
                LockTableExecutor::IDENTITY,
            ],
            [
                "create-table",
                "stmt-noop",
                "trx-noop",
                "insert-seq",
                "insert-rand",
                "table-ddl",
                "lookup-seq",
                "lookup-rand",
                "table-scan",
                "index-scan",
                "index-stream",
                "index-ddl",
                "lock-table",
            ]
        );
    }
}
