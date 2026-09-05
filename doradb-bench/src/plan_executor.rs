use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, FixtureRuntimeState,
};
use crate::measurement::{
    BenchmarkAccumulator, BenchmarkAggregate, InternalMetric, LatencyDistribution,
    MeasuredRunResult, MeasurementClock, WorkloadCounters, WorkloadMetrics, operations_per_second,
};
use crate::output::{capture_internal_stats, plan_internal_metrics};
use crate::plan::{Phase, Plan, ResolvedWorkload, load_plan};
use crate::plan_output::{
    InvocationReport, PreparePhaseResult, absolute_result_path, render_stdout_summary,
    write_plan_output,
};
use crate::workload::{
    CatalogCheckpointExecutor, CatalogCheckpointPrepareExecutor, CheckpointTableExecutor,
    CreateTableExecutor, FreezeTableExecutor, IndexDdlExecutor, IndexScanExecutor,
    IndexStreamExecutor, InsertRandExecutor, InsertSeqExecutor, LockTableExecutor,
    LookupRandExecutor, LookupSeqExecutor, ManagedBindingsPrepareExecutor,
    ParallelTableScanExecutor, ParallelTableScanExecutorConfig, ResolveTableBindingExecutor,
    RunCancellation, SessionPlan, StmtNoopExecutor, TableDdlExecutor, TableScanExecutor,
    TrxNoopExecutor, UpdateRandExecutor,
};
use doradb_storage::{Engine, Session};
use easy_parallel::Parallel;
use rustix::process::{Signal, getpid, kill_process};
use smol::{Executor, channel};
use std::fs;
use std::future::Future;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Resolved plan input and runtime fixture binding used to build one executor.
pub(crate) struct SessionExecutorConfig<C> {
    /// Workload-specific resolved plan configuration.
    pub(crate) resolved: C,
    /// Runtime fixture capability selected by the phase coordinator.
    pub(crate) binding: FixtureBinding,
    /// Zero-based execution position across warm-up and measured repetitions.
    pub(crate) execution_ordinal: u32,
}

impl<C> SessionExecutorConfig<C> {
    fn new(resolved: C, binding: FixtureBinding, execution_ordinal: u32) -> Self {
        Self {
            resolved,
            binding,
            execution_ordinal,
        }
    }
}

/// Scoped submission handle for owned tasks on the run's driven executor.
#[derive(Clone)]
pub(crate) struct RunTaskSpawner<'tasks> {
    executor: Arc<Executor<'tasks>>,
}

impl RunTaskSpawner<'_> {
    /// Submit one owned task without changing runtimes or detaching it.
    pub(crate) fn spawn<F, T>(&self, future: F) -> smol::Task<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.executor.spawn(future)
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

    /// Project verified workload-specific metrics before consuming the outcome.
    fn workload_metrics(&self) -> Option<WorkloadMetrics> {
        None
    }

    /// Consume the typed outcome after workload verification.
    fn into_measurement(self) -> SessionMeasurement;
}

/// Workload implementation used by the generic public-session runner.
pub(crate) trait SessionExecutor: Clone + Send + Sync + Sized {
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
    fn execute(
        &self,
        engine: &Engine,
        session: &mut Session,
        plan: &SessionPlan,
        clock: &MeasurementClock,
        sample_latency: bool,
        cancellation: &RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send;

    /// Complete workload-specific timing that ends after successful close.
    fn after_session_close(
        &self,
        _outcome: &mut Self::Outcome,
        _clock: Option<&MeasurementClock>,
    ) -> Result<()> {
        Ok(())
    }

    /// Verify workload-specific state after every declared session has joined.
    fn finish_run(&self, _engine: &Engine) -> impl Future<Output = Result<()>> + Send {
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
    workload_metrics: Option<WorkloadMetrics>,
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
    let detailed_result = absolute_result_path(&report.root)?;
    let stdout_summary = render_stdout_summary(&report, &detailed_result)?;
    write_plan_output(&report)?;
    println!("{stdout_summary}");
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
                    dispatch_workload(engine, clock, workload, binding, fixture_effect, false, 0)
                        .await?;
                fixture.apply(outcome.effect)?;
                prepare_phases.push(PreparePhaseResult {
                    phase_index,
                    workload: workload.identity().to_owned(),
                    elapsed_nanos: outcome.elapsed_nanos,
                    counters: outcome.counters,
                    workload_metrics: outcome.workload_metrics,
                    internal_metrics: outcome.internal_metrics,
                });
            }
            Phase::Benchmark {
                measurement,
                workload,
                fixture_effect,
            } => {
                if measurement.pause {
                    pause_for_profiler(phase_index, workload.identity())?;
                }
                for execution_ordinal in 0..measurement.warmup_runs {
                    let binding = fixture.bind(workload.fixture_requirement())?;
                    dispatch_workload(
                        engine,
                        clock,
                        workload,
                        binding,
                        fixture_effect,
                        true,
                        execution_ordinal,
                    )
                    .await?;
                }

                let mut aggregate = BenchmarkAccumulator::new()?;
                let mut phase_effect = None;
                for run_index in 1..=measurement.measured_runs.get() {
                    let execution_ordinal = measurement
                        .warmup_runs
                        .checked_add(run_index - 1)
                        .ok_or_else(|| {
                            BenchError::message("benchmark execution ordinal overflow")
                        })?;
                    let binding = fixture.bind(workload.fixture_requirement())?;
                    let outcome = dispatch_workload(
                        engine,
                        clock,
                        workload,
                        binding,
                        fixture_effect,
                        true,
                        execution_ordinal,
                    )
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
                        workload_metrics: outcome.workload_metrics,
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

fn pause_for_profiler(phase_index: usize, workload: &str) -> Result<()> {
    let pid = getpid();
    let raw_pid = pid.as_raw_pid();
    {
        let stderr = io::stderr();
        let mut stderr = stderr.lock();
        write_pausing_notice(&mut stderr, raw_pid, phase_index, workload)?;
    }

    kill_process(pid, Signal::STOP).map_err(|error| {
        BenchError::message(format!(
            "failed to send SIGSTOP to benchmark process {raw_pid}: {error}"
        ))
    })?;

    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    write_resumed_notice(&mut stderr, raw_pid, phase_index, workload)
}

fn write_pausing_notice(
    writer: &mut impl Write,
    pid: i32,
    phase_index: usize,
    workload: &str,
) -> Result<()> {
    writeln!(
        writer,
        "DORADB_BENCH_PAUSING pid={pid} phase={phase_index} workload={workload} resume=SIGCONT"
    )
    .and_then(|()| {
        writeln!(
            writer,
            "Attach the profiler to PID {pid} and verify that the process is stopped."
        )
    })
    .and_then(|()| writeln!(writer, "Resume with: kill -CONT {pid}"))
    .map_err(|error| {
        BenchError::message(format!(
            "failed to write profiler pause notice for process {pid}: {error}"
        ))
    })?;
    writer.flush().map_err(|error| {
        BenchError::message(format!(
            "failed to flush profiler pause notice for process {pid}: {error}"
        ))
    })
}

fn write_resumed_notice(
    writer: &mut impl Write,
    pid: i32,
    phase_index: usize,
    workload: &str,
) -> Result<()> {
    writeln!(
        writer,
        "DORADB_BENCH_RESUMED pid={pid} phase={phase_index} workload={workload}"
    )
    .map_err(|error| {
        BenchError::message(format!(
            "failed to write profiler resume notice for process {pid}: {error}"
        ))
    })?;
    writer.flush().map_err(|error| {
        BenchError::message(format!(
            "failed to flush profiler resume notice for process {pid}: {error}"
        ))
    })
}

async fn dispatch_workload(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    binding: FixtureBinding,
    planned_effect: &FixturePlanEffect,
    sample_latency: bool,
    execution_ordinal: u32,
) -> Result<RunOutcome> {
    match workload {
        ResolvedWorkload::CreateTable(config) => {
            run_executor::<CreateTableExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::UpdateRand(config) => {
            run_executor::<UpdateRandExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::ParallelTableScan(config) => {
            run_executor_with::<ParallelTableScanExecutor<'_>, _>(
                engine,
                clock,
                workload,
                move |spawner| {
                    ParallelTableScanExecutorConfig::new(
                        SessionExecutorConfig::new(*config, binding, execution_ordinal),
                        spawner,
                    )
                },
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::FreezeTable(config) => {
            run_executor::<FreezeTableExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::CheckpointTable(config) => {
            run_executor::<CheckpointTableExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::ManagedBindingsPrepare(config) => {
            run_executor::<ManagedBindingsPrepareExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::ResolveTableBinding(config) => {
            run_executor::<ResolveTableBindingExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::CatalogCheckpointPrepare(config) => {
            run_executor::<CatalogCheckpointPrepareExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::CatalogCheckpoint(config) => {
            run_executor::<CatalogCheckpointExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
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
    verify_executor_identity::<E>(workload)?;
    let executor = E::new(config)?;
    run_executor_with_setup::<E, _>(
        engine,
        clock,
        workload,
        move || Ok((executor, Arc::new(Executor::new()))),
        planned_effect,
        sample_latency,
    )
    .await
}

async fn run_executor_with<'run, E, F>(
    engine: &'run Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    config_factory: F,
    planned_effect: &FixturePlanEffect,
    sample_latency: bool,
) -> Result<RunOutcome>
where
    E: SessionExecutor + 'run,
    F: FnOnce(RunTaskSpawner<'run>) -> E::Config,
{
    verify_executor_identity::<E>(workload)?;
    let task_executor: Arc<Executor<'run>> = Arc::new(Executor::new());
    let task_spawner = RunTaskSpawner {
        executor: Arc::clone(&task_executor),
    };
    let executor = E::new(config_factory(task_spawner))?;
    run_executor_with_setup::<E, _>(
        engine,
        clock,
        workload,
        move || Ok((executor, task_executor)),
        planned_effect,
        sample_latency,
    )
    .await
}

fn verify_executor_identity<E>(workload: &ResolvedWorkload) -> Result<()>
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
    Ok(())
}

async fn run_executor_with_setup<'run, E, F>(
    engine: &'run Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    setup: F,
    planned_effect: &FixturePlanEffect,
    sample_latency: bool,
) -> Result<RunOutcome>
where
    E: SessionExecutor + 'run,
    F: FnOnce() -> Result<(E, Arc<Executor<'run>>)>,
{
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
    let run_result = match setup() {
        Ok((executor, task_executor)) => {
            run_session_workers(engine, clock, &executor, task_executor, sample_latency)
                .await
                .map(|outcome| (executor, outcome))
        }
        Err(error) => Err(error),
    };
    let stopped = clock.now();
    let elapsed_result = clock.wall_delta_nanos(started, stopped);
    let (executor, outcome) = match run_result {
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
    let workload_metrics = outcome.workload_metrics();
    let measurement = outcome.into_measurement();
    Ok(RunOutcome {
        elapsed_nanos,
        counters: measurement.counters,
        latency: measurement.latency,
        internal_metrics,
        workload_metrics,
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

async fn run_session_workers<'run, E>(
    engine: &'run Engine,
    clock: &MeasurementClock,
    executor: &E,
    task_executor: Arc<Executor<'run>>,
    sample_latency: bool,
) -> Result<E::Outcome>
where
    E: SessionExecutor + 'run,
{
    let plans = executor.session_plans()?;
    let cancellation = Arc::new(RunCancellation::new());
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
                        &clock,
                        sample_latency,
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
                                .after_session_close(outcome, sample_latency.then_some(&clock))
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
    let outcome = drive_session_tasks(
        task_executor.as_ref(),
        executor.threads(),
        tasks,
        cancellation,
    )?;
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
    use std::io::ErrorKind;
    use std::sync::{Condvar, Mutex as StdMutex, mpsc};
    use std::thread::{self, ThreadId};
    use std::time::Duration;

    struct WriteFailure;

    impl Write for WriteFailure {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(ErrorKind::BrokenPipe, "write failed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FlushFailure(Vec<u8>);

    impl Write for FlushFailure {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(ErrorKind::BrokenPipe, "flush failed"))
        }
    }

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
    fn profiler_protocol_records_are_stable() {
        let mut output = Vec::new();
        write_pausing_notice(&mut output, 42, 3, "checkpoint-table").unwrap();
        write_resumed_notice(&mut output, 42, 3, "checkpoint-table").unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "DORADB_BENCH_PAUSING pid=42 phase=3 workload=checkpoint-table resume=SIGCONT\n\
             Attach the profiler to PID 42 and verify that the process is stopped.\n\
             Resume with: kill -CONT 42\n\
             DORADB_BENCH_RESUMED pid=42 phase=3 workload=checkpoint-table\n"
        );
    }

    #[test]
    fn profiler_pause_notice_maps_write_and_flush_failures() {
        let write_error = write_pausing_notice(&mut WriteFailure, 42, 3, "trx-noop").unwrap_err();
        assert_eq!(
            write_error.to_string(),
            "failed to write profiler pause notice for process 42: write failed"
        );

        let flush_error =
            write_pausing_notice(&mut FlushFailure(Vec::new()), 42, 3, "trx-noop").unwrap_err();
        assert_eq!(
            flush_error.to_string(),
            "failed to flush profiler pause notice for process 42: flush failed"
        );
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
                UpdateRandExecutor::IDENTITY,
                TableDdlExecutor::IDENTITY,
                LookupSeqExecutor::IDENTITY,
                LookupRandExecutor::IDENTITY,
                TableScanExecutor::IDENTITY,
                ParallelTableScanExecutor::IDENTITY,
                IndexScanExecutor::IDENTITY,
                IndexStreamExecutor::IDENTITY,
                IndexDdlExecutor::IDENTITY,
                LockTableExecutor::IDENTITY,
                FreezeTableExecutor::IDENTITY,
                CheckpointTableExecutor::IDENTITY,
            ],
            [
                "create-table",
                "stmt-noop",
                "trx-noop",
                "insert-seq",
                "insert-rand",
                "update-rand",
                "table-ddl",
                "lookup-seq",
                "lookup-rand",
                "table-scan",
                "parallel-table-scan",
                "index-scan",
                "index-stream",
                "index-ddl",
                "lock-table",
                "freeze-table",
                "checkpoint-table",
            ]
        );
    }

    #[test]
    fn run_spawner_uses_distinct_driven_executor_workers() {
        struct Rendezvous {
            arrived: usize,
            released: bool,
        }

        let executor = Arc::new(Executor::new());
        let spawner = RunTaskSpawner {
            executor: Arc::clone(&executor),
        };
        let rendezvous = Arc::new((
            StdMutex::new(Rendezvous {
                arrived: 0,
                released: false,
            }),
            Condvar::new(),
        ));
        let (identity_sender, identity_receiver) = mpsc::channel();
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let rendezvous = Arc::clone(&rendezvous);
            let identity_sender = identity_sender.clone();
            tasks.push(spawner.spawn(async move {
                let identity = thread::current().id();
                let (state, ready) = &*rendezvous;
                let mut state = state.lock().unwrap();
                state.arrived += 1;
                identity_sender.send(identity).unwrap();
                if state.arrived == 2 {
                    state.released = true;
                    ready.notify_all();
                }
                while !state.released {
                    state = ready.wait(state).unwrap();
                }
                identity
            }));
        }
        drop(identity_sender);

        let (signal, shutdown) = channel::unbounded::<()>();
        let shutdown_receiver = shutdown.clone();
        let worker_executor = Arc::clone(&executor);
        let release_on_timeout = Arc::clone(&rendezvous);
        let (_workers, result) = Parallel::new()
            .each(0..2, move |_| {
                let _ = smol::block_on(worker_executor.run(shutdown_receiver.recv()));
            })
            .finish(move || {
                let _signal = signal;
                let identities = (|| {
                    let first = identity_receiver
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|error| error.to_string())?;
                    let second = identity_receiver
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|error| error.to_string())?;
                    Ok::<[ThreadId; 2], String>([first, second])
                })();
                if identities.is_err() {
                    let (state, ready) = &*release_on_timeout;
                    let mut state = state.lock().unwrap();
                    state.released = true;
                    ready.notify_all();
                }
                let task_identities = smol::block_on(async move {
                    let first = tasks.remove(0).await;
                    let second = tasks.remove(0).await;
                    [first, second]
                });
                identities.map(|identities| (identities, task_identities))
            });
        let (identities, task_identities) = result.unwrap();
        assert_ne!(identities[0], identities[1]);
        assert_ne!(task_identities[0], task_identities[1]);
        assert!(
            task_identities
                .into_iter()
                .all(|identity| identities.contains(&identity))
        );
    }
}
