use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, FixtureRuntimeState,
};
use crate::measurement::{
    BenchmarkAccumulator, BenchmarkAggregate, InternalMetric, LatencyDistribution,
    MeasuredRunResult, MeasurementClock, WorkloadCounters, WorkloadMetrics, operations_per_second,
};
use crate::plan::{Phase, Plan, ResolvedWorkload, load_plan};
use crate::plan_output::{
    InvocationReport, PreparePhaseResult, absolute_result_path, render_stdout_summary,
    write_plan_output,
};
use crate::workload::{
    CatalogCheckpointExecutor, CatalogCheckpointPrepareExecutor, CheckpointTableExecutor,
    CreateIndexExecutor, CreateTableExecutor, DeleteAllExecutor, DeleteRandExecutor,
    FreezeTableExecutor, IndexDdlExecutor, IndexScanExecutor, IndexStreamExecutor,
    InsertRandExecutor, InsertSeqExecutor, LockTableExecutor, LookupRandExecutor,
    LookupSeqExecutor, ManagedBindingsPrepareExecutor, ParallelTableScanExecutor,
    ParallelTableScanExecutorConfig, ResolveTableBindingExecutor, RunCancellation, SessionPlan,
    StmtNoopExecutor, TableDdlExecutor, TableScanExecutor, TrxNoopExecutor, UpdateAllExecutor,
    UpdatePointRandExecutor, UpdateRandExecutor, UpsertPointRandExecutor, complete_create_index,
    complete_delete, complete_update, complete_upsert, prepare_create_fixture, run_recovery,
};
use doradb_storage::profiling::InternalStatsSnapshot;
use doradb_storage::{Engine, EngineConfig, Session};
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
    elapsed_nanos: u64,
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

    let reopen_config = loaded.engine_config.clone();
    let mut owner = Some(Engine::bootstrap(loaded.engine_config).await?);
    // Phase orchestration retains bootstrap futures and diagnostic snapshots.
    // Allocate its large state once, before any workload measurement window.
    let operation_result = Box::pin(execute_phases(
        &mut owner,
        &reopen_config,
        &clock,
        &loaded.plan,
    ))
    .await;
    if let Some(engine) = owner.take() {
        engine.shutdown();
        drop(engine);
    }
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
    owner: &mut Option<Engine>,
    reopen_config: &EngineConfig,
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
                let outcome = dispatch_workload(
                    current_engine(owner.as_ref())?,
                    clock,
                    workload,
                    binding,
                    fixture_effect,
                    false,
                    0,
                )
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
                if measurement.pause && !matches!(workload, ResolvedWorkload::Recovery(_)) {
                    pause_for_profiler(phase_index, workload.identity())?;
                }
                for execution_ordinal in 0..measurement.warmup_runs {
                    let binding = fixture.bind(workload.fixture_requirement())?;
                    dispatch_workload(
                        current_engine(owner.as_ref())?,
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
                    let outcome = if let ResolvedWorkload::Recovery(config) = workload {
                        let FixtureBinding::Recoverable(table) = binding else {
                            return Err(BenchError::message(
                                "recovery requires a recoverable fixture binding",
                            ));
                        };
                        let run = run_recovery(
                            owner,
                            reopen_config.clone(),
                            clock,
                            table,
                            config.include_stats,
                            config.fixture,
                            || {
                                if measurement.pause {
                                    pause_for_profiler(phase_index, workload.identity())
                                } else {
                                    Ok(())
                                }
                            },
                        )
                        .await?;
                        let mut latency = LatencyDistribution::new()?;
                        latency.record(run.elapsed_nanos)?;
                        RunOutcome {
                            elapsed_nanos: run.elapsed_nanos,
                            counters: WorkloadCounters {
                                operations: 1,
                                ..WorkloadCounters::default()
                            },
                            latency,
                            internal_metrics: run.internal_metrics,
                            workload_metrics: Some(WorkloadMetrics::Recovery {
                                report: Box::new(run.report),
                                verification: run.verification,
                            }),
                            effect: FixtureRuntimeEffect::None,
                        }
                    } else {
                        dispatch_workload(
                            current_engine(owner.as_ref())?,
                            clock,
                            workload,
                            binding,
                            fixture_effect,
                            true,
                            execution_ordinal,
                        )
                        .await?
                    };
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
                if matches!(
                    workload,
                    ResolvedWorkload::UpdateAll(_) | ResolvedWorkload::UpdatePointRand(_)
                ) {
                    let FixtureBinding::Primary(primary) =
                        fixture.bind(workload.fixture_requirement())?
                    else {
                        return Err(BenchError::message(
                            "update workload has no primary fixture binding",
                        ));
                    };
                    // All worker sessions and final statistics snapshots have completed.
                    // Verification must not warm data for a later measured run.
                    complete_update(current_engine(owner.as_ref())?, primary).await?;
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

fn current_engine(owner: Option<&Engine>) -> Result<&Engine> {
    owner.ok_or_else(|| BenchError::message("phase requires an occupied engine owner"))
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
        ResolvedWorkload::Recovery(_) => Err(BenchError::message(
            "recovery must execute at the coordinator lifecycle boundary",
        )),
        ResolvedWorkload::CreateIndex(config) => {
            let binding = if config.fixture.is_some() {
                prepare_create_fixture(engine, config).await?
            } else {
                binding
            };
            let mut outcome = run_executor::<CreateIndexExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(config.clone(), binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await?;
            // The generic envelope, all sessions, and final engine snapshots have ended.
            let Some(WorkloadMetrics::CreateIndex { report }) = outcome.workload_metrics.as_mut()
            else {
                return Err(BenchError::message(
                    "CREATE coordinator received no measurements",
                ));
            };
            #[cfg(test)]
            tests::run_create_completion_hook(engine, report);
            let effect = complete_create_index(engine, report).await?;
            outcome.effect = if config.fixture.is_some() {
                FixtureRuntimeEffect::None
            } else {
                effect
            };
            Ok(outcome)
        }
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
        ResolvedWorkload::UpdateAll(config) => {
            run_executor::<UpdateAllExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::UpdatePointRand(config) => {
            run_executor::<UpdatePointRandExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await
        }
        ResolvedWorkload::UpsertPointRand(config) => {
            let FixtureBinding::Primary(primary) = &binding else {
                return Err(BenchError::message(
                    "upsert workload has no primary fixture binding",
                ));
            };
            let primary = *primary;
            let outcome = run_executor::<UpsertPointRandExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await?;
            // Worker sessions and final statistics snapshots have completed; scans are unmeasured.
            complete_upsert(engine, primary, outcome.counters.inserted_rows).await?;
            Ok(outcome)
        }
        ResolvedWorkload::DeleteAll(config) => {
            let FixtureBinding::Primary(primary) = &binding else {
                return Err(BenchError::message(
                    "delete workload has no primary fixture binding",
                ));
            };
            let primary = *primary;
            let outcome = run_executor::<DeleteAllExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await?;
            // Worker sessions and final statistics snapshots have completed; scans are unmeasured.
            complete_delete(engine, primary, outcome.counters.deleted_rows).await?;
            Ok(outcome)
        }
        ResolvedWorkload::DeleteRand(config) => {
            let FixtureBinding::Primary(primary) = &binding else {
                return Err(BenchError::message(
                    "delete workload has no primary fixture binding",
                ));
            };
            let primary = *primary;
            let outcome = run_executor::<DeleteRandExecutor>(
                engine,
                clock,
                workload,
                SessionExecutorConfig::new(*config, binding, execution_ordinal),
                planned_effect,
                sample_latency,
            )
            .await?;
            // Worker sessions and final statistics snapshots have completed; scans are unmeasured.
            complete_delete(engine, primary, outcome.counters.deleted_rows).await?;
            Ok(outcome)
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
        match InternalStatsSnapshot::capture(&session) {
            Ok(before) => Some((session, before)),
            Err(error) => return close_stats_session(session, Err(error.into())).await,
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
    let elapsed_nanos = match elapsed_result {
        Ok(elapsed) => elapsed,
        Err(error) => {
            if let Some((session, _)) = stats_state {
                return close_stats_session(session, Err(error)).await;
            }
            return Err(error);
        }
    };

    let internal_metrics = if let Some((mut session, before)) = stats_state {
        let metrics_result = InternalStatsSnapshot::capture(&session)
            .map(|after| after.delta_since(&before))
            .map_err(BenchError::from);
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
    use crate::fixture::PrimaryBinding;
    use crate::measurement::CreateIndexReport;
    use doradb_storage::id::TableID;
    use std::cell::RefCell;
    use std::io::ErrorKind;
    use std::sync::{Condvar, Mutex as StdMutex, mpsc};
    use std::thread::{self, ThreadId};
    use std::time::Duration;

    type CreateCompletionHook = Box<dyn FnOnce(&Engine, &mut CreateIndexReport)>;

    thread_local! {
        static CREATE_COMPLETION_HOOK: RefCell<Option<CreateCompletionHook>> = const { RefCell::new(None) };
    }

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

    /// Apply a coordinator-local observation or fault before CREATE publication.
    pub(super) fn run_create_completion_hook(engine: &Engine, report: &mut CreateIndexReport) {
        if let Some(hook) = CREATE_COMPLETION_HOOK.with(|slot| slot.borrow_mut().take()) {
            hook(engine, report);
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

    /// Purpose: Compose persisted reads and recovery with initial or preparation-created indexes, including nonzero retained IDs.
    /// Expected: Measured counters exclude preparation, CREATE reports are verified, and every table/index row matches an independent key and payload oracle.
    #[test]
    fn composed_indexed_reads_preserve_contents_and_measurement_boundaries() {
        use doradb_storage::{
            CallbackResult, IndexID, ScanRowDecision, SelectMvcc, TableIndex, Val,
        };
        use tempfile::TempDir;
        smol::block_on(async {
            for (index, prepared, placement, workload) in [
                ("unique", false, "cold", "lookup-seq"),
                ("unique", true, "cold", "lookup-rand"),
                ("unique", true, "mixed", "lookup-seq"),
                ("non-unique", false, "cold", "index-scan"),
                ("non-unique", true, "hot", "index-stream"),
                ("unique", true, "cold", "recovery"),
            ] {
                let temp = TempDir::new().unwrap();
                let root = temp.path().join("root");
                let source = temp.path().join("plan.toml");
                let initial = if prepared { "none" } else { index };
                let rows = if placement == "mixed" { 6 } else { 8 };
                let mut input = format!(
                    "[workload_defaults]\nvalue_size = '8 B'\nbatch_size = 2\ninclude_stats = true\n[[phase]]\nworkload = {{ type = 'create-table', index = '{initial}' }}\n[[phase]]\nworkload = {{ type = 'insert-seq', num = {rows}, seed = 0 }}\n"
                );
                if placement != "hot" {
                    input.push_str("[[phase]]\nworkload = { type = 'freeze-table', all = true }\n[[phase]]\nworkload = { type = 'checkpoint-table' }\n");
                }
                if placement == "mixed" {
                    input.push_str(
                        "[[phase]]\nworkload = { type = 'insert-seq', num = 2, seed = 0 }\n",
                    );
                }
                if prepared {
                    input.push_str(&format!("[[phase]]\nworkload = {{ type = 'index-ddl', num = 2 }}\n[[phase]]\nworkload = {{ type = 'create-index', index = '{index}', columns = ['c0'] }}\n"));
                }
                let controls = match workload {
                    "recovery" => String::new(),
                    "index-stream" | "index-scan" => ", num = 8, range = 8".to_owned(),
                    _ => ", num = 8".to_owned(),
                };
                let repeats = if workload == "recovery" {
                    ""
                } else {
                    "warmup_runs = 1\nmeasured_runs = 2\n"
                };
                input.push_str(&format!("[[phase]]\nkind = 'benchmark'\n{repeats}workload = {{ type = '{workload}'{controls} }}\n"));
                fs::write(&source, input).unwrap();
                let loaded = load_plan(&source, &root).unwrap();
                let mut owner = Some(
                    Engine::bootstrap(loaded.engine_config.clone())
                        .await
                        .unwrap(),
                );
                let result = execute_phases(
                    &mut owner,
                    &loaded.engine_config,
                    &MeasurementClock::new(),
                    &loaded.plan,
                )
                .await
                .unwrap();
                let index_id = if prepared {
                    let phase = result
                        .prepare_phases
                        .iter()
                        .find(|phase| phase.workload == "create-index")
                        .unwrap();
                    assert_eq!(
                        phase.counters,
                        WorkloadCounters {
                            operations: 1,
                            ..WorkloadCounters::default()
                        }
                    );
                    let Some(WorkloadMetrics::CreateIndex { report }) = &phase.workload_metrics
                    else {
                        panic!("CREATE")
                    };
                    report.validate().unwrap();
                    assert_eq!(report.index_id, 2);
                    assert_eq!(report.total_rows, 8);
                    assert_eq!(
                        report.rows.hot_rows,
                        match placement {
                            "hot" => 8,
                            "mixed" => 2,
                            _ => 0,
                        }
                    );
                    IndexID::new(report.index_id)
                } else {
                    IndexID::new(0)
                };
                if workload == "recovery" {
                    let Some(WorkloadMetrics::Recovery { verification, .. }) =
                        &result.measured_runs[0].workload_metrics
                    else {
                        panic!("recovery")
                    };
                    assert!(verification.index_verified);
                    assert_eq!(verification.verified_rows, 8);
                } else {
                    let stream = workload == "index-stream";
                    let scan = stream || workload == "index-scan";
                    let expected = WorkloadCounters {
                        operations: 16,
                        found: if stream { 0 } else { 16 },
                        rows_returned: if scan { 128 } else { 16 },
                        ..WorkloadCounters::default()
                    };
                    assert_eq!(result.aggregate.counters, expected, "{workload}");
                    assert_eq!(
                        result.aggregate.latency.sample_count,
                        if stream { 16 } else { 8 }
                    );
                    assert_eq!(result.measured_runs.len(), 2);
                    for run in &result.measured_runs {
                        for metric in run
                            .internal_metrics
                            .iter()
                            .filter(|metric| metric.name == "create_index.completed_builds")
                        {
                            assert_eq!(metric.value, 0, "CREATE leaked into {workload}");
                        }
                    }
                }
                // Fixed seed-zero eight-byte payload vectors, independent of the runtime generator.
                let payloads = [
                    0x1b2b_3d64_92a3_41dbu64,
                    0xd07c_2af7_190a_3766,
                    0x059e_66e7_2fa8_49ec,
                    0xd83c_afc0_ccb4_5867,
                    0x0b90_47cf_5875_ce68,
                    0x4048_68ee_fff1_175b,
                    0xe6c4_9c53_5f15_9652,
                    0xde9a_e8ac_66d4_50de,
                ];
                let expected: Vec<_> = payloads
                    .into_iter()
                    .enumerate()
                    .map(|(key, payload)| {
                        vec![
                            Val::from(key as u64),
                            Val::from(payload.to_le_bytes().to_vec()),
                        ]
                    })
                    .collect();
                let mut session = owner.as_ref().unwrap().new_session().unwrap();
                let table_id = session.list_table_ids().unwrap()[0];
                let mut trx = session.begin_trx().unwrap();
                let mut actual = Vec::new();
                let mut scan = trx
                    .table_scan_mvcc_stream(table_id, &[0, 1], |_| -> CallbackResult<_> {
                        Ok(ScanRowDecision::Include)
                    })
                    .await
                    .unwrap();
                while let Some(row) = scan.next().await.unwrap() {
                    actual.push(row);
                }
                drop(scan);
                actual.sort_by_key(|row| row[0].as_u64().unwrap());
                assert_eq!(actual, expected, "{workload}: table content");
                let mut scan = trx
                    .table_index_scan_mvcc_stream(TableIndex(table_id, index_id), .., &[0, 1])
                    .await
                    .unwrap();
                for row in &expected {
                    assert_eq!(scan.next().await.unwrap().as_ref(), Some(row));
                }
                assert_eq!(scan.next().await.unwrap(), None);
                drop(scan);
                if index == "unique" {
                    for row in &expected {
                        let found = trx
                            .table_lookup_unique_mvcc(
                                TableIndex(table_id, index_id),
                                &row[..1],
                                &[0, 1],
                            )
                            .await
                            .unwrap();
                        let SelectMvcc::Found(found) = found else {
                            panic!("missing row")
                        };
                        assert_eq!(&found, row);
                    }
                }
                trx.commit().await.unwrap();
                session.close().await.unwrap();
                let report = InvocationReport {
                    root: root.clone(),
                    plan_source: source,
                    plan: loaded.plan,
                    prepare_phases: result.prepare_phases,
                    measured_runs: result.measured_runs,
                    aggregate: result.aggregate,
                };
                write_plan_output(&report).unwrap();
                owner.take().unwrap().shutdown();
            }
        });
    }

    /// Purpose: Stop composed plans when CREATE or its completion verification fails.
    /// Expected: The later insert never runs, no successful artifact is installed, and the root remains reopenable after participant cleanup.
    #[test]
    fn failed_create_preparation_stops_later_phases_and_result_publication() {
        use doradb_storage::{CallbackResult, RowMutation, ScanRowDecision};
        use tempfile::TempDir;
        smol::block_on(async {
            for failure in ["create", "identity", "report", "content"] {
                let temp = TempDir::new().unwrap();
                let root = temp.path().join("root");
                let source = temp.path().join("plan.toml");
                let insert = if failure == "create" {
                    "insert-rand"
                } else {
                    "insert-seq"
                };
                fs::write(&source, format!("[[phase]]\nworkload = {{ type = 'create-table', index = 'none' }}\n[[phase]]\nworkload = {{ type = '{insert}', num = 8, seed = 42 }}\n[[phase]]\nworkload = {{ type = 'create-index', index = 'unique' }}\n[[phase]]\nkind = 'benchmark'\nworkload = {{ type = 'insert-seq', num = 1 }}\n")).unwrap();
                if failure != "create" {
                    CREATE_COMPLETION_HOOK.with(|slot| {
                        *slot.borrow_mut() = Some(Box::new(move |engine, report| match failure {
                            "identity" => report.index_id = u32::MAX,
                            "report" => report.rows.hot_rows += 1,
                            "content" => smol::block_on(async {
                                let mut session = engine.new_session().unwrap();
                                let mut trx = session.begin_trx().unwrap();
                                trx.table_mutate_mvcc(
                                    TableID::new(report.table_id),
                                    |row| -> CallbackResult<_> {
                                        Ok(if row.val(0)?.as_u64() == Some(0) {
                                            RowMutation::Delete
                                        } else {
                                            RowMutation::Skip
                                        })
                                    },
                                )
                                .await
                                .unwrap();
                                trx.commit().await.unwrap();
                                session.close().await.unwrap();
                            }),
                            _ => unreachable!(),
                        }));
                    });
                }
                let error = execute_plan(root.clone(), source.clone())
                    .await
                    .unwrap_err();
                assert!(
                    !root.join("benchmark-result.toml").exists(),
                    "{failure}: {error}"
                );
                assert!(CREATE_COMPLETION_HOOK.with(|slot| slot.borrow().is_none()));
                let loaded = load_plan(&source, &root).unwrap();
                let engine = Engine::bootstrap(loaded.engine_config).await.unwrap();
                let mut session = engine.new_session().unwrap();
                let table = session.list_table_ids().unwrap()[0];
                let mut trx = session.begin_trx().unwrap();
                let mut scan = trx
                    .table_scan_mvcc_stream(table, &[0, 1], |_| -> CallbackResult<_> {
                        Ok(ScanRowDecision::Include)
                    })
                    .await
                    .unwrap();
                let mut rows = 0;
                while let Some(row) = scan.next().await.unwrap() {
                    assert!(
                        row[0].as_u64().unwrap() < 8,
                        "later phase inserted its key after {failure}"
                    );
                    rows += 1;
                }
                assert_eq!(
                    rows,
                    if failure == "content" { 7 } else { 8 },
                    "{failure}: {error}"
                );
                drop(scan);
                trx.commit().await.unwrap();
                session.close().await.unwrap();
                engine.shutdown();
            }
        });
    }

    /// Purpose: Protect the public profiler pause and resume protocol.
    /// Expected: Notices retain stable records, workload context, and actionable attach and
    /// resume instructions.
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

    /// Purpose: Preserve context when profiler notice delivery fails.
    /// Expected: Write and flush errors retain their cause and identify the affected operation
    /// and process.
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

    /// Purpose: Verify new update workloads only once after warmups and all measurements, without warming a later run.
    /// Expected: Completion observes final replay parity and drained workers; its clock advance and held lock are absent from run time, latency, and diagnostics.
    #[test]
    fn update_completion_follows_all_runs_and_statistics() {
        use crate::workload::set_update_completion_hook;
        use doradb_storage::{
            CallbackResult, IndexID, ScanRowDecision, TableIndex, TableLockMode, Val,
        };
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;
        use tempfile::TempDir;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            for controls in [
                "type = 'update-all'",
                "type = 'update-point-rand', num = 2, batch_size = 2",
            ] {
                let source = temp.path().join("update.toml");
                let root = temp.path().join(if controls.contains("update-all") {
                    "all"
                } else {
                    "point"
                });
                fs::write(&source, format!("[[phase]]\nworkload = {{ type = 'create-table', index = 'unique' }}\n[[phase]]\nworkload = {{ type = 'insert-seq', num = 1 }}\n[[phase]]\nkind = 'benchmark'\nwarmup_runs = 1\nmeasured_runs = 2\nworkload = {{ {controls}, change_key = true, value_size = '1 B', include_stats = true }}")).unwrap();
                let loaded = load_plan(&source, &root).unwrap();
                let mut owner = Some(
                    Engine::bootstrap(loaded.engine_config.clone())
                        .await
                        .unwrap(),
                );
                let (clock, mock) = MeasurementClock::mock();
                let calls = Rc::new(Cell::new(0));
                let observed = Rc::clone(&calls);
                let completion_session = Rc::new(RefCell::new(None));
                let held_session = Rc::clone(&completion_session);
                set_update_completion_hook(move |engine, primary| {
                    observed.set(observed.get() + 1);
                    smol::block_on(async {
                        let mut session = engine.new_session().unwrap();
                        let stats = session.logical_lock_stats().unwrap();
                        assert_eq!(stats.current_physical_resources, 0);
                        assert_eq!(stats.current_linked_waiters, 0);
                        let mut trx = session.begin_trx().unwrap();
                        // Three executions end in the alternate domain with parity-zero payload.
                        let mut stream = trx
                            .table_scan_mvcc_stream(
                                primary.table_id,
                                &[0, 1],
                                |_| -> CallbackResult<_> { Ok(ScanRowDecision::Include) },
                            )
                            .await
                            .unwrap();
                        assert_eq!(
                            stream.next().await.unwrap(),
                            Some(vec![Val::from(1u64), Val::from(vec![0u8])])
                        );
                        assert_eq!(stream.next().await.unwrap(), None);
                        drop(stream);
                        let mut stream = trx
                            .table_index_scan_mvcc_stream(
                                TableIndex(primary.table_id, IndexID::new(0)),
                                ..,
                                &[0, 1],
                            )
                            .await
                            .unwrap();
                        assert_eq!(
                            stream.next().await.unwrap(),
                            Some(vec![Val::from(1u64), Val::from(vec![0u8])])
                        );
                        assert_eq!(stream.next().await.unwrap(), None);
                        drop(stream);
                        trx.commit().await.unwrap();
                        // Keep a diagnostic marker until results are inspected. Unlike
                        // redo counters, lock gauges are updated before the API returns.
                        session
                            .lock_table(primary.table_id, TableLockMode::Shared)
                            .await
                            .unwrap();
                        *held_session.borrow_mut() = Some(session);
                    });
                    mock.increment(1_000_000);
                });
                let start = clock.now();
                let result =
                    execute_phases(&mut owner, &loaded.engine_config, &clock, &loaded.plan)
                        .await
                        .unwrap();
                assert_eq!(calls.get(), 1);
                assert_eq!(
                    clock.wall_delta_nanos(start, clock.now()).unwrap(),
                    1_000_000
                );
                assert_eq!(result.measured_runs.len(), 2);
                assert_eq!(result.aggregate.elapsed_nanos, 0);
                assert_eq!(result.aggregate.latency.sum_nanos, 0);
                assert_eq!(result.aggregate.latency.sample_count, 2);
                assert_eq!(result.aggregate.counters.updated_rows, 2);
                let mut completion_session = completion_session.borrow_mut().take().unwrap();
                let stats = completion_session.logical_lock_stats().unwrap();
                assert!(stats.current_physical_resources > 0, "{stats:?}");
                for run in result.measured_runs {
                    assert_eq!(run.elapsed_nanos, 0);
                    assert_eq!(run.latency.sample_count, 1);
                    assert_eq!(run.counters.updated_rows, 1);
                    let locks = run
                        .internal_metrics
                        .iter()
                        .find(|metric| metric.name == "logical_lock.current_physical_resources")
                        .unwrap();
                    assert_eq!(
                        locks.value, 0,
                        "{controls}: run {} included the completion lock: {locks:?}",
                        run.run_index
                    );
                }
                completion_session.close().await.unwrap();
                owner.take().unwrap().shutdown();
            }
        });
    }

    /// Purpose: Suppress canonical publication when final update verification finds unexpected rows.
    /// Expected: Verification returns an error and keeps the diagnostic root without creating a success artifact.
    #[test]
    fn update_completion_failure_prevents_canonical_publication() {
        use crate::workload::set_update_completion_hook;
        use doradb_storage::Val;
        use tempfile::TempDir;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            let source = temp.path().join("update.toml");
            fs::write(&source, "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'update-all' }").unwrap();
            set_update_completion_hook(|engine, primary| {
                smol::block_on(async {
                    let mut session = engine.new_session().unwrap();
                    let mut trx = session.begin_trx().unwrap();
                    trx.table_insert_mvcc(
                        primary.table_id,
                        vec![Val::from(99u64), Val::from("unexpected")],
                    )
                    .await
                    .unwrap();
                    trx.commit().await.unwrap();
                    session.close().await.unwrap();
                });
            });
            let root = temp.path().join("root");
            let error = execute_plan(root.clone(), source).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("update content verification failed")
            );
            assert!(root.exists());
            assert!(!root.join("benchmark-result.toml").exists());
            assert!(!root.join("benchmark-result.toml.tmp").exists());
        });
    }

    /// Purpose: Keep terminal mutation verification outside the production dispatch timer and diagnostic interval.
    /// Expected: Worker locks drain first, verification advances neither reported time nor acquisition counters.
    #[test]
    fn terminal_mutation_dispatch_completes_measurement_before_content_scans() {
        use crate::workload::{set_delete_completion_hook, set_upsert_completion_hook};
        use std::cell::Cell;
        use std::rc::Rc;
        use tempfile::TempDir;
        smol::block_on(async {
            let temp = TempDir::new().unwrap();
            for controls in [
                "type = 'delete-all'",
                "type = 'delete-rand', num = 2, sessions = 3",
                "type = 'upsert-point-rand', num = 2, sessions = 3, key_range = { start = 1, len = 4 }",
            ] {
                let root = temp.path().join(if controls.contains("delete-all") {
                    "all"
                } else if controls.contains("upsert-point-rand") {
                    "upsert"
                } else {
                    "rand"
                });
                let source = temp.path().join("plan.toml");
                fs::write(&source, format!("[[phase]]\nworkload = {{ type = 'create-table', index = 'unique' }}\n[[phase]]\nworkload = {{ type = 'insert-seq', num = 3 }}\n[[phase]]\nkind = 'benchmark'\nworkload = {{ {controls}, include_stats = true }}")).unwrap();
                let loaded = load_plan(&source, &root).unwrap();
                let engine = Engine::bootstrap(loaded.engine_config).await.unwrap();
                let (clock, mock) = MeasurementClock::mock();
                let mut fixture = FixtureRuntimeState::default();
                for phase in &loaded.plan.phases[..2] {
                    let Phase::Prepare {
                        workload,
                        fixture_effect,
                    } = phase
                    else {
                        unreachable!()
                    };
                    let binding = fixture.bind(workload.fixture_requirement()).unwrap();
                    let outcome = dispatch_workload(
                        &engine,
                        &clock,
                        workload,
                        binding,
                        fixture_effect,
                        false,
                        0,
                    )
                    .await
                    .unwrap();
                    fixture.apply(outcome.effect).unwrap();
                }
                let mut inspector = engine.new_session().unwrap();
                let before = inspector
                    .logical_lock_stats()
                    .unwrap()
                    .immediate_physical_acquisitions;
                let boundary = Rc::new(Cell::new(0));
                let captured = Rc::clone(&boundary);
                let advanced = Arc::clone(&mock);
                let hook = move |engine: &Engine, _: PrimaryBinding| {
                    smol::block_on(async {
                        let mut session = engine.new_session().unwrap();
                        let stats = session.logical_lock_stats().unwrap();
                        assert_eq!(stats.current_physical_resources, 0);
                        assert_eq!(stats.current_linked_waiters, 0);
                        captured.set(stats.immediate_physical_acquisitions);
                        session.close().await.unwrap();
                    });
                    advanced.increment(1_000_000);
                };
                if controls.contains("upsert") {
                    set_upsert_completion_hook(hook);
                } else {
                    set_delete_completion_hook(hook);
                }
                let start = clock.now();
                let workload = loaded.plan.phases[2].workload();
                let binding = fixture.bind(workload.fixture_requirement()).unwrap();
                let outcome = dispatch_workload(
                    &engine,
                    &clock,
                    workload,
                    binding,
                    &FixturePlanEffect::None,
                    true,
                    0,
                )
                .await
                .unwrap();
                assert_eq!(
                    clock.wall_delta_nanos(start, clock.now()).unwrap(),
                    1_000_000
                );
                assert_eq!(outcome.elapsed_nanos, 0);
                assert_eq!(
                    outcome
                        .latency
                        .summary(workload.latency_unit())
                        .unwrap()
                        .sum_nanos,
                    0
                );
                let acquired = outcome
                    .internal_metrics
                    .iter()
                    .find(|metric| metric.name == "logical_lock.immediate_physical_acquisitions")
                    .unwrap();
                assert_eq!(acquired.value, boundary.get() - before);
                let after = inspector.logical_lock_stats().unwrap();
                assert!(after.immediate_physical_acquisitions > boundary.get());
                assert_eq!(after.current_physical_resources, 0);
                inspector.close().await.unwrap();
                engine.shutdown();
            }
        });
    }

    /// Purpose: Prevent success publication when final terminal mutation verification discovers unexpected surviving rows.
    /// Expected: The invocation retains its diagnostic root, returns the verification error, and creates no artifact.
    #[test]
    fn terminal_mutation_completion_failure_prevents_canonical_publication() {
        use crate::workload::{set_delete_completion_hook, set_upsert_completion_hook};
        use doradb_storage::Val;
        use tempfile::TempDir;
        smol::block_on(async {
            for upsert in [false, true] {
                let temp = TempDir::new().unwrap();
                let source = temp.path().join("delete.toml");
                let raw = "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'delete-all' }";
                let raw = if upsert {
                    raw.replace("type = 'delete-all'", "type = 'upsert-point-rand', num = 5")
                } else {
                    raw.to_owned()
                };
                fs::write(&source, raw).unwrap();
                let hook = |engine: &Engine, primary: PrimaryBinding| {
                    smol::block_on(async {
                        let mut session = engine.new_session().unwrap();
                        let mut trx = session.begin_trx().unwrap();
                        trx.table_insert_mvcc(
                            primary.table_id,
                            vec![Val::from(99u64), Val::from("unexpected")],
                        )
                        .await
                        .unwrap();
                        trx.commit().await.unwrap();
                        session.close().await.unwrap();
                    });
                };
                if upsert {
                    set_upsert_completion_hook(hook);
                } else {
                    set_delete_completion_hook(hook);
                }
                let root = temp.path().join("root");
                let error = execute_plan(root.clone(), source).await.unwrap_err();
                assert!(error.to_string().contains("content verification failed"));
                assert!(root.exists());
                assert!(!root.join("benchmark-result.toml").exists());
                assert!(!root.join("benchmark-result.toml.tmp").exists());
            }
        });
    }

    /// Purpose: Preserve type compatibility within related executor families.
    /// Expected: Related executors satisfy their shared configuration and outcome type
    /// contracts.
    #[test]
    fn related_executor_identities_share_associated_types() {
        assert_shared_config::<StmtNoopExecutor, TrxNoopExecutor>();
        assert_shared_outcome::<StmtNoopExecutor, TrxNoopExecutor>();
        assert_shared_outcome::<DeleteAllExecutor, DeleteRandExecutor>();
        assert_shared_outcome::<UpdateRandExecutor, UpdateAllExecutor>();
        assert_shared_outcome::<UpdateAllExecutor, UpdatePointRandExecutor>();
        assert_shared_outcome::<UpdatePointRandExecutor, UpsertPointRandExecutor>();
        assert_shared_config::<InsertSeqExecutor, InsertRandExecutor>();
        assert_shared_outcome::<InsertSeqExecutor, InsertRandExecutor>();
        assert_shared_config::<TableDdlExecutor, IndexDdlExecutor>();
        assert_shared_outcome::<TableDdlExecutor, IndexDdlExecutor>();
        assert_shared_config::<LookupSeqExecutor, LookupRandExecutor>();
        assert_shared_outcome::<LookupSeqExecutor, IndexStreamExecutor>();
    }

    /// Purpose: Keep executor identities aligned with public workload names.
    /// Expected: Registered executors retain the expected workload spelling and identity
    /// mapping.
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
                UpdateAllExecutor::IDENTITY,
                UpdatePointRandExecutor::IDENTITY,
                UpsertPointRandExecutor::IDENTITY,
                DeleteAllExecutor::IDENTITY,
                DeleteRandExecutor::IDENTITY,
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
                "update-all",
                "update-point-rand",
                "upsert-point-rand",
                "delete-all",
                "delete-rand",
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

    /// Purpose: Support concurrent task progress across executor workers.
    /// Expected: Rendezvousing tasks execute on distinct driven worker threads.
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
