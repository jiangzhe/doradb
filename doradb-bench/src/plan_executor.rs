use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, FixtureRuntimeState, KeyRange, PrimaryTableShape,
};
use crate::manifest::write_plan_manifest_exclusive;
use crate::measurement::{
    BenchmarkAccumulator, BenchmarkAggregate, ExpectedOutcomeCounters, InternalMetric,
    LatencyDistribution, MeasuredRunResult, MeasurementClock, WorkloadCounters,
    operations_per_second,
};
use crate::output::{capture_internal_stats, plan_internal_metrics};
use crate::plan::{
    InsertConfig, Phase, Plan, ResolvedWorkload, StmtNoopConfig, TableDdlConfig, TrxNoopConfig,
    load_plan,
};
use crate::plan_output::{InvocationReport, PreparePhaseResult, write_plan_outputs};
use crate::workload::{
    InsertOperationSpec, RunCancellation, SessionPlan, build_session_plans,
    run_create_table_operation, run_insert_operations, run_stmt_noop_operations,
    run_table_ddl_operations, run_trx_noop_operations,
};
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{Engine, Session};
use easy_parallel::Parallel;
use smol::{Executor, channel};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct RunOutcome {
    elapsed_nanos: u128,
    counters: WorkloadCounters,
    latency: LatencyDistribution,
    internal_metrics: Vec<InternalMetric>,
    effect: FixtureRuntimeEffect,
}

struct SessionOutcome {
    counters: WorkloadCounters,
    latency: LatencyDistribution,
    table_id: Option<TableID>,
    latest_write_fence: Option<TrxID>,
}

#[derive(Clone, Copy)]
struct InsertSessionExecution {
    config: InsertConfig,
    table_id: TableID,
    random: bool,
    sample_latency: bool,
}

struct InvocationResults {
    prepare_phases: Vec<PreparePhaseResult>,
    measured_runs: Vec<MeasuredRunResult>,
    aggregate: BenchmarkAggregate,
}

/// Parse and execute one plan against one new invocation-owned storage root.
pub async fn execute_plan(storage_root: PathBuf, plan_source: PathBuf) -> Result<()> {
    let loaded = load_plan(&plan_source, &storage_root)?;
    // Construct/calibrate after complete validation and before root creation.
    let clock = MeasurementClock::new();
    prepare_plan_root(&storage_root)?;
    if let Err(error) = write_plan_manifest_exclusive(
        &storage_root,
        &loaded.plan.source,
        loaded.plan.name.as_deref(),
    ) {
        let _ = fs::remove_dir_all(&storage_root);
        return Err(error);
    }

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
    write_plan_outputs(&report)?;
    println!(
        "completed benchmark plan={} storage_root={} measured_runs={}",
        report.plan_source.display(),
        report.root.display(),
        report.measured_runs.len()
    );
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
                let table_id = bind_runtime_fixture(workload, &fixture)?;
                let outcome = run_once(engine, clock, workload, table_id, false).await?;
                verify_run_outcome(workload, *fixture_effect, &outcome, false)?;
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
                    let table_id = bind_runtime_fixture(workload, &fixture)?;
                    let outcome = run_once(engine, clock, workload, table_id, true).await?;
                    verify_run_outcome(workload, *fixture_effect, &outcome, true)?;
                }

                let mut aggregate = BenchmarkAccumulator::new()?;
                let mut phase_effect = None;
                for run_index in 1..=measurement.measured_runs.get() {
                    let table_id = bind_runtime_fixture(workload, &fixture)?;
                    let outcome = run_once(engine, clock, workload, table_id, true).await?;
                    verify_run_outcome(workload, *fixture_effect, &outcome, true)?;
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

fn bind_runtime_fixture(
    workload: &ResolvedWorkload,
    fixture: &FixtureRuntimeState,
) -> Result<Option<TableID>> {
    match workload {
        ResolvedWorkload::CreateTable(_) => {
            if fixture.primary().is_some() {
                return Err(BenchError::message(
                    "create-table runtime binding found an existing primary fixture",
                ));
            }
            Ok(None)
        }
        ResolvedWorkload::InsertSeq(config) | ResolvedWorkload::InsertRand(config) => {
            let primary = fixture.primary().ok_or_else(|| {
                BenchError::message("insert runtime binding requires a primary fixture")
            })?;
            if primary.shape.index != config.index {
                return Err(BenchError::message(
                    "insert runtime primary shape differs from the resolved plan",
                ));
            }
            if primary.next_key != config.key_start
                || config.attempted_range.start != config.key_start
                || config.attempted_range.len != config.num
            {
                return Err(BenchError::message(
                    "insert runtime key cursor differs from the resolved plan",
                ));
            }
            Ok(Some(primary.table_id))
        }
        ResolvedWorkload::StmtNoop(_)
        | ResolvedWorkload::TrxNoop(_)
        | ResolvedWorkload::TableDdl(_) => Ok(None),
    }
}

fn verify_run_outcome(
    workload: &ResolvedWorkload,
    planned_effect: FixturePlanEffect,
    outcome: &RunOutcome,
    sampled: bool,
) -> Result<()> {
    let expected_samples = if sampled {
        workload.expected_samples()?
    } else {
        0
    };
    if outcome.latency.sample_count() != expected_samples {
        return Err(BenchError::message(format!(
            "{} latency sample count ({}) does not match expected samples ({expected_samples})",
            workload.identity(),
            outcome.latency.sample_count()
        )));
    }

    match workload {
        ResolvedWorkload::CreateTable(config) => {
            require_simple_counters(workload, outcome.counters, 1)?;
            match (planned_effect, outcome.effect) {
                (
                    FixturePlanEffect::CreatePrimary { shape: planned },
                    FixtureRuntimeEffect::CreatePrimary { shape, .. },
                ) if planned == config.shape && shape == config.shape => Ok(()),
                _ => Err(BenchError::message(
                    "create-table runtime effect differs from the resolved fixture effect",
                )),
            }
        }
        ResolvedWorkload::StmtNoop(config) => {
            require_simple_counters(workload, outcome.counters, config.num)?;
            require_none_effect(planned_effect, outcome.effect)
        }
        ResolvedWorkload::TrxNoop(config) => {
            require_simple_counters(workload, outcome.counters, config.num)?;
            require_none_effect(planned_effect, outcome.effect)
        }
        ResolvedWorkload::TableDdl(config) => {
            require_simple_counters(workload, outcome.counters, config.operations)?;
            require_none_effect(planned_effect, outcome.effect)
        }
        ResolvedWorkload::InsertSeq(config) | ResolvedWorkload::InsertRand(config) => {
            let counters = outcome.counters;
            let terminal = counters
                .inserted_rows
                .checked_add(counters.expected_outcomes.duplicate_key)
                .and_then(|value| value.checked_add(counters.expected_outcomes.write_conflict))
                .ok_or_else(|| BenchError::message("insert terminal counter overflow"))?;
            if counters.operations != config.num || counters.operations != terminal {
                return Err(BenchError::message(format!(
                    "{} counters do not satisfy operations = inserted_rows + duplicate_key + write_conflict",
                    workload.identity()
                )));
            }
            if counters.found != 0 || counters.not_found != 0 || counters.rows_returned != 0 {
                return Err(BenchError::message(format!(
                    "{} produced unexpected read counters",
                    workload.identity()
                )));
            }
            match (planned_effect, outcome.effect) {
                (
                    FixturePlanEffect::Insert {
                        attempted_range: planned,
                    },
                    FixtureRuntimeEffect::Insert {
                        attempted_range,
                        inserted_rows,
                        latest_write_fence,
                    },
                ) if planned == config.attempted_range
                    && attempted_range == planned
                    && inserted_rows == counters.inserted_rows
                    && (inserted_rows == 0) == latest_write_fence.is_none() =>
                {
                    Ok(())
                }
                _ => Err(BenchError::message(
                    "insert runtime effect differs from the resolved fixture effect",
                )),
            }
        }
    }
}

fn require_simple_counters(
    workload: &ResolvedWorkload,
    counters: WorkloadCounters,
    operations: u64,
) -> Result<()> {
    if counters.operations != operations
        || counters.inserted_rows != 0
        || counters.found != 0
        || counters.not_found != 0
        || counters.rows_returned != 0
        || counters.expected_outcomes != ExpectedOutcomeCounters::default()
    {
        return Err(BenchError::message(format!(
            "{} produced invalid counters",
            workload.identity()
        )));
    }
    Ok(())
}

fn require_none_effect(
    planned_effect: FixturePlanEffect,
    runtime_effect: FixtureRuntimeEffect,
) -> Result<()> {
    if planned_effect != FixturePlanEffect::None || runtime_effect != FixtureRuntimeEffect::None {
        return Err(BenchError::message(
            "no-effect workload produced a fixture transition",
        ));
    }
    Ok(())
}

async fn run_once(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    table_id: Option<TableID>,
    sample_latency: bool,
) -> Result<RunOutcome> {
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
    let run_result = run_session_workers(engine, clock, workload, table_id, sample_latency).await;
    let stopped = clock.now();
    let elapsed_result = clock.wall_delta_nanos(started, stopped);

    let session_outcome = match run_result {
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
            if let Some((mut session, _)) = stats_state {
                let _ = session.close().await;
            }
            return Err(error);
        }
    };

    let internal_metrics = if let Some((mut session, before)) = stats_state {
        let metrics_result =
            capture_internal_stats(&session).map(|after| plan_internal_metrics(&before, &after));
        let close_result = session.close().await.map_err(BenchError::from);
        match (metrics_result, close_result) {
            (Ok(metrics), Ok(())) => metrics,
            (Err(error), _) => return Err(error),
            (Ok(_), Err(error)) => return Err(error),
        }
    } else {
        Vec::new()
    };

    let effect = match workload {
        ResolvedWorkload::CreateTable(config) => FixtureRuntimeEffect::CreatePrimary {
            shape: config.shape,
            table_id: session_outcome.table_id.ok_or_else(|| {
                BenchError::message("create-table completed without a returned table ID")
            })?,
        },
        ResolvedWorkload::InsertSeq(config) | ResolvedWorkload::InsertRand(config) => {
            FixtureRuntimeEffect::Insert {
                attempted_range: config.attempted_range,
                inserted_rows: session_outcome.counters.inserted_rows,
                latest_write_fence: session_outcome.latest_write_fence,
            }
        }
        ResolvedWorkload::StmtNoop(_)
        | ResolvedWorkload::TrxNoop(_)
        | ResolvedWorkload::TableDdl(_) => FixtureRuntimeEffect::None,
    };
    Ok(RunOutcome {
        elapsed_nanos,
        counters: session_outcome.counters,
        latency: session_outcome.latency,
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

async fn run_session_workers(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    table_id: Option<TableID>,
    sample_latency: bool,
) -> Result<SessionOutcome> {
    let cancellation = Arc::new(RunCancellation::new());
    match workload {
        ResolvedWorkload::CreateTable(config) => {
            execute_create_session(
                engine,
                clock.clone(),
                config.shape,
                sample_latency,
                cancellation,
            )
            .await
        }
        ResolvedWorkload::StmtNoop(config) => {
            let plans = build_session_plans(
                KeyRange {
                    start: 0,
                    len: config.num,
                },
                config.sessions,
            )?;
            let executor = Executor::new();
            let tasks = plans
                .into_iter()
                .map(|plan| {
                    executor.spawn(execute_stmt_noop_session(
                        engine,
                        clock.clone(),
                        *config,
                        plan,
                        sample_latency,
                        Arc::clone(&cancellation),
                    ))
                })
                .collect();
            drive_session_tasks(&executor, config.threads, tasks, cancellation)
        }
        ResolvedWorkload::TrxNoop(config) => {
            let plans = build_session_plans(
                KeyRange {
                    start: 0,
                    len: config.num,
                },
                config.sessions,
            )?;
            let executor = Executor::new();
            let tasks = plans
                .into_iter()
                .map(|plan| {
                    executor.spawn(execute_trx_noop_session(
                        engine,
                        clock.clone(),
                        *config,
                        plan,
                        sample_latency,
                        Arc::clone(&cancellation),
                    ))
                })
                .collect();
            drive_session_tasks(&executor, config.threads, tasks, cancellation)
        }
        ResolvedWorkload::InsertSeq(config) | ResolvedWorkload::InsertRand(config) => {
            let table_id = table_id
                .ok_or_else(|| BenchError::message("insert workload has no bound table ID"))?;
            let plans = build_session_plans(config.attempted_range, config.sessions)?;
            let random = matches!(workload, ResolvedWorkload::InsertRand(_));
            let executor = Executor::new();
            let tasks = plans
                .into_iter()
                .map(|plan| {
                    executor.spawn(execute_insert_session(
                        engine,
                        clock.clone(),
                        InsertSessionExecution {
                            config: *config,
                            table_id,
                            random,
                            sample_latency,
                        },
                        plan,
                        Arc::clone(&cancellation),
                    ))
                })
                .collect();
            drive_session_tasks(&executor, config.threads, tasks, cancellation)
        }
        ResolvedWorkload::TableDdl(config) => {
            let plans = build_session_plans(
                KeyRange {
                    start: 0,
                    len: config.num,
                },
                config.sessions,
            )?;
            let executor = Executor::new();
            let tasks = plans
                .into_iter()
                .map(|plan| {
                    executor.spawn(execute_table_ddl_session(
                        engine,
                        clock.clone(),
                        *config,
                        plan,
                        sample_latency,
                        Arc::clone(&cancellation),
                    ))
                })
                .collect();
            drive_session_tasks(&executor, config.threads, tasks, cancellation)
        }
    }
}

async fn execute_create_session(
    engine: &Engine,
    clock: MeasurementClock,
    shape: PrimaryTableShape,
    sample_latency: bool,
    cancellation: Arc<RunCancellation>,
) -> Result<SessionOutcome> {
    let mut session = match engine.new_session() {
        Ok(session) => session,
        Err(error) => {
            cancellation.fail(error.into());
            return Err(cancellation
                .take_error()
                .ok_or_else(|| BenchError::message("missing create-table session error"))?);
        }
    };
    let run_result =
        run_create_table_operation(&mut session, shape, sample_latency.then_some(&clock)).await;
    let outcome = match run_result {
        Ok(result) => Some(SessionOutcome {
            counters: WorkloadCounters {
                operations: 1,
                ..WorkloadCounters::default()
            },
            latency: result.latency,
            table_id: Some(result.table_id),
            latest_write_fence: None,
        }),
        Err(error) => {
            cancellation.fail(error);
            None
        }
    };
    if let Err(error) = session.close().await {
        cancellation.fail(error.into());
    }
    if let Some(error) = cancellation.take_error() {
        Err(error)
    } else {
        outcome.ok_or_else(|| BenchError::message("create-table produced no outcome"))
    }
}

async fn execute_stmt_noop_session(
    engine: &Engine,
    clock: MeasurementClock,
    _config: StmtNoopConfig,
    plan: SessionPlan,
    sample_latency: bool,
    cancellation: Arc<RunCancellation>,
) -> Option<SessionOutcome> {
    let mut session = open_workload_session(engine, &cancellation)?;
    let result = run_stmt_noop_operations(
        &mut session,
        plan.number,
        sample_latency.then_some(&clock),
        Some(&cancellation),
    )
    .await
    .map(|result| SessionOutcome {
        counters: WorkloadCounters {
            operations: result.operations,
            ..WorkloadCounters::default()
        },
        latency: result.latency,
        table_id: None,
        latest_write_fence: None,
    });
    finish_workload_session(session, result, &cancellation).await
}

async fn execute_trx_noop_session(
    engine: &Engine,
    clock: MeasurementClock,
    _config: TrxNoopConfig,
    plan: SessionPlan,
    sample_latency: bool,
    cancellation: Arc<RunCancellation>,
) -> Option<SessionOutcome> {
    let mut session = open_workload_session(engine, &cancellation)?;
    let result = run_trx_noop_operations(
        &mut session,
        plan.number,
        sample_latency.then_some(&clock),
        Some(&cancellation),
    )
    .await
    .map(|result| SessionOutcome {
        counters: WorkloadCounters {
            operations: result.operations,
            ..WorkloadCounters::default()
        },
        latency: result.latency,
        table_id: None,
        latest_write_fence: None,
    });
    finish_workload_session(session, result, &cancellation).await
}

async fn execute_insert_session(
    engine: &Engine,
    clock: MeasurementClock,
    execution: InsertSessionExecution,
    plan: SessionPlan,
    cancellation: Arc<RunCancellation>,
) -> Option<SessionOutcome> {
    let mut session = open_workload_session(engine, &cancellation)?;
    let result = run_insert_operations(
        &mut session,
        InsertOperationSpec {
            table_id: execution.table_id,
            random: execution.random,
            index: execution.config.index,
            seed: execution.config.seed,
            value_size: execution.config.value_size_bytes,
            batch_size: execution.config.batch_size,
        },
        &plan,
        execution.sample_latency.then_some(&clock),
        Some(&cancellation),
    )
    .await
    .map(|result| SessionOutcome {
        counters: WorkloadCounters {
            operations: result.operations,
            inserted_rows: result.inserted_rows,
            expected_outcomes: ExpectedOutcomeCounters {
                duplicate_key: result.duplicate_key,
                write_conflict: result.write_conflict,
            },
            ..WorkloadCounters::default()
        },
        latency: result.latency,
        table_id: None,
        latest_write_fence: result.latest_write_fence,
    });
    finish_workload_session(session, result, &cancellation).await
}

async fn execute_table_ddl_session(
    engine: &Engine,
    clock: MeasurementClock,
    _config: TableDdlConfig,
    plan: SessionPlan,
    sample_latency: bool,
    cancellation: Arc<RunCancellation>,
) -> Option<SessionOutcome> {
    let mut session = open_workload_session(engine, &cancellation)?;
    let result = run_table_ddl_operations(
        &mut session,
        plan.number,
        sample_latency.then_some(&clock),
        Some(&cancellation),
    )
    .await
    .map(|result| SessionOutcome {
        counters: WorkloadCounters {
            operations: result.operations,
            ..WorkloadCounters::default()
        },
        latency: result.latency,
        table_id: None,
        latest_write_fence: None,
    });
    finish_workload_session(session, result, &cancellation).await
}

fn open_workload_session(engine: &Engine, cancellation: &RunCancellation) -> Option<Session> {
    match engine.new_session() {
        Ok(session) => Some(session),
        Err(error) => {
            cancellation.fail(error.into());
            None
        }
    }
}

async fn finish_workload_session<T>(
    mut session: Session,
    result: Result<T>,
    cancellation: &RunCancellation,
) -> Option<T> {
    let value = match result {
        Ok(value) => Some(value),
        Err(error) => {
            cancellation.fail(error);
            None
        }
    };
    if let Err(error) = session.close().await {
        cancellation.fail(error.into());
    }
    value
}

fn drive_session_tasks(
    executor: &Executor<'_>,
    threads: usize,
    tasks: Vec<smol::Task<Option<SessionOutcome>>>,
    cancellation: Arc<RunCancellation>,
) -> Result<SessionOutcome> {
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

async fn collect_session_results(
    tasks: Vec<smol::Task<Option<SessionOutcome>>>,
    cancellation: Arc<RunCancellation>,
) -> Result<SessionOutcome> {
    let mut counters = WorkloadCounters::default();
    let mut latency = LatencyDistribution::new()?;
    let mut table_id = None;
    let mut latest_write_fence = None;
    for task in tasks {
        let Some(result) = task.await else {
            continue;
        };
        if let Err(error) = counters.merge(result.counters) {
            cancellation.fail(error);
        }
        if let Err(error) = latency.merge(&result.latency) {
            cancellation.fail(error);
        }
        if let Some(result_table_id) = result.table_id
            && table_id.replace(result_table_id).is_some()
        {
            cancellation.fail(BenchError::message(
                "multiple sessions returned a primary table ID",
            ));
        }
        if let Some(fence) = result.latest_write_fence {
            latest_write_fence =
                Some(latest_write_fence.map_or(fence, |current: TrxID| current.max(fence)));
        }
    }
    if let Some(error) = cancellation.take_error() {
        Err(error)
    } else {
        Ok(SessionOutcome {
            counters,
            latency,
            table_id,
            latest_write_fence,
        })
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
