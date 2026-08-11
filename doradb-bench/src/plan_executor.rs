use crate::error::{BenchError, Result};
use crate::manifest::{KeyRange, write_plan_manifest_exclusive};
use crate::measurement::{
    BenchmarkAccumulator, InternalMetric, LatencyDistribution, LatencyUnit, MeasuredRunResult,
    MeasurementClock, SessionRunResult, WorkloadCounters, operations_per_second,
};
use crate::output::{capture_internal_stats, plan_internal_metrics};
use crate::plan::{
    FixtureEffect, FixtureRuntimeState, Phase, PhaseKind, Plan, ResolvedWorkload, TrxNoopConfig,
    load_plan,
};
use crate::plan_output::{
    FailureBoundary, InvocationFailure, InvocationReport, PreparePhaseResult, write_plan_outputs,
};
use crate::workload::{SessionPlan, build_session_plans, run_trx_noop_operations};
use doradb_storage::{Engine, Session};
use easy_parallel::Parallel;
use smol::{Executor, channel};
use std::fs;
use std::path::{Path, PathBuf};

struct RunOutcome {
    elapsed_nanos: u128,
    counters: WorkloadCounters,
    latency: LatencyDistribution,
    internal_metrics: Vec<InternalMetric>,
}

/// Parse and execute one plan against one new invocation-owned storage root.
pub async fn execute_plan(storage_root: PathBuf, plan_source: PathBuf) -> Result<()> {
    let loaded = load_plan(&plan_source, &storage_root)?;
    // Construct/calibrate after complete validation and before root creation.
    let clock = MeasurementClock::new();
    prepare_plan_root(&storage_root)?;
    if let Err(err) = write_plan_manifest_exclusive(
        &storage_root,
        &loaded.plan.source,
        loaded.plan.name.as_deref(),
    ) {
        let _ = fs::remove_dir_all(&storage_root);
        return Err(err);
    }

    let mut report = InvocationReport::new(storage_root.clone(), loaded.plan.clone());
    let engine = match Engine::bootstrap(loaded.engine_config).await {
        Ok(engine) => engine,
        Err(err) => {
            let operation_error = BenchError::from(err);
            report.fail(InvocationFailure {
                boundary: FailureBoundary::Bootstrap,
                phase_index: None,
                phase_kind: None,
                run_index: None,
                message: operation_error.to_string(),
            });
            return finish_with_outputs(&report, Err(operation_error));
        }
    };

    let operation_result = execute_phases(&engine, &clock, &loaded.plan, &mut report).await;
    if let Err(err) = &operation_result
        && report.failure.is_none()
    {
        report.fail(InvocationFailure {
            boundary: FailureBoundary::Measured,
            phase_index: Some(loaded.plan.phases.len()),
            phase_kind: Some(PhaseKind::Benchmark),
            run_index: None,
            message: err.to_string(),
        });
    }
    engine.shutdown();
    finish_with_outputs(&report, operation_result)
}

fn finish_with_outputs(report: &InvocationReport, operation_result: Result<()>) -> Result<()> {
    match (operation_result, write_plan_outputs(report)) {
        (Ok(()), Ok(())) => {
            println!(
                "completed benchmark plan={} storage_root={} measured_runs={}",
                report.plan_source.display(),
                report.root.display(),
                report.measured_runs.len()
            );
            Ok(())
        }
        (Err(operation), Ok(())) => Err(operation),
        (Ok(()), Err(artifact)) => Err(artifact),
        (Err(operation), Err(artifact)) => Err(BenchError::message(format!(
            "{artifact}; original execution error: {operation}"
        ))),
    }
}

async fn execute_phases(
    engine: &Engine,
    clock: &MeasurementClock,
    plan: &Plan,
    report: &mut InvocationReport,
) -> Result<()> {
    let mut fixture = FixtureRuntimeState;
    for (phase_offset, phase) in plan.phases.iter().enumerate() {
        let phase_index = phase_offset + 1;
        match phase {
            Phase::Prepare { workload } => {
                let result = run_once(engine, clock, workload, false).await;
                match result {
                    Ok(outcome) => {
                        report.prepare_phases.push(PreparePhaseResult {
                            phase_index,
                            workload: workload.identity().to_owned(),
                            elapsed_nanos: outcome.elapsed_nanos,
                            counters: outcome.counters,
                            internal_metrics: outcome.internal_metrics,
                        });
                        fixture.apply(FixtureEffect::None);
                    }
                    Err(err) => {
                        return fail_report(
                            report,
                            err,
                            FailureBoundary::Prepare,
                            phase_index,
                            PhaseKind::Prepare,
                            None,
                        );
                    }
                }
            }
            Phase::Benchmark {
                measurement,
                workload,
            } => {
                for run_index in 1..=measurement.warmup_runs {
                    if let Err(err) = run_once(engine, clock, workload, true).await {
                        return fail_report(
                            report,
                            err,
                            FailureBoundary::Warmup,
                            phase_index,
                            PhaseKind::Benchmark,
                            Some(run_index),
                        );
                    }
                }

                let mut aggregate = match BenchmarkAccumulator::new() {
                    Ok(aggregate) => aggregate,
                    Err(err) => {
                        return fail_report(
                            report,
                            err,
                            FailureBoundary::Measured,
                            phase_index,
                            PhaseKind::Benchmark,
                            None,
                        );
                    }
                };
                for run_index in 1..=measurement.measured_runs.get() {
                    let outcome = match run_once(engine, clock, workload, true).await {
                        Ok(outcome) => outcome,
                        Err(err) => {
                            return fail_report(
                                report,
                                err,
                                FailureBoundary::Measured,
                                phase_index,
                                PhaseKind::Benchmark,
                                Some(run_index),
                            );
                        }
                    };
                    if outcome.latency.sample_count() != outcome.counters.operations {
                        let err = BenchError::message(format!(
                            "{} latency sample count ({}) does not match operations ({})",
                            workload.identity(),
                            outcome.latency.sample_count(),
                            outcome.counters.operations
                        ));
                        return fail_report(
                            report,
                            err,
                            FailureBoundary::Measured,
                            phase_index,
                            PhaseKind::Benchmark,
                            Some(run_index),
                        );
                    }
                    let latency = match outcome.latency.summary(LatencyUnit::TransactionLifecycle) {
                        Ok(latency) => latency,
                        Err(err) => {
                            return fail_report(
                                report,
                                err,
                                FailureBoundary::Measured,
                                phase_index,
                                PhaseKind::Benchmark,
                                Some(run_index),
                            );
                        }
                    };
                    if let Err(err) =
                        aggregate.add_run(outcome.elapsed_nanos, outcome.counters, &outcome.latency)
                    {
                        return fail_report(
                            report,
                            err,
                            FailureBoundary::Measured,
                            phase_index,
                            PhaseKind::Benchmark,
                            Some(run_index),
                        );
                    }
                    report.measured_runs.push(MeasuredRunResult {
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
                }
                let aggregate = match aggregate.finish(LatencyUnit::TransactionLifecycle) {
                    Ok(aggregate) => aggregate,
                    Err(err) => {
                        return fail_report(
                            report,
                            err,
                            FailureBoundary::Measured,
                            phase_index,
                            PhaseKind::Benchmark,
                            None,
                        );
                    }
                };
                report.aggregate = Some(aggregate);
                fixture.apply(FixtureEffect::None);
            }
        }
    }
    Ok(())
}

fn fail_report(
    report: &mut InvocationReport,
    err: BenchError,
    boundary: FailureBoundary,
    phase_index: usize,
    phase_kind: PhaseKind,
    run_index: Option<u32>,
) -> Result<()> {
    report.fail(InvocationFailure {
        boundary,
        phase_index: Some(phase_index),
        phase_kind: Some(phase_kind),
        run_index,
        message: err.to_string(),
    });
    Err(err)
}

async fn run_once(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    sample_latency: bool,
) -> Result<RunOutcome> {
    let stats_state = if workload.include_stats() {
        let session = engine.new_session()?;
        match capture_internal_stats(&session) {
            Ok(before) => Some((session, before)),
            Err(err) => {
                return close_stats_session(session, Err(err)).await;
            }
        }
    } else {
        None
    };

    let started = clock.now();
    let run_result = run_session_workers(engine, clock, workload, sample_latency);
    let stopped = clock.now();
    let elapsed_nanos = clock.wall_delta_nanos(started, stopped);

    let (counters, latency) = match run_result {
        Ok(result) => result,
        Err(err) => {
            if let Some((mut session, _)) = stats_state {
                let _ = session.close().await;
            }
            return Err(err);
        }
    };

    let internal_metrics = if let Some((mut session, before)) = stats_state {
        let metrics_result =
            capture_internal_stats(&session).map(|after| plan_internal_metrics(&before, &after));
        let close_result = session.close().await.map_err(BenchError::from);
        match (metrics_result, close_result) {
            (Ok(metrics), Ok(())) => metrics,
            (Err(err), _) => return Err(err),
            (Ok(_), Err(err)) => return Err(err),
        }
    } else {
        Vec::new()
    };
    Ok(RunOutcome {
        elapsed_nanos: elapsed_nanos?,
        counters,
        latency,
        internal_metrics,
    })
}

async fn close_stats_session<T>(mut session: Session, result: Result<T>) -> Result<T> {
    let close_result = session.close().await.map_err(BenchError::from);
    match (result, close_result) {
        (Err(err), _) => Err(err),
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(err)) => Err(err),
    }
}

fn run_session_workers(
    engine: &Engine,
    clock: &MeasurementClock,
    workload: &ResolvedWorkload,
    sample_latency: bool,
) -> Result<(WorkloadCounters, LatencyDistribution)> {
    match workload {
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
                    ))
                })
                .collect();
            let (signal, shutdown) = channel::unbounded::<()>();
            let executor_ref = &executor;
            let shutdown_receiver = shutdown.clone();
            let (_workers, result) = Parallel::new()
                .each(0..config.threads, move |_| {
                    let _ = smol::block_on(executor_ref.run(shutdown_receiver.recv()));
                })
                .finish(move || {
                    let _signal = signal;
                    smol::block_on(collect_session_results(tasks))
                });
            result
        }
    }
}

async fn execute_trx_noop_session(
    engine: &Engine,
    clock: MeasurementClock,
    _config: TrxNoopConfig,
    plan: SessionPlan,
    sample_latency: bool,
) -> Result<SessionRunResult> {
    let mut session = engine.new_session()?;
    let run_result = async {
        let latency =
            run_trx_noop_operations(&mut session, plan.number, sample_latency.then_some(&clock))
                .await?;
        Ok(SessionRunResult {
            counters: WorkloadCounters {
                operations: plan.number,
                ..WorkloadCounters::default()
            },
            latency,
        })
    }
    .await;
    finish_workload_session(session, run_result).await
}

async fn finish_workload_session<T>(mut session: Session, result: Result<T>) -> Result<T> {
    let close_result = session.close().await.map_err(BenchError::from);
    match (result, close_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err),
    }
}

async fn collect_session_results(
    tasks: Vec<smol::Task<Result<SessionRunResult>>>,
) -> Result<(WorkloadCounters, LatencyDistribution)> {
    let mut counters = WorkloadCounters::default();
    let mut latency = LatencyDistribution::new()?;
    let mut first_error = None;
    for task in tasks {
        match task.await {
            Ok(result) => {
                let counter_error = counters.merge(result.counters).err();
                let latency_error = latency.merge(&result.latency).err();
                if first_error.is_none() {
                    first_error = counter_error.or(latency_error);
                }
            }
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }
    if let Some(err) = first_error {
        Err(err)
    } else {
        Ok((counters, latency))
    }
}

fn prepare_plan_root(storage_root: &Path) -> Result<()> {
    if storage_root.exists() {
        return Err(BenchError::message(format!(
            "--root {} must not exist for plan execution",
            storage_root.display()
        )));
    }
    fs::create_dir_all(storage_root).map_err(|err| {
        BenchError::message(format!(
            "failed to create storage root {}: {err}",
            storage_root.display()
        ))
    })
}
