use crate::error::{BenchError, Result};
use crate::fixture::{FixturePlanEffect, FixtureRuntimeEffect};
use crate::measurement::{LatencyDistribution, MeasurementClock, WorkloadCounters};
use crate::plan::CountConfig;
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    merge_measurement, operation_plans, require_no_binding, verify_no_effect, verify_samples,
    verify_simple_counters,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::{Engine, Session};

/// Statement-noop session executor.
#[derive(Clone, Copy)]
pub(crate) struct StmtNoopExecutor {
    config: CountConfig,
}

impl SessionExecutor for StmtNoopExecutor {
    type Config = SessionExecutorConfig<CountConfig>;
    type Outcome = NoopSessionOutcome;

    const IDENTITY: &'static str = "stmt-noop";

    fn new(config: Self::Config) -> Result<Self> {
        require_no_binding(config.binding, Self::IDENTITY)?;
        Ok(Self {
            config: config.resolved,
        })
    }

    fn threads(&self) -> usize {
        self.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.config.num, self.config.sessions)
    }

    async fn execute(
        &self,
        _engine: &Engine,
        session: &mut Session,
        plan: &SessionPlan,
        clock: &MeasurementClock,
        sample_latency: bool,
        cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        execute_stmt_noop_session(session, plan, sample_latency.then_some(clock), cancellation)
            .await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_noop(
            Self::IDENTITY,
            self.config.num,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Transaction-noop session executor.
#[derive(Clone, Copy)]
pub(crate) struct TrxNoopExecutor {
    config: CountConfig,
}

impl SessionExecutor for TrxNoopExecutor {
    type Config = SessionExecutorConfig<CountConfig>;
    type Outcome = NoopSessionOutcome;

    const IDENTITY: &'static str = "trx-noop";

    fn new(config: Self::Config) -> Result<Self> {
        require_no_binding(config.binding, Self::IDENTITY)?;
        Ok(Self {
            config: config.resolved,
        })
    }

    fn threads(&self) -> usize {
        self.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.config.num, self.config.sessions)
    }

    async fn execute(
        &self,
        _engine: &Engine,
        session: &mut Session,
        plan: &SessionPlan,
        clock: &MeasurementClock,
        sample_latency: bool,
        cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        execute_trx_noop_session(session, plan, sample_latency.then_some(clock), cancellation).await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_noop(
            Self::IDENTITY,
            self.config.num,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Session-local outcome shared by both no-op identities.
pub(crate) struct NoopSessionOutcome {
    measurement: SessionMeasurement,
}

impl SessionOutcome for NoopSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Result of one no-op operation loop.
struct NoopOperationResult {
    /// Number of completely settled logical operations.
    operations: u64,
    /// Optional exact operation latency samples.
    latency: LatencyDistribution,
}

async fn execute_stmt_noop_session(
    session: &mut Session,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<NoopSessionOutcome> {
    let result = run_stmt_noop_operations(session, plan.number, clock, Some(cancellation)).await?;
    Ok(noop_outcome(result))
}

async fn execute_trx_noop_session(
    session: &mut Session,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<NoopSessionOutcome> {
    let result = run_trx_noop_operations(session, plan.number, clock, Some(cancellation)).await?;
    Ok(noop_outcome(result))
}

fn noop_outcome(result: NoopOperationResult) -> NoopSessionOutcome {
    NoopSessionOutcome {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: result.operations,
                ..WorkloadCounters::default()
            },
            latency: result.latency,
        },
    }
}

fn verify_noop(
    identity: &str,
    operations: u64,
    planned_effect: &FixturePlanEffect,
    outcome: &NoopSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(identity, &outcome.measurement.latency, expected_samples)?;
    verify_simple_counters(identity, outcome.measurement.counters, operations)?;
    verify_no_effect(planned_effect)
}

/// Execute public transaction-noop operations.
async fn run_trx_noop_operations(
    session: &mut Session,
    number: u64,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<NoopOperationResult> {
    let mut latency = LatencyDistribution::new()?;
    let mut operations = 0u64;
    for _ in 0..number {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        session.begin_trx()?.commit().await?;
        if let (Some(clock), Some(started)) = (clock, started) {
            latency.record(clock.raw_delta_nanos(started, clock.raw())?)?;
        }
        operations = operations
            .checked_add(1)
            .ok_or_else(|| BenchError::message("no-op counter overflow"))?;
    }
    Ok(NoopOperationResult {
        operations,
        latency,
    })
}

/// Execute public statement-noop operations in one session transaction.
async fn run_stmt_noop_operations(
    session: &mut Session,
    number: u64,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<NoopOperationResult> {
    let mut latency = LatencyDistribution::new()?;
    if number == 0 || cancellation.is_some_and(RunCancellation::is_cancelled) {
        return Ok(NoopOperationResult {
            operations: 0,
            latency,
        });
    }
    let mut trx = session.begin_trx()?;
    let mut operations = 0u64;
    for _ in 0..number {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            let _ = trx.rollback().await;
            return Ok(NoopOperationResult {
                operations: 0,
                latency: LatencyDistribution::new()?,
            });
        }
        let started = clock.map(MeasurementClock::raw);
        if let Err(error) = trx.noop().await {
            let _ = trx.rollback().await;
            return Err(error.into());
        }
        let next_operations = (|| -> Result<u64> {
            if let (Some(clock), Some(started)) = (clock, started) {
                latency.record(clock.raw_delta_nanos(started, clock.raw())?)?;
            }
            operations
                .checked_add(1)
                .ok_or_else(|| BenchError::message("no-op counter overflow"))
        })();
        operations = match next_operations {
            Ok(operations) => operations,
            Err(error) => {
                let _ = trx.rollback().await;
                return Err(error);
            }
        };
    }
    trx.commit().await?;
    Ok(NoopOperationResult {
        operations,
        latency,
    })
}
