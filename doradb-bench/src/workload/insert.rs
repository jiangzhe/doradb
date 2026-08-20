use crate::error::{BenchError, Result};
use crate::fixture::{FixturePlanEffect, FixtureRuntimeEffect, IndexMode, PrimaryBinding};
use crate::measurement::{
    ExpectedOutcomeCounters, LatencyDistribution, MeasurementClock, WorkloadCounters,
};
use crate::plan::InsertConfig;
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{effective_batch_size, generate_insert_keys, generate_payload};
use crate::workload::util::{merge_measurement, require_primary, verify_samples};
use crate::workload::{RunCancellation, SessionPlan, build_session_plans};
use doradb_storage::id::{TableID, TrxID};
use doradb_storage::{Engine, OperationError, Session, Val};
use std::future::Future;

/// Sequential-insert session executor.
#[derive(Clone, Copy)]
pub(crate) struct InsertSeqExecutor {
    state: InsertExecutorState,
}

impl SessionExecutor for InsertSeqExecutor {
    type Config = SessionExecutorConfig<InsertConfig>;
    type Outcome = InsertSessionOutcome;

    const IDENTITY: &'static str = "insert-seq";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            state: build_insert_state(config, Self::IDENTITY, false)?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        build_session_plans(
            self.state.config.attempted_range,
            self.state.config.sessions,
        )
    }

    fn execute<'a>(
        &'a self,
        _engine: &'a Engine,
        session: &'a mut Session,
        plan: &'a SessionPlan,
        clock: &'a MeasurementClock,
        sample_latency: bool,
        cancellation: &'a RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send + 'a {
        execute_insert_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_insert_outcome(
            Self::IDENTITY,
            &self.state,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Seeded-random-insert session executor.
#[derive(Clone, Copy)]
pub(crate) struct InsertRandExecutor {
    state: InsertExecutorState,
}

impl SessionExecutor for InsertRandExecutor {
    type Config = SessionExecutorConfig<InsertConfig>;
    type Outcome = InsertSessionOutcome;

    const IDENTITY: &'static str = "insert-rand";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            state: build_insert_state(config, Self::IDENTITY, true)?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        build_session_plans(
            self.state.config.attempted_range,
            self.state.config.sessions,
        )
    }

    fn execute<'a>(
        &'a self,
        _engine: &'a Engine,
        session: &'a mut Session,
        plan: &'a SessionPlan,
        clock: &'a MeasurementClock,
        sample_latency: bool,
        cancellation: &'a RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send + 'a {
        execute_insert_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_insert_outcome(
            Self::IDENTITY,
            &self.state,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

#[derive(Clone, Copy)]
struct InsertExecutorState {
    config: InsertConfig,
    primary: PrimaryBinding,
    random: bool,
}

/// Session-local outcome shared by both insert identities.
pub(crate) struct InsertSessionOutcome {
    measurement: SessionMeasurement,
    latest_write_fence: Option<TrxID>,
}

impl SessionOutcome for InsertSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
            latest_write_fence: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(fence) = other.latest_write_fence {
            self.latest_write_fence = Some(
                self.latest_write_fence
                    .map_or(fence, |current| current.max(fence)),
            );
        }
        Ok(())
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Result of one session's completely settled insert batches.
struct InsertOperationResult {
    /// Terminal logical insert attempts.
    operations: u64,
    /// Successful row insertions.
    inserted_rows: u64,
    /// Expected duplicate-key outcomes.
    duplicate_key: u64,
    /// Expected write-conflict outcomes.
    write_conflict: u64,
    /// Exact batch-transaction latency samples.
    latency: LatencyDistribution,
    /// Greatest write-bearing batch commit ID.
    latest_write_fence: Option<TrxID>,
}

/// Storage and generation inputs shared by one insert operation core.
#[derive(Clone, Copy)]
struct InsertOperationSpec {
    /// Runtime primary table target.
    table_id: TableID,
    /// Whether keys use the seeded random order.
    random: bool,
    /// Bound primary-table index shape.
    index: IndexMode,
    /// Deterministic payload and key seed.
    seed: u64,
    /// Generated payload bytes.
    value_size: usize,
    /// Maximum operations per transaction.
    batch_size: u64,
}

fn build_insert_state(
    config: SessionExecutorConfig<InsertConfig>,
    identity: &str,
    random: bool,
) -> Result<InsertExecutorState> {
    let primary = require_primary(config.binding, identity)?;
    let resolved = config.resolved;
    if primary.shape.index != resolved.index
        || primary
            .loaded_range
            .map_or(0, |range| range.end().unwrap_or(u64::MAX))
            != resolved.key_start
        || resolved.attempted_range.start != resolved.key_start
        || resolved.attempted_range.len != resolved.num
    {
        return Err(BenchError::message(
            "insert runtime binding differs from the resolved plan",
        ));
    }
    Ok(InsertExecutorState {
        config: resolved,
        primary,
        random,
    })
}

async fn execute_insert_session(
    state: &InsertExecutorState,
    session: &mut Session,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<InsertSessionOutcome> {
    let result = run_insert_operations(
        session,
        InsertOperationSpec {
            table_id: state.primary.table_id,
            random: state.random,
            index: state.config.index,
            seed: state.config.seed,
            value_size: state.config.value_size_bytes,
            batch_size: state.config.batch_size,
        },
        plan,
        clock,
        Some(cancellation),
    )
    .await?;
    Ok(InsertSessionOutcome {
        measurement: SessionMeasurement {
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
        },
        latest_write_fence: result.latest_write_fence,
    })
}

fn verify_insert_outcome(
    identity: &str,
    state: &InsertExecutorState,
    planned_effect: &FixturePlanEffect,
    outcome: &InsertSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(identity, &outcome.measurement.latency, expected_samples)?;
    let counters = outcome.measurement.counters;
    let terminal = counters
        .inserted_rows
        .checked_add(counters.expected_outcomes.duplicate_key)
        .and_then(|value| value.checked_add(counters.expected_outcomes.write_conflict))
        .ok_or_else(|| BenchError::message("insert terminal counter overflow"))?;
    if counters.operations != state.config.num
        || counters.operations != terminal
        || counters.found != 0
        || counters.not_found != 0
        || counters.rows_returned != 0
    {
        return Err(BenchError::message(format!(
            "{identity} counters violate the insert equation"
        )));
    }
    let FixturePlanEffect::Insert {
        attempted_range: planned,
    } = planned_effect
    else {
        return Err(BenchError::message(
            "insert runtime effect differs from the resolved fixture effect",
        ));
    };
    if *planned != state.config.attempted_range
        || (counters.inserted_rows == 0) != outcome.latest_write_fence.is_none()
    {
        return Err(BenchError::message(
            "insert runtime effect differs from the resolved fixture effect",
        ));
    }
    Ok(FixtureRuntimeEffect::Insert {
        attempted_range: *planned,
        inserted_rows: counters.inserted_rows,
        latest_write_fence: outcome.latest_write_fence,
    })
}

/// Execute generated inserts with expected terminal-outcome classification.
async fn run_insert_operations(
    session: &mut Session,
    spec: InsertOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<InsertOperationResult> {
    let keys = generate_insert_keys(spec.random, spec.index, spec.seed, plan)?;
    let mut result = InsertOperationResult {
        operations: 0,
        inserted_rows: 0,
        duplicate_key: 0,
        write_conflict: 0,
        latency: LatencyDistribution::new()?,
        latest_write_fence: None,
    };
    if keys.is_empty() {
        return Ok(result);
    }
    let batch_size = effective_batch_size(spec.batch_size, keys.len() as u64)?;
    for batch in keys.chunks(batch_size) {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let mut batch_inserted = 0u64;
        for key in batch {
            let payload = generate_payload(*key, spec.seed, spec.value_size);
            let row = vec![Val::from(*key), Val::from(&payload[..])];
            match trx.table_insert_mvcc(spec.table_id, row).await {
                Ok(_) => {
                    result.inserted_rows = checked(result.inserted_rows, 1, "inserted rows")?;
                    batch_inserted = checked(batch_inserted, 1, "batch inserted rows")?;
                }
                Err(error) => match error.operation_error() {
                    Some(OperationError::DuplicateKey) => {
                        result.duplicate_key = checked(result.duplicate_key, 1, "duplicate keys")?;
                    }
                    Some(OperationError::WriteConflict) => {
                        result.write_conflict =
                            checked(result.write_conflict, 1, "write conflicts")?;
                    }
                    _ => {
                        let primary = BenchError::from(error);
                        let _ = trx.rollback().await;
                        return Err(primary);
                    }
                },
            }
            result.operations = checked(result.operations, 1, "insert operations")?;
        }
        let fence = trx.commit().await?;
        if batch_inserted != 0 {
            result.latest_write_fence = Some(
                result
                    .latest_write_fence
                    .map_or(fence, |current| current.max(fence)),
            );
        }
        if let (Some(clock), Some(started)) = (clock, started) {
            result
                .latency
                .record(clock.raw_delta_nanos(started, clock.raw())?)?;
        }
    }
    Ok(result)
}

fn checked(current: u64, addition: u64, label: &str) -> Result<u64> {
    current
        .checked_add(addition)
        .ok_or_else(|| BenchError::message(format!("{label} counter overflow")))
}
