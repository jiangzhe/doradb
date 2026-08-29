use crate::error::{BenchError, Result};
use crate::fixture::{FixturePlanEffect, FixtureRuntimeEffect, KeyRange, PrimaryBinding};
use crate::measurement::{LatencyDistribution, MeasurementClock, WorkloadCounters};
use crate::plan::{IndexStreamConfig, ReadConfig};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    RandomScanRangeGenerator, effective_batch_size, generate_random_read_keys,
    generate_sequential_read_keys, merge_measurement, operation_plans, require_primary,
    verify_no_effect, verify_no_write_counters, verify_read_shape, verify_samples,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{Engine, IndexID, SelectMvcc, Session, Val};

/// Sequential lookup session executor.
#[derive(Clone, Copy)]
pub(crate) struct LookupSeqExecutor {
    state: ReadExecutorState,
}

impl SessionExecutor for LookupSeqExecutor {
    type Config = SessionExecutorConfig<ReadConfig>;
    type Outcome = ReadSessionOutcome;

    const IDENTITY: &'static str = "lookup-seq";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            state: build_read_state(config, Self::IDENTITY, ReadOperationType::LookupSeq)?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.state.config.num, self.state.config.sessions)
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
        execute_read_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
        .await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_read_outcome(
            Self::IDENTITY,
            &self.state,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Seeded-random lookup session executor.
#[derive(Clone, Copy)]
pub(crate) struct LookupRandExecutor {
    state: ReadExecutorState,
}

impl SessionExecutor for LookupRandExecutor {
    type Config = SessionExecutorConfig<ReadConfig>;
    type Outcome = ReadSessionOutcome;

    const IDENTITY: &'static str = "lookup-rand";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            state: build_read_state(config, Self::IDENTITY, ReadOperationType::LookupRand)?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.state.config.num, self.state.config.sessions)
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
        execute_read_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
        .await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_read_outcome(
            Self::IDENTITY,
            &self.state,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Materialized index-scan session executor.
#[derive(Clone, Copy)]
pub(crate) struct IndexScanExecutor {
    state: ReadExecutorState,
}

impl SessionExecutor for IndexScanExecutor {
    type Config = SessionExecutorConfig<ReadConfig>;
    type Outcome = ReadSessionOutcome;

    const IDENTITY: &'static str = "index-scan";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            state: build_read_state(config, Self::IDENTITY, ReadOperationType::IndexScan)?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.state.config.num, self.state.config.sessions)
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
        execute_read_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
        .await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_read_outcome(
            Self::IDENTITY,
            &self.state,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Public index-stream session executor.
#[derive(Clone, Copy)]
pub(crate) struct IndexStreamExecutor {
    state: ReadExecutorState,
}

impl SessionExecutor for IndexStreamExecutor {
    type Config = SessionExecutorConfig<IndexStreamConfig>;
    type Outcome = ReadSessionOutcome;

    const IDENTITY: &'static str = "index-stream";

    fn new(config: Self::Config) -> Result<Self> {
        let resolved = config.resolved;
        let read_config = ReadConfig {
            num: resolved.num,
            seed: resolved.seed,
            threads: resolved.threads,
            sessions: resolved.sessions,
            batch_size: 1,
            loaded_range: resolved.loaded_range,
            range: Some(resolved.range),
            include_stats: resolved.include_stats,
        };
        Ok(Self {
            state: build_read_state(
                SessionExecutorConfig {
                    resolved: read_config,
                    binding: config.binding,
                    execution_ordinal: config.execution_ordinal,
                },
                Self::IDENTITY,
                ReadOperationType::IndexStream,
            )?,
        })
    }

    fn threads(&self) -> usize {
        self.state.config.threads
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.state.config.num, self.state.config.sessions)
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
        execute_read_session(
            &self.state,
            session,
            plan,
            sample_latency.then_some(clock),
            cancellation,
        )
        .await
    }

    fn verify_outcome(
        &self,
        planned_effect: &FixturePlanEffect,
        outcome: &Self::Outcome,
        expected_samples: u64,
    ) -> Result<FixtureRuntimeEffect> {
        verify_read_outcome(
            Self::IDENTITY,
            &self.state,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

#[derive(Clone, Copy)]
struct ReadExecutorState {
    config: ReadConfig,
    primary: PrimaryBinding,
    operation: ReadOperationType,
}

/// Session-local outcome shared by all read identities.
pub(crate) struct ReadSessionOutcome {
    measurement: SessionMeasurement,
}

impl SessionOutcome for ReadSessionOutcome {
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

/// Closed read operation shape.
#[derive(Clone, Copy)]
enum ReadOperationType {
    /// Sequential unique-index lookups.
    LookupSeq,
    /// Seeded random unique-index lookups.
    LookupRand,
    /// Materialized secondary-index range scans.
    IndexScan,
    /// Public secondary-index range streams.
    IndexStream,
}

/// Bound storage and generation inputs for one read session.
#[derive(Clone, Copy)]
struct ReadOperationSpec {
    /// Read operation shape.
    operation: ReadOperationType,
    /// Bound primary table ID.
    table_id: TableID,
    /// Candidate loaded logical-key range.
    loaded_range: KeyRange,
    /// Deterministic request seed.
    seed: u64,
    /// Maximum operations per transaction.
    batch_size: u64,
    /// Logical-key width for index range reads.
    range: Option<u64>,
}

/// Result of one session's committed read operations.
struct ReadOperationResult {
    /// Successful read counters.
    counters: WorkloadCounters,
    /// Exact transaction-lifecycle latency samples.
    latency: LatencyDistribution,
}

fn build_read_state(
    config: SessionExecutorConfig<ReadConfig>,
    identity: &str,
    operation: ReadOperationType,
) -> Result<ReadExecutorState> {
    let primary = require_primary(config.binding, identity)?;
    if primary.loaded_range != Some(config.resolved.loaded_range) {
        return Err(BenchError::message(format!(
            "{identity} runtime loaded range differs from the resolved plan"
        )));
    }
    Ok(ReadExecutorState {
        config: config.resolved,
        primary,
        operation,
    })
}

async fn execute_read_session(
    state: &ReadExecutorState,
    session: &mut Session,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<ReadSessionOutcome> {
    let result = run_read_operations(
        session,
        ReadOperationSpec {
            operation: state.operation,
            table_id: state.primary.table_id,
            loaded_range: state.config.loaded_range,
            seed: state.config.seed,
            batch_size: state.config.batch_size,
            range: state.config.range,
        },
        plan,
        clock,
        Some(cancellation),
    )
    .await?;
    Ok(ReadSessionOutcome {
        measurement: SessionMeasurement {
            counters: result.counters,
            latency: result.latency,
        },
    })
}

fn verify_read_outcome(
    identity: &str,
    state: &ReadExecutorState,
    planned_effect: &FixturePlanEffect,
    outcome: &ReadSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(identity, &outcome.measurement.latency, expected_samples)?;
    let counters = outcome.measurement.counters;
    match state.operation {
        ReadOperationType::LookupSeq | ReadOperationType::LookupRand => {
            verify_no_write_counters(identity, counters)?;
            if counters.operations != state.config.num
                || counters.found.checked_add(counters.not_found) != Some(state.config.num)
                || counters.rows_returned != counters.found
            {
                return Err(BenchError::message(format!(
                    "{identity} counters violate the lookup equation"
                )));
            }
        }
        ReadOperationType::IndexStream => {
            verify_read_shape(identity, counters, state.config.num, false)?;
        }
        ReadOperationType::IndexScan => {
            verify_read_shape(identity, counters, state.config.num, true)?;
        }
    }
    verify_no_effect(planned_effect)
}

/// Execute one session's read operations with cooperative batch boundaries.
async fn run_read_operations(
    session: &mut Session,
    spec: ReadOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<ReadOperationResult> {
    match spec.operation {
        ReadOperationType::LookupSeq | ReadOperationType::LookupRand => {
            let keys = if matches!(spec.operation, ReadOperationType::LookupSeq) {
                generate_sequential_read_keys(spec.loaded_range, plan)?
            } else {
                generate_random_read_keys(spec.seed, spec.loaded_range, plan)?
            };
            lookup_keys(
                session,
                spec.batch_size,
                spec.table_id,
                &keys,
                clock,
                cancellation,
            )
            .await
        }
        ReadOperationType::IndexScan => index_scans(session, spec, plan, clock, cancellation).await,
        ReadOperationType::IndexStream => {
            index_streams(session, spec, plan, clock, cancellation).await
        }
    }
}

async fn lookup_keys(
    session: &mut Session,
    batch_size: u64,
    table_id: TableID,
    keys: &[u64],
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<ReadOperationResult> {
    let mut result = empty_result()?;
    if keys.is_empty() {
        return Ok(result);
    }
    let batch_size = effective_batch_size(batch_size, keys.len() as u64)?;
    for batch in keys.chunks(batch_size) {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let mut batch_counters = WorkloadCounters::default();
        for key in batch {
            let key_vals = [Val::from(*key)];
            let lookup = trx
                .table_lookup_unique_mvcc(table_id, IndexID::new(0), &key_vals, &[0, 1])
                .await;
            match lookup {
                Ok(SelectMvcc::Found(_)) => {
                    batch_counters.operations =
                        checked(batch_counters.operations, 1, "operations")?;
                    batch_counters.found = checked(batch_counters.found, 1, "found")?;
                    batch_counters.rows_returned =
                        checked(batch_counters.rows_returned, 1, "rows returned")?;
                }
                Ok(SelectMvcc::NotFound) => {
                    batch_counters.operations =
                        checked(batch_counters.operations, 1, "operations")?;
                    batch_counters.not_found = checked(batch_counters.not_found, 1, "not found")?;
                }
                Err(error) => {
                    let primary = BenchError::from(error);
                    let _ = trx.rollback().await;
                    return Err(primary);
                }
            }
        }
        trx.commit().await?;
        result.counters.merge(batch_counters)?;
        record_latency(&mut result.latency, clock, started)?;
    }
    Ok(result)
}

async fn index_scans(
    session: &mut Session,
    spec: ReadOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<ReadOperationResult> {
    let mut result = empty_result()?;
    if plan.number == 0 {
        return Ok(result);
    }
    let batch_size = effective_batch_size(spec.batch_size, plan.number)? as u64;
    let mut ranges = RandomScanRangeGenerator::new(
        spec.seed,
        spec.loaded_range,
        required_range(spec.range)?,
        plan,
    )?;
    let mut remaining = plan.number;
    while remaining != 0 {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let count = remaining.min(batch_size);
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let mut batch = WorkloadCounters::default();
        for _ in 0..count {
            let range = ranges.next_range()?;
            let lower = [Val::from(range.start)];
            let upper = [Val::from(range.end()?)];
            let scan = trx
                .table_index_scan_mvcc(
                    spec.table_id,
                    IndexID::new(0),
                    &lower[..]..&upper[..],
                    &[0, 1],
                )
                .await;
            match scan {
                Ok(scan) => {
                    let rows = u64::try_from(scan.unwrap_rows().len())
                        .map_err(|_| BenchError::message("scan row count exceeds u64"))?;
                    batch.operations = checked(batch.operations, 1, "operations")?;
                    batch.rows_returned = checked(batch.rows_returned, rows, "rows returned")?;
                    if rows == 0 {
                        batch.not_found = checked(batch.not_found, 1, "not found")?;
                    } else {
                        batch.found = checked(batch.found, 1, "found")?;
                    }
                }
                Err(error) => {
                    let primary = BenchError::from(error);
                    let _ = trx.rollback().await;
                    return Err(primary);
                }
            }
        }
        trx.commit().await?;
        result.counters.merge(batch)?;
        record_latency(&mut result.latency, clock, started)?;
        remaining -= count;
    }
    Ok(result)
}

async fn index_streams(
    session: &mut Session,
    spec: ReadOperationSpec,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<ReadOperationResult> {
    let mut result = empty_result()?;
    let mut ranges = RandomScanRangeGenerator::new(
        spec.seed,
        spec.loaded_range,
        required_range(spec.range)?,
        plan,
    )?;
    for _ in 0..plan.number {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let range = ranges.next_range()?;
        let lower = [Val::from(range.start)];
        let upper = [Val::from(range.end()?)];
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let scan_result = async {
            let mut stream = trx
                .table_index_scan_mvcc_stream(
                    spec.table_id,
                    IndexID::new(0),
                    &lower[..]..&upper[..],
                    &[0, 1],
                )
                .await?;
            let mut rows = 0u64;
            while stream.next().await?.is_some() {
                rows = rows
                    .checked_add(1)
                    .ok_or_else(|| BenchError::message("stream row count overflow"))?;
            }
            Ok::<u64, BenchError>(rows)
        }
        .await;
        let rows = match scan_result {
            Ok(rows) => rows,
            Err(error) => {
                let primary = error;
                let _ = trx.rollback().await;
                return Err(primary);
            }
        };
        trx.commit().await?;
        result.counters.operations = checked(result.counters.operations, 1, "operations")?;
        result.counters.rows_returned =
            checked(result.counters.rows_returned, rows, "rows returned")?;
        record_latency(&mut result.latency, clock, started)?;
    }
    Ok(result)
}

fn empty_result() -> Result<ReadOperationResult> {
    Ok(ReadOperationResult {
        counters: WorkloadCounters::default(),
        latency: LatencyDistribution::new()?,
    })
}

fn required_range(range: Option<u64>) -> Result<u64> {
    range.ok_or_else(|| BenchError::message("index read has no resolved range"))
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

fn checked(current: u64, addition: u64, label: &str) -> Result<u64> {
    current
        .checked_add(addition)
        .ok_or_else(|| BenchError::message(format!("read {label} counter overflow")))
}
