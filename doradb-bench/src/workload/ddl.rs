use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, PrimaryBinding, PrimaryTableShape,
    benchmark_index_specs, benchmark_non_unique_index_spec, benchmark_table_spec,
};
use crate::measurement::{LatencyDistribution, MeasurementClock, WorkloadCounters};
use crate::plan::{CreateTableConfig, DdlConfig};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    merge_measurement, operation_plans, require_no_binding, require_primary, verify_no_effect,
    verify_samples, verify_simple_counters,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{Engine, Session};
use std::sync::Arc;

/// Ordered table-pool creation executor.
#[derive(Clone, Copy)]
pub(crate) struct CreateTableExecutor {
    config: CreateTableConfig,
}

impl SessionExecutor for CreateTableExecutor {
    type Config = SessionExecutorConfig<CreateTableConfig>;
    type Outcome = CreateTableSessionOutcome;

    const IDENTITY: &'static str = "create-table";

    fn new(config: Self::Config) -> Result<Self> {
        require_no_binding(config.binding, Self::IDENTITY)?;
        Ok(Self {
            config: config.resolved,
        })
    }

    fn threads(&self) -> usize {
        1
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(
            u64::try_from(self.config.table_count)
                .map_err(|_| BenchError::message("table count exceeds u64"))?,
            1,
        )
    }

    async fn execute(
        &self,
        _engine: &Engine,
        session: &mut Session,
        _plan: &SessionPlan,
        clock: &MeasurementClock,
        sample_latency: bool,
        _cancellation: &RunCancellation,
    ) -> Result<Self::Outcome> {
        execute_create_table_session(self.config, session, sample_latency.then_some(clock)).await
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
            self.config.table_count as u64,
        )?;
        let (
            FixturePlanEffect::CreateTables {
                shape: planned_shape,
                table_count: planned_count,
            },
            Some(table_ids),
        ) = (planned_effect, outcome.table_ids.as_ref())
        else {
            return Err(BenchError::message(
                "create-table runtime effect differs from the resolved fixture effect",
            ));
        };
        if *planned_shape != self.config.shape
            || *planned_count != self.config.table_count
            || table_ids.len() != self.config.table_count
            || !table_ids
                .iter()
                .enumerate()
                .all(|(index, id)| !table_ids[..index].contains(id))
        {
            return Err(BenchError::message(
                "create-table runtime effect differs from the resolved fixture effect",
            ));
        }
        Ok(FixtureRuntimeEffect::CreateTables {
            shape: self.config.shape,
            table_ids: Arc::clone(table_ids),
        })
    }
}

/// Transient table-DDL executor.
#[derive(Clone, Copy)]
pub(crate) struct TableDdlExecutor {
    config: DdlConfig,
}

impl SessionExecutor for TableDdlExecutor {
    type Config = SessionExecutorConfig<DdlConfig>;
    type Outcome = DdlSessionOutcome;

    const IDENTITY: &'static str = "table-ddl";

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
        execute_ddl_session(
            session,
            None,
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
        verify_ddl_outcome(
            Self::IDENTITY,
            self.config,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Bound index-DDL executor.
#[derive(Clone, Copy)]
pub(crate) struct IndexDdlExecutor {
    config: DdlConfig,
    primary: PrimaryBinding,
}

impl SessionExecutor for IndexDdlExecutor {
    type Config = SessionExecutorConfig<DdlConfig>;
    type Outcome = DdlSessionOutcome;

    const IDENTITY: &'static str = "index-ddl";

    fn new(config: Self::Config) -> Result<Self> {
        Ok(Self {
            config: config.resolved,
            primary: require_primary(config.binding, Self::IDENTITY)?,
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
        execute_ddl_session(
            session,
            Some(self.primary.table_id),
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
        verify_ddl_outcome(
            Self::IDENTITY,
            self.config,
            planned_effect,
            outcome,
            expected_samples,
        )
    }
}

/// Session-local table-pool creation outcome.
pub(crate) struct CreateTableSessionOutcome {
    measurement: SessionMeasurement,
    table_ids: Option<Arc<[TableID]>>,
}

impl SessionOutcome for CreateTableSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
            table_ids: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(table_ids) = other.table_ids
            && self.table_ids.replace(table_ids).is_some()
        {
            return Err(BenchError::message(
                "multiple sessions returned created table IDs",
            ));
        }
        Ok(())
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Session-local outcome shared by table and index DDL.
pub(crate) struct DdlSessionOutcome {
    measurement: SessionMeasurement,
    cycles: u64,
}

impl SessionOutcome for DdlSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: SessionMeasurement {
                counters: WorkloadCounters::default(),
                latency: LatencyDistribution::new()?,
            },
            cycles: 0,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        self.cycles = self
            .cycles
            .checked_add(other.cycles)
            .ok_or_else(|| BenchError::message("DDL cycle counter overflow"))?;
        Ok(())
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Result of ordered homogeneous table creation.
struct CreateTableOperationResult {
    /// Public IDs returned in creation order.
    table_ids: Arc<[TableID]>,
    /// Exact per-create latency samples.
    latency: LatencyDistribution,
}

/// Result of one session's create/drop DDL cycles.
struct DdlOperationResult {
    /// Successful create and drop operations.
    operations: u64,
    /// Completely settled create/drop cycles.
    cycles: u64,
    /// Exact create-through-drop latency samples.
    latency: LatencyDistribution,
}

async fn execute_create_table_session(
    config: CreateTableConfig,
    session: &mut Session,
    clock: Option<&MeasurementClock>,
) -> Result<CreateTableSessionOutcome> {
    let result =
        run_create_table_operations(session, config.shape, config.table_count, clock).await?;
    Ok(CreateTableSessionOutcome {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: config.table_count as u64,
                ..WorkloadCounters::default()
            },
            latency: result.latency,
        },
        table_ids: Some(result.table_ids),
    })
}

async fn execute_ddl_session(
    session: &mut Session,
    table_id: Option<TableID>,
    plan: &SessionPlan,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<DdlSessionOutcome> {
    let result = if let Some(table_id) = table_id {
        run_index_ddl_operations(session, table_id, plan.number, clock, Some(cancellation)).await
    } else {
        run_table_ddl_operations(session, plan.number, clock, Some(cancellation)).await
    }?;
    Ok(DdlSessionOutcome {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: result.operations,
                ..WorkloadCounters::default()
            },
            latency: result.latency,
        },
        cycles: result.cycles,
    })
}

fn verify_ddl_outcome(
    identity: &str,
    config: DdlConfig,
    planned_effect: &FixturePlanEffect,
    outcome: &DdlSessionOutcome,
    expected_samples: u64,
) -> Result<FixtureRuntimeEffect> {
    verify_samples(identity, &outcome.measurement.latency, expected_samples)?;
    verify_simple_counters(identity, outcome.measurement.counters, config.operations)?;
    if outcome.cycles != config.num {
        return Err(BenchError::message(format!(
            "{identity} cycle count differs from the resolved plan"
        )));
    }
    verify_no_effect(planned_effect)
}

/// Create an ordered homogeneous table pool through public requests.
async fn run_create_table_operations(
    session: &mut Session,
    shape: PrimaryTableShape,
    table_count: usize,
    clock: Option<&MeasurementClock>,
) -> Result<CreateTableOperationResult> {
    let mut table_ids = Vec::with_capacity(table_count);
    let mut latency = LatencyDistribution::new()?;
    for _ in 0..table_count {
        let started = clock.map(MeasurementClock::raw);
        table_ids.push(
            session
                .create_table(benchmark_table_spec(), benchmark_index_specs(shape.index))
                .await?,
        );
        if let (Some(clock), Some(started)) = (clock, started) {
            latency.record(clock.raw_delta_nanos(started, clock.raw())?)?;
        }
    }
    Ok(CreateTableOperationResult {
        table_ids: table_ids.into(),
        latency,
    })
}

/// Execute transient table create/drop cycles.
async fn run_table_ddl_operations(
    session: &mut Session,
    cycles: u64,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<DdlOperationResult> {
    let mut result = empty_result()?;
    for _ in 0..cycles {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        let table_id = session
            .create_table(benchmark_table_spec(), Vec::new())
            .await?;
        result.operations = checked(result.operations, 1)?;
        session.drop_table(table_id).await?;
        complete_cycle(&mut result, clock, started)?;
    }
    Ok(result)
}

/// Execute non-unique index create/drop cycles against one bound primary.
async fn run_index_ddl_operations(
    session: &mut Session,
    table_id: TableID,
    cycles: u64,
    clock: Option<&MeasurementClock>,
    cancellation: Option<&RunCancellation>,
) -> Result<DdlOperationResult> {
    let mut result = empty_result()?;
    for _ in 0..cycles {
        if cancellation.is_some_and(RunCancellation::is_cancelled) {
            break;
        }
        let started = clock.map(MeasurementClock::raw);
        let index_no = session
            .create_index(table_id, benchmark_non_unique_index_spec())
            .await?;
        result.operations = checked(result.operations, 1)?;
        session.drop_index(table_id, index_no).await?;
        complete_cycle(&mut result, clock, started)?;
    }
    Ok(result)
}

fn empty_result() -> Result<DdlOperationResult> {
    Ok(DdlOperationResult {
        operations: 0,
        cycles: 0,
        latency: LatencyDistribution::new()?,
    })
}

fn complete_cycle(
    result: &mut DdlOperationResult,
    clock: Option<&MeasurementClock>,
    started: Option<u64>,
) -> Result<()> {
    result.operations = checked(result.operations, 1)?;
    result.cycles = checked(result.cycles, 1)?;
    if let (Some(clock), Some(started)) = (clock, started) {
        result
            .latency
            .record(clock.raw_delta_nanos(started, clock.raw())?)?;
    }
    Ok(())
}

fn checked(current: u64, addition: u64) -> Result<u64> {
    current
        .checked_add(addition)
        .ok_or_else(|| BenchError::message("DDL counter overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::IndexMode;
    use doradb_storage::IndexAttributes;

    #[test]
    fn schema_index_specs_match_index_mode_without_primary_key() {
        assert!(benchmark_index_specs(IndexMode::None).is_empty());
        let unique = benchmark_index_specs(IndexMode::Unique);
        assert!(unique[0].attributes.contains(IndexAttributes::UK));
        assert!(!unique[0].attributes.contains(IndexAttributes::PK));
        assert!(
            benchmark_index_specs(IndexMode::NonUnique)[0]
                .attributes
                .is_empty()
        );
    }
}
