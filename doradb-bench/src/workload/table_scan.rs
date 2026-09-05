use crate::error::{BenchError, Result};
use crate::fixture::{
    FixtureBinding, FixturePlanEffect, FixtureRuntimeEffect, KeyRange, PrimaryBinding,
};
use crate::measurement::{
    LatencyDistribution, MeasurementClock, WorkloadCounters, WorkloadMetrics,
};
use crate::plan::{ParallelTableScanConfig, ReadConfig};
use crate::plan_executor::{
    RunTaskSpawner, SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    effective_batch_size, merge_measurement, operation_plans, require_primary, verify_no_effect,
    verify_no_write_counters, verify_samples,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{
    CallbackResult, Engine, ScanRowDecision, Session, TableScanOptions, TableScanPartitionStream,
};
use parking_lot::Mutex;
use smol::future::zip;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Existing transaction-scoped full table-scan session executor.
#[derive(Clone, Copy)]
pub(crate) struct TableScanExecutor {
    config: ReadConfig,
    primary: PrimaryBinding,
}

impl SessionExecutor for TableScanExecutor {
    type Config = SessionExecutorConfig<ReadConfig>;
    type Outcome = TableScanSessionOutcome;

    const IDENTITY: &'static str = "table-scan";

    fn new(config: Self::Config) -> Result<Self> {
        let primary =
            require_scan_primary(config.binding, Self::IDENTITY, config.resolved.loaded_range)?;
        Ok(Self {
            config: config.resolved,
            primary,
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
        let measurement = run_sequential_scans(
            session,
            self.config.batch_size,
            self.primary.table_id,
            plan.number,
            sample_latency.then_some(clock),
            cancellation,
        )
        .await?;
        Ok(TableScanSessionOutcome { measurement })
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
        verify_scan_counters(
            Self::IDENTITY,
            outcome.measurement.counters,
            self.config.num,
            None,
        )?;
        verify_no_effect(planned_effect)
    }
}

/// Shared-snapshot caller-scheduled parallel table-scan executor.
#[derive(Clone)]
pub(crate) struct ParallelTableScanExecutor<'run> {
    config: ParallelTableScanConfig,
    primary: PrimaryBinding,
    spawner: RunTaskSpawner<'run>,
}

impl<'run> SessionExecutor for ParallelTableScanExecutor<'run> {
    type Config = ParallelTableScanExecutorConfig<'run>;
    type Outcome = ParallelTableScanSessionOutcome;

    const IDENTITY: &'static str = "parallel-table-scan";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_scan_primary(
            config.common.binding,
            Self::IDENTITY,
            config.common.resolved.loaded_range,
        )?;
        Ok(Self {
            config: config.common.resolved,
            primary,
            spawner: config.spawner,
        })
    }

    fn threads(&self) -> usize {
        self.config.target_partitions
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(self.config.num, 1)
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
        let target_partitions = NonZeroUsize::new(self.config.target_partitions)
            .ok_or_else(|| BenchError::message("parallel table scan target must be positive"))?;
        let mut measurement = SessionMeasurement {
            counters: WorkloadCounters::default(),
            latency: LatencyDistribution::new()?,
        };
        let mut stable_actual = None;

        for _ in 0..plan.number {
            if cancellation.is_cancelled() {
                break;
            }
            let started = sample_latency.then(|| clock.raw());
            let (rows, actual_partitions) = run_parallel_scan(
                session,
                self.primary.table_id,
                target_partitions,
                &self.spawner,
            )
            .await?;
            if let Some(stable_actual) = stable_actual
                && stable_actual != actual_partitions
            {
                return Err(BenchError::message(format!(
                    "parallel-table-scan actual partition count changed from {} to {actual_partitions}",
                    stable_actual
                )));
            }
            stable_actual = Some(actual_partitions);
            add_scan_success(&mut measurement.counters, rows)?;
            record_latency(&mut measurement.latency, Some(clock), started)?;
        }

        Ok(ParallelTableScanSessionOutcome {
            measurement,
            partitions: stable_actual.map(|actual_partitions| ParallelScanPartitions {
                target_partitions: self.config.target_partitions,
                actual_partitions,
            }),
        })
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
        let expected_rows = self
            .config
            .num
            .checked_mul(self.primary.inserted_rows)
            .ok_or_else(|| {
                BenchError::message("parallel table scan expected row count overflow")
            })?;
        verify_scan_counters(
            Self::IDENTITY,
            outcome.measurement.counters,
            self.config.num,
            Some(expected_rows),
        )?;
        let partitions = outcome.partitions.ok_or_else(|| {
            BenchError::message("parallel-table-scan produced no partition metrics")
        })?;
        if partitions.target_partitions != self.config.target_partitions
            || partitions.actual_partitions == 0
        {
            return Err(BenchError::message(
                "parallel-table-scan produced invalid partition metrics",
            ));
        }
        verify_no_effect(planned_effect)
    }
}

/// Run-scoped configuration for the caller-scheduled parallel scan executor.
pub(crate) struct ParallelTableScanExecutorConfig<'run> {
    common: SessionExecutorConfig<ParallelTableScanConfig>,
    spawner: RunTaskSpawner<'run>,
}

impl<'run> ParallelTableScanExecutorConfig<'run> {
    /// Combines common session configuration with the current run's task spawner.
    pub(crate) fn new(
        common: SessionExecutorConfig<ParallelTableScanConfig>,
        spawner: RunTaskSpawner<'run>,
    ) -> Self {
        Self { common, spawner }
    }
}

/// Session-local result for the existing sequential scan identity.
pub(crate) struct TableScanSessionOutcome {
    measurement: SessionMeasurement,
}

impl SessionOutcome for TableScanSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: empty_measurement()?,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ParallelScanPartitions {
    target_partitions: usize,
    actual_partitions: usize,
}

/// Session-local result retaining stable parallel partition diagnostics.
pub(crate) struct ParallelTableScanSessionOutcome {
    measurement: SessionMeasurement,
    partitions: Option<ParallelScanPartitions>,
}

impl SessionOutcome for ParallelTableScanSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: empty_measurement()?,
            partitions: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        match (self.partitions, other.partitions) {
            (None, partitions) => self.partitions = partitions,
            (Some(left), Some(right)) if left == right => {}
            (Some(_), Some(_)) => {
                return Err(BenchError::message(
                    "parallel-table-scan partition metrics differ across sessions",
                ));
            }
            (Some(_), None) => {}
        }
        Ok(())
    }

    fn workload_metrics(&self) -> Option<WorkloadMetrics> {
        self.partitions
            .map(|partitions| WorkloadMetrics::ParallelTableScan {
                target_partitions: partitions.target_partitions,
                actual_partitions: partitions.actual_partitions,
            })
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

#[derive(Default)]
struct ScanFailure {
    first: Mutex<Option<BenchError>>,
}

impl ScanFailure {
    fn has_failed(&self) -> bool {
        self.first.lock().is_some()
    }

    fn fail(&self, error: BenchError) {
        let mut first = self.first.lock();
        if first.is_none() {
            *first = Some(error);
        }
    }

    fn take(&self) -> Option<BenchError> {
        self.first.lock().take()
    }
}

async fn run_sequential_scans(
    session: &mut Session,
    batch_size: u64,
    table_id: TableID,
    iterations: u64,
    clock: Option<&MeasurementClock>,
    cancellation: &RunCancellation,
) -> Result<SessionMeasurement> {
    let mut measurement = empty_measurement()?;
    let batch_size = effective_batch_size(batch_size, iterations)? as u64;
    let mut remaining = iterations;
    while remaining != 0 {
        if cancellation.is_cancelled() {
            break;
        }
        let count = remaining.min(batch_size);
        let started = clock.map(MeasurementClock::raw);
        let mut trx = session.begin_trx()?;
        let mut batch = WorkloadCounters::default();
        for _ in 0..count {
            let scan_result = async {
                let mut stream = trx
                    .table_scan_mvcc_stream(table_id, &[0, 1], |_| -> CallbackResult<_> {
                        Ok(ScanRowDecision::Include)
                    })
                    .await?;
                let mut rows = 0u64;
                while stream.next().await?.is_some() {
                    rows = rows
                        .checked_add(1)
                        .ok_or_else(|| BenchError::message("table scan row count overflow"))?;
                }
                Ok::<u64, BenchError>(rows)
            }
            .await;
            match scan_result {
                Ok(rows) => add_scan_success(&mut batch, rows)?,
                Err(error) => {
                    let _ = trx.rollback().await;
                    return Err(error);
                }
            }
        }
        trx.commit().await?;
        measurement.counters.merge(batch)?;
        record_latency(&mut measurement.latency, clock, started)?;
        remaining -= count;
    }
    Ok(measurement)
}

async fn run_parallel_scan(
    session: &mut Session,
    table_id: TableID,
    target_partitions: NonZeroUsize,
    spawner: &RunTaskSpawner<'_>,
) -> Result<(u64, usize)> {
    let snapshot = session
        .begin_read_snapshot()?
        .acquire_tables([table_id])
        .await?;
    let failure = Arc::new(ScanFailure::default());
    let mut tasks = Vec::new();
    let mut actual_partitions = None;

    match snapshot
        .prepare_table_scan(
            table_id,
            TableScanOptions {
                projection: vec![0, 1],
            },
        )
        .await
    {
        Ok(mut plan) => {
            match plan.repartition(target_partitions) {
                Ok(Some(repartitioned)) => plan = repartitioned,
                Ok(None) => {}
                Err(error) => failure.fail(error.into()),
            }
            if !failure.has_failed() {
                let partition_count = plan.partition_count();
                if partition_count == 0 {
                    failure.fail(BenchError::message(
                        "parallel table scan plan has no partitions",
                    ));
                } else {
                    actual_partitions = Some(partition_count);
                    tasks.reserve(partition_count);
                    for partition_idx in 0..partition_count {
                        match plan.open(partition_idx) {
                            Ok(stream) => {
                                let task_failure = Arc::clone(&failure);
                                tasks.push(spawner.spawn(async move {
                                    match drain_partition(stream).await {
                                        Ok(rows) => Some(rows),
                                        Err(error) => {
                                            task_failure.fail(error);
                                            None
                                        }
                                    }
                                }));
                            }
                            Err(error) => {
                                failure.fail(error.into());
                                break;
                            }
                        }
                    }
                }
            }
        }
        Err(error) => failure.fail(error.into()),
    }

    let collect_failure = Arc::clone(&failure);
    let close_failure = Arc::clone(&failure);
    let collect = async move { collect_partition_rows(tasks, &collect_failure).await };
    let close = async move {
        if let Err(error) = snapshot.close().await {
            close_failure.fail(error.into());
        }
    };
    let (rows, ()) = zip(collect, close).await;
    if let Some(error) = failure.take() {
        Err(error)
    } else {
        Ok((
            rows,
            actual_partitions.ok_or_else(|| {
                BenchError::message("parallel table scan has no actual partition count")
            })?,
        ))
    }
}

async fn drain_partition(mut stream: TableScanPartitionStream) -> Result<u64> {
    let mut rows = 0u64;
    while stream.next().await?.is_some() {
        rows = rows
            .checked_add(1)
            .ok_or_else(|| BenchError::message("parallel table scan partition row overflow"))?;
    }
    Ok(rows)
}

async fn collect_partition_rows(tasks: Vec<smol::Task<Option<u64>>>, failure: &ScanFailure) -> u64 {
    let mut rows = 0u64;
    for task in tasks {
        let Some(partition_rows) = task.await else {
            continue;
        };
        match rows.checked_add(partition_rows) {
            Some(total) => rows = total,
            None => failure.fail(BenchError::message(
                "parallel table scan operation row count overflow",
            )),
        }
    }
    rows
}

fn require_scan_primary(
    binding: FixtureBinding,
    identity: &str,
    loaded_range: KeyRange,
) -> Result<PrimaryBinding> {
    let primary = require_primary(binding, identity)?;
    if primary.loaded_range != Some(loaded_range) {
        return Err(BenchError::message(format!(
            "{identity} runtime loaded range differs from the resolved plan"
        )));
    }
    Ok(primary)
}

fn empty_measurement() -> Result<SessionMeasurement> {
    Ok(SessionMeasurement {
        counters: WorkloadCounters::default(),
        latency: LatencyDistribution::new()?,
    })
}

fn add_scan_success(counters: &mut WorkloadCounters, rows: u64) -> Result<()> {
    counters.operations = counters
        .operations
        .checked_add(1)
        .ok_or_else(|| BenchError::message("table scan operation counter overflow"))?;
    counters.rows_returned = counters
        .rows_returned
        .checked_add(rows)
        .ok_or_else(|| BenchError::message("table scan rows returned counter overflow"))?;
    Ok(())
}

fn verify_scan_counters(
    identity: &str,
    counters: WorkloadCounters,
    operations: u64,
    expected_rows: Option<u64>,
) -> Result<()> {
    verify_no_write_counters(identity, counters)?;
    if counters.operations != operations
        || counters.found != 0
        || counters.not_found != 0
        || expected_rows.is_some_and(|rows| counters.rows_returned != rows)
    {
        Err(BenchError::message(format!(
            "{identity} produced invalid scan counters"
        )))
    } else {
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_counter_equations_and_overflow_are_checked() {
        let counters = WorkloadCounters {
            operations: 2,
            rows_returned: 16,
            ..WorkloadCounters::default()
        };
        verify_scan_counters("parallel-table-scan", counters, 2, Some(16)).unwrap();

        let mut invalid = counters;
        invalid.found = 1;
        assert!(verify_scan_counters("parallel-table-scan", invalid, 2, Some(16)).is_err());
        assert!(verify_scan_counters("parallel-table-scan", counters, 2, Some(15)).is_err());

        let mut overflowing = WorkloadCounters {
            operations: u64::MAX,
            ..WorkloadCounters::default()
        };
        assert!(add_scan_success(&mut overflowing, 1).is_err());
        overflowing.operations = 0;
        overflowing.rows_returned = u64::MAX;
        assert!(add_scan_success(&mut overflowing, 1).is_err());
    }

    #[test]
    fn parallel_outcome_merge_requires_stable_partition_metrics() {
        let mut aggregate = ParallelTableScanSessionOutcome::empty().unwrap();
        aggregate
            .merge(ParallelTableScanSessionOutcome {
                measurement: empty_measurement().unwrap(),
                partitions: Some(ParallelScanPartitions {
                    target_partitions: 4,
                    actual_partitions: 3,
                }),
            })
            .unwrap();
        let error = aggregate
            .merge(ParallelTableScanSessionOutcome {
                measurement: empty_measurement().unwrap(),
                partitions: Some(ParallelScanPartitions {
                    target_partitions: 4,
                    actual_partitions: 2,
                }),
            })
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "parallel-table-scan partition metrics differ across sessions"
        );
    }
}
