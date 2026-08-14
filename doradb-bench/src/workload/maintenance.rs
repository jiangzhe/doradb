use crate::error::{BenchError, Result};
use crate::fixture::{
    FixturePlanEffect, FixtureRuntimeEffect, FrozenFixtureSummary, PrimaryBinding,
};
use crate::measurement::{
    LatencyDistribution, MeasurementClock, WorkloadCounters, WorkloadMetrics,
};
use crate::plan::{CheckpointTableConfig, FreezeTableConfig};
use crate::plan_executor::{
    SessionExecutor, SessionExecutorConfig, SessionMeasurement, SessionOutcome,
};
use crate::workload::util::{
    merge_measurement, operation_plans, require_primary, verify_samples, verify_simple_counters,
};
use crate::workload::{RunCancellation, SessionPlan};
use doradb_storage::id::TableID;
use doradb_storage::{CheckpointDelayReason, CheckpointOutcome, Engine, FreezeOutcome, Session};
use smol::future::or;
use std::future::Future;

/// Single-table frozen-prefix executor.
#[derive(Clone, Copy)]
pub(crate) struct FreezeTableExecutor {
    config: FreezeTableConfig,
    primary: PrimaryBinding,
}

impl SessionExecutor for FreezeTableExecutor {
    type Config = SessionExecutorConfig<FreezeTableConfig>;
    type Outcome = FreezeSessionOutcome;

    const IDENTITY: &'static str = "freeze-table";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        let max_rows = u64::try_from(config.resolved.max_rows)
            .map_err(|_| BenchError::message("freeze-table max_rows exceeds u64"))?;
        if max_rows == 0 || max_rows >= primary.inserted_rows || primary.frozen.is_some() {
            return Err(BenchError::message(
                "freeze-table runtime binding is not a proper unfrozen prefix candidate",
            ));
        }
        Ok(Self {
            config: config.resolved,
            primary,
        })
    }

    fn threads(&self) -> usize {
        1
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(1, 1)
    }

    fn execute<'a>(
        &'a self,
        _engine: &'a Engine,
        session: &'a mut Session,
        _plan: &'a SessionPlan,
        clock: &'a MeasurementClock,
        sample_latency: bool,
        _cancellation: &'a RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send + 'a {
        execute_freeze_session(session, self.primary, self.config, clock, sample_latency)
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
        verify_simple_counters(Self::IDENTITY, outcome.measurement.counters, 1)?;
        let FixturePlanEffect::Freeze { max_rows } = planned_effect else {
            return Err(BenchError::message(
                "freeze-table received an incompatible fixture effect",
            ));
        };
        let summary = outcome.summary.ok_or_else(|| {
            BenchError::message("freeze-table produced no verified frozen summary")
        })?;
        if *max_rows != self.config.max_rows || summary.max_rows != self.config.max_rows {
            return Err(BenchError::message(
                "freeze-table runtime effect differs from the resolved fixture effect",
            ));
        }
        Ok(FixtureRuntimeEffect::Freeze { summary })
    }
}

/// Single-table checkpoint-through-publication executor.
#[derive(Clone, Copy)]
pub(crate) struct CheckpointTableExecutor {
    primary: PrimaryBinding,
}

impl SessionExecutor for CheckpointTableExecutor {
    type Config = SessionExecutorConfig<CheckpointTableConfig>;
    type Outcome = CheckpointSessionOutcome;

    const IDENTITY: &'static str = "checkpoint-table";

    fn new(config: Self::Config) -> Result<Self> {
        let primary = require_primary(config.binding, Self::IDENTITY)?;
        if primary.frozen.is_none() {
            return Err(BenchError::message(
                "checkpoint-table runtime binding has no frozen batch",
            ));
        }
        Ok(Self { primary })
    }

    fn threads(&self) -> usize {
        1
    }

    fn session_plans(&self) -> Result<Vec<SessionPlan>> {
        operation_plans(1, 1)
    }

    fn execute<'a>(
        &'a self,
        _engine: &'a Engine,
        session: &'a mut Session,
        _plan: &'a SessionPlan,
        clock: &'a MeasurementClock,
        sample_latency: bool,
        cancellation: &'a RunCancellation,
    ) -> impl Future<Output = Result<Self::Outcome>> + Send + 'a {
        execute_checkpoint_session(
            session,
            self.primary.table_id,
            clock,
            sample_latency,
            cancellation,
        )
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
        verify_simple_counters(Self::IDENTITY, outcome.measurement.counters, 1)?;
        if !matches!(planned_effect, FixturePlanEffect::Checkpoint) {
            return Err(BenchError::message(
                "checkpoint-table received an incompatible fixture effect",
            ));
        }
        let metrics = outcome.metrics.ok_or_else(|| {
            BenchError::message("checkpoint-table produced no attempt/wait metrics")
        })?;
        verify_checkpoint_equation(metrics)?;
        Ok(FixtureRuntimeEffect::Checkpoint)
    }
}

/// Aggregated result from the single freeze-table session.
pub(crate) struct FreezeSessionOutcome {
    measurement: SessionMeasurement,
    summary: Option<FrozenFixtureSummary>,
}

impl SessionOutcome for FreezeSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: empty_measurement()?,
            summary: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(summary) = other.summary
            && self.summary.replace(summary).is_some()
        {
            return Err(BenchError::message(
                "multiple freeze-table sessions returned frozen summaries",
            ));
        }
        Ok(())
    }

    fn workload_metrics(&self) -> Option<WorkloadMetrics> {
        self.summary.map(|summary| WorkloadMetrics::FreezeTable {
            approximate_rows: summary.approximate_rows,
            page_count: summary.page_count,
            stable_page_count: summary.stable_page_count,
        })
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

/// Aggregated result from the single checkpoint-table session.
pub(crate) struct CheckpointSessionOutcome {
    measurement: SessionMeasurement,
    metrics: Option<CheckpointBreakdown>,
}

impl SessionOutcome for CheckpointSessionOutcome {
    fn empty() -> Result<Self> {
        Ok(Self {
            measurement: empty_measurement()?,
            metrics: None,
        })
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        merge_measurement(&mut self.measurement, other.measurement)?;
        if let Some(metrics) = other.metrics
            && self.metrics.replace(metrics).is_some()
        {
            return Err(BenchError::message(
                "multiple checkpoint-table sessions returned retry metrics",
            ));
        }
        Ok(())
    }

    fn workload_metrics(&self) -> Option<WorkloadMetrics> {
        self.metrics.map(CheckpointBreakdown::workload_metrics)
    }

    fn into_measurement(self) -> SessionMeasurement {
        self.measurement
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CheckpointBreakdown {
    attempt_count: u64,
    attempt_elapsed_nanos: u128,
    retry_wait_count: u64,
    retry_wait_elapsed_nanos: u128,
}

impl CheckpointBreakdown {
    fn workload_metrics(self) -> WorkloadMetrics {
        WorkloadMetrics::CheckpointTable {
            attempt_count: self.attempt_count,
            attempt_elapsed_nanos: self.attempt_elapsed_nanos,
            retry_wait_count: self.retry_wait_count,
            retry_wait_elapsed_nanos: self.retry_wait_elapsed_nanos,
        }
    }
}

struct CheckpointOperationResult {
    measurement: SessionMeasurement,
    metrics: CheckpointBreakdown,
}

trait CheckpointSession {
    fn attempt_checkpoint(
        &mut self,
        table_id: TableID,
    ) -> impl Future<Output = Result<CheckpointOutcome>> + Send;

    fn wait_for_retry(
        &mut self,
        reason: CheckpointDelayReason,
    ) -> impl Future<Output = Result<()>> + Send;
}

fn empty_measurement() -> Result<SessionMeasurement> {
    Ok(SessionMeasurement {
        counters: WorkloadCounters::default(),
        latency: LatencyDistribution::new()?,
    })
}

async fn execute_freeze_session(
    session: &mut Session,
    primary: PrimaryBinding,
    config: FreezeTableConfig,
    clock: &MeasurementClock,
    sample_latency: bool,
) -> Result<FreezeSessionOutcome> {
    let started = clock.raw();
    let freeze_result = session
        .freeze_table(primary.table_id, config.max_rows)
        .await;
    let stopped = clock.raw();
    let elapsed_result = clock.raw_delta_nanos(started, stopped);
    let outcome = freeze_result?;
    let elapsed_nanos = elapsed_result?;
    let summary = verify_frozen_outcome(primary, config.max_rows, outcome)?;
    let mut latency = LatencyDistribution::new()?;
    if sample_latency {
        latency.record(elapsed_nanos)?;
    }
    Ok(FreezeSessionOutcome {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: 1,
                ..WorkloadCounters::default()
            },
            latency,
        },
        summary: Some(summary),
    })
}

fn verify_frozen_outcome(
    primary: PrimaryBinding,
    max_rows: usize,
    outcome: FreezeOutcome,
) -> Result<FrozenFixtureSummary> {
    let batch = match outcome {
        FreezeOutcome::Frozen { batch } => batch,
        FreezeOutcome::AlreadyFrozen { batch } => {
            return Err(BenchError::message(format!(
                "freeze-table unexpectedly found an existing batch for table {} with {} pages",
                batch.table_id(),
                batch.page_count()
            )));
        }
        FreezeOutcome::Cancelled { reason } => {
            return Err(BenchError::message(format!(
                "freeze-table was unexpectedly cancelled: {reason:?}"
            )));
        }
    };
    let approximate_rows = u64::try_from(batch.approximate_rows())
        .map_err(|_| BenchError::message("frozen approximate row count exceeds u64"))?;
    let page_count = u64::try_from(batch.page_count())
        .map_err(|_| BenchError::message("frozen page count exceeds u64"))?;
    let stable_page_count = u64::try_from(batch.stable_page_count())
        .map_err(|_| BenchError::message("stable frozen page count exceeds u64"))?;
    if batch.table_id() != primary.table_id
        || batch.is_empty()
        || approximate_rows == 0
        || approximate_rows >= primary.inserted_rows
        || stable_page_count > page_count
    {
        return Err(BenchError::message(format!(
            "freeze-table did not install a nonempty proper prefix: expected_table={}, actual_table={}, inserted_rows={}, approximate_rows={}, page_count={}, stable_page_count={}",
            primary.table_id,
            batch.table_id(),
            primary.inserted_rows,
            approximate_rows,
            page_count,
            stable_page_count
        )));
    }
    Ok(FrozenFixtureSummary {
        max_rows,
        approximate_rows,
        page_count,
        stable_page_count,
    })
}

impl CheckpointSession for Session {
    async fn attempt_checkpoint(&mut self, table_id: TableID) -> Result<CheckpointOutcome> {
        self.checkpoint_table(table_id)
            .await
            .map_err(BenchError::from)
    }

    async fn wait_for_retry(&mut self, reason: CheckpointDelayReason) -> Result<()> {
        self.wait_for_checkpoint_retry(reason)
            .await
            .map_err(BenchError::from)
    }
}

async fn execute_checkpoint_session(
    session: &mut Session,
    table_id: TableID,
    clock: &MeasurementClock,
    sample_latency: bool,
    cancellation: &RunCancellation,
) -> Result<CheckpointSessionOutcome> {
    let result =
        run_checkpoint_operations(session, table_id, clock, sample_latency, cancellation).await?;
    Ok(CheckpointSessionOutcome {
        measurement: result.measurement,
        metrics: Some(result.metrics),
    })
}

async fn run_checkpoint_operations<S: CheckpointSession>(
    session: &mut S,
    table_id: TableID,
    clock: &MeasurementClock,
    sample_latency: bool,
    cancellation: &RunCancellation,
) -> Result<CheckpointOperationResult> {
    let total_started = clock.raw();
    let mut metrics = CheckpointBreakdown {
        attempt_count: 0,
        attempt_elapsed_nanos: 0,
        retry_wait_count: 0,
        retry_wait_elapsed_nanos: 0,
    };
    let total_stopped = loop {
        ensure_checkpoint_active(cancellation)?;
        let attempt_started = clock.raw();
        let attempt_result = session.attempt_checkpoint(table_id).await;
        let attempt_stopped = clock.raw();
        let outcome = attempt_result?;
        ensure_checkpoint_active(cancellation)?;
        let attempt_elapsed = clock.raw_delta_nanos(attempt_started, attempt_stopped)?;
        metrics.attempt_count = increment(metrics.attempt_count, "checkpoint attempt count")?;
        metrics.attempt_elapsed_nanos = accumulate_elapsed(
            metrics.attempt_elapsed_nanos,
            attempt_elapsed,
            "checkpoint attempt duration",
        )?;
        match outcome {
            CheckpointOutcome::Published { silent: false, .. } => break clock.raw(),
            CheckpointOutcome::Published { silent: true, .. } => {
                return Err(BenchError::message(
                    "checkpoint-table unexpectedly published only a silent watermark",
                ));
            }
            CheckpointOutcome::Delayed { reason } => {
                let wait_started = clock.raw();
                let wait_result = or(session.wait_for_retry(reason), async {
                    cancellation.wait_for_cancellation().await;
                    Err(checkpoint_cancelled_error())
                })
                .await;
                let wait_stopped = clock.raw();
                wait_result?;
                ensure_checkpoint_active(cancellation)?;
                let wait_elapsed = clock.raw_delta_nanos(wait_started, wait_stopped)?;
                metrics.retry_wait_count =
                    increment(metrics.retry_wait_count, "checkpoint retry wait count")?;
                metrics.retry_wait_elapsed_nanos = accumulate_elapsed(
                    metrics.retry_wait_elapsed_nanos,
                    wait_elapsed,
                    "checkpoint retry wait duration",
                )?;
            }
            CheckpointOutcome::Cancelled { reason } => {
                return Err(BenchError::message(format!(
                    "checkpoint-table was unexpectedly cancelled: {reason:?}"
                )));
            }
        }
    };
    verify_checkpoint_equation(metrics)?;
    let total_elapsed_nanos = clock.raw_delta_nanos(total_started, total_stopped)?;
    let mut latency = LatencyDistribution::new()?;
    if sample_latency {
        latency.record(total_elapsed_nanos)?;
    }
    Ok(CheckpointOperationResult {
        measurement: SessionMeasurement {
            counters: WorkloadCounters {
                operations: 1,
                ..WorkloadCounters::default()
            },
            latency,
        },
        metrics,
    })
}

fn ensure_checkpoint_active(cancellation: &RunCancellation) -> Result<()> {
    if cancellation.is_cancelled() {
        Err(checkpoint_cancelled_error())
    } else {
        Ok(())
    }
}

fn checkpoint_cancelled_error() -> BenchError {
    BenchError::message("checkpoint-table stopped after run cancellation")
}

fn verify_checkpoint_equation(metrics: CheckpointBreakdown) -> Result<()> {
    let expected_attempts = metrics
        .retry_wait_count
        .checked_add(1)
        .ok_or_else(|| BenchError::message("checkpoint attempt/wait equation overflow"))?;
    if metrics.attempt_count != expected_attempts {
        return Err(BenchError::message(format!(
            "checkpoint attempts ({}) do not equal retry waits ({}) plus one",
            metrics.attempt_count, metrics.retry_wait_count
        )));
    }
    Ok(())
}

fn increment(value: u64, label: &str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| BenchError::message(format!("{label} overflow")))
}

fn accumulate_elapsed(total: u128, elapsed: u64, label: &str) -> Result<u128> {
    total
        .checked_add(u128::from(elapsed))
        .ok_or_else(|| BenchError::message(format!("{label} overflow")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measurement::LatencyUnit;
    use doradb_storage::CheckpointCancelReason;
    use doradb_storage::id::TrxID;
    use event_listener::Event;
    use quanta::Mock;
    use std::collections::VecDeque;
    use std::future::{pending, ready};
    use std::sync::Arc;
    use std::time::Duration;

    struct MockCheckpointSession {
        outcomes: VecDeque<Result<CheckpointOutcome>>,
        waits: Vec<CheckpointDelayReason>,
        clock: Arc<Mock>,
        attempt_nanos: u64,
        wait_nanos: u64,
    }

    impl CheckpointSession for MockCheckpointSession {
        fn attempt_checkpoint(
            &mut self,
            _table_id: TableID,
        ) -> impl Future<Output = Result<CheckpointOutcome>> + Send {
            self.clock.increment(self.attempt_nanos);
            ready(
                self.outcomes
                    .pop_front()
                    .unwrap_or_else(|| Err(BenchError::message("missing mock outcome"))),
            )
        }

        fn wait_for_retry(
            &mut self,
            reason: CheckpointDelayReason,
        ) -> impl Future<Output = Result<()>> + Send {
            self.waits.push(reason);
            self.clock.increment(self.wait_nanos);
            ready(Ok(()))
        }
    }

    struct BlockingCheckpointSession {
        outcome: Option<Result<CheckpointOutcome>>,
        wait_started: Arc<Event>,
    }

    impl CheckpointSession for BlockingCheckpointSession {
        fn attempt_checkpoint(
            &mut self,
            _table_id: TableID,
        ) -> impl Future<Output = Result<CheckpointOutcome>> + Send {
            ready(
                self.outcome
                    .take()
                    .unwrap_or_else(|| Err(BenchError::message("missing mock outcome"))),
            )
        }

        fn wait_for_retry(
            &mut self,
            _reason: CheckpointDelayReason,
        ) -> impl Future<Output = Result<()>> + Send {
            self.wait_started.notify(usize::MAX);
            pending()
        }
    }

    fn published(silent: bool) -> CheckpointOutcome {
        CheckpointOutcome::Published {
            checkpoint_ts: TrxID::new(10),
            redo_cts: TrxID::new(11),
            silent,
        }
    }

    fn delay(table_id: TableID, effective_ts: u64) -> CheckpointOutcome {
        CheckpointOutcome::Delayed {
            reason: CheckpointDelayReason::ActiveRoot {
                table_id,
                effective_ts: TrxID::new(effective_ts),
                min_active_sts: TrxID::new(effective_ts - 1),
            },
        }
    }

    #[test]
    fn checkpoint_accounts_attempts_waits_and_one_total_sample() {
        let table_id = TableID::new(7);
        let (clock, mock) = MeasurementClock::mock();
        let cancellation = RunCancellation::new();
        let expected_reasons =
            [delay(table_id, 3), delay(table_id, 5)].map(|outcome| match outcome {
                CheckpointOutcome::Delayed { reason } => reason,
                _ => unreachable!(),
            });
        let mut session = MockCheckpointSession {
            outcomes: VecDeque::from([
                Ok(delay(table_id, 3)),
                Ok(delay(table_id, 5)),
                Ok(published(false)),
            ]),
            waits: Vec::new(),
            clock: mock,
            attempt_nanos: 10,
            wait_nanos: 20,
        };
        let result = smol::block_on(run_checkpoint_operations(
            &mut session,
            table_id,
            &clock,
            true,
            &cancellation,
        ))
        .unwrap();
        assert_eq!(session.waits, expected_reasons);
        assert_eq!(result.metrics.attempt_count, 3);
        assert_eq!(result.metrics.attempt_elapsed_nanos, 30);
        assert_eq!(result.metrics.retry_wait_count, 2);
        assert_eq!(result.metrics.retry_wait_elapsed_nanos, 40);
        let latency = result
            .measurement
            .latency
            .summary(LatencyUnit::TableCheckpoint)
            .unwrap();
        assert_eq!(latency.sample_count, 1);
        assert_eq!(latency.sum_nanos, 70);
    }

    #[test]
    fn checkpoint_prepare_retains_breakdown_without_a_latency_sample() {
        let table_id = TableID::new(7);
        let (clock, mock) = MeasurementClock::mock();
        let cancellation = RunCancellation::new();
        let mut session = MockCheckpointSession {
            outcomes: VecDeque::from([Ok(published(false))]),
            waits: Vec::new(),
            clock: mock,
            attempt_nanos: 10,
            wait_nanos: 20,
        };
        let result = smol::block_on(run_checkpoint_operations(
            &mut session,
            table_id,
            &clock,
            false,
            &cancellation,
        ))
        .unwrap();
        assert_eq!(result.metrics.attempt_count, 1);
        assert_eq!(result.metrics.retry_wait_count, 0);
        assert_eq!(result.measurement.latency.sample_count(), 0);
    }

    #[test]
    fn checkpoint_rejects_silent_publication_and_public_errors() {
        let table_id = TableID::new(7);
        for outcome in [
            Ok(published(true)),
            Ok(CheckpointOutcome::Cancelled {
                reason: CheckpointCancelReason::TableDropping,
            }),
            Err(BenchError::message("injected checkpoint error")),
        ] {
            let (clock, mock) = MeasurementClock::mock();
            let cancellation = RunCancellation::new();
            let mut session = MockCheckpointSession {
                outcomes: VecDeque::from([outcome]),
                waits: Vec::new(),
                clock: mock,
                attempt_nanos: 10,
                wait_nanos: 20,
            };
            assert!(
                smol::block_on(run_checkpoint_operations(
                    &mut session,
                    table_id,
                    &clock,
                    true,
                    &cancellation,
                ))
                .is_err()
            );
        }
    }

    #[test]
    fn checkpoint_does_not_start_after_run_cancellation() {
        let table_id = TableID::new(7);
        let (clock, mock) = MeasurementClock::mock();
        let cancellation = RunCancellation::new();
        cancellation.fail(BenchError::message("injected peer failure"));
        let mut session = MockCheckpointSession {
            outcomes: VecDeque::from([Ok(published(false))]),
            waits: Vec::new(),
            clock: mock,
            attempt_nanos: 10,
            wait_nanos: 20,
        };

        let result = smol::block_on(run_checkpoint_operations(
            &mut session,
            table_id,
            &clock,
            true,
            &cancellation,
        ));

        assert!(result.is_err());
        assert_eq!(session.outcomes.len(), 1);
        assert!(session.waits.is_empty());
    }

    #[test]
    fn checkpoint_retry_wait_stops_after_run_cancellation() {
        smol::block_on(async {
            let table_id = TableID::new(7);
            let (clock, _mock) = MeasurementClock::mock();
            let cancellation = Arc::new(RunCancellation::new());
            let wait_started = Arc::new(Event::new());
            let wait_listener = wait_started.listen();
            let task_cancellation = Arc::clone(&cancellation);
            let mut session = BlockingCheckpointSession {
                outcome: Some(Ok(delay(table_id, 3))),
                wait_started: Arc::clone(&wait_started),
            };
            let task = smol::spawn(async move {
                run_checkpoint_operations(&mut session, table_id, &clock, true, &task_cancellation)
                    .await
            });

            wait_listener.await;
            cancellation.fail(BenchError::message("injected peer failure"));
            let completed = or(async { Some(task.await) }, async {
                smol::Timer::after(Duration::from_secs(1)).await;
                None
            })
            .await;

            assert!(matches!(completed, Some(Err(_))));
        });
    }
}
