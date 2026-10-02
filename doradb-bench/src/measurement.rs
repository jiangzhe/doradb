use crate::error::{BenchError, Result};
use crate::fixture::{CatalogCardinalities, IndexMode, KeyRange, PlacementKind, RowPlacement};
use crate::plan::{CatalogCheckpointCase, CatalogCheckpointProfile};
use doradb_storage::CatalogCheckpointResult;
pub use doradb_storage::profiling::{
    InternalMetric, InternalMetricKind, InternalMetricUnit, ProcessRssSampler,
    RecoveryHotIndexMeasurements, RecoveryMeasurements as RecoveryReport,
    RecoveryPhaseMeasurements as RecoveryPhaseTimings,
    RecoveryRedoMeasurements as RecoveryRedoMetrics, RecoveryWorkCounts, SampledProcessRss,
    process_cpu_nanos,
};
use hdrhistogram::Histogram;
use quanta::{Clock, Instant};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

const LOWEST_LATENCY_NANOS: u64 = 1;
const HIGHEST_LATENCY_NANOS: u64 = 3_600_000_000_000;
const LATENCY_SIGNIFICANT_DIGITS: u8 = 3;

/// One calibrated monotonic source shared by a complete plan invocation.
#[derive(Clone)]
pub struct MeasurementClock {
    clock: Arc<Clock>,
}

impl MeasurementClock {
    /// Calibrate and construct a production measurement clock.
    #[inline]
    pub fn new() -> Self {
        Self {
            clock: Arc::new(Clock::new()),
        }
    }

    /// Capture a scaled wall-clock boundary.
    #[inline]
    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// Capture a low-overhead raw timestamp.
    #[inline]
    pub fn raw(&self) -> u64 {
        self.clock.raw()
    }

    /// Validate timestamp order and convert a raw interval to nanoseconds.
    #[inline]
    pub fn raw_delta_nanos(&self, start: u64, end: u64) -> Result<u64> {
        if end < start {
            return Err(BenchError::message(format!(
                "measurement clock moved backwards: start={start}, end={end}"
            )));
        }
        Ok(self.clock.delta_as_nanos(start, end))
    }

    /// Convert a scaled wall interval to an exact nanosecond count.
    #[inline]
    pub fn wall_delta_nanos(&self, start: Instant, end: Instant) -> Result<u64> {
        let duration = end
            .checked_duration_since(start)
            .ok_or_else(|| BenchError::message("measurement wall clock moved backwards"))?;
        duration_nanos(duration)
    }

    /// Construct a deterministic clock and its controllable mock source.
    #[cfg(test)]
    pub(crate) fn mock() -> (Self, Arc<quanta::Mock>) {
        let (clock, mock) = Clock::mock();
        (
            Self {
                clock: Arc::new(clock),
            },
            mock,
        )
    }
}

impl Default for MeasurementClock {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Semantic unit represented by latency samples.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LatencyUnit {
    /// One complete public CREATE INDEX call through publication.
    IndexCreation,
    /// One complete successful public engine bootstrap.
    EngineRecovery,
    /// Public transaction begin-through-successful-commit lifecycle.
    TransactionLifecycle,
    /// One public statement execution inside an active transaction.
    StatementExecution,
    /// One public primary-table creation request.
    TableCreation,
    /// One complete public binding resolution through operation-claim release.
    TableBindingResolution,
    /// One insert batch transaction from begin through successful commit.
    InsertBatchTransaction,
    /// One index update range transaction from begin through successful commit.
    UpdateRangeTransaction,
    /// One full-table delete transaction from begin through successful commit.
    DeleteAllTransaction,
    /// One point-delete batch transaction from begin through successful commit.
    DeleteBatchTransaction,
    /// One transient table create-through-successful-drop cycle.
    TableCreateDropCycle,
    /// One lookup batch transaction from begin through successful commit.
    LookupBatchTransaction,
    /// One table-scan batch transaction from begin through successful commit.
    TableScanBatchTransaction,
    /// One shared-snapshot parallel scan from begin through drains and close.
    ParallelTableScanLifecycle,
    /// One materialized index-scan batch transaction.
    IndexScanBatchTransaction,
    /// One public index stream from begin through exhaustion and commit.
    IndexStreamTransaction,
    /// One index create-through-successful-drop cycle.
    IndexCreateDropCycle,
    /// One session-retained table-lock lifecycle including session close.
    TableLockSessionRetainedLifecycle,
    /// One transaction-retained table-lock lifecycle including commit.
    TableLockTransactionRetainedLifecycle,
    /// One paired or specialized table-lock lifecycle.
    TableLockOperationLifecycle,
    /// One public table-freeze request.
    TableFreeze,
    /// One public table-checkpoint retry lifecycle through publication.
    TableCheckpoint,
    /// One complete deterministic catalog population and pending public DDL setup.
    CatalogCheckpointPreparation,
    /// One public catalog checkpoint through durable publication.
    CatalogCheckpoint,
}

impl fmt::Display for LatencyUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::IndexCreation => "index-creation",
            Self::EngineRecovery => "engine-recovery",
            Self::TransactionLifecycle => "transaction-lifecycle",
            Self::StatementExecution => "statement-execution",
            Self::TableCreation => "table-creation",
            Self::TableBindingResolution => "table-binding-resolution",
            Self::InsertBatchTransaction => "insert-batch-transaction",
            Self::UpdateRangeTransaction => "update-range-transaction",
            Self::DeleteAllTransaction => "delete-all-transaction",
            Self::DeleteBatchTransaction => "delete-batch-transaction",
            Self::TableCreateDropCycle => "table-create-drop-cycle",
            Self::LookupBatchTransaction => "lookup-batch-transaction",
            Self::TableScanBatchTransaction => "table-scan-batch-transaction",
            Self::ParallelTableScanLifecycle => "parallel-table-scan-lifecycle",
            Self::IndexScanBatchTransaction => "index-scan-batch-transaction",
            Self::IndexStreamTransaction => "index-stream-transaction",
            Self::IndexCreateDropCycle => "index-create-drop-cycle",
            Self::TableLockSessionRetainedLifecycle => "table-lock-session-retained-lifecycle",
            Self::TableLockTransactionRetainedLifecycle => {
                "table-lock-transaction-retained-lifecycle"
            }
            Self::TableLockOperationLifecycle => "table-lock-operation-lifecycle",
            Self::TableFreeze => "table-freeze",
            Self::TableCheckpoint => "table-checkpoint",
            Self::CatalogCheckpointPreparation => "catalog-checkpoint-preparation",
            Self::CatalogCheckpoint => "catalog-checkpoint",
        })
    }
}

/// Strict workload-specific metrics retained beside generic counters and latency.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum WorkloadMetrics {
    /// One public CREATE call and its subsequent complete verification.
    CreateIndex {
        /// Exact placement, identity, process measurements, and verification.
        report: CreateIndexReport,
    },
    /// Complete startup attribution and verified recovered content.
    Recovery {
        /// Normalized immutable storage startup report.
        report: Box<RecoveryReport>,
        /// Full-table and optional index content verification.
        verification: RecoveryVerification,
    },
    /// Requested and realized shared-snapshot table-scan partition counts.
    ParallelTableScan {
        /// Best-effort partition target and executor thread count.
        target_partitions: usize,
        /// Stable positive physical partition count produced by planning.
        actual_partitions: usize,
    },
    /// Verified canonical frozen-page batch summary.
    FreezeTable {
        /// Approximate non-deleted rows selected by the frozen batch.
        approximate_rows: u64,
        /// Number of selected row pages.
        page_count: u64,
        /// Number of pages whose undo chains no longer need rescanning.
        stable_page_count: u64,
    },
    /// Public checkpoint-attempt and semantic retry-wait breakdown.
    CheckpointTable {
        /// Number of public checkpoint attempts through publication.
        attempt_count: u64,
        /// Time spent inside public checkpoint attempts.
        attempt_elapsed_nanos: u64,
        /// Number of public semantic retry waits.
        retry_wait_count: u64,
        /// Time spent inside public semantic retry waits.
        retry_wait_elapsed_nanos: u64,
    },
    /// Deterministic catalog state, process RSS, and public checkpoint report.
    CatalogCheckpoint {
        /// Fixed deterministic population profile.
        profile: CatalogCheckpointProfile,
        /// Public managed DDL effect included in the checkpoint.
        case: CatalogCheckpointCase,
        /// Equivalent baseline cardinalities before the pending DDL effect.
        before: CatalogCardinalities,
        /// Cardinalities after applying the pending DDL effect.
        final_state: CatalogCardinalities,
        /// Sampled process-RSS measurements around the checkpoint.
        sampled_process_rss: SampledProcessRss,
        /// Checkpoint-owned logical image and successful-write measurement.
        checkpoint: CatalogCheckpointResult,
    },
}

/// Content proof for a clean reopen in the same process with uncontrolled cache state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryVerification {
    /// Number of independently verified ordinary user tables.
    pub table_count: u64,
    /// Public table identity, present for the single-table fixture.
    pub table_id: Option<u64>,
    /// Prepared index shape, present for the single-table fixture.
    pub index: Option<IndexMode>,
    /// Prepared candidate key range, which may contain gaps or duplicates.
    pub candidate_range: Option<KeyRange>,
    /// Checked row count, also compared with successful preparation inserts.
    pub verified_rows: u64,
    /// Per-table sums of BLAKE3 row hashes modulo 2^256, separated by colons.
    pub fingerprint: String,
    /// Whether the complete unbounded index stream matched the table scan.
    pub index_verified: bool,
}

/// Complete CREATE measurements; verification is filled after the runner ends.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateIndexReport {
    /// Public primary-table identity.
    pub table_id: u64,
    /// Stable index identity returned by CREATE.
    pub index_id: u32,
    /// Installed secondary-index mode.
    pub index: IndexMode,
    /// Placement category derived from the exact counts.
    pub placement: PlacementKind,
    /// Successful committed fixture inserts.
    pub total_rows: u64,
    /// Exact hot and checkpointed row counts before CREATE.
    pub rows: RowPlacement,
    /// Exact public CREATE latency, in nanoseconds.
    pub create_elapsed_nanos: u64,
    /// Process CPU consumed across all threads, in nanoseconds.
    pub process_cpu_nanos: u64,
    /// Optional one-millisecond sampled process RSS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_process_rss: Option<SampledProcessRss>,
    /// Full content verification performed after all measurement windows end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification: Option<CreateIndexVerification>,
}

impl CreateIndexReport {
    /// Reject incomplete or inconsistent records before success publication.
    pub(crate) fn validate(&self) -> Result<()> {
        self.rows.validate(self.total_rows)?;
        let verification = self
            .verification
            .as_ref()
            .ok_or_else(|| BenchError::message("CREATE verification is incomplete"))?;
        if self.index == IndexMode::None
            || self.placement != self.rows.kind()
            || verification.table_rows != self.total_rows
            || verification.index_rows != self.total_rows
            || verification.fingerprint.len() != 64
            || !verification
                .fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(BenchError::message("invalid CREATE result or verification"));
        }
        Ok(())
    }
}

/// Observed complete table and index content agreement.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateIndexVerification {
    /// Rows drained from the full MVCC table scan.
    pub table_rows: u64,
    /// Rows drained from the unbounded stable-ID index scan.
    pub index_rows: u64,
    /// Matching order-independent, multiplicity-preserving row fingerprint.
    pub fingerprint: String,
}

/// Exact session-local latency samples and their HDR distribution.
#[derive(Clone, Debug)]
pub struct LatencyDistribution {
    histogram: Histogram<u64>,
    sample_count: u64,
    sum_nanos: u64,
}

impl LatencyDistribution {
    /// Construct the fixed one-nanosecond-through-one-hour histogram.
    #[inline]
    pub fn new() -> Result<Self> {
        let mut histogram = Histogram::new_with_bounds(
            LOWEST_LATENCY_NANOS,
            HIGHEST_LATENCY_NANOS,
            LATENCY_SIGNIFICANT_DIGITS,
        )
        .map_err(|err| {
            BenchError::message(format!("failed to construct latency histogram: {err}"))
        })?;
        histogram.auto(false);
        Ok(Self {
            histogram,
            sample_count: 0,
            sum_nanos: 0,
        })
    }

    /// Record one uncorrected closed-loop latency sample.
    #[inline]
    pub fn record(&mut self, nanos: u64) -> Result<()> {
        if nanos > HIGHEST_LATENCY_NANOS {
            return Err(BenchError::message(format!(
                "latency sample {nanos}ns exceeds the one-hour histogram limit"
            )));
        }
        let sample_count = self
            .sample_count
            .checked_add(1)
            .ok_or_else(|| BenchError::message("latency sample count overflow"))?;
        let sum_nanos = self
            .sum_nanos
            .checked_add(nanos)
            .ok_or_else(|| BenchError::message("latency duration sum overflow"))?;
        self.histogram.record(nanos).map_err(|err| {
            BenchError::message(format!(
                "latency sample {nanos}ns is outside the supported histogram range: {err}"
            ))
        })?;
        self.sample_count = sample_count;
        self.sum_nanos = sum_nanos;
        Ok(())
    }

    /// Merge another compatible distribution without averaging percentiles.
    #[inline]
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        let sample_count = self
            .sample_count
            .checked_add(other.sample_count)
            .ok_or_else(|| BenchError::message("latency sample count overflow"))?;
        let sum_nanos = self
            .sum_nanos
            .checked_add(other.sum_nanos)
            .ok_or_else(|| BenchError::message("latency duration sum overflow"))?;
        self.histogram.add(&other.histogram).map_err(|err| {
            BenchError::message(format!("failed to merge latency histograms: {err}"))
        })?;
        self.sample_count = sample_count;
        self.sum_nanos = sum_nanos;
        Ok(())
    }

    /// Return the exact sample count.
    #[inline]
    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Build a summary from this exact merged distribution.
    #[inline]
    pub fn summary(&self, unit: LatencyUnit) -> Result<LatencySummary> {
        if self.sample_count == 0 {
            return Err(BenchError::message(
                "cannot summarize an empty latency distribution",
            ));
        }
        Ok(LatencySummary {
            unit,
            sample_count: self.sample_count,
            sum_nanos: self.sum_nanos,
            average_nanos: self.sum_nanos as f64 / self.sample_count as f64,
            p95_nanos: self.histogram.value_at_quantile(0.95),
            p99_nanos: self.histogram.value_at_quantile(0.99),
        })
    }
}

/// Checked counters for allowlisted terminal operation outcomes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedOutcomeCounters {
    /// Insert attempts rejected because a unique key already exists.
    pub duplicate_key: u64,
    /// Insert attempts rejected by concurrent write ownership.
    pub write_conflict: u64,
}

/// Additive successful workload counters.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadCounters {
    /// Workload-defined logical operations used for throughput.
    pub operations: u64,
    /// Rows inserted by successful operations.
    pub inserted_rows: u64,
    /// Rows updated by successful range-mutation operations.
    pub updated_rows: u64,
    /// Rows deleted by successfully committed mutations.
    pub deleted_rows: u64,
    /// Successful point requests that found at least one row.
    pub found: u64,
    /// Successful point requests that found no row.
    pub not_found: u64,
    /// Rows returned by successful scans or streams.
    pub rows_returned: u64,
    /// Expected operation outcomes owned by the workload.
    pub expected_outcomes: ExpectedOutcomeCounters,
}

impl WorkloadCounters {
    /// Checked additive merge.
    #[inline]
    pub fn merge(&mut self, other: Self) -> Result<()> {
        self.operations = checked_counter(self.operations, other.operations, "operations")?;
        self.inserted_rows =
            checked_counter(self.inserted_rows, other.inserted_rows, "inserted_rows")?;
        self.updated_rows = checked_counter(self.updated_rows, other.updated_rows, "updated_rows")?;
        self.deleted_rows = checked_counter(self.deleted_rows, other.deleted_rows, "deleted_rows")?;
        self.found = checked_counter(self.found, other.found, "found")?;
        self.not_found = checked_counter(self.not_found, other.not_found, "not_found")?;
        self.rows_returned =
            checked_counter(self.rows_returned, other.rows_returned, "rows_returned")?;
        self.expected_outcomes.duplicate_key = checked_counter(
            self.expected_outcomes.duplicate_key,
            other.expected_outcomes.duplicate_key,
            "expected_outcomes.duplicate_key",
        )?;
        self.expected_outcomes.write_conflict = checked_counter(
            self.expected_outcomes.write_conflict,
            other.expected_outcomes.write_conflict,
            "expected_outcomes.write_conflict",
        )?;
        Ok(())
    }
}

/// One joined public session's plan-mode result.
#[derive(Clone, Debug)]
pub struct SessionRunResult {
    /// Successful logical counters from this session.
    pub counters: WorkloadCounters,
    /// Exact latency samples recorded by this session.
    pub latency: LatencyDistribution,
}

/// Latency summary calculated from an exact merged distribution.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    /// Semantic operation represented by every sample.
    pub unit: LatencyUnit,
    /// Exact number of merged latency samples.
    pub sample_count: u64,
    /// Exact sum of all latency samples in nanoseconds.
    pub sum_nanos: u64,
    /// Arithmetic mean latency in nanoseconds.
    pub average_nanos: f64,
    /// Direct merged-distribution 95th percentile in nanoseconds.
    pub p95_nanos: u64,
    /// Direct merged-distribution 99th percentile in nanoseconds.
    pub p99_nanos: u64,
}

/// One complete measured benchmark repetition.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredRunResult {
    /// One-based measured repetition index.
    pub run_index: u32,
    /// Full session/worker wall envelope in nanoseconds.
    pub elapsed_nanos: u64,
    /// Successful logical workload counters.
    pub counters: WorkloadCounters,
    /// Successful operations divided by wall time.
    pub operations_per_second: f64,
    /// Latency summary for this measured repetition.
    pub latency: LatencySummary,
    /// Optional workload-specific metrics for this repetition.
    pub workload_metrics: Option<WorkloadMetrics>,
    /// Optional typed engine diagnostics captured around the run.
    pub internal_metrics: Vec<InternalMetric>,
}

/// Aggregate of all equivalent successful measured runs.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkAggregate {
    /// Number of merged measured repetitions.
    pub measured_runs: u32,
    /// Sum of measured wall envelopes in nanoseconds.
    pub elapsed_nanos: u64,
    /// Sum of successful logical workload counters.
    pub counters: WorkloadCounters,
    /// Total operations divided by total wall time.
    pub operations_per_second: f64,
    /// Summary of the directly merged latency distribution.
    pub latency: LatencySummary,
}

/// Accumulator retaining exact histograms while public results stay serializable.
pub struct BenchmarkAccumulator {
    measured_runs: u32,
    elapsed_nanos: u64,
    counters: WorkloadCounters,
    latency: LatencyDistribution,
}

impl BenchmarkAccumulator {
    /// Construct an empty benchmark accumulator.
    #[inline]
    pub fn new() -> Result<Self> {
        Ok(Self {
            measured_runs: 0,
            elapsed_nanos: 0,
            counters: WorkloadCounters::default(),
            latency: LatencyDistribution::new()?,
        })
    }

    /// Merge one complete measured run's exact envelope and distribution.
    #[inline]
    pub fn add_run(
        &mut self,
        elapsed_nanos: u64,
        counters: WorkloadCounters,
        latency: &LatencyDistribution,
    ) -> Result<()> {
        self.measured_runs = self
            .measured_runs
            .checked_add(1)
            .ok_or_else(|| BenchError::message("measured run count overflow"))?;
        self.elapsed_nanos = self
            .elapsed_nanos
            .checked_add(elapsed_nanos)
            .ok_or_else(|| BenchError::message("measured wall duration overflow"))?;
        self.counters.merge(counters)?;
        self.latency.merge(latency)
    }

    /// Finish the aggregate using total operations divided by total wall time.
    #[inline]
    pub fn finish(self, unit: LatencyUnit) -> Result<BenchmarkAggregate> {
        Ok(BenchmarkAggregate {
            measured_runs: self.measured_runs,
            elapsed_nanos: self.elapsed_nanos,
            counters: self.counters,
            operations_per_second: operations_per_second(
                self.counters.operations,
                self.elapsed_nanos,
            ),
            latency: self.latency.summary(unit)?,
        })
    }
}

/// Calculate throughput from one operation total and exact wall duration.
pub fn operations_per_second(operations: u64, elapsed_nanos: u64) -> f64 {
    if elapsed_nanos == 0 {
        0.0
    } else {
        operations as f64 * 1_000_000_000.0 / elapsed_nanos as f64
    }
}

/// Convert a duration to exact nanoseconds, rejecting values outside the metric range.
fn duration_nanos(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| BenchError::message("measurement duration exceeds u64 nanoseconds"))
}

fn checked_counter(left: u64, right: u64, name: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| BenchError::message(format!("workload counter overflow: {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use doradb_storage::id::{TableID, TrxID};
    use doradb_storage::{
        CatalogCheckpointOutcome, CatalogCheckpointReport, CatalogTableCheckpointChange,
        CatalogTableCheckpointIoStats,
    };

    /// Purpose: Enforce monotonic ordering for raw measurement timestamps.
    /// Expected: Equal timestamps represent no elapsed time and reversed timestamps are
    /// rejected.
    #[test]
    fn raw_timestamp_order_is_checked() {
        let (clock, _mock) = Clock::mock();
        let clock = MeasurementClock {
            clock: Arc::new(clock),
        };
        assert_eq!(clock.raw_delta_nanos(12, 12).unwrap(), 0);
        assert!(clock.raw_delta_nanos(13, 12).is_err());
    }

    /// Purpose: Preserve duration precision at representable boundaries.
    /// Expected: Valid durations convert exactly and out-of-range durations are rejected.
    #[test]
    fn duration_nanoseconds_conversion_checks_bounds() {
        for nanos in [0, 1, u64::MAX] {
            assert_eq!(duration_nanos(Duration::from_nanos(nanos)).unwrap(), nanos);
        }
        let too_large = Duration::from_nanos(u64::MAX) + Duration::from_nanos(1);
        assert!(duration_nanos(too_large).is_err());
    }

    /// Purpose: Preserve elapsed wall time while enforcing timestamp order.
    /// Expected: Forward intervals retain their duration, equal timestamps imply no elapsed
    /// time, and reversal fails.
    #[test]
    fn wall_timestamp_order_is_checked() {
        let (clock, mock) = MeasurementClock::mock();
        let start = clock.now();
        mock.increment(17);
        let end = clock.now();
        assert_eq!(clock.wall_delta_nanos(start, start).unwrap(), 0);
        assert_eq!(clock.wall_delta_nanos(start, end).unwrap(), 17);
        assert!(clock.wall_delta_nanos(end, start).is_err());
    }

    /// Purpose: Derive latency statistics from the combined sample distribution.
    /// Expected: Merged summaries preserve sample accounting and calculate percentiles from
    /// pooled observations.
    #[test]
    fn merged_distribution_calculates_direct_percentiles() {
        let mut left = LatencyDistribution::new().unwrap();
        let mut right = LatencyDistribution::new().unwrap();
        for value in [10, 20] {
            left.record(value).unwrap();
        }
        for value in [30, 40] {
            right.record(value).unwrap();
        }
        left.merge(&right).unwrap();
        let summary = left.summary(LatencyUnit::TransactionLifecycle).unwrap();
        assert_eq!(summary.sample_count, 4);
        assert_eq!(summary.sum_nanos, 100);
        assert_eq!(summary.average_nanos, 25.0);
        assert!(summary.p95_nanos >= 40);
    }

    /// Purpose: Enforce the supported latency histogram range.
    /// Expected: Samples above the supported duration limit are rejected.
    #[test]
    fn histogram_rejects_values_over_one_hour() {
        let mut distribution = LatencyDistribution::new().unwrap();
        assert!(distribution.record(HIGHEST_LATENCY_NANOS + 1).is_err());
    }

    /// Purpose: Keep latency accumulation atomic when recording or merging would overflow.
    /// Expected: Failed updates preserve the existing samples and summary.
    #[test]
    fn latency_sum_overflow_rejects_record_and_merge() {
        let mut full = LatencyDistribution::new().unwrap();
        full.sum_nanos = u64::MAX - 1;
        full.record(1).unwrap();
        let before = full.summary(LatencyUnit::TransactionLifecycle).unwrap();
        assert!(full.record(1).is_err());
        let mut other = LatencyDistribution::new().unwrap();
        other.record(1).unwrap();
        assert!(full.merge(&other).is_err());
        assert_eq!(
            full.summary(LatencyUnit::TransactionLifecycle).unwrap(),
            before
        );
        assert_eq!(full.histogram.len(), 1);
    }

    /// Purpose: Preserve updated-row accounting when combining workload results.
    /// Expected: Valid contributions accumulate exactly and overflow is rejected.
    #[test]
    fn updated_row_counter_merge_is_checked() {
        let mut counters = WorkloadCounters {
            updated_rows: 2,
            ..WorkloadCounters::default()
        };
        counters
            .merge(WorkloadCounters {
                updated_rows: 3,
                ..WorkloadCounters::default()
            })
            .unwrap();
        assert_eq!(counters.updated_rows, 5);
        counters.updated_rows = u64::MAX;
        assert!(
            counters
                .merge(WorkloadCounters {
                    updated_rows: 1,
                    ..WorkloadCounters::default()
                })
                .is_err()
        );
    }

    /// Purpose: Preserve deleted-row accounting when combining workload results.
    /// Expected: Valid contributions accumulate exactly and overflow is rejected.
    #[test]
    fn deleted_row_counter_merge_is_checked() {
        let mut counters = WorkloadCounters {
            deleted_rows: 2,
            ..WorkloadCounters::default()
        };
        counters
            .merge(WorkloadCounters {
                deleted_rows: 3,
                ..WorkloadCounters::default()
            })
            .unwrap();
        assert_eq!(counters.deleted_rows, 5);
        counters.deleted_rows = u64::MAX;
        assert!(
            counters
                .merge(WorkloadCounters {
                    deleted_rows: 1,
                    ..WorkloadCounters::default()
                })
                .is_err()
        );
    }

    /// Purpose: Weight aggregate throughput by the full measured wall duration.
    /// Expected: Throughput reflects total completed work divided by total elapsed time.
    #[test]
    fn aggregate_uses_total_wall_duration() {
        let mut latency = LatencyDistribution::new().unwrap();
        latency.record(10).unwrap();
        let mut aggregate = BenchmarkAccumulator::new().unwrap();
        aggregate
            .add_run(
                1_000_000_000,
                WorkloadCounters {
                    operations: 1,
                    ..WorkloadCounters::default()
                },
                &latency,
            )
            .unwrap();
        aggregate
            .add_run(
                3_000_000_000,
                WorkloadCounters {
                    operations: 1,
                    ..WorkloadCounters::default()
                },
                &latency,
            )
            .unwrap();
        let result = aggregate.finish(LatencyUnit::TransactionLifecycle).unwrap();
        assert_eq!(result.elapsed_nanos, 4_000_000_000);
        assert_eq!(result.operations_per_second, 0.5);
    }

    /// Purpose: Prevent accumulated measurement time from exceeding its representation.
    /// Expected: Wall-duration overflow is rejected with a diagnostic identifying the affected
    /// metric.
    #[test]
    fn aggregate_wall_duration_overflow_is_checked() {
        let mut latency = LatencyDistribution::new().unwrap();
        latency.record(1).unwrap();
        let mut aggregate = BenchmarkAccumulator::new().unwrap();
        aggregate
            .add_run(u64::MAX, WorkloadCounters::default(), &latency)
            .unwrap();
        let error = aggregate
            .add_run(1, WorkloadCounters::default(), &latency)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("measured wall duration overflow")
        );
    }

    /// Purpose: Preserve unsigned metric values across serialization boundaries.
    /// Expected: Representable values remain numeric and lossless while invalid numeric forms
    /// are rejected.
    #[test]
    fn metrics_serialize_as_unsigned_integers() {
        for value in [0, 1, u64::MAX] {
            let metric = InternalMetric {
                name: "test".to_owned(),
                value,
                kind: InternalMetricKind::CounterDelta,
                unit: InternalMetricUnit::Nanoseconds,
            };
            let encoded = toml::to_string(&metric).unwrap();
            assert!(encoded.contains(&format!("value = {value}\n")));
            assert_eq!(toml::from_str::<InternalMetric>(&encoded).unwrap(), metric);
        }
        for value in ["-1", "18446744073709551616", "\"1\""] {
            let encoded = format!(
                "name = 'test'\nvalue = {value}\nkind = 'counter-delta'\nunit = 'nanoseconds'\n"
            );
            assert!(toml::from_str::<InternalMetric>(&encoded).is_err());
        }
    }

    /// Purpose: Preserve typed workload metrics through a strict serialization schema.
    /// Expected: Supported metric records round-trip unchanged while obsolete tags and unknown
    /// fields are rejected.
    #[test]
    fn workload_metrics_round_trip_strictly() {
        let cases = vec![
            WorkloadMetrics::ParallelTableScan {
                target_partitions: 4,
                actual_partitions: 3,
            },
            WorkloadMetrics::FreezeTable {
                approximate_rows: 4,
                page_count: 2,
                stable_page_count: 1,
            },
            WorkloadMetrics::CheckpointTable {
                attempt_count: 3,
                attempt_elapsed_nanos: u64::MAX,
                retry_wait_count: 2,
                retry_wait_elapsed_nanos: u64::MAX - 1,
            },
            WorkloadMetrics::CatalogCheckpoint {
                profile: CatalogCheckpointProfile::Small,
                case: CatalogCheckpointCase::ManagedCreate,
                before: CatalogCardinalities {
                    user_tables: 1_000,
                    columns: 2_000,
                    indexes: 0,
                    bindings: 10_000,
                    descriptor_rows: 1_000,
                    descriptor_bytes: 6_710_886,
                },
                final_state: CatalogCardinalities {
                    user_tables: 1_001,
                    columns: 2_002,
                    indexes: 0,
                    bindings: 10_010,
                    descriptor_rows: 1_001,
                    descriptor_bytes: 6_710_886,
                },
                sampled_process_rss: SampledProcessRss {
                    baseline_bytes: 10,
                    peak_bytes: 20,
                    peak_above_baseline_bytes: 10,
                },
                checkpoint: CatalogCheckpointResult {
                    outcome: CatalogCheckpointOutcome::Published {
                        catalog_replay_start_ts: TrxID::new(42),
                    },
                    report: CatalogCheckpointReport {
                        catalog_ddl_txn_count: 1,
                        table_changes: vec![CatalogTableCheckpointChange {
                            table_id: TableID::new(9),
                            before_row_count: 1,
                            after_row_count: 2,
                        }]
                        .into_boxed_slice(),
                        table_io: vec![CatalogTableCheckpointIoStats {
                            table_id: TableID::new(9),
                            compact_bytes_read: 16_384,
                            final_compact_bytes: 32_768,
                            lwc_bytes_written: 16_384,
                            index_bytes_written: 16_384,
                        }]
                        .into_boxed_slice(),
                        metadata_bytes_written: 24_576,
                    },
                },
            },
        ];
        for metrics in cases {
            let encoded = toml::to_string(&metrics).unwrap();
            if matches!(&metrics, WorkloadMetrics::CatalogCheckpoint { .. }) {
                assert!(encoded.contains("type = \"catalog-checkpoint\""));
                let value: toml::Value = toml::from_str(&encoded).unwrap();
                let checkpoint = value["checkpoint"].as_table().unwrap();
                let mut fields = checkpoint.keys().map(String::as_str).collect::<Vec<_>>();
                fields.sort_unstable();
                assert_eq!(
                    fields,
                    [
                        "catalog_ddl_txn_count",
                        "metadata_bytes_written",
                        "outcome",
                        "table_changes",
                        "table_io",
                    ]
                );
                for level in ["checkpoint", "outcome", "table_changes", "table_io"] {
                    let mut invalid = value.clone();
                    let checkpoint = &mut invalid["checkpoint"];
                    let target = match level {
                        "checkpoint" => checkpoint,
                        "outcome" => &mut checkpoint["outcome"],
                        _ => &mut checkpoint[level][0],
                    };
                    target
                        .as_table_mut()
                        .unwrap()
                        .insert("unknown".to_owned(), toml::Value::Integer(1));
                    assert!(
                        toml::from_str::<WorkloadMetrics>(&toml::to_string(&invalid).unwrap())
                            .is_err(),
                        "unknown field accepted in {level}"
                    );
                }
                let obsolete = encoded.replacen(
                    "type = \"catalog-checkpoint\"",
                    "type = \"catalog-checkpoint-scale\"",
                    1,
                );
                assert!(toml::from_str::<WorkloadMetrics>(&obsolete).is_err());
            }
            assert_eq!(
                toml::from_str::<WorkloadMetrics>(&encoded).unwrap(),
                metrics
            );
        }
        assert!(
            toml::from_str::<WorkloadMetrics>(
                "type = \"freeze-table\"\napproximate_rows = 1\npage_count = 1\nstable_page_count = 1\nunknown = 1\n"
            )
            .is_err()
        );
    }
}
