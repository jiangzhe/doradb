use crate::error::{BenchError, Result};
use hdrhistogram::Histogram;
use quanta::{Clock, Instant};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::result::Result as StdResult;
use std::sync::Arc;

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
    pub fn wall_delta_nanos(&self, start: Instant, end: Instant) -> Result<u128> {
        end.checked_duration_since(start)
            .map(|duration| duration.as_nanos())
            .ok_or_else(|| BenchError::message("measurement wall clock moved backwards"))
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
    /// Public transaction begin-through-successful-commit lifecycle.
    TransactionLifecycle,
    /// One public statement execution inside an active transaction.
    StatementExecution,
    /// One public primary-table creation request.
    TableCreation,
    /// One insert batch transaction from begin through successful commit.
    InsertBatchTransaction,
    /// One index update range transaction from begin through successful commit.
    UpdateRangeTransaction,
    /// One transient table create-through-successful-drop cycle.
    TableCreateDropCycle,
    /// One lookup batch transaction from begin through successful commit.
    LookupBatchTransaction,
    /// One table-scan batch transaction from begin through successful commit.
    TableScanBatchTransaction,
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
}

impl fmt::Display for LatencyUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TransactionLifecycle => "transaction-lifecycle",
            Self::StatementExecution => "statement-execution",
            Self::TableCreation => "table-creation",
            Self::InsertBatchTransaction => "insert-batch-transaction",
            Self::UpdateRangeTransaction => "update-range-transaction",
            Self::TableCreateDropCycle => "table-create-drop-cycle",
            Self::LookupBatchTransaction => "lookup-batch-transaction",
            Self::TableScanBatchTransaction => "table-scan-batch-transaction",
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
        })
    }
}

/// Strict workload-specific metrics retained beside generic counters and latency.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum WorkloadMetrics {
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
        #[serde(with = "u128_decimal")]
        attempt_elapsed_nanos: u128,
        /// Number of public semantic retry waits.
        retry_wait_count: u64,
        /// Time spent inside public semantic retry waits.
        #[serde(with = "u128_decimal")]
        retry_wait_elapsed_nanos: u128,
    },
}

/// Exact session-local latency samples and their HDR distribution.
#[derive(Clone, Debug)]
pub struct LatencyDistribution {
    histogram: Histogram<u64>,
    sample_count: u64,
    sum_nanos: u128,
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
            .checked_add(u128::from(nanos))
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
    /// Successful point lookups that found a row.
    pub found: u64,
    /// Successful point lookups that found no row.
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

/// Classification that controls interpretation of an engine diagnostic.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InternalMetricKind {
    CounterDelta,
    EndGauge,
    LifetimePeak,
}

/// Physical unit of an engine diagnostic.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InternalMetricUnit {
    Count,
    Bytes,
    Nanoseconds,
    Frames,
}

/// Typed optional storage-engine diagnostic.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InternalMetric {
    /// Stable diagnostic name.
    pub name: String,
    /// Exact diagnostic value.
    #[serde(with = "u128_decimal")]
    pub value: u128,
    /// Interpretation of the metric value.
    pub kind: InternalMetricKind,
    /// Physical unit of the metric value.
    pub unit: InternalMetricUnit,
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
    #[serde(with = "u128_decimal")]
    pub sum_nanos: u128,
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
    #[serde(with = "u128_decimal")]
    pub elapsed_nanos: u128,
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
    #[serde(with = "u128_decimal")]
    pub elapsed_nanos: u128,
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
    elapsed_nanos: u128,
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
        elapsed_nanos: u128,
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
pub fn operations_per_second(operations: u64, elapsed_nanos: u128) -> f64 {
    if elapsed_nanos == 0 {
        0.0
    } else {
        operations as f64 * 1_000_000_000.0 / elapsed_nanos as f64
    }
}

fn checked_counter(left: u64, right: u64, name: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| BenchError::message(format!("workload counter overflow: {name}")))
}

/// Decimal-string serde for exact values wider than TOML's signed integer.
pub(crate) mod u128_decimal {
    use super::*;

    pub fn serialize<S>(value: &u128, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> StdResult<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(DeserializeError::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_timestamp_order_is_checked() {
        let (clock, _mock) = Clock::mock();
        let clock = MeasurementClock {
            clock: Arc::new(clock),
        };
        assert_eq!(clock.raw_delta_nanos(12, 12).unwrap(), 0);
        assert!(clock.raw_delta_nanos(13, 12).is_err());
    }

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

    #[test]
    fn histogram_rejects_values_over_one_hour() {
        let mut distribution = LatencyDistribution::new().unwrap();
        assert!(distribution.record(HIGHEST_LATENCY_NANOS + 1).is_err());
    }

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

    #[test]
    fn u128_fields_serialize_as_decimal_strings() {
        let metric = InternalMetric {
            name: "test".to_owned(),
            value: u128::MAX,
            kind: InternalMetricKind::CounterDelta,
            unit: InternalMetricUnit::Count,
        };
        let encoded = toml::to_string(&metric).unwrap();
        assert!(encoded.contains(&format!("value = \"{}\"", u128::MAX)));
        assert_eq!(toml::from_str::<InternalMetric>(&encoded).unwrap(), metric);
    }

    #[test]
    fn workload_metrics_round_trip_strictly() {
        let cases = [
            WorkloadMetrics::FreezeTable {
                approximate_rows: 4,
                page_count: 2,
                stable_page_count: 1,
            },
            WorkloadMetrics::CheckpointTable {
                attempt_count: 3,
                attempt_elapsed_nanos: u128::MAX,
                retry_wait_count: 2,
                retry_wait_elapsed_nanos: u128::MAX - 1,
            },
        ];
        for metrics in cases {
            let encoded = toml::to_string(&metrics).unwrap();
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
