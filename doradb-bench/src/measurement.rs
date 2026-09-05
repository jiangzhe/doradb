use crate::error::{BenchError, Result};
use crate::fixture::CatalogCardinalities;
use crate::plan::{CatalogCheckpointCase, CatalogCheckpointProfile};
use doradb_storage::CatalogCheckpointReport;
use hdrhistogram::Histogram;
use quanta::{Clock, Instant};
use rustix::param::page_size;
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::fs;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
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
            Self::TransactionLifecycle => "transaction-lifecycle",
            Self::StatementExecution => "statement-execution",
            Self::TableCreation => "table-creation",
            Self::InsertBatchTransaction => "insert-batch-transaction",
            Self::UpdateRangeTransaction => "update-range-transaction",
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
        #[serde(with = "u128_decimal")]
        attempt_elapsed_nanos: u128,
        /// Number of public semantic retry waits.
        retry_wait_count: u64,
        /// Time spent inside public semantic retry waits.
        #[serde(with = "u128_decimal")]
        retry_wait_elapsed_nanos: u128,
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
        checkpoint: CatalogCheckpointReport,
    },
}

/// Benchmark-local sampled process resident-set measurements.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SampledProcessRss {
    /// Synchronous RSS sample immediately before starting the sampler.
    pub baseline_bytes: usize,
    /// Greatest one-millisecond or terminal synchronous RSS sample.
    pub peak_bytes: usize,
    /// Saturating sampled peak above the pre-checkpoint baseline.
    pub peak_above_baseline_bytes: usize,
}

/// Running one-millisecond Linux process-RSS sampler.
pub(crate) struct ProcessRssSampler {
    baseline_bytes: usize,
    peak_bytes: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: JoinHandle<Result<()>>,
}

impl ProcessRssSampler {
    /// Capture the baseline, start sampling, and wait for sampler readiness.
    pub(crate) fn start() -> Result<Self> {
        let baseline_bytes = current_process_rss()?;
        let peak_bytes = Arc::new(AtomicUsize::new(baseline_bytes));
        let stop = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let thread_peak = Arc::clone(&peak_bytes);
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("doradb-bench-rss".to_owned())
            .spawn(move || {
                let first = current_process_rss();
                match first {
                    Ok(bytes) => {
                        thread_peak.fetch_max(bytes, Ordering::Relaxed);
                        let _ = ready_tx.send(Ok(()));
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let _ = ready_tx.send(Err(message));
                        return Err(error);
                    }
                }
                while !thread_stop.load(Ordering::Acquire) {
                    thread::sleep(Duration::from_millis(1));
                    let bytes = current_process_rss()?;
                    thread_peak.fetch_max(bytes, Ordering::Relaxed);
                }
                Ok(())
            })
            .map_err(|error| {
                BenchError::message(format!("failed to start process RSS sampler: {error}"))
            })?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                baseline_bytes,
                peak_bytes,
                stop,
                thread,
            }),
            Ok(Err(message)) => {
                let _ = thread.join();
                Err(BenchError::message(format!(
                    "process RSS sampler could not read Linux procfs: {message}"
                )))
            }
            Err(error) => {
                let _ = thread.join();
                Err(BenchError::message(format!(
                    "process RSS sampler readiness channel closed: {error}"
                )))
            }
        }
    }

    /// Take the terminal sample, stop and join the sampler, and return its peak.
    pub(crate) fn stop(self) -> Result<SampledProcessRss> {
        let final_sample = current_process_rss();
        if let Ok(bytes) = &final_sample {
            self.peak_bytes.fetch_max(*bytes, Ordering::Relaxed);
        }
        self.stop.store(true, Ordering::Release);
        let thread_result = self.thread.join().map_err(|_| {
            BenchError::message("process RSS sampler thread panicked before joining")
        })?;
        thread_result?;
        final_sample?;
        let peak_bytes = self.peak_bytes.load(Ordering::Relaxed);
        Ok(SampledProcessRss {
            baseline_bytes: self.baseline_bytes,
            peak_bytes,
            peak_above_baseline_bytes: peak_bytes.saturating_sub(self.baseline_bytes),
        })
    }
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

fn checked_counter(left: u64, right: u64, name: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| BenchError::message(format!("workload counter overflow: {name}")))
}

fn current_process_rss() -> Result<usize> {
    read_process_rss(Path::new("/proc/self/statm"), page_size())
}

fn read_process_rss(path: &Path, page_size: usize) -> Result<usize> {
    let contents = fs::read_to_string(path).map_err(|error| {
        BenchError::message(format!(
            "failed to read process RSS from {}: {error}",
            path.display()
        ))
    })?;
    parse_statm_rss(&contents, page_size)
}

fn parse_statm_rss(contents: &str, page_size: usize) -> Result<usize> {
    let resident_pages = contents
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| BenchError::message("/proc/self/statm has no resident-page field"))?
        .parse::<usize>()
        .map_err(|error| {
            BenchError::message(format!(
                "/proc/self/statm resident-page field is malformed: {error}"
            ))
        })?;
    resident_pages.checked_mul(page_size).ok_or_else(|| {
        BenchError::message(format!(
            "process RSS byte count overflow: resident_pages={resident_pages}, page_size={page_size}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use doradb_storage::id::{TableID, TrxID};
    use doradb_storage::{
        CatalogCheckpointOutcome, CatalogTableCheckpointChange, CatalogTableCheckpointIoStats,
    };
    use tempfile::TempDir;

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
                attempt_elapsed_nanos: u128::MAX,
                retry_wait_count: 2,
                retry_wait_elapsed_nanos: u128::MAX - 1,
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
                checkpoint: CatalogCheckpointReport {
                    outcome: CatalogCheckpointOutcome::Published {
                        catalog_replay_start_ts: TrxID::new(42),
                    },
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
        ];
        for metrics in cases {
            let encoded = toml::to_string(&metrics).unwrap();
            if matches!(&metrics, WorkloadMetrics::CatalogCheckpoint { .. }) {
                assert!(encoded.contains("type = \"catalog-checkpoint\""));
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

    #[test]
    fn process_rss_parser_checks_shape_and_overflow() {
        assert_eq!(parse_statm_rss("100 7 2 1\n", 4_096).unwrap(), 28_672);
        assert!(parse_statm_rss("100\n", 4_096).is_err());
        assert!(parse_statm_rss("100 nope\n", 4_096).is_err());
        assert!(parse_statm_rss("1 2\n", usize::MAX).is_err());
    }

    #[test]
    fn process_rss_sampler_synchronizes_and_returns_a_nondecreasing_peak() {
        let sample = ProcessRssSampler::start().unwrap().stop().unwrap();
        assert!(sample.peak_bytes >= sample.baseline_bytes);
        assert_eq!(
            sample.peak_above_baseline_bytes,
            sample.peak_bytes.saturating_sub(sample.baseline_bytes)
        );
    }

    #[test]
    fn process_rss_reader_rejects_unavailable_input() {
        let temp = TempDir::new().unwrap();
        let error = read_process_rss(&temp.path().join("missing-statm"), 4_096).unwrap_err();
        assert!(error.to_string().contains("failed to read process RSS"));
    }
}
