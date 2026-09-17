use crate::error::{BenchError, Result};
use crate::fixture::{CatalogCardinalities, IndexMode, KeyRange, PlacementKind, RowPlacement};
use crate::plan::{CatalogCheckpointCase, CatalogCheckpointProfile};
use doradb_storage::{
    CatalogCheckpointReport, RecoveryPhaseTimings as StorageRecoveryPhaseTimings,
    RecoveryRedoMetrics as StorageRecoveryRedoMetrics, RecoveryReport as StorageRecoveryReport,
    RecoveryWorkCounts as StorageRecoveryWorkCounts,
};
use hdrhistogram::Histogram;
use quanta::{Clock, Instant};
use rustix::param::page_size;
use rustix::time::{ClockId, Timespec, clock_gettime};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::Path;
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
        checkpoint: CatalogCheckpointReport,
    },
}

/// Strict benchmark representation of storage `RecoveryReport`; durations are u64 nanoseconds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryReport {
    /// Internal startup envelope through ready owner assembly.
    pub bootstrap_elapsed_nanos: u64,
    /// Configuration, root setup, and components preceding the catalog.
    pub engine_setup_elapsed_nanos: u64,
    /// Complete catalog construction, including checkpoint loading.
    pub catalog_bootstrap_elapsed_nanos: u64,
    /// Complete transaction-system component construction.
    pub transaction_bootstrap_elapsed_nanos: u64,
    /// Remaining workers, header durability, layout handling, and owner assembly.
    pub runtime_startup_elapsed_nanos: u64,
    /// Intervals nested within transaction-system construction.
    pub phases: RecoveryPhaseTimings,
    /// Observed replay and reconstruction work.
    pub work: RecoveryWorkCounts,
    /// Consumer-side redo stream attribution.
    pub redo: RecoveryRedoMetrics,
    /// At least one diagnostic overflow or invalid subtraction occurred.
    pub saturated: bool,
}

impl RecoveryReport {
    /// Validate and copy one immutable successful storage report.
    pub(crate) fn from_storage(report: &StorageRecoveryReport) -> Result<Self> {
        let [
            bootstrap_elapsed_nanos,
            engine_setup_elapsed_nanos,
            catalog_bootstrap_elapsed_nanos,
            transaction_bootstrap_elapsed_nanos,
            runtime_startup_elapsed_nanos,
        ] = durations_nanos([
            report.bootstrap_elapsed,
            report.engine_setup_elapsed,
            report.catalog_bootstrap_elapsed,
            report.transaction_bootstrap_elapsed,
            report.runtime_startup_elapsed,
        ])?;
        let report = Self {
            bootstrap_elapsed_nanos,
            engine_setup_elapsed_nanos,
            catalog_bootstrap_elapsed_nanos,
            transaction_bootstrap_elapsed_nanos,
            runtime_startup_elapsed_nanos,
            phases: RecoveryPhaseTimings::from_storage(&report.phases)?,
            work: RecoveryWorkCounts::from_storage(&report.work),
            redo: RecoveryRedoMetrics::from_storage(&report.redo)?,
            saturated: report.saturated,
        };
        validate_recovery_report(&report)?;
        Ok(report)
    }
}

/// Strict benchmark representation of storage `RecoveryPhaseTimings`; durations are u64 nanoseconds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPhaseTimings {
    /// Recovery resources, redo discovery, and coordinator construction.
    pub preparation_elapsed_nanos: u64,
    /// Checkpointed user tables, cleanup, and replay-bound seeding.
    pub user_table_bootstrap_elapsed_nanos: u64,
    /// Replay-suffix planning and read-ahead launch.
    pub redo_planning_elapsed_nanos: u64,
    /// Redo stream consumption, application, and termination.
    pub redo_replay_elapsed_nanos: u64,
    /// Catalog, descriptor, table-root, and index lifecycle validation.
    pub validation_elapsed_nanos: u64,
    /// Post-replay provisional-file cleanup.
    pub absent_file_cleanup_elapsed_nanos: u64,
    /// Replay-sidecar consumption and final hot-index reconstruction.
    pub hot_index_rebuild_elapsed_nanos: u64,
    /// Accepted-prefix repair and startup-file policy selection; excludes later repair IO.
    pub redo_repair_planning_elapsed_nanos: u64,
    /// Construction of writable redo startup resources.
    pub redo_finalize_elapsed_nanos: u64,
    /// Remaining transaction-component construction time.
    pub other_elapsed_nanos: u64,
}

impl RecoveryPhaseTimings {
    fn from_storage(report: &StorageRecoveryPhaseTimings) -> Result<Self> {
        let [
            preparation_elapsed_nanos,
            user_table_bootstrap_elapsed_nanos,
            redo_planning_elapsed_nanos,
            redo_replay_elapsed_nanos,
            validation_elapsed_nanos,
            absent_file_cleanup_elapsed_nanos,
            hot_index_rebuild_elapsed_nanos,
            redo_repair_planning_elapsed_nanos,
            redo_finalize_elapsed_nanos,
            other_elapsed_nanos,
        ] = durations_nanos([
            report.preparation_elapsed,
            report.user_table_bootstrap_elapsed,
            report.redo_planning_elapsed,
            report.redo_replay_elapsed,
            report.validation_elapsed,
            report.absent_file_cleanup_elapsed,
            report.hot_index_rebuild_elapsed,
            report.redo_repair_planning_elapsed,
            report.redo_finalize_elapsed,
            report.other_elapsed,
        ])?;
        Ok(Self {
            preparation_elapsed_nanos,
            user_table_bootstrap_elapsed_nanos,
            redo_planning_elapsed_nanos,
            redo_replay_elapsed_nanos,
            validation_elapsed_nanos,
            absent_file_cleanup_elapsed_nanos,
            hot_index_rebuild_elapsed_nanos,
            redo_repair_planning_elapsed_nanos,
            redo_finalize_elapsed_nanos,
            other_elapsed_nanos,
        })
    }
}

/// Strict benchmark representation of storage `RecoveryWorkCounts`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryWorkCounts {
    /// Redo segment filenames discovered at startup.
    pub redo_segments_discovered: u64,
    /// Segments selected for body replay.
    pub redo_segments_selected: u64,
    /// Decoded catalog RowRedo entries, before filtering.
    pub catalog_row_ops_seen: u64,
    /// Successfully applied catalog RowRedo entries, including DDL payloads.
    pub catalog_row_ops_applied: u64,
    /// Decoded catalog RowRedo entries excluded by replay boundaries.
    pub catalog_row_ops_skipped: u64,
    /// Decoded user RowRedo entries, before filtering.
    pub user_row_ops_seen: u64,
    /// Successfully applied user RowRedo entries.
    pub user_row_ops_applied: u64,
    /// Decoded user RowRedo entries excluded by replay boundaries.
    pub user_row_ops_skipped: u64,
    /// Successfully replayed hot inserts.
    pub hot_inserts: u64,
    /// Successfully replayed hot updates.
    pub hot_updates: u64,
    /// Successfully replayed hot deletes.
    pub hot_deletes: u64,
    /// Successfully replayed cold deletes.
    pub cold_deletes: u64,
    /// User tables loaded from the catalog checkpoint.
    pub checkpoint_user_tables: u64,
    /// Successfully allocated replay pages, including pages later dropped.
    pub hot_pages_reconstructed: u64,
    /// Final hot pages visited by index reconstruction.
    pub index_rebuild_pages: u64,
    /// Successful insertions across all active hot indexes.
    pub index_entries_inserted: u64,
}

impl RecoveryWorkCounts {
    fn from_storage(report: &StorageRecoveryWorkCounts) -> Self {
        Self {
            redo_segments_discovered: report.redo_segments_discovered,
            redo_segments_selected: report.redo_segments_selected,
            catalog_row_ops_seen: report.catalog_row_ops_seen,
            catalog_row_ops_applied: report.catalog_row_ops_applied,
            catalog_row_ops_skipped: report.catalog_row_ops_skipped,
            user_row_ops_seen: report.user_row_ops_seen,
            user_row_ops_applied: report.user_row_ops_applied,
            user_row_ops_skipped: report.user_row_ops_skipped,
            hot_inserts: report.hot_inserts,
            hot_updates: report.hot_updates,
            hot_deletes: report.hot_deletes,
            cold_deletes: report.cold_deletes,
            checkpoint_user_tables: report.checkpoint_user_tables,
            hot_pages_reconstructed: report.hot_pages_reconstructed,
            index_rebuild_pages: report.index_rebuild_pages,
            index_entries_inserted: report.index_entries_inserted,
        }
    }
}

/// Strict benchmark representation of storage `RecoveryRedoMetrics`; durations are u64 nanoseconds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRedoMetrics {
    /// Complete consumer refill calls, including waits, decoding, and reader termination.
    pub stream_refill_elapsed_nanos: u64,
    /// Channel receive time, including scheduling and immediate receive overhead.
    pub receive_wait_elapsed_nanos: u64,
    /// Transaction-frame deserialization, timed once per validated group.
    pub group_decode_elapsed_nanos: u64,
    /// Reader stop and join time inside stream termination.
    pub reader_shutdown_elapsed_nanos: u64,
    /// Refill time excluding receives, decoding, and reader shutdown.
    pub stream_other_elapsed_nanos: u64,
    /// Replay time excluding refill; includes application, dispatch, and filtering.
    pub apply_and_dispatch_elapsed_nanos: u64,
    /// Complete validated groups decoded.
    pub groups_decoded: u64,
    /// Decoded transactions, including those later filtered.
    pub transactions_decoded: u64,
    /// Data blocks received by the consumer, including terminal/tail blocks.
    pub data_blocks_consumed: u64,
    /// Full buffer bytes consumed, excluding metadata and unused read-ahead.
    pub consumed_bytes: u64,
    /// Logical payload bytes in complete validated groups.
    pub validated_payload_bytes: u64,
}

impl RecoveryRedoMetrics {
    fn from_storage(report: &StorageRecoveryRedoMetrics) -> Result<Self> {
        Ok(Self {
            stream_refill_elapsed_nanos: duration_nanos(report.stream_refill_elapsed)?,
            receive_wait_elapsed_nanos: duration_nanos(report.receive_wait_elapsed)?,
            group_decode_elapsed_nanos: duration_nanos(report.group_decode_elapsed)?,
            reader_shutdown_elapsed_nanos: duration_nanos(report.reader_shutdown_elapsed)?,
            stream_other_elapsed_nanos: duration_nanos(report.stream_other_elapsed)?,
            apply_and_dispatch_elapsed_nanos: duration_nanos(report.apply_and_dispatch_elapsed)?,
            groups_decoded: report.groups_decoded,
            transactions_decoded: report.transactions_decoded,
            data_blocks_consumed: report.data_blocks_consumed,
            consumed_bytes: report.consumed_bytes,
            validated_payload_bytes: report.validated_payload_bytes,
        })
    }
}

/// Content proof for a clean reopen in the same process with uncontrolled cache state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryVerification {
    /// Number of verified ordinary user tables, zero or one.
    pub table_count: u64,
    /// Public table identity, present for the single-table fixture.
    pub table_id: Option<u64>,
    /// Prepared index shape, present for the single-table fixture.
    pub index: Option<IndexMode>,
    /// Prepared candidate key range, which may contain gaps or duplicates.
    pub candidate_range: Option<KeyRange>,
    /// Checked row count, also compared with successful preparation inserts.
    pub verified_rows: u64,
    /// Sum of BLAKE3 row hashes modulo 2^256, encoded in little-endian byte order.
    pub fingerprint: String,
    /// Whether the complete unbounded index stream matched the table scan.
    pub index_verified: bool,
}

/// Benchmark-local sampled process resident-set measurements.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SampledProcessRss {
    /// Synchronous RSS sample immediately before starting the sampler.
    pub baseline_bytes: usize,
    /// Greatest one-millisecond or terminal synchronous RSS sample.
    pub peak_bytes: usize,
    /// Saturating sampled peak above the pre-operation baseline.
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
            || self.total_rows == 0
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
    /// Counter observed since the fresh engine startup, including background activity.
    CumulativeCounter,
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
    pub value: u64,
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

/// Read all process threads' accumulated CPU time using the safe Linux clock API.
pub(crate) fn process_cpu_nanos() -> Result<u64> {
    timespec_nanos(clock_gettime(ClockId::ProcessCPUTime))
}

/// Validate a nonnegative process CPU delta.
pub(crate) fn process_cpu_delta(start: u64, end: u64) -> Result<u64> {
    end.checked_sub(start)
        .ok_or_else(|| BenchError::message("process CPU clock moved backwards"))
}

/// Convert a duration to exact nanoseconds, rejecting values outside the metric range.
fn duration_nanos(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| BenchError::message("measurement duration exceeds u64 nanoseconds"))
}

/// Convert a fixed-size group of durations to exact nanoseconds.
fn durations_nanos<const N: usize>(durations: [Duration; N]) -> Result<[u64; N]> {
    let mut nanos = [0; N];
    for (target, duration) in nanos.iter_mut().zip(durations) {
        *target = duration_nanos(duration)?;
    }
    Ok(nanos)
}

fn timespec_nanos(value: Timespec) -> Result<u64> {
    let seconds = u64::try_from(value.tv_sec)
        .map_err(|_| BenchError::message("negative process CPU seconds"))?;
    let nanos = u64::try_from(value.tv_nsec)
        .ok()
        .filter(|nanos| *nanos < 1_000_000_000)
        .ok_or_else(|| BenchError::message("invalid process CPU nanoseconds"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(nanos))
        .ok_or_else(|| BenchError::message("process CPU duration exceeds u64 nanoseconds"))
}

fn check_recovery_sum(actual: u64, components: &[u64]) -> Result<()> {
    let expected = components.iter().try_fold(0u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| BenchError::message("recovery metric sum overflow"))
    })?;
    if actual != expected {
        return Err(BenchError::message("recovery metric accounting mismatch"));
    }
    Ok(())
}

fn validate_recovery_report(report: &RecoveryReport) -> Result<()> {
    if report.saturated {
        return Err(BenchError::message(
            "recovery report contains saturated diagnostics",
        ));
    }
    check_recovery_sum(
        report.bootstrap_elapsed_nanos,
        &[
            report.engine_setup_elapsed_nanos,
            report.catalog_bootstrap_elapsed_nanos,
            report.transaction_bootstrap_elapsed_nanos,
            report.runtime_startup_elapsed_nanos,
        ],
    )?;
    let phases = &report.phases;
    check_recovery_sum(
        report.transaction_bootstrap_elapsed_nanos,
        &[
            phases.preparation_elapsed_nanos,
            phases.user_table_bootstrap_elapsed_nanos,
            phases.redo_planning_elapsed_nanos,
            phases.redo_replay_elapsed_nanos,
            phases.validation_elapsed_nanos,
            phases.absent_file_cleanup_elapsed_nanos,
            phases.hot_index_rebuild_elapsed_nanos,
            phases.redo_repair_planning_elapsed_nanos,
            phases.redo_finalize_elapsed_nanos,
            phases.other_elapsed_nanos,
        ],
    )?;
    validate_recovery_redo(phases.redo_replay_elapsed_nanos, &report.redo)?;
    validate_recovery_work(&report.work)
}

fn validate_recovery_redo(replay_elapsed_nanos: u64, redo: &RecoveryRedoMetrics) -> Result<()> {
    check_recovery_sum(
        replay_elapsed_nanos,
        &[
            redo.stream_refill_elapsed_nanos,
            redo.apply_and_dispatch_elapsed_nanos,
        ],
    )?;
    check_recovery_sum(
        redo.stream_refill_elapsed_nanos,
        &[
            redo.receive_wait_elapsed_nanos,
            redo.group_decode_elapsed_nanos,
            redo.reader_shutdown_elapsed_nanos,
            redo.stream_other_elapsed_nanos,
        ],
    )?;
    if redo.consumed_bytes < redo.validated_payload_bytes {
        return Err(BenchError::message(
            "recovery validated payload bytes exceed consumed bytes",
        ));
    }
    Ok(())
}

fn validate_recovery_work(work: &RecoveryWorkCounts) -> Result<()> {
    check_recovery_sum(
        work.catalog_row_ops_seen,
        &[work.catalog_row_ops_applied, work.catalog_row_ops_skipped],
    )?;
    check_recovery_sum(
        work.user_row_ops_seen,
        &[work.user_row_ops_applied, work.user_row_ops_skipped],
    )?;
    check_recovery_sum(
        work.user_row_ops_applied,
        &[
            work.hot_inserts,
            work.hot_updates,
            work.hot_deletes,
            work.cold_deletes,
        ],
    )
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
    fn process_cpu_conversion_and_deltas_are_checked() {
        assert_eq!(
            timespec_nanos(Timespec {
                tv_sec: 1,
                tv_nsec: 2
            })
            .unwrap(),
            1_000_000_002
        );
        assert_eq!(
            timespec_nanos(Timespec {
                tv_sec: 0,
                tv_nsec: 0
            })
            .unwrap(),
            0
        );
        for value in [
            Timespec {
                tv_sec: -1,
                tv_nsec: 0,
            },
            Timespec {
                tv_sec: 0,
                tv_nsec: -1,
            },
            Timespec {
                tv_sec: 0,
                tv_nsec: 1_000_000_000,
            },
            Timespec {
                tv_sec: i64::MAX,
                tv_nsec: 0,
            },
        ] {
            assert!(timespec_nanos(value).is_err());
        }
        assert_eq!(process_cpu_delta(10, 15).unwrap(), 5);
        assert_eq!(process_cpu_delta(10, 10).unwrap(), 0);
        assert!(process_cpu_delta(10, 9).is_err());
        let first = process_cpu_nanos().unwrap();
        assert!(process_cpu_delta(first, process_cpu_nanos().unwrap()).is_ok());
    }

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
    fn duration_nanoseconds_conversion_checks_bounds() {
        for nanos in [0, 1, u64::MAX] {
            assert_eq!(duration_nanos(Duration::from_nanos(nanos)).unwrap(), nanos);
        }
        let too_large = Duration::from_nanos(u64::MAX) + Duration::from_nanos(1);
        assert!(duration_nanos(too_large).is_err());
    }

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

    #[test]
    fn recovery_duration_conversion_and_numeric_round_trip_check_bounds() {
        let duration = Duration::from_nanos(u64::MAX);
        let mut storage = StorageRecoveryReport {
            bootstrap_elapsed: duration,
            transaction_bootstrap_elapsed: duration,
            phases: StorageRecoveryPhaseTimings {
                redo_replay_elapsed: duration,
                ..StorageRecoveryPhaseTimings::default()
            },
            redo: StorageRecoveryRedoMetrics {
                stream_refill_elapsed: duration,
                receive_wait_elapsed: duration,
                ..StorageRecoveryRedoMetrics::default()
            },
            ..StorageRecoveryReport::default()
        };
        let report = RecoveryReport::from_storage(&storage).unwrap();
        assert_eq!(report.bootstrap_elapsed_nanos, u64::MAX);
        assert_eq!(report.phases.redo_replay_elapsed_nanos, u64::MAX);
        assert_eq!(report.redo.receive_wait_elapsed_nanos, u64::MAX);
        let encoded = toml::to_string(&report).unwrap();
        assert!(encoded.contains(&format!("bootstrap_elapsed_nanos = {}\n", u64::MAX)));
        assert_eq!(toml::from_str::<RecoveryReport>(&encoded).unwrap(), report);

        let oversized = duration + Duration::from_nanos(1);
        storage.bootstrap_elapsed = oversized;
        assert!(RecoveryReport::from_storage(&storage).is_err());
        storage.bootstrap_elapsed = duration;
        storage.phases.redo_replay_elapsed = oversized;
        assert!(RecoveryReport::from_storage(&storage).is_err());
        storage.phases.redo_replay_elapsed = duration;
        storage.redo.receive_wait_elapsed = oversized;
        assert!(RecoveryReport::from_storage(&storage).is_err());
    }

    #[test]
    fn recovery_report_duration_conversion_preserves_field_mapping() {
        let storage = StorageRecoveryReport {
            bootstrap_elapsed: Duration::from_nanos(110),
            engine_setup_elapsed: Duration::from_nanos(11),
            catalog_bootstrap_elapsed: Duration::from_nanos(22),
            transaction_bootstrap_elapsed: Duration::from_nanos(33),
            runtime_startup_elapsed: Duration::from_nanos(44),
            phases: StorageRecoveryPhaseTimings {
                other_elapsed: Duration::from_nanos(33),
                ..StorageRecoveryPhaseTimings::default()
            },
            ..StorageRecoveryReport::default()
        };
        let report = RecoveryReport::from_storage(&storage).unwrap();
        assert_eq!(report.bootstrap_elapsed_nanos, 110);
        assert_eq!(report.engine_setup_elapsed_nanos, 11);
        assert_eq!(report.catalog_bootstrap_elapsed_nanos, 22);
        assert_eq!(report.transaction_bootstrap_elapsed_nanos, 33);
        assert_eq!(report.runtime_startup_elapsed_nanos, 44);
    }

    #[test]
    fn recovery_phase_duration_conversion_preserves_field_mapping() {
        let storage = StorageRecoveryPhaseTimings {
            preparation_elapsed: Duration::from_nanos(1),
            user_table_bootstrap_elapsed: Duration::from_nanos(2),
            redo_planning_elapsed: Duration::from_nanos(3),
            redo_replay_elapsed: Duration::from_nanos(4),
            validation_elapsed: Duration::from_nanos(5),
            absent_file_cleanup_elapsed: Duration::from_nanos(6),
            hot_index_rebuild_elapsed: Duration::from_nanos(7),
            redo_repair_planning_elapsed: Duration::from_nanos(8),
            redo_finalize_elapsed: Duration::from_nanos(9),
            other_elapsed: Duration::from_nanos(10),
        };
        let phases = RecoveryPhaseTimings::from_storage(&storage).unwrap();
        assert_eq!(phases.preparation_elapsed_nanos, 1);
        assert_eq!(phases.user_table_bootstrap_elapsed_nanos, 2);
        assert_eq!(phases.redo_planning_elapsed_nanos, 3);
        assert_eq!(phases.redo_replay_elapsed_nanos, 4);
        assert_eq!(phases.validation_elapsed_nanos, 5);
        assert_eq!(phases.absent_file_cleanup_elapsed_nanos, 6);
        assert_eq!(phases.hot_index_rebuild_elapsed_nanos, 7);
        assert_eq!(phases.redo_repair_planning_elapsed_nanos, 8);
        assert_eq!(phases.redo_finalize_elapsed_nanos, 9);
        assert_eq!(phases.other_elapsed_nanos, 10);
    }

    #[test]
    fn recovery_redo_payload_cannot_exceed_consumed_bytes() {
        for (consumed_bytes, validated_payload_bytes, valid) in [
            (0, 0, true),
            (64, 64, true),
            (4096, 128, true),
            (u64::MAX, u64::MAX, true),
            (u64::MAX, 0, true),
            (0, 1, false),
            (127, 128, false),
            (u64::MAX - 1, u64::MAX, false),
        ] {
            let storage = StorageRecoveryReport {
                redo: StorageRecoveryRedoMetrics {
                    consumed_bytes,
                    validated_payload_bytes,
                    ..StorageRecoveryRedoMetrics::default()
                },
                ..StorageRecoveryReport::default()
            };
            let result = RecoveryReport::from_storage(&storage);
            assert_eq!(
                result.is_ok(),
                valid,
                "consumed_bytes={consumed_bytes}, validated_payload_bytes={validated_payload_bytes}"
            );
            if !valid {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "recovery validated payload bytes exceed consumed bytes"
                );
            }
        }
    }

    #[test]
    fn recovery_accounting_sums_reject_overflow() {
        let empty = RecoveryReport::from_storage(&StorageRecoveryReport::default()).unwrap();
        for case in ["bootstrap", "phases", "replay", "refill", "rows"] {
            let mut report = empty.clone();
            match case {
                "bootstrap" => {
                    report.engine_setup_elapsed_nanos = u64::MAX;
                    report.catalog_bootstrap_elapsed_nanos = 1;
                }
                "phases" => {
                    report.phases.preparation_elapsed_nanos = u64::MAX;
                    report.phases.other_elapsed_nanos = 1;
                }
                "replay" => {
                    report.redo.stream_refill_elapsed_nanos = u64::MAX;
                    report.redo.apply_and_dispatch_elapsed_nanos = 1;
                }
                "refill" => {
                    report.redo.receive_wait_elapsed_nanos = u64::MAX;
                    report.redo.stream_other_elapsed_nanos = 1;
                }
                "rows" => {
                    report.work.user_row_ops_applied = u64::MAX;
                    report.work.user_row_ops_skipped = 1;
                }
                _ => unreachable!(),
            }
            let error = validate_recovery_report(&report).unwrap_err();
            assert_eq!(error.to_string(), "recovery metric sum overflow", "{case}");
        }
    }

    #[test]
    fn recovery_conversion_rejects_saturation_and_inconsistent_accounting() {
        let mut report = StorageRecoveryReport::default();
        assert!(RecoveryReport::from_storage(&report).is_ok());
        report.saturated = true;
        assert!(RecoveryReport::from_storage(&report).is_err());
        report.saturated = false;
        report.work.user_row_ops_seen = 1;
        assert!(RecoveryReport::from_storage(&report).is_err());
        report.work.user_row_ops_skipped = 1;
        assert!(RecoveryReport::from_storage(&report).is_ok());
        report.redo.receive_wait_elapsed = Duration::from_nanos(1);
        assert!(RecoveryReport::from_storage(&report).is_err());
    }
}
