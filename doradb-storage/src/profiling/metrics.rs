use crate::Session;
use crate::error::Result;
use crate::profiling::{
    BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats, CreateIndexMeasurements,
    HotIndexBuildStats, LogicalLockStats, MandatoryRuntimeStats, MandatoryTaskStats,
    StorageIoStats, TransactionSystemStats,
};
use serde::{Deserialize, Serialize};

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

/// Independently sampled component diagnostics; capture is not an atomic engine snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InternalStatsSnapshot {
    trx: TransactionSystemStats,
    storage: StorageIoStats,
    buffer: BufferPoolStats,
    mandatory: MandatoryRuntimeStats,
    logical_lock: LogicalLockStats,
    hot_index_build: HotIndexBuildStats,
}

impl InternalStatsSnapshot {
    /// Capture the component diagnostics exposed by one live session.
    pub fn capture(session: &Session) -> Result<Self> {
        Ok(Self {
            trx: session.transaction_system_stats()?,
            storage: session.storage_io_stats()?,
            buffer: session.buffer_pool_stats()?,
            mandatory: session.mandatory_runtime_stats()?,
            logical_lock: session.logical_lock_stats()?,
            hot_index_build: session.hot_index_build_stats()?,
        })
    }

    /// Project the ordered interval metrics relative to an earlier snapshot.
    pub fn delta_since(&self, earlier: &Self) -> Vec<InternalMetric> {
        plan_internal_metrics(earlier, self)
    }

    /// Project counters for a fresh engine without comparing different instances.
    pub fn cumulative(&self) -> Vec<InternalMetric> {
        cumulative_internal_metrics(self)
    }
}

struct Metric {
    name: String,
    value: u64,
}

/// Translate public diagnostics into typed plan metrics in stable order.
fn plan_internal_metrics(
    before: &InternalStatsSnapshot,
    after: &InternalStatsSnapshot,
) -> Vec<InternalMetric> {
    internal_metrics(before, after)
        .into_iter()
        .map(|metric| {
            let kind = if metric.name.ends_with(".capacity")
                || metric.name.ends_with(".allocated")
                || metric.name.ends_with(".active_count")
                || metric.name.starts_with("logical_lock.current_")
            {
                InternalMetricKind::EndGauge
            } else if metric.name.starts_with("logical_lock.peak_")
                || metric.name.starts_with("hot_index_build.max_")
                || metric.name == "hot_index_build.scratch_peak_bytes"
                || create_index_peak(&metric.name)
            {
                InternalMetricKind::LifetimePeak
            } else {
                InternalMetricKind::CounterDelta
            };
            let unit = if metric.name == "transaction.log_bytes" || metric.name.ends_with("_bytes")
            {
                InternalMetricUnit::Bytes
            } else if metric.name.ends_with("_nanos") {
                InternalMetricUnit::Nanoseconds
            } else if metric.name.ends_with(".capacity") || metric.name.ends_with(".allocated") {
                InternalMetricUnit::Frames
            } else {
                InternalMetricUnit::Count
            };
            InternalMetric {
                name: metric.name,
                value: metric.value,
                kind,
                unit,
            }
        })
        .collect()
}

/// Capture fresh-engine counters without comparing different engine instances.
fn cumulative_internal_metrics(snapshot: &InternalStatsSnapshot) -> Vec<InternalMetric> {
    let mut metrics = plan_internal_metrics(&InternalStatsSnapshot::default(), snapshot);
    for metric in &mut metrics {
        if metric.kind == InternalMetricKind::CounterDelta {
            metric.kind = InternalMetricKind::CumulativeCounter;
        }
    }
    metrics
}

fn internal_metrics(before: &InternalStatsSnapshot, after: &InternalStatsSnapshot) -> Vec<Metric> {
    let mut metrics = Vec::new();
    push_transaction_metrics(&mut metrics, before.trx, after.trx);
    push_storage_metrics(&mut metrics, before.storage, after.storage);
    push_buffer_metrics(&mut metrics, &before.buffer, &after.buffer);
    push_mandatory_metrics(&mut metrics, before.mandatory, after.mandatory);
    push_logical_lock_metrics(&mut metrics, before.logical_lock, after.logical_lock);
    push_hot_index_build_metrics(
        &mut metrics,
        &before.hot_index_build,
        &after.hot_index_build,
    );
    push_create_index_metrics(
        &mut metrics,
        &before.hot_index_build.create,
        &after.hot_index_build.create,
    );
    metrics
}

fn push_hot_index_build_metrics(
    metrics: &mut Vec<Metric>,
    before: &HotIndexBuildStats,
    after: &HotIndexBuildStats,
) {
    // Until production callers migrate, there are no samples to report. Also
    // omit intervals without completed work instead of presenting lifetime peaks
    // as measurements of the current benchmark operation.
    if before.completed_builds == after.completed_builds {
        return;
    }
    macro_rules! counter {
        ($field:ident) => {
            push_metric(
                metrics,
                concat!("hot_index_build.", stringify!($field)),
                after.$field - before.$field,
            );
        };
    }
    counter!(completed_builds);
    counter!(source_pages);
    counter!(entries);
    counter!(planned_groups);
    counter!(nonempty_runs);
    counter!(capture_elapsed_nanos);
    counter!(extraction_worker_time_nanos);
    counter!(sort_worker_time_nanos);
    counter!(duplicate_worker_time_nanos);
    counter!(extraction_wall_elapsed_nanos);
    counter!(sort_wall_elapsed_nanos);
    counter!(duplicate_wall_elapsed_nanos);
    counter!(pipeline_wall_elapsed_nanos);
    counter!(total_elapsed_nanos);
    push_metric(
        metrics,
        "hot_index_build.max_job_elapsed_nanos",
        after.max_job_elapsed_nanos,
    );
    push_metric(
        metrics,
        "hot_index_build.max_sort_elapsed_nanos",
        after.max_sort_elapsed_nanos,
    );
    push_metric(
        metrics,
        "hot_index_build.scratch_peak_bytes",
        after.scratch_peak_bytes,
    );
}

fn push_create_index_metrics(
    metrics: &mut Vec<Metric>,
    before: &CreateIndexMeasurements,
    after: &CreateIndexMeasurements,
) {
    if before.hot.completed_builds == after.hot.completed_builds {
        return;
    }
    macro_rules! counter {
        ($name:literal, $($field:ident).+) => {
            push_metric(metrics, concat!("create_index.", $name), after.$($field).+ - before.$($field).+);
        };
    }
    macro_rules! peak {
        ($name:literal, $($field:ident).+) => {
            push_metric(metrics, concat!("create_index.", $name), after.$($field).+);
        };
    }
    counter!("cold_entries", cold_entries);
    counter!("cold_collect_nanos", cold_collect_nanos);
    counter!("cold_sort_nanos", cold_sort_nanos);
    counter!("cold_build_nanos", cold_build_nanos);
    counter!("cold_hot_comparisons", cold_hot_comparisons);
    counter!("cold_hot_worker_nanos", cold_hot_worker_nanos);
    counter!("total_elapsed_nanos", total_elapsed_nanos);
    peak!("retained_cold_bytes", retained_cold_bytes);
    peak!("max_cold_hot_sync_nanos", max_cold_hot_sync_nanos);
    counter!("completed_builds", hot.completed_builds);
    counter!("leaf_pages", hot.leaf_pages);
    counter!("branch_pages", hot.branch_pages);
    counter!("leaf_occupied_bytes", hot.leaf_occupied_bytes);
    counter!("branch_occupied_bytes", hot.branch_occupied_bytes);
    counter!("allocation_nanos", hot.allocation_nanos);
    counter!("packing_nanos", hot.packing_nanos);
    counter!("leaf_planning_nanos", hot.leaf_planning_nanos);
    counter!("parent_planning_nanos", hot.parent_planning_nanos);
    counter!("direct_parent_nanos", hot.direct_parent_nanos);
    counter!("serial_upper_nanos", hot.serial_upper_nanos);
    counter!("install_nanos", hot.install_nanos);
    counter!("cleanup_nanos", hot.cleanup_nanos);
    peak!("max_sync_nanos", hot.max_sync_nanos);
    peak!("max_job_nanos", hot.max_job_nanos);
    peak!("scratch_peak_bytes", hot.scratch_peak_bytes);
    counter!(
        "extraction.capture_elapsed_nanos",
        hot.extraction.capture_elapsed_nanos
    );
    counter!(
        "extraction.extraction_worker_time_nanos",
        hot.extraction.extraction_worker_time_nanos
    );
    counter!(
        "extraction.sort_worker_time_nanos",
        hot.extraction.sort_worker_time_nanos
    );
    counter!(
        "extraction.duplicate_worker_time_nanos",
        hot.extraction.duplicate_worker_time_nanos
    );
    counter!(
        "extraction.extraction_wall_elapsed_nanos",
        hot.extraction.extraction_wall_elapsed_nanos
    );
    counter!(
        "extraction.sort_wall_elapsed_nanos",
        hot.extraction.sort_wall_elapsed_nanos
    );
    counter!(
        "extraction.duplicate_wall_elapsed_nanos",
        hot.extraction.duplicate_wall_elapsed_nanos
    );
    counter!(
        "extraction.pipeline_wall_elapsed_nanos",
        hot.extraction.pipeline_wall_elapsed_nanos
    );
    counter!(
        "extraction.total_elapsed_nanos",
        hot.extraction.total_elapsed_nanos
    );
    counter!("extraction.source_pages", hot.extraction.source_pages);
    counter!("extraction.entries", hot.extraction.entries);
    counter!("extraction.planned_groups", hot.extraction.planned_groups);
    counter!("extraction.nonempty_runs", hot.extraction.nonempty_runs);
    peak!(
        "extraction.max_job_elapsed_nanos",
        hot.extraction.max_job_elapsed_nanos
    );
    peak!(
        "extraction.max_sort_elapsed_nanos",
        hot.extraction.max_sort_elapsed_nanos
    );
    peak!(
        "extraction.scratch_peak_bytes",
        hot.extraction.scratch_peak_bytes
    );
    peak!("extraction.workers", hot.extraction.workers);
    peak!("extraction.page_target", hot.extraction.page_target);
    counter!("merge.entries", hot.merge.entries);
    counter!("merge.runs", hot.merge.runs);
    counter!("merge.partitions", hot.merge.partitions);
    counter!("merge.boundary_wall_nanos", hot.merge.boundary_wall_nanos);
    counter!("merge.cut_worker_nanos", hot.merge.cut_worker_nanos);
    counter!(
        "merge.consumption_wall_nanos",
        hot.merge.consumption_wall_nanos
    );
    counter!("merge.merge_check_nanos", hot.merge.merge_check_nanos);
    counter!("merge.job_worker_nanos", hot.merge.job_worker_nanos);
    counter!(
        "merge.consumer_worker_nanos",
        hot.merge.consumer_worker_nanos
    );
    counter!(
        "merge.duplicate_comparisons",
        hot.merge.duplicate_comparisons
    );
    peak!("merge.workers", hot.merge.workers);
    peak!("merge.batch_entries", hot.merge.batch_entries);
    peak!("merge.max_cut_nanos", hot.merge.max_cut_nanos);
    peak!("merge.first_batch_nanos", hot.merge.first_batch_nanos);
    peak!("merge.max_batch_nanos", hot.merge.max_batch_nanos);
    peak!("merge.max_job_nanos", hot.merge.max_job_nanos);
    peak!("merge.boundary_bytes", hot.merge.boundary_bytes);
    peak!("merge.max_reference_bytes", hot.merge.max_reference_bytes);
    peak!(
        "merge.active_reference_bytes",
        hot.merge.active_reference_bytes
    );
    peak!("merge.validation_bytes", hot.merge.validation_bytes);
    peak!("merge.scratch_peak_bytes", hot.merge.scratch_peak_bytes);
}

fn create_index_peak(name: &str) -> bool {
    matches!(
        name,
        "create_index.retained_cold_bytes"
            | "create_index.max_cold_hot_sync_nanos"
            | "create_index.max_sync_nanos"
            | "create_index.max_job_nanos"
            | "create_index.scratch_peak_bytes"
            | "create_index.extraction.max_job_elapsed_nanos"
            | "create_index.extraction.max_sort_elapsed_nanos"
            | "create_index.extraction.scratch_peak_bytes"
            | "create_index.extraction.workers"
            | "create_index.extraction.page_target"
            | "create_index.merge.workers"
            | "create_index.merge.batch_entries"
            | "create_index.merge.max_cut_nanos"
            | "create_index.merge.first_batch_nanos"
            | "create_index.merge.max_batch_nanos"
            | "create_index.merge.max_job_nanos"
            | "create_index.merge.boundary_bytes"
            | "create_index.merge.max_reference_bytes"
            | "create_index.merge.active_reference_bytes"
            | "create_index.merge.validation_bytes"
            | "create_index.merge.scratch_peak_bytes"
    )
}

fn push_logical_lock_metrics(
    metrics: &mut Vec<Metric>,
    before: LogicalLockStats,
    after: LogicalLockStats,
) {
    macro_rules! delta_metric {
        ($field:ident) => {
            push_metric(
                metrics,
                concat!("logical_lock.", stringify!($field)),
                delta_u64(after.$field, before.$field),
            );
        };
    }
    delta_metric!(owner_local_exact_covered_hits);
    delta_metric!(owner_local_covered_publications);
    delta_metric!(owner_local_mode_preserving_conversions);
    delta_metric!(owner_local_mode_preserving_releases);
    delta_metric!(resource_transitions);
    delta_metric!(mode_slots_examined);
    delta_metric!(immediate_physical_acquisitions);
    delta_metric!(physical_upgrades);
    delta_metric!(enqueued_waiters);
    delta_metric!(queue_link_mutations);
    delta_metric!(cancelled_head_waiters);
    delta_metric!(cancelled_middle_waiters);
    delta_metric!(cancelled_tail_waiters);
    delta_metric!(provisional_observations);
    delta_metric!(promoted_waiters);
    delta_metric!(scope_close_claims_visited);
    delta_metric!(scope_close_physical_changes);
    delta_metric!(completion_allocations);
    delta_metric!(waiter_slab_growths);
    delta_metric!(waiter_slab_reuses);
    push_metric(
        metrics,
        "logical_lock.current_physical_resources",
        after.current_physical_resources,
    );
    push_metric(
        metrics,
        "logical_lock.peak_physical_resources",
        after.peak_physical_resources,
    );
    push_metric(
        metrics,
        "logical_lock.current_physical_families",
        after.current_physical_families,
    );
    push_metric(
        metrics,
        "logical_lock.peak_physical_families",
        after.peak_physical_families,
    );
    push_metric(
        metrics,
        "logical_lock.current_linked_waiters",
        after.current_linked_waiters,
    );
    push_metric(
        metrics,
        "logical_lock.peak_linked_waiters",
        after.peak_linked_waiters,
    );
    push_metric(
        metrics,
        "logical_lock.current_live_waiter_nodes",
        after.current_live_waiter_nodes,
    );
    push_metric(
        metrics,
        "logical_lock.peak_live_waiter_nodes",
        after.peak_live_waiter_nodes,
    );
}

fn push_transaction_metrics(
    metrics: &mut Vec<Metric>,
    before: TransactionSystemStats,
    after: TransactionSystemStats,
) {
    for (name, value) in [
        (
            "transaction.commit_count",
            delta(after.commit_count, before.commit_count),
        ),
        (
            "transaction.trx_count",
            delta(after.trx_count, before.trx_count),
        ),
        (
            "transaction.log_bytes",
            delta(after.log_bytes, before.log_bytes),
        ),
        (
            "transaction.sync_count",
            delta(after.sync_count, before.sync_count),
        ),
        (
            "transaction.sync_nanos",
            delta(after.sync_nanos, before.sync_nanos),
        ),
        (
            "transaction.seal_failure_count",
            delta(after.seal_failure_count, before.seal_failure_count),
        ),
        (
            "transaction.io_submit_and_wait_count",
            delta(
                after.io_submit_and_wait_count,
                before.io_submit_and_wait_count,
            ),
        ),
        (
            "transaction.io_submit_and_wait_nanos",
            delta(
                after.io_submit_and_wait_nanos,
                before.io_submit_and_wait_nanos,
            ),
        ),
        (
            "transaction.purge_trx_count",
            delta(after.purge_trx_count, before.purge_trx_count),
        ),
        (
            "transaction.purge_row_count",
            delta(after.purge_row_count, before.purge_row_count),
        ),
        (
            "transaction.purge_index_count",
            delta(after.purge_index_count, before.purge_index_count),
        ),
    ] {
        push_metric(metrics, name, value);
    }
}

fn push_storage_metrics(metrics: &mut Vec<Metric>, before: StorageIoStats, after: StorageIoStats) {
    for (name, value) in [
        (
            "storage.backend.submit_and_wait_calls",
            delta(
                after.backend.submit_and_wait_calls,
                before.backend.submit_and_wait_calls,
            ),
        ),
        (
            "storage.backend.submitted_ops",
            delta(after.backend.submitted_ops, before.backend.submitted_ops),
        ),
        (
            "storage.backend.submit_and_wait_nanos",
            delta(
                after.backend.submit_and_wait_nanos,
                before.backend.submit_and_wait_nanos,
            ),
        ),
        (
            "storage.backend.wait_completions",
            delta(
                after.backend.wait_completions,
                before.backend.wait_completions,
            ),
        ),
        (
            "storage.table_read_requests",
            delta(after.table_read_requests, before.table_read_requests),
        ),
        (
            "storage.pool_read_requests",
            delta(after.pool_read_requests, before.pool_read_requests),
        ),
        (
            "storage.background_write_requests",
            delta(
                after.background_write_requests,
                before.background_write_requests,
            ),
        ),
        (
            "storage.table_read_turns",
            delta(after.table_read_turns, before.table_read_turns),
        ),
        (
            "storage.pool_read_turns",
            delta(after.pool_read_turns, before.pool_read_turns),
        ),
        (
            "storage.background_write_turns",
            delta(after.background_write_turns, before.background_write_turns),
        ),
    ] {
        push_metric(metrics, name, value);
    }
}

fn push_buffer_metrics(
    metrics: &mut Vec<Metric>,
    before: &BufferPoolStats,
    after: &BufferPoolStats,
) {
    push_one_buffer_pool(metrics, "buffer.meta", before.meta, after.meta);
    push_one_buffer_pool(metrics, "buffer.mem", before.mem, after.mem);
    push_one_buffer_pool(metrics, "buffer.index", before.index, after.index);
    push_one_buffer_pool(metrics, "buffer.disk", before.disk, after.disk);
}

fn push_mandatory_metrics(
    metrics: &mut Vec<Metric>,
    before: MandatoryRuntimeStats,
    after: MandatoryRuntimeStats,
) {
    push_mandatory_task_metrics(
        metrics,
        "mandatory.operation",
        before.operation,
        after.operation,
    );
    push_mandatory_task_metrics(
        metrics,
        "mandatory.transaction_cleanup",
        before.transaction_cleanup,
        after.transaction_cleanup,
    );
}

fn push_mandatory_task_metrics(
    metrics: &mut Vec<Metric>,
    prefix: &str,
    before: MandatoryTaskStats,
    after: MandatoryTaskStats,
) {
    for (name, value) in [
        (
            "submitted_count",
            delta(after.submitted_count, before.submitted_count),
        ),
        (
            "started_count",
            delta(after.started_count, before.started_count),
        ),
        (
            "completed_count",
            delta(after.completed_count, before.completed_count),
        ),
        ("error_count", delta(after.error_count, before.error_count)),
        ("panic_count", delta(after.panic_count, before.panic_count)),
        (
            "detached_observer_count",
            delta(
                after.detached_observer_count,
                before.detached_observer_count,
            ),
        ),
        ("active_count", after.active_count as u64),
        (
            "admission_wait_nanos",
            delta(after.admission_wait_nanos, before.admission_wait_nanos),
        ),
        (
            "queue_wait_nanos",
            delta(after.queue_wait_nanos, before.queue_wait_nanos),
        ),
        (
            "execution_nanos",
            delta(after.execution_nanos, before.execution_nanos),
        ),
    ] {
        push_metric(metrics, &format!("{prefix}.{name}"), value);
    }
}

fn push_one_buffer_pool(
    metrics: &mut Vec<Metric>,
    prefix: &str,
    before: BufferPoolRuntimeStats,
    after: BufferPoolRuntimeStats,
) {
    push_metric(
        metrics,
        &format!("{prefix}.capacity"),
        after.capacity as u64,
    );
    push_metric(
        metrics,
        &format!("{prefix}.allocated"),
        after.allocated as u64,
    );
    push_buffer_counters(metrics, prefix, before.counters, after.counters);
}

fn push_buffer_counters(
    metrics: &mut Vec<Metric>,
    prefix: &str,
    before: BufferPoolCounters,
    after: BufferPoolCounters,
) {
    for (name, value) in [
        ("cache_hits", delta(after.cache_hits, before.cache_hits)),
        (
            "cache_misses",
            delta(after.cache_misses, before.cache_misses),
        ),
        ("miss_joins", delta(after.miss_joins, before.miss_joins)),
        (
            "queued_reads",
            delta(after.queued_reads, before.queued_reads),
        ),
        (
            "running_reads",
            delta(after.running_reads, before.running_reads),
        ),
        (
            "completed_reads",
            delta(after.completed_reads, before.completed_reads),
        ),
        ("read_errors", delta(after.read_errors, before.read_errors)),
        (
            "queued_writes",
            delta(after.queued_writes, before.queued_writes),
        ),
        (
            "running_writes",
            delta(after.running_writes, before.running_writes),
        ),
        (
            "completed_writes",
            delta(after.completed_writes, before.completed_writes),
        ),
        (
            "write_errors",
            delta(after.write_errors, before.write_errors),
        ),
    ] {
        push_metric(metrics, &format!("{prefix}.{name}"), value);
    }
}

fn push_metric(metrics: &mut Vec<Metric>, name: &str, value: u64) {
    metrics.push(Metric {
        name: name.to_owned(),
        value,
    });
}

fn delta(after: usize, before: usize) -> u64 {
    after.saturating_sub(before) as u64
}

fn delta_u64(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

#[cfg(test)]
mod tests {
    use super::{InternalMetricKind, InternalMetricUnit};
    use super::{InternalStatsSnapshot, cumulative_internal_metrics, plan_internal_metrics};
    use crate::profiling::HotIndexBuildStats;

    /// Purpose: Report successful CREATE intervals independently of extraction and preserve capacities as lifetime peaks.
    /// Expected: Failed or idle publication intervals omit CREATE metrics; completed intervals subtract additive work and retain absolute peaks with correct units.
    #[test]
    fn create_metrics_require_publication_and_distinguish_peaks() {
        let mut before = InternalStatsSnapshot::default();
        before.hot_index_build.create.hot.completed_builds = 2;
        before.hot_index_build.create.cold_hot_worker_nanos = 70;
        before.hot_index_build.create.retained_cold_bytes = 4096;
        before.hot_index_build.create.hot.extraction.workers = 8;
        let mut after = before;
        after.hot_index_build.completed_builds = 1;
        assert!(
            plan_internal_metrics(&before, &after)
                .iter()
                .all(|m| !m.name.starts_with("create_index."))
        );
        after.hot_index_build.create.hot.completed_builds = 3;
        after.hot_index_build.create.cold_hot_worker_nanos = 95;
        let metrics = plan_internal_metrics(&before, &after);
        for (name, value, kind, unit) in [
            (
                "completed_builds",
                1,
                InternalMetricKind::CounterDelta,
                InternalMetricUnit::Count,
            ),
            (
                "cold_hot_worker_nanos",
                25,
                InternalMetricKind::CounterDelta,
                InternalMetricUnit::Nanoseconds,
            ),
            (
                "retained_cold_bytes",
                4096,
                InternalMetricKind::LifetimePeak,
                InternalMetricUnit::Bytes,
            ),
            (
                "extraction.workers",
                8,
                InternalMetricKind::LifetimePeak,
                InternalMetricUnit::Count,
            ),
        ] {
            let metric = metrics
                .iter()
                .find(|metric| metric.name == format!("create_index.{name}"))
                .unwrap();
            assert_eq!(
                (metric.value, metric.kind, metric.unit),
                (value, kind, unit),
                "{name}"
            );
        }
    }

    /// Purpose: Preserve hot-build delta, lifetime-peak, and fresh-engine metric semantics.
    /// Expected: Empty intervals emit no profile; counts/times subtract while maxima retain absolute values and correct units.
    #[test]
    fn hot_build_metrics_distinguish_deltas_and_peaks() {
        let empty = InternalStatsSnapshot::default();
        assert!(
            plan_internal_metrics(&empty, &empty)
                .iter()
                .all(|m| !m.name.starts_with("hot_index_build."))
        );
        let before = InternalStatsSnapshot {
            hot_index_build: HotIndexBuildStats {
                completed_builds: 2,
                entries: 20,
                total_elapsed_nanos: 100,
                max_job_elapsed_nanos: 80,
                scratch_peak_bytes: 4096,
                ..HotIndexBuildStats::default()
            },
            ..InternalStatsSnapshot::default()
        };
        assert!(
            plan_internal_metrics(&before, &before)
                .iter()
                .all(|m| !m.name.starts_with("hot_index_build."))
        );
        let after = InternalStatsSnapshot {
            hot_index_build: HotIndexBuildStats {
                completed_builds: 3,
                entries: 27,
                total_elapsed_nanos: 140,
                max_job_elapsed_nanos: 80,
                scratch_peak_bytes: 8192,
                ..HotIndexBuildStats::default()
            },
            ..InternalStatsSnapshot::default()
        };
        let metrics = plan_internal_metrics(&before, &after);
        for (suffix, value, kind, unit) in [
            (
                "completed_builds",
                1,
                InternalMetricKind::CounterDelta,
                InternalMetricUnit::Count,
            ),
            (
                "entries",
                7,
                InternalMetricKind::CounterDelta,
                InternalMetricUnit::Count,
            ),
            (
                "total_elapsed_nanos",
                40,
                InternalMetricKind::CounterDelta,
                InternalMetricUnit::Nanoseconds,
            ),
            (
                "max_job_elapsed_nanos",
                80,
                InternalMetricKind::LifetimePeak,
                InternalMetricUnit::Nanoseconds,
            ),
            (
                "scratch_peak_bytes",
                8192,
                InternalMetricKind::LifetimePeak,
                InternalMetricUnit::Bytes,
            ),
        ] {
            let metric = metrics
                .iter()
                .find(|m| m.name == format!("hot_index_build.{suffix}"))
                .unwrap();
            assert_eq!(
                (metric.value, metric.kind, metric.unit),
                (value, kind, unit),
                "{suffix}"
            );
        }
        let fresh = cumulative_internal_metrics(&after);
        let entries = fresh
            .iter()
            .find(|m| m.name == "hot_index_build.entries")
            .unwrap();
        assert_eq!(
            (entries.value, entries.kind),
            (27, InternalMetricKind::CumulativeCounter)
        );
    }

    /// Purpose: Preserve the complete ordered internal metric contract during profiling relocation.
    /// Expected: Every metric keeps its name, value, kind, unit, and cumulative interpretation.
    #[test]
    fn complete_metric_contract() {
        let mut before = InternalStatsSnapshot::default();
        let mut after = InternalStatsSnapshot::default();
        before.trx.commit_count = 101;
        after.trx.commit_count = 203;
        before.trx.trx_count = 102;
        after.trx.trx_count = 206;
        before.trx.log_bytes = 103;
        after.trx.log_bytes = 209;
        before.trx.sync_count = 104;
        after.trx.sync_count = 212;
        before.trx.sync_nanos = 105;
        after.trx.sync_nanos = 215;
        before.trx.seal_failure_count = 106;
        after.trx.seal_failure_count = 218;
        before.trx.io_submit_and_wait_count = 107;
        after.trx.io_submit_and_wait_count = 221;
        before.trx.io_submit_and_wait_nanos = 108;
        after.trx.io_submit_and_wait_nanos = 224;
        before.trx.purge_trx_count = 109;
        after.trx.purge_trx_count = 227;
        before.trx.purge_row_count = 110;
        after.trx.purge_row_count = 230;
        before.trx.purge_index_count = 111;
        after.trx.purge_index_count = 233;
        before.storage.backend.submit_and_wait_calls = 112;
        after.storage.backend.submit_and_wait_calls = 236;
        before.storage.backend.submitted_ops = 113;
        after.storage.backend.submitted_ops = 239;
        before.storage.backend.submit_and_wait_nanos = 114;
        after.storage.backend.submit_and_wait_nanos = 242;
        before.storage.backend.wait_completions = 115;
        after.storage.backend.wait_completions = 245;
        before.storage.table_read_requests = 116;
        after.storage.table_read_requests = 248;
        before.storage.pool_read_requests = 117;
        after.storage.pool_read_requests = 251;
        before.storage.background_write_requests = 118;
        after.storage.background_write_requests = 254;
        before.storage.table_read_turns = 119;
        after.storage.table_read_turns = 257;
        before.storage.pool_read_turns = 120;
        after.storage.pool_read_turns = 260;
        before.storage.background_write_turns = 121;
        after.storage.background_write_turns = 263;
        before.buffer.meta.capacity = 122;
        after.buffer.meta.capacity = 266;
        before.buffer.meta.allocated = 123;
        after.buffer.meta.allocated = 269;
        before.buffer.meta.counters.cache_hits = 124;
        after.buffer.meta.counters.cache_hits = 272;
        before.buffer.meta.counters.cache_misses = 125;
        after.buffer.meta.counters.cache_misses = 275;
        before.buffer.meta.counters.miss_joins = 126;
        after.buffer.meta.counters.miss_joins = 278;
        before.buffer.meta.counters.queued_reads = 127;
        after.buffer.meta.counters.queued_reads = 281;
        before.buffer.meta.counters.running_reads = 128;
        after.buffer.meta.counters.running_reads = 284;
        before.buffer.meta.counters.completed_reads = 129;
        after.buffer.meta.counters.completed_reads = 287;
        before.buffer.meta.counters.read_errors = 130;
        after.buffer.meta.counters.read_errors = 290;
        before.buffer.meta.counters.queued_writes = 131;
        after.buffer.meta.counters.queued_writes = 293;
        before.buffer.meta.counters.running_writes = 132;
        after.buffer.meta.counters.running_writes = 296;
        before.buffer.meta.counters.completed_writes = 133;
        after.buffer.meta.counters.completed_writes = 299;
        before.buffer.meta.counters.write_errors = 134;
        after.buffer.meta.counters.write_errors = 302;
        before.buffer.mem.capacity = 135;
        after.buffer.mem.capacity = 305;
        before.buffer.mem.allocated = 136;
        after.buffer.mem.allocated = 308;
        before.buffer.mem.counters.cache_hits = 137;
        after.buffer.mem.counters.cache_hits = 311;
        before.buffer.mem.counters.cache_misses = 138;
        after.buffer.mem.counters.cache_misses = 314;
        before.buffer.mem.counters.miss_joins = 139;
        after.buffer.mem.counters.miss_joins = 317;
        before.buffer.mem.counters.queued_reads = 140;
        after.buffer.mem.counters.queued_reads = 320;
        before.buffer.mem.counters.running_reads = 141;
        after.buffer.mem.counters.running_reads = 323;
        before.buffer.mem.counters.completed_reads = 142;
        after.buffer.mem.counters.completed_reads = 326;
        before.buffer.mem.counters.read_errors = 143;
        after.buffer.mem.counters.read_errors = 329;
        before.buffer.mem.counters.queued_writes = 144;
        after.buffer.mem.counters.queued_writes = 332;
        before.buffer.mem.counters.running_writes = 145;
        after.buffer.mem.counters.running_writes = 335;
        before.buffer.mem.counters.completed_writes = 146;
        after.buffer.mem.counters.completed_writes = 338;
        before.buffer.mem.counters.write_errors = 147;
        after.buffer.mem.counters.write_errors = 341;
        before.buffer.index.capacity = 148;
        after.buffer.index.capacity = 344;
        before.buffer.index.allocated = 149;
        after.buffer.index.allocated = 347;
        before.buffer.index.counters.cache_hits = 150;
        after.buffer.index.counters.cache_hits = 350;
        before.buffer.index.counters.cache_misses = 151;
        after.buffer.index.counters.cache_misses = 353;
        before.buffer.index.counters.miss_joins = 152;
        after.buffer.index.counters.miss_joins = 356;
        before.buffer.index.counters.queued_reads = 153;
        after.buffer.index.counters.queued_reads = 359;
        before.buffer.index.counters.running_reads = 154;
        after.buffer.index.counters.running_reads = 362;
        before.buffer.index.counters.completed_reads = 155;
        after.buffer.index.counters.completed_reads = 365;
        before.buffer.index.counters.read_errors = 156;
        after.buffer.index.counters.read_errors = 368;
        before.buffer.index.counters.queued_writes = 157;
        after.buffer.index.counters.queued_writes = 371;
        before.buffer.index.counters.running_writes = 158;
        after.buffer.index.counters.running_writes = 374;
        before.buffer.index.counters.completed_writes = 159;
        after.buffer.index.counters.completed_writes = 377;
        before.buffer.index.counters.write_errors = 160;
        after.buffer.index.counters.write_errors = 380;
        before.buffer.disk.capacity = 161;
        after.buffer.disk.capacity = 383;
        before.buffer.disk.allocated = 162;
        after.buffer.disk.allocated = 386;
        before.buffer.disk.counters.cache_hits = 163;
        after.buffer.disk.counters.cache_hits = 389;
        before.buffer.disk.counters.cache_misses = 164;
        after.buffer.disk.counters.cache_misses = 392;
        before.buffer.disk.counters.miss_joins = 165;
        after.buffer.disk.counters.miss_joins = 395;
        before.buffer.disk.counters.queued_reads = 166;
        after.buffer.disk.counters.queued_reads = 398;
        before.buffer.disk.counters.running_reads = 167;
        after.buffer.disk.counters.running_reads = 401;
        before.buffer.disk.counters.completed_reads = 168;
        after.buffer.disk.counters.completed_reads = 404;
        before.buffer.disk.counters.read_errors = 169;
        after.buffer.disk.counters.read_errors = 407;
        before.buffer.disk.counters.queued_writes = 170;
        after.buffer.disk.counters.queued_writes = 410;
        before.buffer.disk.counters.running_writes = 171;
        after.buffer.disk.counters.running_writes = 413;
        before.buffer.disk.counters.completed_writes = 172;
        after.buffer.disk.counters.completed_writes = 416;
        before.buffer.disk.counters.write_errors = 173;
        after.buffer.disk.counters.write_errors = 419;
        before.mandatory.operation.submitted_count = 174;
        after.mandatory.operation.submitted_count = 422;
        before.mandatory.operation.started_count = 175;
        after.mandatory.operation.started_count = 425;
        before.mandatory.operation.completed_count = 176;
        after.mandatory.operation.completed_count = 428;
        before.mandatory.operation.error_count = 177;
        after.mandatory.operation.error_count = 431;
        before.mandatory.operation.panic_count = 178;
        after.mandatory.operation.panic_count = 434;
        before.mandatory.operation.detached_observer_count = 179;
        after.mandatory.operation.detached_observer_count = 437;
        before.mandatory.operation.active_count = 180;
        after.mandatory.operation.active_count = 440;
        before.mandatory.operation.admission_wait_nanos = 181;
        after.mandatory.operation.admission_wait_nanos = 443;
        before.mandatory.operation.queue_wait_nanos = 182;
        after.mandatory.operation.queue_wait_nanos = 446;
        before.mandatory.operation.execution_nanos = 183;
        after.mandatory.operation.execution_nanos = 449;
        before.mandatory.transaction_cleanup.submitted_count = 184;
        after.mandatory.transaction_cleanup.submitted_count = 452;
        before.mandatory.transaction_cleanup.started_count = 185;
        after.mandatory.transaction_cleanup.started_count = 455;
        before.mandatory.transaction_cleanup.completed_count = 186;
        after.mandatory.transaction_cleanup.completed_count = 458;
        before.mandatory.transaction_cleanup.error_count = 187;
        after.mandatory.transaction_cleanup.error_count = 461;
        before.mandatory.transaction_cleanup.panic_count = 188;
        after.mandatory.transaction_cleanup.panic_count = 464;
        before.mandatory.transaction_cleanup.detached_observer_count = 189;
        after.mandatory.transaction_cleanup.detached_observer_count = 467;
        before.mandatory.transaction_cleanup.active_count = 190;
        after.mandatory.transaction_cleanup.active_count = 470;
        before.mandatory.transaction_cleanup.admission_wait_nanos = 191;
        after.mandatory.transaction_cleanup.admission_wait_nanos = 473;
        before.mandatory.transaction_cleanup.queue_wait_nanos = 192;
        after.mandatory.transaction_cleanup.queue_wait_nanos = 476;
        before.mandatory.transaction_cleanup.execution_nanos = 193;
        after.mandatory.transaction_cleanup.execution_nanos = 479;
        before.logical_lock.owner_local_exact_covered_hits = 194;
        after.logical_lock.owner_local_exact_covered_hits = 482;
        before.logical_lock.owner_local_covered_publications = 195;
        after.logical_lock.owner_local_covered_publications = 485;
        before.logical_lock.owner_local_mode_preserving_conversions = 196;
        after.logical_lock.owner_local_mode_preserving_conversions = 488;
        before.logical_lock.owner_local_mode_preserving_releases = 197;
        after.logical_lock.owner_local_mode_preserving_releases = 491;
        before.logical_lock.resource_transitions = 198;
        after.logical_lock.resource_transitions = 494;
        before.logical_lock.mode_slots_examined = 199;
        after.logical_lock.mode_slots_examined = 497;
        before.logical_lock.immediate_physical_acquisitions = 200;
        after.logical_lock.immediate_physical_acquisitions = 500;
        before.logical_lock.physical_upgrades = 201;
        after.logical_lock.physical_upgrades = 503;
        before.logical_lock.enqueued_waiters = 202;
        after.logical_lock.enqueued_waiters = 506;
        before.logical_lock.queue_link_mutations = 203;
        after.logical_lock.queue_link_mutations = 509;
        before.logical_lock.cancelled_head_waiters = 204;
        after.logical_lock.cancelled_head_waiters = 512;
        before.logical_lock.cancelled_middle_waiters = 205;
        after.logical_lock.cancelled_middle_waiters = 515;
        before.logical_lock.cancelled_tail_waiters = 206;
        after.logical_lock.cancelled_tail_waiters = 518;
        before.logical_lock.provisional_observations = 207;
        after.logical_lock.provisional_observations = 521;
        before.logical_lock.promoted_waiters = 208;
        after.logical_lock.promoted_waiters = 524;
        before.logical_lock.scope_close_claims_visited = 209;
        after.logical_lock.scope_close_claims_visited = 527;
        before.logical_lock.scope_close_physical_changes = 210;
        after.logical_lock.scope_close_physical_changes = 530;
        before.logical_lock.completion_allocations = 211;
        after.logical_lock.completion_allocations = 533;
        before.logical_lock.waiter_slab_growths = 212;
        after.logical_lock.waiter_slab_growths = 536;
        before.logical_lock.waiter_slab_reuses = 213;
        after.logical_lock.waiter_slab_reuses = 539;
        before.logical_lock.current_physical_resources = 214;
        after.logical_lock.current_physical_resources = 542;
        before.logical_lock.peak_physical_resources = 215;
        after.logical_lock.peak_physical_resources = 545;
        before.logical_lock.current_physical_families = 216;
        after.logical_lock.current_physical_families = 548;
        before.logical_lock.peak_physical_families = 217;
        after.logical_lock.peak_physical_families = 551;
        before.logical_lock.current_linked_waiters = 218;
        after.logical_lock.current_linked_waiters = 554;
        before.logical_lock.peak_linked_waiters = 219;
        after.logical_lock.peak_linked_waiters = 557;
        before.logical_lock.current_live_waiter_nodes = 220;
        after.logical_lock.current_live_waiter_nodes = 560;
        before.logical_lock.peak_live_waiter_nodes = 221;
        after.logical_lock.peak_live_waiter_nodes = 563;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .capture_elapsed_nanos = 222;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .capture_elapsed_nanos = 566;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .extraction_worker_time_nanos = 223;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .extraction_worker_time_nanos = 569;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .sort_worker_time_nanos = 224;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .sort_worker_time_nanos = 572;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .duplicate_worker_time_nanos = 225;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .duplicate_worker_time_nanos = 575;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .max_job_elapsed_nanos = 226;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .max_job_elapsed_nanos = 578;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .max_sort_elapsed_nanos = 227;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .max_sort_elapsed_nanos = 581;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .extraction_wall_elapsed_nanos = 228;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .extraction_wall_elapsed_nanos = 584;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .sort_wall_elapsed_nanos = 229;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .sort_wall_elapsed_nanos = 587;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .duplicate_wall_elapsed_nanos = 230;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .duplicate_wall_elapsed_nanos = 590;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .pipeline_wall_elapsed_nanos = 231;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .pipeline_wall_elapsed_nanos = 593;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .total_elapsed_nanos = 232;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .total_elapsed_nanos = 596;
        before
            .hot_index_build
            .create
            .hot
            .extraction
            .scratch_peak_bytes = 233;
        after
            .hot_index_build
            .create
            .hot
            .extraction
            .scratch_peak_bytes = 599;
        before.hot_index_build.create.hot.extraction.source_pages = 234;
        after.hot_index_build.create.hot.extraction.source_pages = 602;
        before.hot_index_build.create.hot.extraction.entries = 235;
        after.hot_index_build.create.hot.extraction.entries = 605;
        before.hot_index_build.create.hot.extraction.workers = 236;
        after.hot_index_build.create.hot.extraction.workers = 608;
        before.hot_index_build.create.hot.extraction.page_target = 237;
        after.hot_index_build.create.hot.extraction.page_target = 611;
        before.hot_index_build.create.hot.extraction.planned_groups = 238;
        after.hot_index_build.create.hot.extraction.planned_groups = 614;
        before.hot_index_build.create.hot.extraction.nonempty_runs = 239;
        after.hot_index_build.create.hot.extraction.nonempty_runs = 617;
        before.hot_index_build.create.hot.merge.entries = 240;
        after.hot_index_build.create.hot.merge.entries = 620;
        before.hot_index_build.create.hot.merge.runs = 241;
        after.hot_index_build.create.hot.merge.runs = 623;
        before.hot_index_build.create.hot.merge.workers = 242;
        after.hot_index_build.create.hot.merge.workers = 626;
        before.hot_index_build.create.hot.merge.partitions = 243;
        after.hot_index_build.create.hot.merge.partitions = 629;
        before.hot_index_build.create.hot.merge.batch_entries = 244;
        after.hot_index_build.create.hot.merge.batch_entries = 632;
        before.hot_index_build.create.hot.merge.boundary_wall_nanos = 245;
        after.hot_index_build.create.hot.merge.boundary_wall_nanos = 635;
        before.hot_index_build.create.hot.merge.cut_worker_nanos = 246;
        after.hot_index_build.create.hot.merge.cut_worker_nanos = 638;
        before.hot_index_build.create.hot.merge.max_cut_nanos = 247;
        after.hot_index_build.create.hot.merge.max_cut_nanos = 641;
        before
            .hot_index_build
            .create
            .hot
            .merge
            .consumption_wall_nanos = 248;
        after
            .hot_index_build
            .create
            .hot
            .merge
            .consumption_wall_nanos = 644;
        before.hot_index_build.create.hot.merge.first_batch_nanos = 249;
        after.hot_index_build.create.hot.merge.first_batch_nanos = 647;
        before.hot_index_build.create.hot.merge.merge_check_nanos = 250;
        after.hot_index_build.create.hot.merge.merge_check_nanos = 650;
        before.hot_index_build.create.hot.merge.max_batch_nanos = 251;
        after.hot_index_build.create.hot.merge.max_batch_nanos = 653;
        before.hot_index_build.create.hot.merge.max_job_nanos = 252;
        after.hot_index_build.create.hot.merge.max_job_nanos = 656;
        before.hot_index_build.create.hot.merge.job_worker_nanos = 253;
        after.hot_index_build.create.hot.merge.job_worker_nanos = 659;
        before
            .hot_index_build
            .create
            .hot
            .merge
            .consumer_worker_nanos = 254;
        after.hot_index_build.create.hot.merge.consumer_worker_nanos = 662;
        before
            .hot_index_build
            .create
            .hot
            .merge
            .duplicate_comparisons = 255;
        after.hot_index_build.create.hot.merge.duplicate_comparisons = 665;
        before.hot_index_build.create.hot.merge.boundary_bytes = 256;
        after.hot_index_build.create.hot.merge.boundary_bytes = 668;
        before.hot_index_build.create.hot.merge.max_reference_bytes = 257;
        after.hot_index_build.create.hot.merge.max_reference_bytes = 671;
        before
            .hot_index_build
            .create
            .hot
            .merge
            .active_reference_bytes = 258;
        after
            .hot_index_build
            .create
            .hot
            .merge
            .active_reference_bytes = 674;
        before.hot_index_build.create.hot.merge.validation_bytes = 259;
        after.hot_index_build.create.hot.merge.validation_bytes = 677;
        before.hot_index_build.create.hot.merge.scratch_peak_bytes = 260;
        after.hot_index_build.create.hot.merge.scratch_peak_bytes = 680;
        before.hot_index_build.create.hot.completed_builds = 261;
        after.hot_index_build.create.hot.completed_builds = 683;
        before.hot_index_build.create.hot.capture_elapsed_nanos = 262;
        after.hot_index_build.create.hot.capture_elapsed_nanos = 686;
        before.hot_index_build.create.hot.leaf_pages = 263;
        after.hot_index_build.create.hot.leaf_pages = 689;
        before.hot_index_build.create.hot.branch_pages = 264;
        after.hot_index_build.create.hot.branch_pages = 692;
        before.hot_index_build.create.hot.leaf_occupied_bytes = 265;
        after.hot_index_build.create.hot.leaf_occupied_bytes = 695;
        before.hot_index_build.create.hot.branch_occupied_bytes = 266;
        after.hot_index_build.create.hot.branch_occupied_bytes = 698;
        before.hot_index_build.create.hot.allocation_nanos = 267;
        after.hot_index_build.create.hot.allocation_nanos = 701;
        before.hot_index_build.create.hot.packing_nanos = 268;
        after.hot_index_build.create.hot.packing_nanos = 704;
        before.hot_index_build.create.hot.leaf_planning_nanos = 269;
        after.hot_index_build.create.hot.leaf_planning_nanos = 707;
        before.hot_index_build.create.hot.parent_planning_nanos = 270;
        after.hot_index_build.create.hot.parent_planning_nanos = 710;
        before.hot_index_build.create.hot.direct_parent_nanos = 271;
        after.hot_index_build.create.hot.direct_parent_nanos = 713;
        before.hot_index_build.create.hot.serial_upper_nanos = 272;
        after.hot_index_build.create.hot.serial_upper_nanos = 716;
        before.hot_index_build.create.hot.install_nanos = 273;
        after.hot_index_build.create.hot.install_nanos = 719;
        before.hot_index_build.create.hot.cleanup_nanos = 274;
        after.hot_index_build.create.hot.cleanup_nanos = 722;
        before.hot_index_build.create.hot.max_sync_nanos = 275;
        after.hot_index_build.create.hot.max_sync_nanos = 725;
        before.hot_index_build.create.hot.max_job_nanos = 276;
        after.hot_index_build.create.hot.max_job_nanos = 728;
        before.hot_index_build.create.hot.scratch_peak_bytes = 277;
        after.hot_index_build.create.hot.scratch_peak_bytes = 731;
        before.hot_index_build.create.cold_entries = 278;
        after.hot_index_build.create.cold_entries = 734;
        before.hot_index_build.create.retained_cold_bytes = 279;
        after.hot_index_build.create.retained_cold_bytes = 737;
        before.hot_index_build.create.cold_collect_nanos = 280;
        after.hot_index_build.create.cold_collect_nanos = 740;
        before.hot_index_build.create.cold_sort_nanos = 281;
        after.hot_index_build.create.cold_sort_nanos = 743;
        before.hot_index_build.create.cold_build_nanos = 282;
        after.hot_index_build.create.cold_build_nanos = 746;
        before.hot_index_build.create.cold_hot_comparisons = 283;
        after.hot_index_build.create.cold_hot_comparisons = 749;
        before.hot_index_build.create.cold_hot_worker_nanos = 284;
        after.hot_index_build.create.cold_hot_worker_nanos = 752;
        before.hot_index_build.create.max_cold_hot_sync_nanos = 285;
        after.hot_index_build.create.max_cold_hot_sync_nanos = 755;
        before.hot_index_build.create.total_elapsed_nanos = 286;
        after.hot_index_build.create.total_elapsed_nanos = 758;
        before.hot_index_build.completed_builds = 287;
        after.hot_index_build.completed_builds = 761;
        before.hot_index_build.source_pages = 288;
        after.hot_index_build.source_pages = 764;
        before.hot_index_build.entries = 289;
        after.hot_index_build.entries = 767;
        before.hot_index_build.planned_groups = 290;
        after.hot_index_build.planned_groups = 770;
        before.hot_index_build.nonempty_runs = 291;
        after.hot_index_build.nonempty_runs = 773;
        before.hot_index_build.capture_elapsed_nanos = 292;
        after.hot_index_build.capture_elapsed_nanos = 776;
        before.hot_index_build.extraction_worker_time_nanos = 293;
        after.hot_index_build.extraction_worker_time_nanos = 779;
        before.hot_index_build.sort_worker_time_nanos = 294;
        after.hot_index_build.sort_worker_time_nanos = 782;
        before.hot_index_build.duplicate_worker_time_nanos = 295;
        after.hot_index_build.duplicate_worker_time_nanos = 785;
        before.hot_index_build.extraction_wall_elapsed_nanos = 296;
        after.hot_index_build.extraction_wall_elapsed_nanos = 788;
        before.hot_index_build.sort_wall_elapsed_nanos = 297;
        after.hot_index_build.sort_wall_elapsed_nanos = 791;
        before.hot_index_build.duplicate_wall_elapsed_nanos = 298;
        after.hot_index_build.duplicate_wall_elapsed_nanos = 794;
        before.hot_index_build.pipeline_wall_elapsed_nanos = 299;
        after.hot_index_build.pipeline_wall_elapsed_nanos = 797;
        before.hot_index_build.total_elapsed_nanos = 300;
        after.hot_index_build.total_elapsed_nanos = 800;
        before.hot_index_build.max_job_elapsed_nanos = 301;
        after.hot_index_build.max_job_elapsed_nanos = 803;
        before.hot_index_build.max_sort_elapsed_nanos = 302;
        after.hot_index_build.max_sort_elapsed_nanos = 806;
        before.hot_index_build.scratch_peak_bytes = 303;
        after.hot_index_build.scratch_peak_bytes = 809;
        let delta = plan_internal_metrics(&before, &after);
        assert_eq!(delta.len(), 202);
        let cumulative = cumulative_internal_metrics(&after);
        let mut rendered = String::new();
        for (name, metrics) in [("delta", delta), ("cumulative", cumulative)] {
            rendered.push_str(name);
            rendered.push('\n');
            for metric in metrics {
                rendered.push_str(&format!(
                    "{} {} {:?} {:?}\n",
                    metric.name, metric.value, metric.kind, metric.unit
                ));
            }
        }
        assert_eq!(rendered, include_str!("metrics-baseline.txt"));
    }

    /// Purpose: Preserve saturating general counters across reset snapshots and omit idle build families.
    /// Expected: Regressing general counters emit zero while gauges retain end values and unchanged build completions emit no build metrics.
    #[test]
    fn general_deltas_saturate_without_fabricating_build_work() {
        let mut before = InternalStatsSnapshot::default();
        before.trx.commit_count = 9;
        before.logical_lock.resource_transitions = 7;
        before.buffer.mem.capacity = 100;
        let mut after = InternalStatsSnapshot::default();
        after.trx.commit_count = 2;
        after.logical_lock.resource_transitions = 3;
        after.buffer.mem.capacity = 50;
        let metrics = after.delta_since(&before);
        for name in [
            "transaction.commit_count",
            "logical_lock.resource_transitions",
        ] {
            let metric = metrics.iter().find(|metric| metric.name == name).unwrap();
            assert_eq!(metric.value, 0, "{name}");
            assert_eq!(metric.kind, InternalMetricKind::CounterDelta, "{name}");
        }
        let capacity = metrics
            .iter()
            .find(|metric| metric.name == "buffer.mem.capacity")
            .unwrap();
        assert_eq!(
            (capacity.value, capacity.kind),
            (50, InternalMetricKind::EndGauge)
        );
        assert!(
            metrics
                .iter()
                .all(|metric| !metric.name.starts_with("hot_index_build.")
                    && !metric.name.starts_with("create_index."))
        );
    }
}
