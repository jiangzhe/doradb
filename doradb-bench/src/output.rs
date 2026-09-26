use crate::error::Result;
use crate::measurement::{InternalMetric, InternalMetricKind, InternalMetricUnit};
#[cfg(feature = "profiling")]
use doradb_storage::profiling::HotIndexBuildStats;
use doradb_storage::{
    BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats, LogicalLockStats,
    MandatoryRuntimeStats, MandatoryTaskStats, Session, StorageIoStats, TransactionSystemStats,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct InternalStatsSnapshot {
    trx: TransactionSystemStats,
    storage: StorageIoStats,
    buffer: BufferPoolStats,
    mandatory: MandatoryRuntimeStats,
    logical_lock: LogicalLockStats,
    #[cfg(feature = "profiling")]
    hot_index_build: HotIndexBuildStats,
}

impl InternalStatsSnapshot {
    fn capture(session: &Session) -> Result<Self> {
        Ok(Self {
            trx: session.transaction_system_stats()?,
            storage: session.storage_io_stats()?,
            buffer: session.buffer_pool_stats()?,
            mandatory: session.mandatory_runtime_stats()?,
            logical_lock: session.logical_lock_stats()?,
            #[cfg(feature = "profiling")]
            hot_index_build: session.hot_index_build_stats()?,
        })
    }
}

struct Metric {
    name: String,
    value: u64,
}

/// Capture the public engine diagnostics used by plan mode.
pub(crate) fn capture_internal_stats(session: &Session) -> Result<InternalStatsSnapshot> {
    InternalStatsSnapshot::capture(session)
}

/// Translate public diagnostics into typed plan metrics in stable order.
pub(crate) fn plan_internal_metrics(
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
                || (cfg!(feature = "profiling")
                    && (metric.name.starts_with("hot_index_build.max_")
                        || metric.name == "hot_index_build.scratch_peak_bytes"))
            {
                InternalMetricKind::LifetimePeak
            } else {
                InternalMetricKind::CounterDelta
            };
            let unit = if metric.name == "transaction.log_bytes"
                || (cfg!(feature = "profiling")
                    && metric.name == "hot_index_build.scratch_peak_bytes")
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
pub(crate) fn cumulative_internal_metrics(snapshot: &InternalStatsSnapshot) -> Vec<InternalMetric> {
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
    #[cfg(feature = "profiling")]
    push_hot_index_build_metrics(&mut metrics, before.hot_index_build, after.hot_index_build);
    metrics
}

#[cfg(feature = "profiling")]
fn push_hot_index_build_metrics(
    metrics: &mut Vec<Metric>,
    before: HotIndexBuildStats,
    after: HotIndexBuildStats,
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
    #[cfg(feature = "profiling")]
    use super::{InternalStatsSnapshot, cumulative_internal_metrics, plan_internal_metrics};
    #[cfg(feature = "profiling")]
    use crate::measurement::{InternalMetricKind, InternalMetricUnit};
    #[cfg(feature = "profiling")]
    use doradb_storage::profiling::HotIndexBuildStats;

    /// Purpose: Preserve hot-build delta, lifetime-peak, and fresh-engine metric semantics.
    /// Expected: Empty intervals emit no profile; counts/times subtract while maxima retain absolute values and correct units.
    #[cfg(feature = "profiling")]
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
}
