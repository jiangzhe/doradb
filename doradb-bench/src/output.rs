use crate::error::Result;
use crate::measurement::{InternalMetric, InternalMetricKind, InternalMetricUnit};
use doradb_storage::{
    BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats, LogicalLockStats,
    MandatoryRuntimeStats, MandatoryTaskStats, Session, StorageIoStats, TransactionSystemStats,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InternalStatsSnapshot {
    trx: TransactionSystemStats,
    storage: StorageIoStats,
    buffer: BufferPoolStats,
    mandatory: MandatoryRuntimeStats,
    logical_lock: LogicalLockStats,
}

impl InternalStatsSnapshot {
    fn capture(session: &Session) -> Result<Self> {
        Ok(Self {
            trx: session.transaction_system_stats()?,
            storage: session.storage_io_stats()?,
            buffer: session.buffer_pool_stats()?,
            mandatory: session.mandatory_runtime_stats()?,
            logical_lock: session.logical_lock_stats()?,
        })
    }
}

struct Metric {
    name: String,
    value: u128,
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
            } else if metric.name.starts_with("logical_lock.peak_") {
                InternalMetricKind::LifetimePeak
            } else {
                InternalMetricKind::CounterDelta
            };
            let unit = if metric.name == "transaction.log_bytes" {
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

fn internal_metrics(before: &InternalStatsSnapshot, after: &InternalStatsSnapshot) -> Vec<Metric> {
    let mut metrics = Vec::new();
    push_transaction_metrics(&mut metrics, before.trx, after.trx);
    push_storage_metrics(&mut metrics, before.storage, after.storage);
    push_buffer_metrics(&mut metrics, &before.buffer, &after.buffer);
    push_mandatory_metrics(&mut metrics, before.mandatory, after.mandatory);
    push_logical_lock_metrics(&mut metrics, before.logical_lock, after.logical_lock);
    metrics
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
        u128::from(after.current_physical_resources),
    );
    push_metric(
        metrics,
        "logical_lock.peak_physical_resources",
        u128::from(after.peak_physical_resources),
    );
    push_metric(
        metrics,
        "logical_lock.current_physical_families",
        u128::from(after.current_physical_families),
    );
    push_metric(
        metrics,
        "logical_lock.peak_physical_families",
        u128::from(after.peak_physical_families),
    );
    push_metric(
        metrics,
        "logical_lock.current_linked_waiters",
        u128::from(after.current_linked_waiters),
    );
    push_metric(
        metrics,
        "logical_lock.peak_linked_waiters",
        u128::from(after.peak_linked_waiters),
    );
    push_metric(
        metrics,
        "logical_lock.current_live_waiter_nodes",
        u128::from(after.current_live_waiter_nodes),
    );
    push_metric(
        metrics,
        "logical_lock.peak_live_waiter_nodes",
        u128::from(after.peak_live_waiter_nodes),
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
        ("active_count", after.active_count as u128),
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
        after.capacity as u128,
    );
    push_metric(
        metrics,
        &format!("{prefix}.allocated"),
        after.allocated as u128,
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

fn push_metric(metrics: &mut Vec<Metric>, name: &str, value: u128) {
    metrics.push(Metric {
        name: name.to_owned(),
        value,
    });
}

fn delta(after: usize, before: usize) -> u128 {
    after.saturating_sub(before) as u128
}

fn delta_u64(after: u64, before: u64) -> u128 {
    u128::from(after.saturating_sub(before))
}
