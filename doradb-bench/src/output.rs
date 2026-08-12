use crate::cli::{LockTableMode, LockTableScenario, LogSyncMode, TableLockScope, Workload};
use crate::error::{BenchError, Result};
use crate::fixture::IndexMode;
use crate::manifest::{internal_stats_csv_path, result_csv_path, result_markdown_path};
use crate::measurement::{InternalMetric, InternalMetricKind, InternalMetricUnit};
use doradb_storage::{
    BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats, LogicalLockStats,
    MandatoryRuntimeStats, MandatoryTaskStats, Session, StorageIoStats, TransactionSystemStats,
};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InternalStatsSnapshot {
    trx: TransactionSystemStats,
    storage: StorageIoStats,
    buffer: BufferPoolStats,
    mandatory: MandatoryRuntimeStats,
    logical_lock: LogicalLockStats,
}

impl InternalStatsSnapshot {
    pub(super) fn capture(session: &Session) -> Result<Self> {
        Ok(Self {
            trx: session.transaction_system_stats()?,
            storage: session.storage_io_stats()?,
            buffer: session.buffer_pool_stats()?,
            mandatory: session.mandatory_runtime_stats()?,
            logical_lock: session.logical_lock_stats()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Metric {
    name: String,
    value: u128,
}

#[derive(Clone, Debug)]
pub(super) struct OutputConfig {
    pub(super) workload: Workload,
    pub(super) storage_root: PathBuf,
    pub(super) num: u64,
    pub(super) range: Option<u64>,
    pub(super) value_size: usize,
    pub(super) batch_size: u64,
    pub(super) rand: bool,
    pub(super) seed: u64,
    pub(super) index: IndexMode,
    pub(super) loaded_key_start: u64,
    pub(super) loaded_key_end: u64,
    pub(super) threads: usize,
    pub(super) sessions: usize,
    pub(super) log_sync: LogSyncMode,
    pub(super) include_stats: bool,
    pub(super) table_id: u64,
    pub(super) scope: Option<TableLockScope>,
    pub(super) unlock: Option<bool>,
    pub(super) tables: Option<usize>,
    pub(super) scenario: Option<LockTableScenario>,
    pub(super) lock_mode: Option<LockTableMode>,
    pub(super) width: Option<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct BenchmarkResult {
    operations: u64,
    inserted_rows: u64,
    found: u64,
    not_found: u64,
    rows_returned: u64,
    elapsed: Duration,
    operations_per_second: f64,
    average_nanos_per_operation: f64,
    failures: u64,
}

impl BenchmarkResult {
    pub(super) fn new(
        operations: u64,
        inserted_rows: u64,
        found: u64,
        not_found: u64,
        rows_returned: u64,
        elapsed: Duration,
        failures: u64,
    ) -> Self {
        let elapsed_seconds = elapsed.as_secs_f64();
        let elapsed_nanos = elapsed.as_nanos() as f64;
        let operations_f64 = operations as f64;
        Self {
            operations,
            inserted_rows,
            found,
            not_found,
            rows_returned,
            elapsed,
            operations_per_second: if elapsed_seconds > 0.0 {
                operations_f64 / elapsed_seconds
            } else {
                0.0
            },
            average_nanos_per_operation: if operations == 0 {
                0.0
            } else {
                elapsed_nanos / operations_f64
            },
            failures,
        }
    }
}

struct OutputArtifact {
    path: PathBuf,
    staged_path: PathBuf,
    contents: String,
}

impl OutputArtifact {
    fn new(path: PathBuf, contents: String) -> Self {
        let staged_path = staged_output_path(&path);
        Self {
            path,
            staged_path,
            contents,
        }
    }
}

/// Capture the public engine diagnostics used by plan mode.
pub(crate) fn capture_internal_stats(session: &Session) -> Result<InternalStatsSnapshot> {
    InternalStatsSnapshot::capture(session)
}

/// Build typed plan-mode diagnostics while preserving legacy metric ordering.
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

pub(super) fn internal_metrics(
    before: &InternalStatsSnapshot,
    after: &InternalStatsSnapshot,
) -> Vec<Metric> {
    let mut metrics = Vec::new();
    push_transaction_metrics(&mut metrics, before.trx, after.trx);
    push_storage_metrics(&mut metrics, before.storage, after.storage);
    push_buffer_metrics(&mut metrics, &before.buffer, &after.buffer);
    push_mandatory_metrics(&mut metrics, before.mandatory, after.mandatory);
    push_logical_lock_metrics(&mut metrics, before.logical_lock, after.logical_lock);
    metrics
}

pub(super) fn write_benchmark_outputs(
    config: &OutputConfig,
    metrics: &[Metric],
    result: &BenchmarkResult,
    command_context: &str,
) -> Result<()> {
    print_stdout(config, metrics, result);
    let stats_path = internal_stats_csv_path(&config.storage_root);
    let staged_stats_path = staged_output_path(&stats_path);
    if !config.include_stats {
        remove_file_if_exists(&staged_stats_path)?;
    }

    let mut artifacts = vec![OutputArtifact::new(
        result_markdown_path(&config.storage_root),
        render_markdown(config, metrics, result, command_context),
    )];
    if config.include_stats {
        artifacts.push(OutputArtifact::new(
            stats_path.clone(),
            render_internal_stats_csv(metrics),
        ));
    }
    artifacts.push(OutputArtifact::new(
        result_csv_path(&config.storage_root),
        render_result_csv(config, result),
    ));

    write_staged_outputs(&artifacts)?;
    if !config.include_stats {
        remove_file_if_exists(&stats_path)?;
        remove_file_if_exists(&staged_stats_path)?;
    }
    Ok(())
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

fn write_staged_outputs(artifacts: &[OutputArtifact]) -> Result<()> {
    let result = (|| {
        for artifact in artifacts {
            fs::write(&artifact.staged_path, &artifact.contents).map_err(|err| {
                BenchError::message(format!(
                    "failed to write benchmark output {}: {err}",
                    artifact.staged_path.display()
                ))
            })?;
        }
        for artifact in artifacts {
            fs::rename(&artifact.staged_path, &artifact.path).map_err(|err| {
                BenchError::message(format!(
                    "failed to install benchmark output {}: {err}",
                    artifact.path.display()
                ))
            })?;
        }
        Ok(())
    })();
    if result.is_err() {
        cleanup_staged_outputs(artifacts);
    }
    result
}

fn cleanup_staged_outputs(artifacts: &[OutputArtifact]) {
    for artifact in artifacts {
        let _ = fs::remove_file(&artifact.staged_path);
    }
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BenchError::message(format!(
            "failed to remove benchmark output {}: {err}",
            path.display()
        ))),
    }
}

fn staged_output_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "benchmark-output".into());
    path.with_file_name(format!(".{file_name}.tmp"))
}

fn print_stdout(config: &OutputConfig, metrics: &[Metric], result: &BenchmarkResult) {
    println!("Configuration");
    for (name, value) in configuration_pairs(config) {
        println!("{name}: {value}");
    }
    println!();
    if config.include_stats {
        println!("Internal Stats");
        for metric in metrics {
            println!("{}: {}", metric.name, metric.value);
        }
        println!();
    }
    println!("Final Result");
    println!("operations: {}", result.operations);
    println!("inserted_rows: {}", result.inserted_rows);
    println!("found: {}", result.found);
    println!("not_found: {}", result.not_found);
    println!("rows_returned: {}", result.rows_returned);
    println!("elapsed_nanos: {}", result.elapsed.as_nanos());
    println!("operations_per_second: {:.3}", result.operations_per_second);
    println!(
        "average_nanos_per_operation: {:.3}",
        result.average_nanos_per_operation
    );
    println!("failures: {}", result.failures);
}

fn render_markdown(
    config: &OutputConfig,
    metrics: &[Metric],
    result: &BenchmarkResult,
    command_context: &str,
) -> String {
    let mut out = String::from("# DoraDB Benchmark Result\n\n## Configuration\n\n");
    for (name, value) in configuration_pairs(config) {
        out.push_str("- `");
        out.push_str(&name);
        out.push_str("`: ");
        out.push_str(&value);
        out.push('\n');
    }
    out.push_str("- `command`: `");
    out.push_str(command_context);
    out.push('`');
    if config.include_stats {
        out.push_str("\n\n## Internal Stats\n\n");
        for metric in metrics {
            out.push_str("- `");
            out.push_str(&metric.name);
            out.push_str("`: ");
            out.push_str(&metric.value.to_string());
            out.push('\n');
        }
    }
    out.push_str("\n## Final Result\n\n");
    out.push_str(&format!(
        "- `operations`: {}\n- `inserted_rows`: {}\n- `found`: {}\n- `not_found`: {}\n- `rows_returned`: {}\n- `elapsed_nanos`: {}\n- `operations_per_second`: {:.3}\n- `average_nanos_per_operation`: {:.3}\n- `failures`: {}\n",
        result.operations,
        result.inserted_rows,
        result.found,
        result.not_found,
        result.rows_returned,
        result.elapsed.as_nanos(),
        result.operations_per_second,
        result.average_nanos_per_operation,
        result.failures
    ));
    out
}

fn render_internal_stats_csv(metrics: &[Metric]) -> String {
    let mut out = String::from("metric-name,metric-value\n");
    for metric in metrics {
        out.push_str(&csv_escape(&metric.name));
        out.push(',');
        out.push_str(&metric.value.to_string());
        out.push('\n');
    }
    out
}

fn render_result_csv(config: &OutputConfig, result: &BenchmarkResult) -> String {
    let header = [
        "workload",
        "rand",
        "include_stats",
        "storage_root",
        "num",
        "range",
        "value_size",
        "batch_size",
        "seed",
        "prepared_index",
        "loaded_key_start",
        "loaded_key_end",
        "threads",
        "sessions",
        "log_sync",
        "table_id",
        "scope",
        "unlock",
        "tables",
        "operations",
        "inserted_rows",
        "found",
        "not_found",
        "rows_returned",
        "elapsed_nanos",
        "operations_per_second",
        "average_nanos_per_operation",
        "failures",
    ];
    let row = [
        config.workload.to_string(),
        config.rand.to_string(),
        config.include_stats.to_string(),
        config.storage_root.display().to_string(),
        config.num.to_string(),
        config
            .range
            .map(|range| range.to_string())
            .unwrap_or_default(),
        config.value_size.to_string(),
        config.batch_size.to_string(),
        config.seed.to_string(),
        config.index.to_string(),
        config.loaded_key_start.to_string(),
        config.loaded_key_end.to_string(),
        config.threads.to_string(),
        config.sessions.to_string(),
        config.log_sync.to_string(),
        config.table_id.to_string(),
        config
            .scope
            .map(|scope| scope.to_string())
            .unwrap_or_default(),
        config
            .unlock
            .map(|unlock| unlock.to_string())
            .unwrap_or_default(),
        config
            .tables
            .map(|tables| tables.to_string())
            .unwrap_or_default(),
        result.operations.to_string(),
        result.inserted_rows.to_string(),
        result.found.to_string(),
        result.not_found.to_string(),
        result.rows_returned.to_string(),
        result.elapsed.as_nanos().to_string(),
        format!("{:.3}", result.operations_per_second),
        format!("{:.3}", result.average_nanos_per_operation),
        result.failures.to_string(),
    ];
    format!(
        "{}\n{}\n",
        header.map(csv_escape).join(","),
        row.map(|value| csv_escape(&value)).join(",")
    )
}

fn configuration_pairs(config: &OutputConfig) -> Vec<(String, String)> {
    let mut pairs = vec![
        ("workload".to_owned(), config.workload.to_string()),
        ("rand".to_owned(), config.rand.to_string()),
        ("include_stats".to_owned(), config.include_stats.to_string()),
        (
            "storage_root".to_owned(),
            config.storage_root.display().to_string(),
        ),
        ("num".to_owned(), config.num.to_string()),
    ];
    if let Some(range) = config.range {
        pairs.push(("range".to_owned(), range.to_string()));
    }
    pairs.extend([
        ("value_size".to_owned(), config.value_size.to_string()),
        ("batch_size".to_owned(), config.batch_size.to_string()),
        ("seed".to_owned(), config.seed.to_string()),
        ("prepared_index".to_owned(), config.index.to_string()),
        (
            "loaded_key_range".to_owned(),
            format!("[{}, {})", config.loaded_key_start, config.loaded_key_end),
        ),
        ("threads".to_owned(), config.threads.to_string()),
        ("sessions".to_owned(), config.sessions.to_string()),
        ("log_sync".to_owned(), config.log_sync.to_string()),
        ("table_id".to_owned(), config.table_id.to_string()),
    ]);
    if let Some(scope) = config.scope {
        pairs.push(("scope".to_owned(), scope.to_string()));
    }
    if let Some(unlock) = config.unlock {
        pairs.push(("unlock".to_owned(), unlock.to_string()));
    }
    if let Some(tables) = config.tables {
        pairs.push(("tables".to_owned(), tables.to_string()));
    }
    if let Some(scenario) = config.scenario {
        pairs.push(("scenario".to_owned(), scenario.to_string()));
    }
    if let Some(mode) = config.lock_mode {
        pairs.push(("lock_mode".to_owned(), mode.to_string()));
    }
    if let Some(width) = config.width {
        pairs.push(("width".to_owned(), width.to_string()));
    }
    pairs
}

fn push_transaction_metrics(
    metrics: &mut Vec<Metric>,
    before: TransactionSystemStats,
    after: TransactionSystemStats,
) {
    push_metric(
        metrics,
        "transaction.commit_count",
        delta(after.commit_count, before.commit_count),
    );
    push_metric(
        metrics,
        "transaction.trx_count",
        delta(after.trx_count, before.trx_count),
    );
    push_metric(
        metrics,
        "transaction.log_bytes",
        delta(after.log_bytes, before.log_bytes),
    );
    push_metric(
        metrics,
        "transaction.sync_count",
        delta(after.sync_count, before.sync_count),
    );
    push_metric(
        metrics,
        "transaction.sync_nanos",
        delta(after.sync_nanos, before.sync_nanos),
    );
    push_metric(
        metrics,
        "transaction.seal_failure_count",
        delta(after.seal_failure_count, before.seal_failure_count),
    );
    push_metric(
        metrics,
        "transaction.io_submit_and_wait_count",
        delta(
            after.io_submit_and_wait_count,
            before.io_submit_and_wait_count,
        ),
    );
    push_metric(
        metrics,
        "transaction.io_submit_and_wait_nanos",
        delta(
            after.io_submit_and_wait_nanos,
            before.io_submit_and_wait_nanos,
        ),
    );
    push_metric(
        metrics,
        "transaction.purge_trx_count",
        delta(after.purge_trx_count, before.purge_trx_count),
    );
    push_metric(
        metrics,
        "transaction.purge_row_count",
        delta(after.purge_row_count, before.purge_row_count),
    );
    push_metric(
        metrics,
        "transaction.purge_index_count",
        delta(after.purge_index_count, before.purge_index_count),
    );
}

fn push_storage_metrics(metrics: &mut Vec<Metric>, before: StorageIoStats, after: StorageIoStats) {
    push_metric(
        metrics,
        "storage.backend.submit_and_wait_calls",
        delta(
            after.backend.submit_and_wait_calls,
            before.backend.submit_and_wait_calls,
        ),
    );
    push_metric(
        metrics,
        "storage.backend.submitted_ops",
        delta(after.backend.submitted_ops, before.backend.submitted_ops),
    );
    push_metric(
        metrics,
        "storage.backend.submit_and_wait_nanos",
        delta(
            after.backend.submit_and_wait_nanos,
            before.backend.submit_and_wait_nanos,
        ),
    );
    push_metric(
        metrics,
        "storage.backend.wait_completions",
        delta(
            after.backend.wait_completions,
            before.backend.wait_completions,
        ),
    );
    push_metric(
        metrics,
        "storage.table_read_requests",
        delta(after.table_read_requests, before.table_read_requests),
    );
    push_metric(
        metrics,
        "storage.pool_read_requests",
        delta(after.pool_read_requests, before.pool_read_requests),
    );
    push_metric(
        metrics,
        "storage.background_write_requests",
        delta(
            after.background_write_requests,
            before.background_write_requests,
        ),
    );
    push_metric(
        metrics,
        "storage.table_read_turns",
        delta(after.table_read_turns, before.table_read_turns),
    );
    push_metric(
        metrics,
        "storage.pool_read_turns",
        delta(after.pool_read_turns, before.pool_read_turns),
    );
    push_metric(
        metrics,
        "storage.background_write_turns",
        delta(after.background_write_turns, before.background_write_turns),
    );
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
    push_metric(
        metrics,
        &format!("{prefix}.submitted_count"),
        delta(after.submitted_count, before.submitted_count),
    );
    push_metric(
        metrics,
        &format!("{prefix}.started_count"),
        delta(after.started_count, before.started_count),
    );
    push_metric(
        metrics,
        &format!("{prefix}.completed_count"),
        delta(after.completed_count, before.completed_count),
    );
    push_metric(
        metrics,
        &format!("{prefix}.error_count"),
        delta(after.error_count, before.error_count),
    );
    push_metric(
        metrics,
        &format!("{prefix}.panic_count"),
        delta(after.panic_count, before.panic_count),
    );
    push_metric(
        metrics,
        &format!("{prefix}.detached_observer_count"),
        delta(
            after.detached_observer_count,
            before.detached_observer_count,
        ),
    );
    push_metric(
        metrics,
        &format!("{prefix}.active_count"),
        after.active_count as u128,
    );
    push_metric(
        metrics,
        &format!("{prefix}.admission_wait_nanos"),
        delta(after.admission_wait_nanos, before.admission_wait_nanos),
    );
    push_metric(
        metrics,
        &format!("{prefix}.queue_wait_nanos"),
        delta(after.queue_wait_nanos, before.queue_wait_nanos),
    );
    push_metric(
        metrics,
        &format!("{prefix}.execution_nanos"),
        delta(after.execution_nanos, before.execution_nanos),
    );
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
    push_metric(
        metrics,
        &format!("{prefix}.cache_hits"),
        delta(after.cache_hits, before.cache_hits),
    );
    push_metric(
        metrics,
        &format!("{prefix}.cache_misses"),
        delta(after.cache_misses, before.cache_misses),
    );
    push_metric(
        metrics,
        &format!("{prefix}.miss_joins"),
        delta(after.miss_joins, before.miss_joins),
    );
    push_metric(
        metrics,
        &format!("{prefix}.queued_reads"),
        delta(after.queued_reads, before.queued_reads),
    );
    push_metric(
        metrics,
        &format!("{prefix}.running_reads"),
        delta(after.running_reads, before.running_reads),
    );
    push_metric(
        metrics,
        &format!("{prefix}.completed_reads"),
        delta(after.completed_reads, before.completed_reads),
    );
    push_metric(
        metrics,
        &format!("{prefix}.read_errors"),
        delta(after.read_errors, before.read_errors),
    );
    push_metric(
        metrics,
        &format!("{prefix}.queued_writes"),
        delta(after.queued_writes, before.queued_writes),
    );
    push_metric(
        metrics,
        &format!("{prefix}.running_writes"),
        delta(after.running_writes, before.running_writes),
    );
    push_metric(
        metrics,
        &format!("{prefix}.completed_writes"),
        delta(after.completed_writes, before.completed_writes),
    );
    push_metric(
        metrics,
        &format!("{prefix}.write_errors"),
        delta(after.write_errors, before.write_errors),
    );
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

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use doradb_storage::IoBackendStats;
    use std::path::Path;
    use tempfile::TempDir;

    fn sample_config(root: &Path) -> OutputConfig {
        OutputConfig {
            workload: Workload::InsertSeq,
            storage_root: root.to_path_buf(),
            num: 10,
            range: None,
            value_size: 16,
            batch_size: 1,
            rand: false,
            seed: 0,
            index: IndexMode::None,
            loaded_key_start: 0,
            loaded_key_end: 10,
            threads: 1,
            sessions: 1,
            log_sync: LogSyncMode::Fsync,
            include_stats: false,
            table_id: 7,
            scope: None,
            unlock: None,
            tables: None,
            scenario: None,
            lock_mode: None,
            width: None,
        }
    }

    fn metric_value(metrics: &[Metric], name: &str) -> u128 {
        metrics
            .iter()
            .find(|metric| metric.name == name)
            .map(|metric| metric.value)
            .unwrap_or_else(|| panic!("missing metric {name}"))
    }

    #[test]
    fn benchmark_result_handles_zero_denominators() {
        let zero_elapsed = BenchmarkResult::new(4, 1, 2, 1, 2, Duration::ZERO, 1);
        assert_eq!(zero_elapsed.operations_per_second, 0.0);
        assert_eq!(zero_elapsed.average_nanos_per_operation, 0.0);
        assert_eq!(zero_elapsed.inserted_rows, 1);
        assert_eq!(zero_elapsed.found, 2);
        assert_eq!(zero_elapsed.not_found, 1);
        assert_eq!(zero_elapsed.rows_returned, 2);
        assert_eq!(zero_elapsed.failures, 1);

        let zero_operations = BenchmarkResult::new(0, 0, 0, 0, 0, Duration::from_nanos(100), 0);
        assert_eq!(zero_operations.operations_per_second, 0.0);
        assert_eq!(zero_operations.average_nanos_per_operation, 0.0);
    }

    #[test]
    fn internal_metrics_include_stat_deltas_and_buffer_capacity() {
        let before = InternalStatsSnapshot {
            trx: TransactionSystemStats {
                commit_count: 10,
                trx_count: 7,
                log_bytes: 4,
                ..TransactionSystemStats::default()
            },
            storage: StorageIoStats {
                backend: IoBackendStats {
                    submitted_ops: 8,
                    ..IoBackendStats::default()
                },
                table_read_requests: 10,
                ..StorageIoStats::default()
            },
            buffer: BufferPoolStats {
                meta: BufferPoolRuntimeStats {
                    capacity: 11,
                    allocated: 2,
                    counters: BufferPoolCounters {
                        cache_hits: 5,
                        write_errors: 4,
                        ..BufferPoolCounters::default()
                    },
                },
                mem: BufferPoolRuntimeStats {
                    counters: BufferPoolCounters {
                        completed_reads: 1,
                        ..BufferPoolCounters::default()
                    },
                    ..BufferPoolRuntimeStats::default()
                },
                ..BufferPoolStats::default()
            },
            mandatory: MandatoryRuntimeStats {
                operation: MandatoryTaskStats {
                    submitted_count: 4,
                    active_count: 1,
                    queue_wait_nanos: 10,
                    ..MandatoryTaskStats::default()
                },
                ..MandatoryRuntimeStats::default()
            },
            logical_lock: LogicalLockStats::default(),
        };
        let after = InternalStatsSnapshot {
            trx: TransactionSystemStats {
                commit_count: 15,
                trx_count: 6,
                log_bytes: 9,
                ..TransactionSystemStats::default()
            },
            storage: StorageIoStats {
                backend: IoBackendStats {
                    submitted_ops: 12,
                    ..IoBackendStats::default()
                },
                table_read_requests: 7,
                ..StorageIoStats::default()
            },
            buffer: BufferPoolStats {
                meta: BufferPoolRuntimeStats {
                    capacity: 13,
                    allocated: 3,
                    counters: BufferPoolCounters {
                        cache_hits: 8,
                        write_errors: 1,
                        ..BufferPoolCounters::default()
                    },
                },
                mem: BufferPoolRuntimeStats {
                    counters: BufferPoolCounters {
                        completed_reads: 4,
                        ..BufferPoolCounters::default()
                    },
                    ..BufferPoolRuntimeStats::default()
                },
                ..BufferPoolStats::default()
            },
            mandatory: MandatoryRuntimeStats {
                operation: MandatoryTaskStats {
                    submitted_count: 7,
                    completed_count: 2,
                    active_count: 3,
                    queue_wait_nanos: 16,
                    ..MandatoryTaskStats::default()
                },
                transaction_cleanup: MandatoryTaskStats {
                    submitted_count: 2,
                    started_count: 2,
                    completed_count: 2,
                    execution_nanos: 20,
                    ..MandatoryTaskStats::default()
                },
            },
            logical_lock: LogicalLockStats::default(),
        };

        let metrics = internal_metrics(&before, &after);
        assert_eq!(metrics.len(), 121);
        assert_eq!(metric_value(&metrics, "transaction.commit_count"), 5);
        assert_eq!(metric_value(&metrics, "transaction.trx_count"), 0);
        assert_eq!(metric_value(&metrics, "transaction.log_bytes"), 5);
        assert_eq!(metric_value(&metrics, "storage.backend.submitted_ops"), 4);
        assert_eq!(metric_value(&metrics, "storage.table_read_requests"), 0);
        assert_eq!(metric_value(&metrics, "buffer.meta.capacity"), 13);
        assert_eq!(metric_value(&metrics, "buffer.meta.allocated"), 3);
        assert_eq!(metric_value(&metrics, "buffer.meta.cache_hits"), 3);
        assert_eq!(metric_value(&metrics, "buffer.meta.write_errors"), 0);
        assert_eq!(metric_value(&metrics, "buffer.mem.completed_reads"), 3);
        assert_eq!(metric_value(&metrics, "buffer.disk.write_errors"), 0);
        assert_eq!(
            metric_value(&metrics, "mandatory.operation.submitted_count"),
            3
        );
        assert_eq!(
            metric_value(&metrics, "mandatory.operation.completed_count"),
            2
        );
        assert_eq!(
            metric_value(&metrics, "mandatory.operation.active_count"),
            3
        );
        assert_eq!(
            metric_value(&metrics, "mandatory.operation.queue_wait_nanos"),
            6
        );
        assert_eq!(
            metric_value(&metrics, "mandatory.transaction_cleanup.submitted_count"),
            2
        );
        assert_eq!(
            metric_value(&metrics, "mandatory.transaction_cleanup.execution_nanos"),
            20
        );
    }

    #[test]
    fn csv_escape_quotes_special_values() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("with,comma"), "\"with,comma\"");
        assert_eq!(csv_escape("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(csv_escape("with\nnewline"), "\"with\nnewline\"");
    }

    #[test]
    fn internal_stats_csv_has_two_columns() {
        let metrics = vec![Metric {
            name: "transaction.commit_count".to_owned(),
            value: 3,
        }];
        assert_eq!(
            render_internal_stats_csv(&metrics),
            "metric-name,metric-value\ntransaction.commit_count,3\n"
        );
    }

    #[test]
    fn result_csv_has_one_header_and_one_row() {
        let temp = TempDir::new().unwrap();
        let csv = render_result_csv(
            &sample_config(temp.path()),
            &BenchmarkResult::new(10, 10, 0, 0, 0, Duration::from_nanos(100), 0),
        );
        assert_eq!(csv.lines().count(), 2);
        assert!(csv.lines().next().unwrap().starts_with("workload,rand"));
        assert!(csv.contains("include_stats"));
        assert!(!csv.contains("manifest_schema_version"));
    }

    #[test]
    fn new_workload_names_and_ddl_counters_use_output_schema() {
        let names = [
            (Workload::StmtNoop, "stmt-noop"),
            (Workload::TrxNoop, "trx-noop"),
            (Workload::IndexStream, "index-stream"),
            (Workload::TableDdl, "table-ddl"),
            (Workload::IndexDdl, "index-ddl"),
            (Workload::LockTable, "lock-table"),
        ];
        for (workload, expected) in names {
            assert_eq!(workload.to_string(), expected);
        }

        let temp = TempDir::new().unwrap();
        let mut config = sample_config(temp.path());
        config.workload = Workload::TableDdl;
        config.num = 2;
        config.loaded_key_end = 0;
        let csv = render_result_csv(
            &config,
            &BenchmarkResult::new(4, 0, 0, 0, 0, Duration::from_nanos(100), 0),
        );
        let row = csv.lines().nth(1).unwrap().split(',').collect::<Vec<_>>();
        assert_eq!(row[0], "table-ddl");
        assert_eq!(row[4], "2");
        assert_eq!(row[5], "");
        assert_eq!(&row[16..19], &["", "", ""]);
        assert_eq!(row[19], "4");
        assert_eq!(&row[20..24], &["0", "0", "0", "0"]);
        assert_eq!(row[27], "0");
    }

    #[test]
    fn lock_table_outputs_include_optional_configuration() {
        let temp = TempDir::new().unwrap();
        let mut config = sample_config(temp.path());
        config.workload = Workload::LockTable;
        config.scope = Some(TableLockScope::Transaction);
        config.unlock = Some(true);
        config.tables = Some(3);
        config.rand = true;
        config.seed = 9;
        let csv = render_result_csv(
            &config,
            &BenchmarkResult::new(5, 0, 0, 0, 0, Duration::from_nanos(100), 0),
        );
        let header = csv.lines().next().unwrap().split(',').collect::<Vec<_>>();
        let row = csv.lines().nth(1).unwrap().split(',').collect::<Vec<_>>();
        assert_eq!(&header[16..19], &["scope", "unlock", "tables"]);
        assert_eq!(&row[16..19], &["transaction", "true", "3"]);
        let pairs = configuration_pairs(&config);
        for expected in [
            ("scope", "transaction"),
            ("unlock", "true"),
            ("tables", "3"),
        ] {
            assert!(
                pairs
                    .iter()
                    .any(|(name, value)| name == expected.0 && value == expected.1)
            );
        }
    }

    #[test]
    fn range_scan_outputs_include_resolved_range() {
        let temp = TempDir::new().unwrap();
        let mut config = sample_config(temp.path());
        config.workload = Workload::IndexStream;
        config.range = Some(3);
        config.rand = true;
        config.seed = 7;
        let csv = render_result_csv(
            &config,
            &BenchmarkResult::new(2, 0, 0, 0, 6, Duration::from_nanos(100), 0),
        );
        let header = csv.lines().next().unwrap().split(',').collect::<Vec<_>>();
        let row = csv.lines().nth(1).unwrap().split(',').collect::<Vec<_>>();
        assert_eq!(header[5], "range");
        assert_eq!(row[5], "3");
        assert!(
            configuration_pairs(&config)
                .iter()
                .any(|(name, value)| name == "range" && value == "3")
        );
    }

    #[test]
    fn configuration_omits_removed_fields() {
        let temp = TempDir::new().unwrap();
        let pairs = configuration_pairs(&sample_config(temp.path()));
        assert!(
            !pairs
                .iter()
                .any(|(name, _)| name == "manifest_schema_version")
        );
        assert!(!pairs.iter().any(|(name, _)| name == "phase"));
        assert!(
            pairs
                .iter()
                .any(|(name, value)| name == "rand" && value == "false")
        );
        assert!(
            pairs
                .iter()
                .any(|(name, value)| name == "include_stats" && value == "false")
        );
        assert!(
            pairs
                .iter()
                .any(|(name, value)| name == "loaded_key_range" && value == "[0, 10)")
        );
        assert!(
            pairs
                .iter()
                .any(|(name, value)| name == "log_sync" && value == "fsync")
        );
    }

    #[test]
    fn writes_result_files_without_stats_by_default() {
        let temp = TempDir::new().unwrap();
        let config = sample_config(temp.path());
        let metrics = vec![Metric {
            name: "transaction.commit_count".to_owned(),
            value: 3,
        }];
        write_benchmark_outputs(
            &config,
            &metrics,
            &BenchmarkResult::new(10, 10, 0, 0, 0, Duration::from_nanos(100), 0),
            "doradb-bench run",
        )
        .unwrap();
        assert!(result_markdown_path(temp.path()).exists());
        assert!(!internal_stats_csv_path(temp.path()).exists());
        assert!(result_csv_path(temp.path()).exists());
        let markdown = fs::read_to_string(result_markdown_path(temp.path())).unwrap();
        assert!(!markdown.contains("## Internal Stats"));
        assert!(!staged_output_path(&result_markdown_path(temp.path())).exists());
        assert!(!staged_output_path(&internal_stats_csv_path(temp.path())).exists());
        assert!(!staged_output_path(&result_csv_path(temp.path())).exists());
    }

    #[test]
    fn writes_internal_stats_when_enabled() {
        let temp = TempDir::new().unwrap();
        let mut config = sample_config(temp.path());
        config.include_stats = true;
        let metrics = vec![Metric {
            name: "transaction.commit_count".to_owned(),
            value: 3,
        }];
        write_benchmark_outputs(
            &config,
            &metrics,
            &BenchmarkResult::new(10, 10, 0, 0, 0, Duration::from_nanos(100), 0),
            "doradb-bench run --include-stats",
        )
        .unwrap();
        assert!(result_markdown_path(temp.path()).exists());
        assert!(internal_stats_csv_path(temp.path()).exists());
        assert!(result_csv_path(temp.path()).exists());
        let markdown = fs::read_to_string(result_markdown_path(temp.path())).unwrap();
        assert!(markdown.contains("## Internal Stats"));
        assert!(markdown.contains("transaction.commit_count"));
    }

    #[test]
    fn disabled_stats_removes_stale_stats_files() {
        let temp = TempDir::new().unwrap();
        let stats_path = internal_stats_csv_path(temp.path());
        let staged_stats_path = staged_output_path(&stats_path);
        fs::write(&stats_path, "stale\n").unwrap();
        fs::write(&staged_stats_path, "stale staged\n").unwrap();

        write_benchmark_outputs(
            &sample_config(temp.path()),
            &[],
            &BenchmarkResult::new(10, 10, 0, 0, 0, Duration::from_nanos(100), 0),
            "doradb-bench run",
        )
        .unwrap();

        assert!(!stats_path.exists());
        assert!(!staged_stats_path.exists());
    }

    #[test]
    fn removes_staged_result_files_after_output_failure() {
        let temp = TempDir::new().unwrap();
        let mut config = sample_config(temp.path());
        config.include_stats = true;
        let metrics = vec![Metric {
            name: "transaction.commit_count".to_owned(),
            value: 3,
        }];
        fs::create_dir(result_csv_path(temp.path())).unwrap();

        let result = write_benchmark_outputs(
            &config,
            &metrics,
            &BenchmarkResult::new(10, 10, 0, 0, 0, Duration::from_nanos(100), 0),
            "doradb-bench run",
        );

        assert!(result.is_err());
        assert!(!staged_output_path(&result_markdown_path(temp.path())).exists());
        assert!(!staged_output_path(&internal_stats_csv_path(temp.path())).exists());
        assert!(!staged_output_path(&result_csv_path(temp.path())).exists());
    }
}
