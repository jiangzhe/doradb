use crate::cli::{IndexMode, Workload};
use crate::error::{BenchError, Result};
use crate::manifest::{internal_stats_csv_path, result_csv_path, result_markdown_path};
use doradb_storage::{
    BufferPoolCounters, BufferPoolRuntimeStats, BufferPoolStats, Session, StorageIoStats,
    TransactionSystemStats,
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
}

impl InternalStatsSnapshot {
    pub(super) fn capture(session: &Session) -> Result<Self> {
        Ok(Self {
            trx: session.transaction_system_stats()?,
            storage: session.storage_io_stats()?,
            buffer: session.buffer_pool_stats()?,
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
    pub(super) value_size: usize,
    pub(super) batch_size: u64,
    pub(super) rand: bool,
    pub(super) seed: u64,
    pub(super) index: IndexMode,
    pub(super) threads: usize,
    pub(super) sessions: usize,
    pub(super) table_id: u64,
}

#[derive(Clone, Debug)]
pub(super) struct BenchmarkResult {
    operations: u64,
    elapsed: Duration,
    operations_per_second: f64,
    average_nanos_per_operation: f64,
    failures: u64,
}

impl BenchmarkResult {
    pub(super) fn new(operations: u64, elapsed: Duration, failures: u64) -> Self {
        let elapsed_seconds = elapsed.as_secs_f64();
        let elapsed_nanos = elapsed.as_nanos() as f64;
        let operations_f64 = operations as f64;
        Self {
            operations,
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

pub(super) fn internal_metrics(
    before: &InternalStatsSnapshot,
    after: &InternalStatsSnapshot,
) -> Vec<Metric> {
    let mut metrics = Vec::new();
    push_transaction_metrics(&mut metrics, before.trx, after.trx);
    push_storage_metrics(&mut metrics, before.storage, after.storage);
    push_buffer_metrics(&mut metrics, &before.buffer, &after.buffer);
    metrics
}

pub(super) fn write_benchmark_outputs(
    config: &OutputConfig,
    metrics: &[Metric],
    result: &BenchmarkResult,
    command_context: &str,
) -> Result<()> {
    print_stdout(config, metrics, result);
    fs::write(
        result_markdown_path(&config.storage_root),
        render_markdown(config, metrics, result, command_context),
    )?;
    fs::write(
        internal_stats_csv_path(&config.storage_root),
        render_internal_stats_csv(metrics),
    )?;
    fs::write(
        result_csv_path(&config.storage_root),
        render_result_csv(config, result),
    )?;
    Ok(())
}

fn print_stdout(config: &OutputConfig, metrics: &[Metric], result: &BenchmarkResult) {
    println!("Configuration");
    for (name, value) in configuration_pairs(config) {
        println!("{name}: {value}");
    }
    println!();
    println!("Internal Stats");
    for metric in metrics {
        println!("{}: {}", metric.name, metric.value);
    }
    println!();
    println!("Final Result");
    println!("operations: {}", result.operations);
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
    out.push_str("`\n\n## Internal Stats\n\n");
    for metric in metrics {
        out.push_str("- `");
        out.push_str(&metric.name);
        out.push_str("`: ");
        out.push_str(&metric.value.to_string());
        out.push('\n');
    }
    out.push_str("\n## Final Result\n\n");
    out.push_str(&format!(
        "- `operations`: {}\n- `elapsed_nanos`: {}\n- `operations_per_second`: {:.3}\n- `average_nanos_per_operation`: {:.3}\n- `failures`: {}\n",
        result.operations,
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
        "storage_root",
        "num",
        "value_size",
        "batch_size",
        "seed",
        "index",
        "threads",
        "sessions",
        "table_id",
        "operations",
        "elapsed_nanos",
        "operations_per_second",
        "average_nanos_per_operation",
        "failures",
    ];
    let row = [
        config.workload.to_string(),
        config.rand.to_string(),
        config.storage_root.display().to_string(),
        config.num.to_string(),
        config.value_size.to_string(),
        config.batch_size.to_string(),
        config.seed.to_string(),
        config.index.to_string(),
        config.threads.to_string(),
        config.sessions.to_string(),
        config.table_id.to_string(),
        result.operations.to_string(),
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
    vec![
        ("workload".to_owned(), config.workload.to_string()),
        ("rand".to_owned(), config.rand.to_string()),
        (
            "storage_root".to_owned(),
            config.storage_root.display().to_string(),
        ),
        ("num".to_owned(), config.num.to_string()),
        ("value_size".to_owned(), config.value_size.to_string()),
        ("batch_size".to_owned(), config.batch_size.to_string()),
        ("seed".to_owned(), config.seed.to_string()),
        ("index".to_owned(), config.index.to_string()),
        ("threads".to_owned(), config.threads.to_string()),
        ("sessions".to_owned(), config.sessions.to_string()),
        ("table_id".to_owned(), config.table_id.to_string()),
    ]
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

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_owned()
    }
}

pub(super) fn remove_result_artifacts(storage_root: &Path) -> Result<()> {
    for path in [
        result_markdown_path(storage_root),
        internal_stats_csv_path(storage_root),
        result_csv_path(storage_root),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                return Err(BenchError::message(format!(
                    "failed to remove result artifact {}: {err}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_config(root: &Path) -> OutputConfig {
        OutputConfig {
            workload: Workload::Insert,
            storage_root: root.to_path_buf(),
            num: 10,
            value_size: 16,
            batch_size: 1,
            rand: false,
            seed: 0,
            index: IndexMode::None,
            threads: 1,
            sessions: 1,
            table_id: 7,
        }
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
            &BenchmarkResult::new(10, Duration::from_nanos(100), 0),
        );
        assert_eq!(csv.lines().count(), 2);
        assert!(csv.lines().next().unwrap().starts_with("workload,rand"));
        assert!(!csv.contains("manifest_schema_version"));
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
    }

    #[test]
    fn writes_fixed_result_files() {
        let temp = TempDir::new().unwrap();
        let config = sample_config(temp.path());
        let metrics = vec![Metric {
            name: "transaction.commit_count".to_owned(),
            value: 3,
        }];
        write_benchmark_outputs(
            &config,
            &metrics,
            &BenchmarkResult::new(10, Duration::from_nanos(100), 0),
            "doradb-bench run",
        )
        .unwrap();
        assert!(result_markdown_path(temp.path()).exists());
        assert!(internal_stats_csv_path(temp.path()).exists());
        assert!(result_csv_path(temp.path()).exists());
    }
}
