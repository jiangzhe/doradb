use crate::error::{BenchError, Result};
use crate::measurement::{
    BenchmarkAggregate, InternalMetric, MeasuredRunResult, WorkloadCounters, WorkloadMetrics,
    u128_decimal,
};
use crate::plan::Plan;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

const RESULT_TOML_FILE_NAME: &str = "benchmark-result.toml";

/// Diagnostics retained from one successful prepare phase.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparePhaseResult {
    /// One-based plan phase index.
    pub phase_index: usize,
    /// Stable workload identity.
    pub workload: String,
    /// Full session/worker wall envelope.
    #[serde(with = "u128_decimal")]
    pub elapsed_nanos: u128,
    /// Successful logical workload counters.
    pub counters: WorkloadCounters,
    /// Optional workload-specific metrics from the prepare execution.
    pub workload_metrics: Option<WorkloadMetrics>,
    /// Optional typed engine diagnostics.
    pub internal_metrics: Vec<InternalMetric>,
}

/// Canonical success-only benchmark result entity.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationReport {
    /// Invocation-owned storage root.
    pub root: PathBuf,
    /// Invocation plan source.
    pub plan_source: PathBuf,
    /// Complete validated plan and normalized configuration.
    pub plan: Plan,
    /// Successful prepare phase results in plan order.
    pub prepare_phases: Vec<PreparePhaseResult>,
    /// Complete measured runs in repetition order.
    pub measured_runs: Vec<MeasuredRunResult>,
    /// Aggregate of every successful measured repetition.
    pub aggregate: BenchmarkAggregate,
}

/// Atomically stage and install the canonical TOML artifact.
pub fn write_plan_output(report: &InvocationReport) -> Result<PathBuf> {
    let toml_path = result_toml_path(&report.root);
    let absolute_path = absolute_result_path(&report.root)?;
    let toml_staged = staged_path(&toml_path);
    let result = (|| {
        remove_if_exists(&toml_staged)?;
        let toml = toml::to_string_pretty(report)?;
        fs::write(&toml_staged, toml).map_err(|err| artifact_error(&toml_staged, err))?;
        fs::rename(&toml_staged, &toml_path).map_err(|err| artifact_error(&toml_path, err))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&toml_staged);
    }
    result.map(|()| absolute_path)
}

/// Render the successful aggregate and detailed result location for stdout.
pub(crate) fn render_stdout_summary(
    report: &InvocationReport,
    detailed_result: &Path,
) -> Result<String> {
    let workload = report
        .plan
        .phases
        .last()
        .ok_or_else(|| BenchError::message("benchmark report has no final workload"))?
        .workload();
    let aggregate = &report.aggregate;
    let mut summary = format!(
        "DoraDB benchmark summary\n\
         workload: {}\n\
         measured_runs: {}\n\
         operations: {}\n\
         elapsed_nanos: {}\n\
         operations_per_second: {:.3}\n\
         latency_unit: {}\n\
         average_latency_nanos: {:.3}\n\
         p95_latency_nanos: {}\n\
         p99_latency_nanos: {}",
        workload.identity(),
        aggregate.measured_runs,
        aggregate.counters.operations,
        aggregate.elapsed_nanos,
        aggregate.operations_per_second,
        aggregate.latency.unit,
        aggregate.latency.average_nanos,
        aggregate.latency.p95_nanos,
        aggregate.latency.p99_nanos
    );
    if workload.identity() == "checkpoint-table" {
        let metrics = report
            .measured_runs
            .first()
            .and_then(|run| run.workload_metrics)
            .ok_or_else(|| BenchError::message("checkpoint report has no workload metrics"))?;
        let WorkloadMetrics::CheckpointTable {
            attempt_count,
            attempt_elapsed_nanos,
            retry_wait_count,
            retry_wait_elapsed_nanos,
        } = metrics
        else {
            return Err(BenchError::message(
                "checkpoint report has incompatible workload metrics",
            ));
        };
        summary.push_str(&format!(
            "\ncheckpoint_attempt_count: {attempt_count}\n\
             checkpoint_attempt_elapsed_nanos: {attempt_elapsed_nanos}\n\
             checkpoint_retry_wait_count: {retry_wait_count}\n\
             checkpoint_retry_wait_elapsed_nanos: {retry_wait_elapsed_nanos}"
        ));
    }
    summary.push_str(&format!("\ndetailed_result: {}", detailed_result.display()));
    Ok(summary)
}

fn result_toml_path(storage_root: &Path) -> PathBuf {
    storage_root.join(RESULT_TOML_FILE_NAME)
}

pub(crate) fn absolute_result_path(storage_root: &Path) -> Result<PathBuf> {
    fs::canonicalize(storage_root)
        .map(|root| root.join(RESULT_TOML_FILE_NAME))
        .map_err(|err| {
            BenchError::message(format!(
                "failed to resolve benchmark artifact directory {}: {err}",
                storage_root.display()
            ))
        })
}

fn staged_path(path: &Path) -> PathBuf {
    let mut staged = path.as_os_str().to_owned();
    staged.push(".tmp");
    PathBuf::from(staged)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(artifact_error(path, err)),
    }
}

fn artifact_error(path: &Path, err: IoError) -> BenchError {
    BenchError::message(format!(
        "failed to write benchmark artifact {}: {err}",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_config::{EngineConfigOverlay, resolve_engine_config};
    use crate::fixture::FixturePlanEffect;
    use crate::measurement::{LatencySummary, LatencyUnit, WorkloadMetrics};
    use crate::plan::{
        CheckpointTableConfig, CountConfig, MeasurementSpec, Phase, ResolvedWorkload,
        ResolvedWorkloadDefaults,
    };
    use std::num::NonZeroU32;
    use tempfile::TempDir;

    fn report(root: &Path) -> InvocationReport {
        let (_, engine) = resolve_engine_config(root, &EngineConfigOverlay::default()).unwrap();
        let counters = WorkloadCounters {
            operations: 1,
            ..WorkloadCounters::default()
        };
        InvocationReport {
            root: root.to_path_buf(),
            plan_source: PathBuf::from("plan.toml"),
            plan: Plan {
                name: Some("test".to_owned()),
                source: PathBuf::from("plan.toml"),
                engine,
                workload_defaults: ResolvedWorkloadDefaults {
                    threads: 1,
                    sessions: 1,
                    value_size_bytes: 128,
                    batch_size: 1,
                    include_stats: false,
                },
                phases: vec![Phase::Benchmark {
                    measurement: MeasurementSpec {
                        warmup_runs: 0,
                        measured_runs: NonZeroU32::MIN,
                    },
                    workload: ResolvedWorkload::TrxNoop(CountConfig {
                        num: 1,
                        threads: 1,
                        sessions: 1,
                        include_stats: false,
                    }),
                    fixture_effect: FixturePlanEffect::None,
                }],
            },
            prepare_phases: Vec::new(),
            measured_runs: Vec::new(),
            aggregate: BenchmarkAggregate {
                measured_runs: 1,
                elapsed_nanos: 10,
                counters,
                operations_per_second: 100_000_000.0,
                latency: LatencySummary {
                    unit: LatencyUnit::TransactionLifecycle,
                    sample_count: 1,
                    sum_nanos: 10,
                    average_nanos: 10.0,
                    p95_nanos: 10,
                    p99_nanos: 10,
                },
            },
        }
    }

    #[test]
    fn canonical_output_round_trips_one_entity() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());
        let installed = write_plan_output(&report).unwrap();
        assert_eq!(
            installed,
            fs::canonicalize(temp.path())
                .unwrap()
                .join(RESULT_TOML_FILE_NAME)
        );
        let encoded = fs::read_to_string(result_toml_path(temp.path())).unwrap();
        let decoded: InvocationReport = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded, report);
        assert!(!encoded.contains("status ="));
        assert!(!encoded.contains("failure"));
        assert!(!staged_path(&installed).exists());
        assert!(!temp.path().join("benchmark-result.md").exists());
    }

    #[test]
    fn output_install_failure_leaves_no_complete_artifact() {
        let temp = TempDir::new().unwrap();
        fs::create_dir(result_toml_path(temp.path())).unwrap();
        let error = write_plan_output(&report(temp.path())).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to write benchmark artifact")
        );
        assert!(result_toml_path(temp.path()).is_dir());
        assert!(!staged_path(&result_toml_path(temp.path())).exists());
    }

    #[test]
    fn stdout_summary_uses_stable_labels_and_absolute_result_path() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());
        let detailed_result = fs::canonicalize(temp.path())
            .unwrap()
            .join(RESULT_TOML_FILE_NAME);
        assert_eq!(
            render_stdout_summary(&report, &detailed_result).unwrap(),
            format!(
                "DoraDB benchmark summary\n\
                 workload: trx-noop\n\
                 measured_runs: 1\n\
                 operations: 1\n\
                 elapsed_nanos: 10\n\
                 operations_per_second: 100000000.000\n\
                 latency_unit: transaction-lifecycle\n\
                 average_latency_nanos: 10.000\n\
                 p95_latency_nanos: 10\n\
                 p99_latency_nanos: 10\n\
                 detailed_result: {}",
                detailed_result.display()
            )
        );
    }

    #[test]
    fn checkpoint_stdout_summary_includes_attempt_and_wait_breakdown() {
        let temp = TempDir::new().unwrap();
        let mut report = report(temp.path());
        let Phase::Benchmark {
            workload,
            fixture_effect,
            ..
        } = &mut report.plan.phases[0]
        else {
            unreachable!()
        };
        *workload = ResolvedWorkload::CheckpointTable(CheckpointTableConfig {
            include_stats: false,
        });
        *fixture_effect = FixturePlanEffect::Checkpoint;
        report.aggregate.latency.unit = LatencyUnit::TableCheckpoint;
        report.measured_runs.push(MeasuredRunResult {
            run_index: 1,
            elapsed_nanos: 10,
            counters: report.aggregate.counters,
            operations_per_second: report.aggregate.operations_per_second,
            latency: report.aggregate.latency.clone(),
            workload_metrics: Some(WorkloadMetrics::CheckpointTable {
                attempt_count: 3,
                attempt_elapsed_nanos: 7,
                retry_wait_count: 2,
                retry_wait_elapsed_nanos: 2,
            }),
            internal_metrics: Vec::new(),
        });
        let summary = render_stdout_summary(&report, Path::new("result.toml")).unwrap();
        assert!(summary.contains("checkpoint_attempt_count: 3\n"));
        assert!(summary.contains("checkpoint_attempt_elapsed_nanos: 7\n"));
        assert!(summary.contains("checkpoint_retry_wait_count: 2\n"));
        assert!(summary.contains("checkpoint_retry_wait_elapsed_nanos: 2\n"));
    }
}
