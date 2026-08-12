use crate::error::{BenchError, Result};
use crate::manifest::{result_markdown_path, result_toml_path};
use crate::measurement::{
    BenchmarkAggregate, InternalMetric, MeasuredRunResult, WorkloadCounters, u128_decimal,
};
use crate::plan::Plan;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

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
    /// Optional typed engine diagnostics.
    pub internal_metrics: Vec<InternalMetric>,
}

/// Canonical success-only entity shared by TOML and Markdown artifacts.
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

/// Atomically stage and install canonical TOML and Markdown artifacts as a pair.
pub fn write_plan_outputs(report: &InvocationReport) -> Result<()> {
    let toml_path = result_toml_path(&report.root);
    let markdown_path = result_markdown_path(&report.root);
    let toml_staged = staged_path(&toml_path);
    let markdown_staged = staged_path(&markdown_path);
    let result = (|| {
        remove_if_exists(&toml_staged)?;
        remove_if_exists(&markdown_staged)?;
        let toml = toml::to_string_pretty(report)?;
        let markdown = render_markdown(report, &toml);
        fs::write(&toml_staged, toml).map_err(|err| artifact_error(&toml_staged, err))?;
        fs::write(&markdown_staged, markdown)
            .map_err(|err| artifact_error(&markdown_staged, err))?;
        fs::rename(&toml_staged, &toml_path).map_err(|err| artifact_error(&toml_path, err))?;
        if let Err(err) = fs::rename(&markdown_staged, &markdown_path) {
            let _ = fs::remove_file(&toml_path);
            return Err(artifact_error(&markdown_path, err));
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&toml_staged);
        let _ = fs::remove_file(&markdown_staged);
    }
    result
}

fn render_markdown(report: &InvocationReport, canonical_toml: &str) -> String {
    let aggregate = &report.aggregate;
    let mut output = String::new();
    output.push_str("# DoraDB Benchmark Plan Result\n\n");
    output.push_str("- Status: `success`\n");
    output.push_str(&format!("- Root: `{}`\n", report.root.display()));
    output.push_str(&format!("- Plan: `{}`\n", report.plan_source.display()));
    if let Some(name) = &report.plan.name {
        output.push_str(&format!("- Name: `{name}`\n"));
    }
    output.push_str("\n## Completed Results\n\n");
    output.push_str(&format!(
        "- Prepare phases: `{}`\n- Measured runs: `{}`\n- Operations: `{}`\n- Elapsed nanoseconds: `{}`\n- Operations/second: `{:.3}`\n- Average latency nanoseconds: `{:.3}`\n- P95 nanoseconds: `{}`\n- P99 nanoseconds: `{}`\n",
        report.prepare_phases.len(),
        report.measured_runs.len(),
        aggregate.counters.operations,
        aggregate.elapsed_nanos,
        aggregate.operations_per_second,
        aggregate.latency.average_nanos,
        aggregate.latency.p95_nanos,
        aggregate.latency.p99_nanos
    ));
    output.push_str("\n## Canonical Result\n\n");
    let fence_len = longest_backtick_run(canonical_toml)
        .saturating_add(1)
        .max(3);
    let fence = "`".repeat(fence_len);
    output.push_str(&fence);
    output.push_str("toml\n");
    output.push_str(canonical_toml);
    if !canonical_toml.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(&fence);
    output.push('\n');
    output
}

fn longest_backtick_run(value: &str) -> usize {
    let mut longest = 0;
    let mut current = 0;
    for byte in value.bytes() {
        if byte == b'`' {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    longest
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
    use crate::measurement::{LatencySummary, LatencyUnit};
    use crate::plan::{
        MeasurementSpec, Phase, ResolvedWorkload, ResolvedWorkloadDefaults, TrxNoopConfig,
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
                    workload: ResolvedWorkload::TrxNoop(TrxNoopConfig {
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
    fn success_output_pair_round_trips_one_entity() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());
        write_plan_outputs(&report).unwrap();
        let encoded = fs::read_to_string(result_toml_path(temp.path())).unwrap();
        let decoded: InvocationReport = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded, report);
        assert!(!encoded.contains("status ="));
        assert!(!encoded.contains("failure"));
        let markdown = fs::read_to_string(result_markdown_path(temp.path())).unwrap();
        assert!(markdown.contains(&encoded));
    }

    #[test]
    fn output_install_failure_leaves_no_complete_pair() {
        let temp = TempDir::new().unwrap();
        fs::create_dir(result_markdown_path(temp.path())).unwrap();
        let error = write_plan_outputs(&report(temp.path())).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to write benchmark artifact")
        );
        assert!(!result_toml_path(temp.path()).exists());
        assert!(result_markdown_path(temp.path()).is_dir());
    }

    #[test]
    fn canonical_toml_uses_adaptive_backtick_fence() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());
        let canonical_toml = "message = \"before ```` after\"";
        let adaptive = render_markdown(&report, canonical_toml);
        let expected = format!("`````toml\n{canonical_toml}\n`````\n");
        assert!(adaptive.ends_with(&expected));
    }
}
