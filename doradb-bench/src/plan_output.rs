use crate::error::{BenchError, Result};
use crate::manifest::{result_markdown_path, result_toml_path};
use crate::measurement::{
    BenchmarkAggregate, InternalMetric, MeasuredRunResult, WorkloadCounters, u128_decimal,
};
use crate::plan::{PhaseKind, Plan};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

/// Terminal status of one plan invocation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InvocationStatus {
    /// Every phase, run, shutdown, and artifact entity completed successfully.
    Success,
    /// Bootstrap or one phase/run failed.
    Failed,
}

/// Execution boundary at which an invocation failed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FailureBoundary {
    /// Storage engine bootstrap.
    Bootstrap,
    /// One prepare phase execution.
    Prepare,
    /// One discarded benchmark warm-up.
    Warmup,
    /// One measured benchmark repetition.
    Measured,
}

/// Structured plan phase/run error context.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationFailure {
    /// Boundary that failed.
    pub boundary: FailureBoundary,
    /// One-based phase index when execution reached a phase.
    pub phase_index: Option<usize>,
    /// Phase role when execution reached a phase.
    pub phase_kind: Option<PhaseKind>,
    /// One-based warm-up or measured run index.
    pub run_index: Option<u32>,
    /// Original error rendering.
    pub message: String,
}

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

/// Canonical entity shared by TOML and Markdown plan artifacts.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationReport {
    /// Terminal invocation status.
    pub status: InvocationStatus,
    /// Structured failure context for failed invocations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<InvocationFailure>,
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
    /// Present only after every measured run succeeds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<BenchmarkAggregate>,
}

impl InvocationReport {
    /// Construct an in-progress report that can be finalized as success/failure.
    #[inline]
    pub fn new(root: PathBuf, plan: Plan) -> Self {
        Self {
            status: InvocationStatus::Success,
            failure: None,
            root,
            plan_source: plan.source.clone(),
            plan,
            prepare_phases: Vec::new(),
            measured_runs: Vec::new(),
            aggregate: None,
        }
    }

    /// Record terminal failure without discarding earlier completed results.
    #[inline]
    pub fn fail(&mut self, failure: InvocationFailure) {
        self.status = InvocationStatus::Failed;
        self.failure = Some(failure);
        self.aggregate = None;
    }
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
    let mut output = String::new();
    output.push_str("# DoraDB Benchmark Plan Result\n\n");
    output.push_str(&format!("- Status: `{}`\n", status_name(report.status)));
    output.push_str(&format!("- Root: `{}`\n", report.root.display()));
    output.push_str(&format!("- Plan: `{}`\n", report.plan_source.display()));
    if let Some(name) = &report.plan.name {
        output.push_str(&format!("- Name: `{name}`\n"));
    }
    if let Some(failure) = &report.failure {
        output.push_str("\n## Failure\n\n");
        output.push_str(&format!("- Boundary: `{:?}`\n", failure.boundary));
        if let Some(phase) = failure.phase_index {
            output.push_str(&format!("- Phase: `{phase}`\n"));
        }
        if let Some(run) = failure.run_index {
            output.push_str(&format!("- Run: `{run}`\n"));
        }
        output.push_str(&format!(
            "- Error: `{}`\n",
            markdown_inline(&failure.message)
        ));
    }
    output.push_str("\n## Completed Results\n\n");
    output.push_str(&format!(
        "- Prepare phases: `{}`\n- Measured runs: `{}`\n- Aggregate: `{}`\n",
        report.prepare_phases.len(),
        report.measured_runs.len(),
        if report.aggregate.is_some() {
            "yes"
        } else {
            "no"
        }
    ));
    if let Some(aggregate) = &report.aggregate {
        output.push_str(&format!(
            "- Operations: `{}`\n- Elapsed nanoseconds: `{}`\n- Operations/second: `{:.3}`\n- Average latency nanoseconds: `{:.3}`\n- P95 nanoseconds: `{}`\n- P99 nanoseconds: `{}`\n",
            aggregate.counters.operations,
            aggregate.elapsed_nanos,
            aggregate.operations_per_second,
            aggregate.latency.average_nanos,
            aggregate.latency.p95_nanos,
            aggregate.latency.p99_nanos
        ));
    }
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

fn status_name(status: InvocationStatus) -> &'static str {
    match status {
        InvocationStatus::Success => "success",
        InvocationStatus::Failed => "failed",
    }
}

fn markdown_inline(value: &str) -> String {
    value.replace('`', "'").replace('\n', " ")
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
    use crate::plan::{MeasurementSpec, ResolvedWorkload, ResolvedWorkloadDefaults, TrxNoopConfig};
    use std::num::NonZeroU32;
    use tempfile::TempDir;

    fn report(root: &Path) -> InvocationReport {
        let (_, engine) = resolve_engine_config(root, &EngineConfigOverlay::default()).unwrap();
        InvocationReport::new(
            root.to_path_buf(),
            Plan {
                name: Some("test".to_owned()),
                source: PathBuf::from("plan.toml"),
                engine,
                workload_defaults: ResolvedWorkloadDefaults {
                    threads: 1,
                    sessions: 1,
                    value_size: 128,
                    batch_size: 1,
                    include_stats: false,
                },
                phases: vec![crate::plan::Phase::Benchmark {
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
                }],
            },
        )
    }

    #[test]
    fn output_pair_round_trips_one_entity() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());
        write_plan_outputs(&report).unwrap();
        let encoded = fs::read_to_string(result_toml_path(temp.path())).unwrap();
        let decoded: InvocationReport = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded, report);
        let markdown = fs::read_to_string(result_markdown_path(temp.path())).unwrap();
        assert!(markdown.contains(&encoded));
    }

    #[test]
    fn failed_output_pair_round_trips_one_entity() {
        let temp = TempDir::new().unwrap();
        let mut report = report(temp.path());
        let failure = InvocationFailure {
            boundary: FailureBoundary::Measured,
            phase_index: Some(1),
            phase_kind: Some(PhaseKind::Benchmark),
            run_index: Some(1),
            message: "synthetic measurement failure".to_owned(),
        };
        report.fail(failure.clone());

        write_plan_outputs(&report).unwrap();
        let encoded = fs::read_to_string(result_toml_path(temp.path())).unwrap();
        let decoded: InvocationReport = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded.failure.as_ref(), Some(&failure));
        assert_eq!(decoded, report);
    }

    #[test]
    fn canonical_toml_uses_adaptive_backtick_fence() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());

        let standard = render_markdown(&report, "plain = true\n");
        assert!(standard.ends_with("```toml\nplain = true\n```\n"));

        let canonical_toml = "message = \"before ```` after\"";
        let adaptive = render_markdown(&report, canonical_toml);
        let expected = format!("`````toml\n{canonical_toml}\n`````\n");
        assert!(adaptive.ends_with(&expected));
    }
}
