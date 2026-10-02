use crate::error::{BenchError, Result};
use crate::measurement::{
    BenchmarkAggregate, InternalMetric, MeasuredRunResult, WorkloadCounters, WorkloadMetrics,
    operations_per_second,
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
    pub elapsed_nanos: u64,
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
    validate_create_result(report)?;
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
    validate_create_result(report)?;
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
    if let Some(WorkloadMetrics::CreateIndex { report: create }) = report
        .measured_runs
        .first()
        .and_then(|run| run.workload_metrics.as_ref())
    {
        summary.push_str(&format!("\ntable_id: {}\nindex_id: {}\nindex: {}\nplacement: {}\ntotal_rows: {}\nhot_rows: {}\ncheckpointed_rows: {}\ncreate_elapsed_nanos: {}\nprocess_cpu_nanos: {}",
            create.table_id, create.index_id, create.index, create.placement,
            create.total_rows, create.rows.hot_rows, create.rows.checkpointed_rows,
            create.create_elapsed_nanos, create.process_cpu_nanos));
        if create.create_elapsed_nanos != 0 {
            summary.push_str(&format!(
                "\ncreate_rows_per_second: {:.3}\naverage_cpu_cores: {:.3}",
                operations_per_second(create.total_rows, create.create_elapsed_nanos),
                create.process_cpu_nanos as f64 / create.create_elapsed_nanos as f64
            ));
        }
        if let Some(rss) = &create.sampled_process_rss {
            summary.push_str(&format!("\nsampled_process_rss_baseline_bytes: {}\nsampled_process_rss_peak_bytes: {}\nsampled_process_rss_peak_above_baseline_bytes: {}",
                rss.baseline_bytes, rss.peak_bytes, rss.peak_above_baseline_bytes));
        }
        summary.push_str(
            "\nverification: complete\nOne CREATE sample; p95/p99 do not establish a distribution.",
        );
    }
    if matches!(workload.identity(), "delete-all" | "delete-rand") {
        summary.push_str(&format!(
            "\ndeleted_rows: {}\ndeleted_rows_per_second: {:.3}",
            aggregate.counters.deleted_rows,
            operations_per_second(aggregate.counters.deleted_rows, aggregate.elapsed_nanos),
        ));
    }
    if workload.identity() == "checkpoint-table" {
        let metrics = report
            .measured_runs
            .first()
            .and_then(|run| run.workload_metrics.as_ref())
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
    if workload.identity() == "parallel-table-scan" {
        let metrics = report
            .measured_runs
            .first()
            .and_then(|run| run.workload_metrics.as_ref())
            .ok_or_else(|| BenchError::message("parallel scan report has no workload metrics"))?;
        let WorkloadMetrics::ParallelTableScan {
            target_partitions,
            actual_partitions,
        } = metrics
        else {
            return Err(BenchError::message(
                "parallel scan report has incompatible workload metrics",
            ));
        };
        if report
            .measured_runs
            .iter()
            .any(|run| run.workload_metrics.as_ref() != Some(metrics))
        {
            return Err(BenchError::message(
                "parallel scan report has inconsistent per-run partition metrics",
            ));
        }
        summary.push_str(&format!(
            "\ntarget_partitions: {target_partitions}\n\
             actual_partitions: {actual_partitions}\n\
             rows_returned: {}\n\
             rows_per_second: {:.3}",
            aggregate.counters.rows_returned,
            operations_per_second(aggregate.counters.rows_returned, aggregate.elapsed_nanos)
        ));
    }
    if workload.identity() == "catalog-checkpoint" {
        let metrics = report
            .measured_runs
            .first()
            .and_then(|run| run.workload_metrics.as_ref())
            .ok_or_else(|| {
                BenchError::message("catalog checkpoint report has no workload metrics")
            })?;
        let WorkloadMetrics::CatalogCheckpoint {
            profile,
            case,
            sampled_process_rss,
            checkpoint,
            ..
        } = metrics
        else {
            return Err(BenchError::message(
                "catalog checkpoint report has incompatible workload metrics",
            ));
        };
        let checkpoint = &checkpoint.report;
        let compact_bytes_read = checkpoint
            .table_io
            .iter()
            .map(|table| table.compact_bytes_read)
            .sum::<usize>();
        let lwc_bytes_written = checkpoint
            .table_io
            .iter()
            .map(|table| table.lwc_bytes_written)
            .sum::<usize>();
        let index_bytes_written = checkpoint
            .table_io
            .iter()
            .map(|table| table.index_bytes_written)
            .sum::<usize>();
        let changed_final_compact_bytes = checkpoint
            .table_io
            .iter()
            .filter(|table| {
                checkpoint
                    .table_changes
                    .iter()
                    .any(|change| change.table_id == table.table_id)
            })
            .map(|table| table.final_compact_bytes)
            .sum::<usize>();
        let checkpoint_bytes_written = lwc_bytes_written
            .saturating_add(index_bytes_written)
            .saturating_add(checkpoint.metadata_bytes_written);
        let write_amplification = if changed_final_compact_bytes == 0 {
            0.0
        } else {
            checkpoint_bytes_written as f64 / changed_final_compact_bytes as f64
        };
        summary.push_str(&format!(
            "\ncatalog_profile: {profile}\n\
             catalog_case: {case}\n\
             sampled_process_rss_baseline_bytes: {}\n\
             sampled_process_rss_peak_bytes: {}\n\
             sampled_process_rss_peak_above_baseline_bytes: {}\n\
             catalog_compact_bytes_read: {compact_bytes_read}\n\
             catalog_lwc_bytes_written: {lwc_bytes_written}\n\
             catalog_index_bytes_written: {index_bytes_written}\n\
             catalog_metadata_bytes_written: {}\n\
             catalog_changed_final_compact_bytes: {changed_final_compact_bytes}\n\
             catalog_checkpoint_bytes_written: {checkpoint_bytes_written}\n\
             catalog_write_amplification: {write_amplification:.6}",
            sampled_process_rss.baseline_bytes,
            sampled_process_rss.peak_bytes,
            sampled_process_rss.peak_above_baseline_bytes,
            checkpoint.metadata_bytes_written,
        ));
    }
    if workload.identity() == "recovery" {
        let Some(WorkloadMetrics::Recovery {
            report: recovery,
            verification,
        }) = report
            .measured_runs
            .first()
            .and_then(|run| run.workload_metrics.as_ref())
        else {
            return Err(BenchError::message(
                "recovery report has no recovery metrics",
            ));
        };
        summary
            .push_str("\nrecovery_scenario: clean-reopen-same-process\ncache_state: uncontrolled");
        summary.push_str(&format!(
            "\npublic_bootstrap_elapsed_nanos: {}",
            aggregate.elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nbootstrap_elapsed_nanos: {}",
            recovery.bootstrap_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nengine_setup_elapsed_nanos: {}",
            recovery.engine_setup_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\ncatalog_bootstrap_elapsed_nanos: {}",
            recovery.catalog_bootstrap_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\ntransaction_bootstrap_elapsed_nanos: {}",
            recovery.transaction_bootstrap_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nruntime_startup_elapsed_nanos: {}",
            recovery.runtime_startup_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\npreparation_elapsed_nanos: {}",
            recovery.phases.preparation_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nuser_table_bootstrap_elapsed_nanos: {}",
            recovery.phases.user_table_bootstrap_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nredo_planning_elapsed_nanos: {}",
            recovery.phases.redo_planning_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nredo_replay_elapsed_nanos: {}",
            recovery.phases.redo_replay_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nvalidation_elapsed_nanos: {}",
            recovery.phases.validation_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nabsent_file_cleanup_elapsed_nanos: {}",
            recovery.phases.absent_file_cleanup_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nhot_index_rebuild_elapsed_nanos: {}",
            recovery.phases.hot_index_rebuild_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nredo_repair_planning_elapsed_nanos: {}",
            recovery.phases.redo_repair_planning_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nredo_finalize_elapsed_nanos: {}",
            recovery.phases.redo_finalize_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\nother_elapsed_nanos: {}",
            recovery.phases.other_elapsed_nanos
        ));
        summary.push_str(&format!(
            "\ntransactions_decoded: {}",
            recovery.redo.transactions_decoded
        ));
        summary.push_str(&format!(
            "\nconsumed_bytes: {}",
            recovery.redo.consumed_bytes
        ));
        summary.push_str(&format!(
            "\nvalidated_payload_bytes: {}",
            recovery.redo.validated_payload_bytes
        ));
        summary.push_str(&format!(
            "\ncatalog_row_ops_applied: {}",
            recovery.work.catalog_row_ops_applied
        ));
        summary.push_str(&format!(
            "\ncatalog_row_ops_skipped: {}",
            recovery.work.catalog_row_ops_skipped
        ));
        summary.push_str(&format!(
            "\nuser_row_ops_applied: {}",
            recovery.work.user_row_ops_applied
        ));
        summary.push_str(&format!(
            "\nuser_row_ops_skipped: {}",
            recovery.work.user_row_ops_skipped
        ));
        summary.push_str(&format!(
            "\nindex_entries_inserted: {}",
            recovery.work.index_entries_inserted
        ));
        summary.push_str(&format!(
            "\nverified_rows: {}\nverified_tables: {}",
            verification.verified_rows, verification.table_count
        ));
        for (name, count) in [
            (
                "decoded_transactions_per_redo_replay_second",
                recovery.redo.transactions_decoded,
            ),
            (
                "validated_payload_bytes_per_redo_replay_second",
                recovery.redo.validated_payload_bytes,
            ),
        ] {
            if count != 0 && recovery.phases.redo_replay_elapsed_nanos != 0 {
                summary.push_str(&format!(
                    "\n{name}: {:.3}",
                    operations_per_second(count, recovery.phases.redo_replay_elapsed_nanos)
                ));
            }
        }
    }
    summary.push_str(&format!("\ndetailed_result: {}", detailed_result.display()));
    Ok(summary)
}

/// Resolve the canonical result artifact to an absolute path.
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

fn validate_create_result(report: &InvocationReport) -> Result<()> {
    if report
        .plan
        .phases
        .last()
        .is_none_or(|phase| phase.workload().identity() != "create-index")
    {
        return Ok(());
    }
    let [run] = report.measured_runs.as_slice() else {
        return Err(BenchError::message(
            "CREATE requires exactly one measured result",
        ));
    };
    let Some(WorkloadMetrics::CreateIndex { report: create }) = &run.workload_metrics else {
        return Err(BenchError::message("CREATE report has no measurements"));
    };
    create.validate()?;
    let counters = WorkloadCounters {
        operations: 1,
        ..WorkloadCounters::default()
    };
    if run.counters != counters
        || report.aggregate.counters != counters
        || run.latency.sample_count != 1
        || run.latency.sum_nanos != create.create_elapsed_nanos
        || report.aggregate.latency.sample_count != 1
        || report.aggregate.latency.sum_nanos != create.create_elapsed_nanos
    {
        return Err(BenchError::message(
            "CREATE counters or exact latency sum mismatch",
        ));
    }
    Ok(())
}

fn result_toml_path(storage_root: &Path) -> PathBuf {
    storage_root.join(RESULT_TOML_FILE_NAME)
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
    use crate::fixture::{FixturePlanEffect, KeyRange};
    use crate::measurement::{LatencySummary, LatencyUnit, WorkloadMetrics};
    use crate::plan::{
        CheckpointTableConfig, CountConfig, MeasurementSpec, ParallelTableScanConfig, Phase,
        ResolvedWorkload, ResolvedWorkloadDefaults,
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
                        pause: false,
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

    fn measured_report(
        root: &Path,
        workload: ResolvedWorkload,
        metrics: WorkloadMetrics,
    ) -> InvocationReport {
        let mut report = report(root);
        report.aggregate.latency.unit = workload.latency_unit();
        let Phase::Benchmark {
            workload: target, ..
        } = &mut report.plan.phases[0]
        else {
            unreachable!()
        };
        *target = workload;
        report.measured_runs.push(MeasuredRunResult {
            run_index: 1,
            elapsed_nanos: report.aggregate.elapsed_nanos,
            counters: report.aggregate.counters,
            operations_per_second: report.aggregate.operations_per_second,
            latency: report.aggregate.latency.clone(),
            workload_metrics: Some(metrics),
            internal_metrics: Vec::new(),
        });
        report
    }

    fn assert_metric_shape_errors(report: &InvocationReport, owner: &str) {
        for (metrics, expected) in [
            (None, "has no workload metrics"),
            (
                Some(WorkloadMetrics::FreezeTable {
                    approximate_rows: 1,
                    page_count: 1,
                    stable_page_count: 0,
                }),
                "has incompatible workload metrics",
            ),
        ] {
            let mut invalid = report.clone();
            invalid.measured_runs[0].workload_metrics = metrics;
            let error = render_stdout_summary(&invalid, Path::new("result.toml")).unwrap_err();
            assert_eq!(error.to_string(), format!("{owner} report {expected}"));
        }
    }

    /// Purpose: Publish index-creation results only when verification and accounting are
    /// complete.
    /// Expected: Valid reports preserve raw metrics and defined rates while invalid reports
    /// produce no artifact.
    #[test]
    fn create_output_requires_complete_verification_and_preserves_raw_units() {
        use crate::fixture::{IndexMode, PlacementKind, RowPlacement};
        use crate::measurement::{CreateIndexReport, CreateIndexVerification, SampledProcessRss};
        use crate::plan::{CreateIndexConfig, CreateIndexKey};
        let temp = TempDir::new().unwrap();
        let mut report = report(temp.path());
        let Phase::Benchmark { workload, .. } = &mut report.plan.phases[0] else {
            panic!("benchmark")
        };
        *workload = ResolvedWorkload::CreateIndex(CreateIndexConfig {
            key: CreateIndexKey::Key,
            fixture: None,
            index: IndexMode::Unique,
            include_stats: true,
        });
        report.aggregate.latency.unit = LatencyUnit::IndexCreation;
        let create = CreateIndexReport {
            table_id: 42,
            index_id: 7,
            index: IndexMode::Unique,
            placement: PlacementKind::Mixed,
            total_rows: 100,
            rows: RowPlacement {
                hot_rows: 10,
                checkpointed_rows: 90,
            },
            create_elapsed_nanos: 10,
            process_cpu_nanos: 25,
            sampled_process_rss: Some(SampledProcessRss {
                baseline_bytes: 1000,
                peak_bytes: 2000,
                peak_above_baseline_bytes: 1000,
            }),
            verification: Some(CreateIndexVerification {
                table_rows: 100,
                index_rows: 100,
                fingerprint: "a".repeat(64),
            }),
        };
        report.measured_runs.push(MeasuredRunResult {
            run_index: 1,
            elapsed_nanos: 50,
            counters: report.aggregate.counters,
            operations_per_second: 20_000_000.0,
            latency: report.aggregate.latency.clone(),
            workload_metrics: Some(WorkloadMetrics::CreateIndex { report: create }),
            internal_metrics: Vec::new(),
        });
        for failure in ["pending", "count", "placement", "counters", "latency"] {
            let mut invalid = report.clone();
            let Some(WorkloadMetrics::CreateIndex { report: create }) =
                invalid.measured_runs[0].workload_metrics.as_mut()
            else {
                panic!("CREATE")
            };
            match failure {
                "pending" => create.verification = None,
                "count" => create.verification.as_mut().unwrap().index_rows -= 1,
                "placement" => create.rows.hot_rows += 1,
                "counters" => invalid.measured_runs[0].counters.operations = 2,
                "latency" => invalid.measured_runs[0].latency.sum_nanos += 1,
                _ => unreachable!(),
            }
            assert!(write_plan_output(&invalid).is_err(), "{failure}");
            assert!(
                render_stdout_summary(&invalid, &temp.path().join(RESULT_TOML_FILE_NAME)).is_err()
            );
            assert!(!result_toml_path(temp.path()).exists());
        }
        let path = write_plan_output(&report).unwrap();
        let encoded = fs::read_to_string(&path).unwrap();
        assert_eq!(
            toml::from_str::<InvocationReport>(&encoded).unwrap(),
            report
        );
        assert!(encoded.contains("process_cpu_nanos = 25"));
        assert!(encoded.contains("index_id = 7"));
        assert!(encoded.contains("placement = \"mixed\""));
        let summary = render_stdout_summary(&report, &path).unwrap();
        assert!(summary.contains("create_rows_per_second: 10000000000.000"));
        assert!(summary.contains("average_cpu_cores: 2.500"));
        let Some(WorkloadMetrics::CreateIndex { report: create }) =
            report.measured_runs[0].workload_metrics.as_mut()
        else {
            panic!("CREATE")
        };
        create.create_elapsed_nanos = 0;
        report.measured_runs[0].latency.sum_nanos = 0;
        report.aggregate.latency.sum_nanos = 0;
        let summary = render_stdout_summary(&report, &path).unwrap();
        assert!(!summary.contains("create_rows_per_second:"));
        assert!(!summary.contains("average_cpu_cores:"));
    }

    /// Purpose: Publish a canonical success report with strict numeric serialization.
    /// Expected: The installed report round-trips at its absolute path without staging residue
    /// or unsupported output fields.
    #[test]
    fn canonical_output_round_trips_one_entity() {
        let temp = TempDir::new().unwrap();
        let report = report(temp.path());
        fs::write(staged_path(&result_toml_path(temp.path())), "stale").unwrap();
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
        assert!(encoded.contains("elapsed_nanos = 10\n"));
        assert!(encoded.contains("sum_nanos = 10\n"));
        for value in ["-1", "18446744073709551616", "\"10\""] {
            let invalid =
                encoded.replace("elapsed_nanos = 10", &format!("elapsed_nanos = {value}"));
            assert!(toml::from_str::<InvocationReport>(&invalid).is_err());
        }
        assert!(encoded.contains("pause = false"));
        assert!(!encoded.contains("status ="));
        assert!(!encoded.contains("failure"));
        assert!(!staged_path(&installed).exists());
        assert!(!temp.path().join("benchmark-result.md").exists());
    }

    /// Purpose: Distinguish delete request throughput from affected-row throughput in canonical output.
    /// Expected: Both delete identities retain counters/units on round trip and print independent rates, including zero time.
    #[test]
    fn delete_output_reports_request_and_row_rates() {
        use crate::fixture::IndexMode;
        use crate::plan::{DeleteAllConfig, DeleteRandConfig};
        let temp = TempDir::new().unwrap();
        for workload in [
            ResolvedWorkload::DeleteAll(DeleteAllConfig {
                include_stats: false,
            }),
            ResolvedWorkload::DeleteRand(DeleteRandConfig {
                num: 3,
                seed: 9,
                threads: 1,
                sessions: 1,
                batch_size: 3,
                index: IndexMode::NonUnique,
                loaded_range: KeyRange { start: 0, len: 10 },
                include_stats: false,
            }),
        ] {
            let mut report = report(temp.path());
            let Phase::Benchmark {
                workload: target, ..
            } = &mut report.plan.phases[0]
            else {
                unreachable!()
            };
            *target = workload.clone();
            let full = matches!(workload, ResolvedWorkload::DeleteAll(_));
            report.aggregate.counters = WorkloadCounters {
                operations: if full { 1 } else { 3 },
                deleted_rows: 7,
                found: if full { 0 } else { 2 },
                not_found: if full { 0 } else { 1 },
                ..WorkloadCounters::default()
            };
            report.aggregate.latency.unit = workload.latency_unit();
            for elapsed in [2_000_000_000, 0] {
                report.aggregate.elapsed_nanos = elapsed;
                report.aggregate.operations_per_second =
                    operations_per_second(report.aggregate.counters.operations, elapsed);
                report.measured_runs = vec![MeasuredRunResult {
                    run_index: 1,
                    elapsed_nanos: elapsed,
                    counters: report.aggregate.counters,
                    operations_per_second: report.aggregate.operations_per_second,
                    latency: report.aggregate.latency.clone(),
                    workload_metrics: None,
                    internal_metrics: vec![],
                }];
                let path = write_plan_output(&report).unwrap();
                let decoded: InvocationReport =
                    toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
                assert_eq!(decoded, report);
                let summary = render_stdout_summary(&decoded, &path).unwrap();
                assert!(summary.contains("deleted_rows: 7\n"));
                let request_rate = if elapsed == 0 {
                    "0.000"
                } else if full {
                    "0.500"
                } else {
                    "1.500"
                };
                let row_rate = if elapsed == 0 { "0.000" } else { "3.500" };
                assert!(summary.contains(&format!("\noperations_per_second: {request_rate}\n")));
                assert!(summary.contains(&format!("\ndeleted_rows_per_second: {row_rate}\n")));
                assert!(summary.contains(&format!("latency_unit: {}\n", workload.latency_unit())));
            }
        }
    }

    /// Purpose: Clean up failed result installation without disturbing an existing destination.
    /// Expected: Installation failure preserves the destination directory and removes staged
    /// output.
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

    /// Purpose: Protect the public benchmark summary format.
    /// Expected: Labels, units, precision, ordering, and the absolute result path remain
    /// stable.
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

    /// Purpose: Preserve catalog checkpoint I/O totals, RSS measurements, and changed-table write amplification.
    /// Expected: Summaries sum every I/O record, exclude unchanged tables from the denominator, and report zero for an empty denominator.
    #[test]
    fn catalog_checkpoint_stdout_summary_preserves_io_totals_and_write_amplification() {
        use crate::fixture::CatalogCardinalities;
        use crate::measurement::SampledProcessRss;
        use crate::plan::{
            CatalogCheckpointCase, CatalogCheckpointConfig, CatalogCheckpointProfile,
        };
        use doradb_storage::id::{TableID, TrxID};
        use doradb_storage::{
            CatalogCheckpointOutcome, CatalogCheckpointReport, CatalogCheckpointResult,
            CatalogTableCheckpointChange, CatalogTableCheckpointIoStats,
        };

        let temp = TempDir::new().unwrap();
        let before = CatalogCardinalities {
            user_tables: 0,
            columns: 0,
            indexes: 0,
            bindings: 0,
            descriptor_rows: 0,
            descriptor_bytes: 0,
        };
        let mut report = measured_report(
            temp.path(),
            ResolvedWorkload::CatalogCheckpoint(CatalogCheckpointConfig {
                profile: CatalogCheckpointProfile::Small,
                case: CatalogCheckpointCase::ManagedCreate,
                include_stats: true,
            }),
            WorkloadMetrics::CatalogCheckpoint {
                profile: CatalogCheckpointProfile::Small,
                case: CatalogCheckpointCase::ManagedCreate,
                before,
                final_state: CatalogCardinalities {
                    user_tables: 1,
                    columns: 2,
                    ..before
                },
                sampled_process_rss: SampledProcessRss {
                    baseline_bytes: 1000,
                    peak_bytes: 1600,
                    peak_above_baseline_bytes: 600,
                },
                checkpoint: CatalogCheckpointResult {
                    outcome: CatalogCheckpointOutcome::Published {
                        catalog_replay_start_ts: TrxID::new(42),
                    },
                    report: CatalogCheckpointReport {
                        catalog_ddl_txn_count: 1,
                        table_changes: vec![
                            CatalogTableCheckpointChange {
                                table_id: TableID::new(1),
                                before_row_count: 0,
                                after_row_count: 1,
                            },
                            CatalogTableCheckpointChange {
                                table_id: TableID::new(2),
                                before_row_count: 0,
                                after_row_count: 2,
                            },
                        ]
                        .into_boxed_slice(),
                        table_io: [
                            (1, 100, 200, 600, 100),
                            (2, 300, 800, 900, 200),
                            (3, 500, 9000, 0, 0),
                        ]
                        .into_iter()
                        .map(
                            |(
                                id,
                                compact_bytes_read,
                                final_compact_bytes,
                                lwc_bytes_written,
                                index_bytes_written,
                            )| CatalogTableCheckpointIoStats {
                                table_id: TableID::new(id),
                                compact_bytes_read,
                                final_compact_bytes,
                                lwc_bytes_written,
                                index_bytes_written,
                            },
                        )
                        .collect(),
                        metadata_bytes_written: 400,
                    },
                },
            },
        );
        assert_metric_shape_errors(&report, "catalog checkpoint");
        for (empty_denominator, expected_compact, expected_amplification) in
            [(false, "1000", "2.200000"), (true, "0", "0.000000")]
        {
            if empty_denominator {
                let Some(WorkloadMetrics::CatalogCheckpoint { checkpoint, .. }) =
                    report.measured_runs[0].workload_metrics.as_mut()
                else {
                    unreachable!()
                };
                for table in &mut checkpoint.report.table_io {
                    table.final_compact_bytes = 0;
                }
            }
            let summary = render_stdout_summary(&report, Path::new("result.toml")).unwrap();
            for (name, expected) in [
                ("catalog_profile", "small"),
                ("catalog_case", "managed-create"),
                ("sampled_process_rss_baseline_bytes", "1000"),
                ("sampled_process_rss_peak_bytes", "1600"),
                ("sampled_process_rss_peak_above_baseline_bytes", "600"),
                ("catalog_compact_bytes_read", "900"),
                ("catalog_lwc_bytes_written", "1500"),
                ("catalog_index_bytes_written", "300"),
                ("catalog_metadata_bytes_written", "400"),
                ("catalog_changed_final_compact_bytes", expected_compact),
                ("catalog_checkpoint_bytes_written", "2200"),
                ("catalog_write_amplification", expected_amplification),
            ] {
                let expected = format!("{name}: {expected}");
                assert!(
                    summary.lines().any(|line| line == expected),
                    "missing {expected:?} in {summary}"
                );
            }
        }
    }

    /// Purpose: Preserve filesystem error context when an output root is missing or a directory blocks the staging path.
    /// Expected: Failures identify the offending path, preserve the blocking directory, and publish no canonical artifact.
    #[test]
    fn output_path_failures_preserve_context_and_blocking_directory() {
        let temp = TempDir::new().unwrap();
        let missing = temp.path().join("missing-root");
        let error = write_plan_output(&report(&missing))
            .unwrap_err()
            .to_string();
        assert!(error.contains("failed to resolve benchmark artifact directory"));
        assert!(error.contains(missing.to_str().unwrap()));
        assert!(!missing.exists());

        let canonical = result_toml_path(temp.path());
        let staged = staged_path(&canonical);
        fs::create_dir(&staged).unwrap();
        let error = write_plan_output(&report(temp.path()))
            .unwrap_err()
            .to_string();
        assert!(error.contains("failed to write benchmark artifact"));
        assert!(error.contains(staged.to_str().unwrap()));
        assert!(staged.is_dir());
        assert!(!canonical.exists());
    }

    /// Purpose: Keep checkpoint work and retry waiting distinguishable in summaries.
    /// Expected: Output preserves separate attempt/wait counts and durations and rejects missing or incompatible metrics.
    #[test]
    fn checkpoint_stdout_summary_includes_attempt_and_wait_breakdown() {
        let temp = TempDir::new().unwrap();
        let report = measured_report(
            temp.path(),
            ResolvedWorkload::CheckpointTable(CheckpointTableConfig {
                include_stats: false,
            }),
            WorkloadMetrics::CheckpointTable {
                attempt_count: 3,
                attempt_elapsed_nanos: 7,
                retry_wait_count: 2,
                retry_wait_elapsed_nanos: 2,
            },
        );
        assert_metric_shape_errors(&report, "checkpoint");
        let summary = render_stdout_summary(&report, Path::new("result.toml")).unwrap();
        assert!(summary.contains("checkpoint_attempt_count: 3\n"));
        assert!(summary.contains("checkpoint_attempt_elapsed_nanos: 7\n"));
        assert!(summary.contains("checkpoint_retry_wait_count: 2\n"));
        assert!(summary.contains("checkpoint_retry_wait_elapsed_nanos: 2\n"));
    }

    /// Purpose: Expose parallel scan partitioning and row throughput consistently.
    /// Expected: Summaries retain partition diagnostics and defined row rates; missing, incompatible, or inconsistent metrics fail.
    #[test]
    fn parallel_scan_stdout_summary_includes_partition_and_row_throughput() {
        let temp = TempDir::new().unwrap();
        let mut report = measured_report(
            temp.path(),
            ResolvedWorkload::ParallelTableScan(ParallelTableScanConfig {
                num: 1,
                target_partitions: 4,
                loaded_range: KeyRange { start: 0, len: 8 },
                include_stats: false,
            }),
            WorkloadMetrics::ParallelTableScan {
                target_partitions: 4,
                actual_partitions: 3,
            },
        );
        report.aggregate.counters.rows_returned = 8;
        report.measured_runs[0].counters.rows_returned = 8;
        assert_metric_shape_errors(&report, "parallel scan");
        let mut inconsistent = report.clone();
        let mut second = inconsistent.measured_runs[0].clone();
        second.run_index = 2;
        second.workload_metrics = Some(WorkloadMetrics::ParallelTableScan {
            target_partitions: 4,
            actual_partitions: 2,
        });
        inconsistent.measured_runs.push(second);
        assert_eq!(
            render_stdout_summary(&inconsistent, Path::new("result.toml"))
                .unwrap_err()
                .to_string(),
            "parallel scan report has inconsistent per-run partition metrics"
        );
        let summary = render_stdout_summary(&report, Path::new("result.toml")).unwrap();
        assert!(summary.contains("target_partitions: 4\n"));
        assert!(summary.contains("actual_partitions: 3\n"));
        assert!(summary.contains("rows_returned: 8\n"));
        assert!(summary.contains("rows_per_second: 800000000.000\n"));

        report.aggregate.elapsed_nanos = 0;
        let summary = render_stdout_summary(&report, Path::new("result.toml")).unwrap();
        assert!(summary.contains("rows_per_second: 0.000\n"));
    }
}
