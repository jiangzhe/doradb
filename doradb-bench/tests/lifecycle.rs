#[cfg(test)]
mod tests {
    use doradb_bench::measurement::{LatencyUnit, WorkloadCounters, WorkloadMetrics};
    use doradb_bench::plan_output::InvocationReport;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use tempfile::TempDir;

    fn run_bench(root: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_doradb-bench"))
            .arg("--root")
            .arg(root)
            .args(args)
            .output()
            .unwrap()
    }

    fn assert_success(output: Output) -> String {
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        if !output.status.success() {
            panic!(
                "command failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                stdout,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        stdout
    }

    fn assert_failure(output: Output) -> String {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if output.status.success() {
            panic!(
                "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                stderr
            );
        }
        stderr
    }

    fn execute_plan(temp: &TempDir, name: &str, phases: &str) -> (PathBuf, InvocationReport) {
        let source = temp.path().join(format!("{name}.toml"));
        fs::write(
            &source,
            format!("name = \"{name}\"\n[engine.transaction]\nlog_sync = \"none\"\n{phases}"),
        )
        .unwrap();
        let root = temp.path().join(format!("{name}-root"));
        let stdout = assert_success(run_bench(&root, &["--plan", source.to_str().unwrap()]));
        let encoded = fs::read_to_string(root.join("benchmark-result.toml")).unwrap();
        let report = toml::from_str(&encoded).unwrap();
        let report: InvocationReport = report;
        let workload = report.plan.phases.last().unwrap().workload().identity();
        assert!(stdout.contains("DoraDB benchmark summary\n"));
        assert!(stdout.contains(&format!("workload: {workload}\n")));
        assert!(stdout.contains(&format!(
            "measured_runs: {}\n",
            report.aggregate.measured_runs
        )));
        assert!(stdout.contains(&format!(
            "operations: {}\n",
            report.aggregate.counters.operations
        )));
        assert!(stdout.contains("operations_per_second: "));
        assert!(stdout.contains("average_latency_nanos: "));
        assert!(stdout.contains("p95_latency_nanos: "));
        assert!(stdout.contains("p99_latency_nanos: "));
        let detailed_result = fs::canonicalize(&root)
            .unwrap()
            .join("benchmark-result.toml");
        assert!(stdout.contains(&format!("detailed_result: {}\n", detailed_result.display())));
        assert!(!root.join("benchmark-result.md").exists());
        assert!(!root.join("benchmark-manifest.toml").exists());
        assert!(!root.join("benchmark-result.csv").exists());
        assert!(!root.join("benchmark-internal-stats.csv").exists());
        (root, report)
    }

    fn assert_update_counters(counters: WorkloadCounters) {
        assert_eq!(counters.operations, counters.updated_rows);
        assert_eq!(counters.inserted_rows, 0);
        assert_eq!(counters.found, 0);
        assert_eq!(counters.not_found, 0);
        assert_eq!(counters.rows_returned, 0);
        assert_eq!(counters.expected_outcomes.duplicate_key, 0);
        assert_eq!(counters.expected_outcomes.write_conflict, 0);
    }

    #[test]
    fn required_plan_is_the_only_cli_contract() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        assert_failure(run_bench(&root, &[]));
        assert!(!root.exists());
        assert_failure(run_bench(&root, &["cleanup"]));
        assert_failure(run_bench(&root, &["prepare"]));
        assert_failure(run_bench(&root, &["run"]));

        let (root, _) = execute_plan(
            &temp,
            "noop",
            "\n[[phase]]\nkind = \"benchmark\"\nworkload = { type = \"trx-noop\", num = 2 }\n",
        );
        assert_failure(run_bench(&root, &["cleanup"]));
        assert!(root.exists());
    }

    #[test]
    fn root_environment_and_explicit_precedence_are_retained() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("noop.toml");
        fs::write(
            &source,
            "[engine.transaction]\nlog_sync = \"none\"\n\
             [[phase]]\nkind = \"benchmark\"\n\
             workload = { type = \"trx-noop\", num = 1 }\n",
        )
        .unwrap();

        let environment_root = temp.path().join("environment-root");
        let output = Command::new(env!("CARGO_BIN_EXE_doradb-bench"))
            .env("DORADB_BENCH_ROOT", &environment_root)
            .args(["--plan", source.to_str().unwrap()])
            .output()
            .unwrap();
        assert_success(output);
        assert!(environment_root.join("benchmark-result.toml").exists());

        let ignored_environment_root = temp.path().join("ignored-environment-root");
        let explicit_root = temp.path().join("explicit-root");
        let output = Command::new(env!("CARGO_BIN_EXE_doradb-bench"))
            .env("DORADB_BENCH_ROOT", &ignored_environment_root)
            .args([
                "--root",
                explicit_root.to_str().unwrap(),
                "--plan",
                source.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert_success(output);
        assert!(explicit_root.join("benchmark-result.toml").exists());
        assert!(!ignored_environment_root.exists());
    }

    #[test]
    fn dependent_read_and_index_ddl_plans_execute_with_exact_equations() {
        let temp = TempDir::new().unwrap();
        let read_cases = [
            (
                "lookup-seq",
                "unique",
                "lookup-seq",
                "num = 7, batch_size = 2",
            ),
            (
                "lookup-rand",
                "unique",
                "lookup-rand",
                "num = 7, seed = 9, batch_size = 2",
            ),
            (
                "table-scan",
                "none",
                "table-scan",
                "num = 2, batch_size = 1",
            ),
            (
                "index-scan",
                "non-unique",
                "index-scan",
                "num = 3, range = 2, seed = 9, batch_size = 2",
            ),
            (
                "index-stream",
                "non-unique",
                "index-stream",
                "num = 3, range = 2, seed = 9",
            ),
        ];
        for (name, index, workload, controls) in read_cases {
            let phases = format!(
                "\n[[phase]]\nworkload = {{ type = \"create-table\", index = \"{index}\" }}\n\
             [[phase]]\nworkload = {{ type = \"insert-seq\", num = 8, batch_size = 4 }}\n\
             [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 2\n\
             workload = {{ type = \"{workload}\", {controls} }}\n"
            );
            let (_root, report) = execute_plan(&temp, name, &phases);
            assert_eq!(report.measured_runs.len(), 2);
            assert_eq!(report.aggregate.measured_runs, 2);
            assert_eq!(
                report.aggregate.counters.operations,
                report
                    .measured_runs
                    .iter()
                    .map(|run| run.counters.operations)
                    .sum::<u64>()
            );
            assert!(report.aggregate.latency.sample_count > 0);
        }

        let phases = "\n[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
                  [[phase]]\nworkload = { type = \"insert-seq\", num = 8, batch_size = 4 }\n\
                  [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"index-ddl\", num = 1 }\n";
        let (_root, report) = execute_plan(&temp, "index-ddl", phases);
        assert_eq!(report.aggregate.counters.operations, 2);
        assert_eq!(report.aggregate.latency.sample_count, 1);
    }

    #[test]
    fn random_index_updates_replay_unique_keys_and_non_unique_payloads() {
        let temp = TempDir::new().unwrap();
        let unique = "\n[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n\
                      [[phase]]\nworkload = { type = \"insert-seq\", num = 12, batch_size = 4 }\n\
                      [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 3\n\
                      workload = { type = \"update-rand\", num = 11, seed = 7, change_key = true, threads = 2, sessions = 3, value_size = \"17 B\", batch_size = 2 }\n";
        let (_root, report) = execute_plan(&temp, "update-unique", unique);
        assert_eq!(report.measured_runs.len(), 3);
        let updated_rows = report.measured_runs[0].counters.updated_rows;
        assert!(updated_rows > 0);
        for run in &report.measured_runs {
            assert_update_counters(run.counters);
            assert_eq!(run.counters.updated_rows, updated_rows);
            assert_eq!(run.latency.unit, LatencyUnit::UpdateRangeTransaction);
            assert_eq!(run.latency.sample_count, 6);
        }
        assert_update_counters(report.aggregate.counters);
        assert_eq!(report.aggregate.counters.updated_rows, updated_rows * 3);
        assert_eq!(report.aggregate.latency.sample_count, 18);

        let non_unique = "\n[[phase]]\nworkload = { type = \"create-table\", index = \"non-unique\" }\n\
                          [[phase]]\nworkload = { type = \"insert-rand\", num = 32, seed = 2, batch_size = 8 }\n\
                          [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 2\n\
                          workload = { type = \"update-rand\", num = 12, seed = 5, change_key = false, threads = 2, sessions = 4, value_size = \"9 B\", batch_size = 2 }\n";
        let (_root, report) = execute_plan(&temp, "update-non-unique", non_unique);
        assert_eq!(report.measured_runs.len(), 2);
        assert!(report.prepare_phases[1].counters.inserted_rows > 0);
        for run in &report.measured_runs {
            assert_update_counters(run.counters);
            assert_eq!(run.latency.unit, LatencyUnit::UpdateRangeTransaction);
            assert_eq!(run.latency.sample_count, 8);
        }
        assert_update_counters(report.aggregate.counters);
        assert_eq!(report.aggregate.latency.sample_count, 16);
    }

    #[test]
    fn checked_in_update_template_executes_end_to_end() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("update-template-root");
        let template = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("templates")
            .join("update-rand.toml");
        let stdout = assert_success(run_bench(&root, &["--plan", template.to_str().unwrap()]));
        let report: InvocationReport =
            toml::from_str(&fs::read_to_string(root.join("benchmark-result.toml")).unwrap())
                .unwrap();
        assert!(stdout.contains("workload: update-rand\n"));
        assert_eq!(report.measured_runs.len(), 3);
        for run in &report.measured_runs {
            assert_update_counters(run.counters);
            assert!(run.counters.updated_rows > 0);
            assert_eq!(run.latency.unit, LatencyUnit::UpdateRangeTransaction);
            assert_eq!(run.latency.sample_count, 12);
        }
        assert_update_counters(report.aggregate.counters);
        assert_eq!(report.aggregate.latency.sample_count, 36);
    }

    #[test]
    fn multi_table_lock_plan_replays_and_releases_all_claims() {
        let temp = TempDir::new().unwrap();
        let phases = "\n[[phase]]\nworkload = { type = \"create-table\", index = \"none\", tables = 4 }\n\
                  [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 2\n\
                  workload = { type = \"lock-table\", num = 8, scenario = \"basic\", mode = \"shared\", scope = \"session\", unlock = true, random = true, seed = 11, threads = 2, sessions = 4 }\n";
        let (_root, report) = execute_plan(&temp, "lock-table", phases);
        assert_eq!(report.aggregate.counters.operations, 16);
        assert_eq!(report.aggregate.latency.sample_count, 16);
    }

    #[test]
    fn specialized_lock_plans_coordinate_and_drain_participants() {
        let temp = TempDir::new().unwrap();
        for (scenario, mode, width, tables) in [
            ("nested-covered", "shared", 3, 3),
            ("convert", "exclusive", 1, 1),
            ("enqueue", "exclusive", 3, 1),
            ("cancel-head", "exclusive", 3, 1),
            ("cancel-middle", "exclusive", 3, 1),
            ("cancel-tail", "exclusive", 3, 1),
            ("promote", "exclusive", 3, 1),
            ("first-touch", "shared", 1, 1),
            ("scope-close", "shared", 3, 3),
        ] {
            let phases = format!(
                "\n[[phase]]\nworkload = {{ type = \"create-table\", index = \"none\", tables = {tables} }}\n\
                 [[phase]]\nkind = \"benchmark\"\n\
                 workload = {{ type = \"lock-table\", num = 1, scenario = \"{scenario}\", mode = \"{mode}\", width = {width}, threads = 1, sessions = 1 }}\n"
            );
            let (_root, report) = execute_plan(&temp, &format!("lock-{scenario}"), &phases);
            assert_eq!(report.aggregate.counters.operations, 1, "{scenario}");
            assert_eq!(report.aggregate.latency.sample_count, 1, "{scenario}");
        }
    }

    #[test]
    fn invalid_dependent_plan_fails_before_root_creation() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("invalid.toml");
        fs::write(
            &source,
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n\
         [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lookup-seq\", num = 1 }\n",
        )
        .unwrap();
        let root = temp.path().join("invalid-root");
        let output = run_bench(&root, &["--plan", source.to_str().unwrap()]);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("DoraDB benchmark summary"));
        assert!(assert_failure(output).contains("requires loaded benchmark data"));
        assert!(!root.exists());
    }

    #[test]
    fn single_table_checkpoint_plan_publishes_canonical_metrics() {
        let temp = TempDir::new().unwrap();
        let phases = "\n[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
                      [[phase]]\nworkload = { type = \"insert-seq\", num = 8, value_size = \"32 KiB\", batch_size = 8 }\n\
                      [[phase]]\nworkload = { type = \"freeze-table\", max_rows = 4 }\n\
                      [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 0\nmeasured_runs = 1\n\
                      workload = { type = \"checkpoint-table\" }\n";
        let source = temp.path().join("checkpoint.toml");
        fs::write(
            &source,
            format!("name = \"checkpoint\"\n[engine.transaction]\nlog_sync = \"none\"\n{phases}"),
        )
        .unwrap();
        let root = temp.path().join("checkpoint-root");
        let stdout = assert_success(run_bench(&root, &["--plan", source.to_str().unwrap()]));
        let report: InvocationReport =
            toml::from_str(&fs::read_to_string(root.join("benchmark-result.toml")).unwrap())
                .unwrap();
        assert_eq!(report.prepare_phases.len(), 3);
        let Some(WorkloadMetrics::FreezeTable {
            approximate_rows,
            page_count,
            stable_page_count,
        }) = report.prepare_phases[2].workload_metrics
        else {
            panic!("freeze prepare phase must retain its canonical metrics")
        };
        assert!(approximate_rows > 0 && approximate_rows < 8);
        assert!(page_count > 0);
        assert!(stable_page_count <= page_count);
        assert_eq!(report.measured_runs.len(), 1);
        let Some(WorkloadMetrics::CheckpointTable {
            attempt_count,
            attempt_elapsed_nanos,
            retry_wait_count,
            retry_wait_elapsed_nanos,
        }) = report.measured_runs[0].workload_metrics
        else {
            panic!("checkpoint measured run must retain retry metrics")
        };
        assert_eq!(attempt_count, retry_wait_count + 1);
        assert!(attempt_elapsed_nanos > 0);
        if retry_wait_count == 0 {
            assert_eq!(retry_wait_elapsed_nanos, 0);
        }
        assert_eq!(report.aggregate.counters.operations, 1);
        assert_eq!(report.aggregate.latency.sample_count, 1);
        assert_eq!(report.aggregate.latency.unit, LatencyUnit::TableCheckpoint);
        assert!(stdout.contains(&format!("checkpoint_attempt_count: {attempt_count}\n")));
        assert!(stdout.contains(&format!(
            "checkpoint_retry_wait_count: {retry_wait_count}\n"
        )));
    }

    #[test]
    fn whole_page_freeze_failure_retains_root_without_success_artifact() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("invalid-freeze.toml");
        fs::write(
            &source,
            "[engine.transaction]\nlog_sync = \"none\"\n\
             [[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
             [[phase]]\nworkload = { type = \"insert-seq\", num = 8, value_size = \"128 B\", batch_size = 8 }\n\
             [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 0\nmeasured_runs = 1\n\
             workload = { type = \"freeze-table\", max_rows = 4 }\n",
        )
        .unwrap();
        let root = temp.path().join("invalid-freeze-root");
        let output = run_bench(&root, &["--plan", source.to_str().unwrap()]);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("DoraDB benchmark summary"));
        assert!(assert_failure(output).contains("did not install a nonempty proper prefix"));
        assert!(root.exists());
        assert!(!root.join("benchmark-result.toml").exists());
    }
}
