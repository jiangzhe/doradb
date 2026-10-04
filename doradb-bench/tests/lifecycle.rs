#[cfg(test)]
mod tests {
    use doradb_bench::fixture::{IndexMode, KeyRange, PlacementKind, RowPlacement};
    use doradb_bench::measurement::{
        ExpectedOutcomeCounters, LatencyUnit, MeasuredRunResult, WorkloadCounters, WorkloadMetrics,
    };
    use doradb_bench::plan::{Phase, ResolvedWorkload};
    use doradb_bench::plan_output::InvocationReport;
    use rustix::process::{Pid, Signal, kill_process};
    use std::fs;
    use std::io::{BufRead, BufReader, Read};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitStatus, Output, Stdio};
    use std::sync::mpsc::{self, Receiver};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    const CREATE_INDEX_ENGINE: &str = concat!(
        "[engine.thread_pool]\nworker_threads = 1\n",
        "[engine.index_buffer]\nmax_mem_size = '16 MiB'\nmax_file_size = '32 MiB'\n",
        "[engine.data_buffer]\nmax_mem_size = '16 MiB'\nmax_file_size = '32 MiB'\n",
        "[engine.file]\nreadonly_buffer_size = '17 MiB'\n",
    );

    const SUBPROCESS_TIMEOUT: Duration = Duration::from_secs(20);

    struct ChildGuard {
        child: Child,
        running: bool,
    }

    impl ChildGuard {
        fn new(child: Child) -> Self {
            Self {
                child,
                running: true,
            }
        }

        fn pid(&self) -> Pid {
            Pid::from_child(&self.child)
        }

        fn resume(&self) {
            kill_process(self.pid(), Signal::CONT).unwrap();
        }

        fn wait_until(&mut self, deadline: Instant) -> ExitStatus {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    self.running = false;
                    return status;
                }
                assert!(Instant::now() < deadline, "benchmark child timed out");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if self.running {
                let _ = kill_process(self.pid(), Signal::CONT);
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }

    fn capture_lines<R>(reader: R) -> (Receiver<String>, JoinHandle<String>)
    where
        R: Read + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let mut captured = String::new();
            for line in BufReader::new(reader).lines() {
                let line = line.unwrap();
                captured.push_str(&line);
                captured.push('\n');
                let _ = sender.send(line);
            }
            captured
        });
        (receiver, handle)
    }

    fn wait_for_stopped(pid: i32, deadline: Instant) {
        let status_path = format!("/proc/{pid}/status");
        loop {
            let status = fs::read_to_string(&status_path).unwrap();
            if status
                .lines()
                .find_map(|line| line.strip_prefix("State:"))
                .is_some_and(|state| state.trim_start().starts_with('T'))
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "child did not enter stopped state"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

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

    #[track_caller]
    fn assert_plan_rejected_before_root_creation(plan: &str, expected_error: &str) {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("invalid.toml");
        fs::write(&source, plan).unwrap();
        let root = temp.path().join("invalid-root");
        let output = run_bench(&root, &["--plan", source.to_str().unwrap()]);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("DoraDB benchmark summary"));
        let stderr = assert_failure(output);
        assert!(
            stderr.contains(expected_error),
            "expected {expected_error:?}: {stderr}"
        );
        assert!(!root.exists());
    }

    fn execute_plan(temp: &TempDir, name: &str, phases: &str) -> (PathBuf, InvocationReport) {
        let source = temp.path().join(format!("{name}.toml"));
        let log_sync = if phases.contains("\"recovery\"") {
            "fsync"
        } else {
            "none"
        };
        fs::write(
            &source,
            format!("name = \"{name}\"\n[engine.transaction]\nlog_sync = \"{log_sync}\"\n{phases}"),
        )
        .unwrap();
        let root = temp.path().join(format!("{name}-root"));
        let output = run_bench(&root, &["--plan", source.to_str().unwrap()]);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("DORADB_BENCH_"));
        let stdout = assert_success(output);
        let encoded = fs::read_to_string(root.join("benchmark-result.toml")).unwrap();
        let report = toml::from_str(&encoded).unwrap();
        let report: InvocationReport = report;
        let Phase::Benchmark { measurement, .. } = report.plan.phases.last().unwrap() else {
            panic!("final phase must be a benchmark")
        };
        assert!(!measurement.pause);
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

    #[track_caller]
    fn assert_drained_transaction_diagnostics(name: &str, run: &MeasuredRunResult) {
        // Redo counters can be published after commit wakes the caller;
        // workload transaction counts are checked through latency samples.
        assert!(
            run.internal_metrics
                .iter()
                .any(|metric| metric.name == "transaction.trx_count"),
            "{name}: run {} omitted transaction diagnostics",
            run.run_index
        );
        let locks = run
            .internal_metrics
            .iter()
            .find(|metric| metric.name == "logical_lock.current_physical_resources")
            .unwrap();
        assert_eq!(
            locks.value, 0,
            "{name}: run {} retained worker locks: {locks:?}",
            run.run_index
        );
    }

    fn assert_update_counters(counters: WorkloadCounters) {
        assert_eq!(counters.operations, counters.updated_rows);
        assert_eq!(counters.inserted_rows, 0);
        assert_eq!(counters.deleted_rows, 0);
        assert_eq!(counters.found, 0);
        assert_eq!(counters.not_found, 0);
        assert_eq!(counters.rows_returned, 0);
        assert_eq!(counters.expected_outcomes.duplicate_key, 0);
        assert_eq!(counters.expected_outcomes.write_conflict, 0);
    }

    #[track_caller]
    fn assert_recovery_fixture(
        name: &str,
        index: Option<&str>,
        insert: Option<&str>,
        checkpoint: bool,
        stats: bool,
    ) {
        use doradb_bench::measurement::InternalMetricKind;
        let temp = TempDir::new().unwrap();
        let rows = if insert.is_none() {
            0
        } else if checkpoint {
            // Keep enough pages for both a checkpointed prefix and a hot suffix.
            4096
        } else {
            // Exercise multiple transaction batches with a small semantic fixture.
            128
        };
        let mut phases = String::new();
        if let Some(index) = index {
            phases.push_str(&format!(
                "[[phase]]\nworkload = {{ type = 'create-table', index = '{index}' }}\n"
            ));
        }
        if let Some(insert) = insert {
            phases.push_str(&format!("[[phase]]\nworkload = {{ type = '{insert}', num = {rows}, threads = 1, sessions = 1, batch_size = 100, value_size = '128 B' }}\n"));
        }
        if checkpoint {
            phases.push_str("[[phase]]\nworkload = { type = 'freeze-table', max_rows = 2048 }\n[[phase]]\nworkload = { type = 'checkpoint-table' }\n");
        }
        // Double quotes also select durable configuration in the shared invocation helper.
        phases.push_str(&format!("[[phase]]\nkind = 'benchmark'\nworkload = {{ type = \"recovery\", include_stats = {stats} }}\n"));
        let (root, result) = execute_plan(&temp, name, &phases);
        let run = &result.measured_runs[0];
        let Some(WorkloadMetrics::Recovery {
            report,
            verification,
        }) = &run.workload_metrics
        else {
            panic!("missing recovery metrics");
        };
        assert_eq!(
            run.counters,
            WorkloadCounters {
                operations: 1,
                ..WorkloadCounters::default()
            }
        );
        assert_eq!(run.latency.sample_count, 1);
        assert_eq!(run.latency.sum_nanos, run.elapsed_nanos);
        assert_eq!(run.latency.unit, LatencyUnit::EngineRecovery);
        assert_eq!(verification.table_count, u64::from(index.is_some()));
        let inserted = result
            .prepare_phases
            .iter()
            .map(|phase| phase.counters.inserted_rows)
            .sum::<u64>();
        assert_eq!(inserted, rows);
        assert_eq!(verification.verified_rows, inserted);
        assert_eq!(
            verification.index_verified,
            index.is_some_and(|index| index != "none")
        );
        assert_eq!(verification.fingerprint.len(), 32);
        assert!(report.redo.consumed_bytes >= report.redo.validated_payload_bytes);
        if insert.is_some() {
            assert_eq!(report.work.user_row_ops_seen, inserted);
            assert_eq!(
                report.work.user_row_ops_seen,
                report.work.user_row_ops_applied + report.work.user_row_ops_skipped
            );
            assert!(report.work.hot_inserts > 0);
            assert!(report.redo.transactions_decoded > 0);
            if checkpoint {
                assert!(report.work.user_row_ops_skipped > 0);
                assert!(report.work.hot_inserts < inserted);
            } else {
                assert_eq!(report.work.hot_inserts, inserted);
                assert_eq!(report.work.user_row_ops_skipped, 0);
            }
            assert_eq!(
                report.work.index_entries_inserted,
                if verification.index_verified {
                    inserted
                } else {
                    0
                }
            );
        }
        assert_eq!(run.internal_metrics.is_empty(), !stats);
        assert_eq!(
            run.internal_metrics
                .iter()
                .any(|metric| metric.name.starts_with("hot_index_build.")),
            stats && index.is_some_and(|index| index != "none"),
        );

        assert!(
            !run.internal_metrics
                .iter()
                .any(|metric| metric.kind == InternalMetricKind::CounterDelta)
        );
        if stats {
            assert!(
                run.internal_metrics
                    .iter()
                    .any(|metric| metric.kind == InternalMetricKind::CumulativeCounter)
            );
        }
        let encoded = toml::to_string_pretty(&result).unwrap();
        let decoded: InvocationReport = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded, result);
        assert!(root.exists());
    }

    fn recovery_profiler_case(corrupt_reopen: bool) {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("pause.toml");
        fs::write(&source, "[engine.transaction]\nlog_sync = 'fsync'\n[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 512, batch_size = 100 }\n[[phase]]\nkind = 'benchmark'\npause = true\nworkload = { type = 'recovery' }").unwrap();
        let root = temp.path().join("root");
        let mut child = Command::new(env!("CARGO_BIN_EXE_doradb-bench"))
            .arg("--root")
            .arg(&root)
            .arg("--plan")
            .arg(&source)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        let (stdout_lines, stdout_handle) = capture_lines(child.stdout.take().unwrap());
        let (stderr_lines, stderr_handle) = capture_lines(child.stderr.take().unwrap());
        let mut child = ChildGuard::new(child);
        let deadline = Instant::now() + SUBPROCESS_TIMEOUT;
        assert_eq!(
            stderr_lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap(),
            format!("DORADB_BENCH_PAUSING pid={pid} phase=3 workload=recovery resume=SIGCONT")
        );
        wait_for_stopped(pid, deadline);
        assert!(root.join("storage-layout.toml").exists());
        assert!(!root.join("benchmark-result.toml").exists());
        assert!(stdout_lines.try_recv().is_err());
        // Owner teardown must release every storage descriptor, including root lease and swap files.
        for entry in fs::read_dir(format!("/proc/{pid}/fd")).unwrap() {
            if let Ok(target) = fs::read_link(entry.unwrap().path()) {
                assert!(
                    !target.starts_with(&root),
                    "storage descriptor survived pause: {}",
                    target.display()
                );
            }
        }
        if corrupt_reopen {
            fs::write(root.join("storage-layout.toml"), "invalid marker").unwrap();
        }
        child.resume();
        let status = child.wait_until(deadline);
        let stdout = stdout_handle.join().unwrap();
        let stderr = stderr_handle.join().unwrap();
        assert_eq!(stderr.matches("DORADB_BENCH_PAUSING").count(), 1);
        assert_eq!(stderr.matches("DORADB_BENCH_RESUMED").count(), 1);
        assert_eq!(status.success(), !corrupt_reopen, "{stderr}");
        assert_eq!(stdout.contains("DoraDB benchmark summary"), !corrupt_reopen);
        assert_eq!(root.join("benchmark-result.toml").exists(), !corrupt_reopen);
        assert!(root.exists());
        if !corrupt_reopen {
            let result: InvocationReport =
                toml::from_str(&fs::read_to_string(root.join("benchmark-result.toml")).unwrap())
                    .unwrap();
            let Some(WorkloadMetrics::Recovery { verification, .. }) =
                &result.measured_runs[0].workload_metrics
            else {
                panic!("recovery metrics missing");
            };
            assert_eq!(verification.verified_rows, 512);
        }
    }

    #[track_caller]
    fn execute_small_template(name: &str) -> (String, InvocationReport) {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("root");
        let templates = Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
        let source = fs::read_to_string(templates.join(format!("{name}.toml"))).unwrap();
        let mut plan: toml::Value = toml::from_str(&source).unwrap();
        // Keep template controls and durable engine settings, with small fixtures
        // and enough batches to exercise partial per-session tails.
        for phase in plan["phase"].as_array_mut().unwrap() {
            let workload = phase["workload"].as_table_mut().unwrap();
            if matches!(
                workload["type"].as_str().unwrap(),
                "insert-seq"
                    | "insert-rand"
                    | "update-rand"
                    | "update-point-rand"
                    | "upsert-point-rand"
                    | "delete-rand"
            ) {
                workload.insert("num".to_owned(), 100.into());
                workload.insert("batch_size".to_owned(), 10.into());
                if let Some(range) = workload.get_mut("key_range") {
                    range["len"] = 200.into();
                }
            }
        }
        let defaults = plan["engine_defaults"].as_str().unwrap();
        fs::copy(templates.join(defaults), temp.path().join(defaults)).unwrap();
        let source = temp.path().join(format!("{name}.toml"));
        fs::write(&source, toml::to_string(&plan).unwrap()).unwrap();
        let stdout = assert_success(run_bench(&root, &["--plan", source.to_str().unwrap()]));
        let report: InvocationReport =
            toml::from_str(&fs::read_to_string(root.join("benchmark-result.toml")).unwrap())
                .unwrap();
        assert_eq!(
            report
                .prepare_phases
                .iter()
                .map(|phase| phase.counters.inserted_rows)
                .sum::<u64>(),
            100,
            "{name} fixture"
        );
        (stdout, report)
    }

    #[track_caller]
    fn check_upsert_template(name: &str, overwrite: bool) {
        let (stdout, report) = execute_small_template(name);
        assert_eq!(report.measured_runs.len(), 1);
        let run = &report.measured_runs[0];
        assert_eq!(run.counters.operations, 100);
        assert_eq!(run.counters.inserted_rows + run.counters.updated_rows, 100);
        assert_eq!(run.counters.found, run.counters.updated_rows);
        assert_eq!(run.counters.not_found, run.counters.inserted_rows);
        assert_eq!(run.counters.deleted_rows, 0);
        assert_eq!(run.counters.rows_returned, 0);
        assert_eq!(
            run.counters.expected_outcomes,
            ExpectedOutcomeCounters::default()
        );
        assert_eq!(run.latency.unit, LatencyUnit::UpsertPointBatchTransaction);
        assert_eq!(run.latency.sample_count, 12);
        assert!(run.counters.updated_rows > 0);
        if overwrite {
            assert_eq!(run.counters.inserted_rows, 0);
        } else {
            assert!(run.counters.inserted_rows > 0);
        }
        for (name, count) in [
            ("operations", 100),
            ("inserted_rows", run.counters.inserted_rows),
            ("updated_rows", run.counters.updated_rows),
        ] {
            assert!(stdout.contains(&format!("{name}: {count}\n")));
            let rate = if run.elapsed_nanos == 0 {
                0.0
            } else {
                count as f64 * 1_000_000_000.0 / run.elapsed_nanos as f64
            };
            assert!(stdout.contains(&format!("{name}_per_second: {rate:.3}\n")));
        }
        let ResolvedWorkload::UpsertPointRand(config) =
            report.plan.phases.last().unwrap().workload()
        else {
            panic!("upsert template identity")
        };
        assert_eq!(
            config.key_range,
            KeyRange {
                start: 0,
                len: if overwrite { 100 } else { 200 }
            }
        );
        assert_eq!(config.seed, 42);
        assert_eq!(
            (config.threads, config.sessions, config.batch_size),
            (2, 4, 10)
        );
        let decoded: InvocationReport = toml::from_str(&toml::to_string(&report).unwrap()).unwrap();
        assert_eq!(decoded, report);
    }

    #[track_caller]
    fn check_update_template(mode: &str, index: &str) {
        let (stdout, report) = execute_small_template(&format!("{mode}-{index}"));
        assert_eq!(report.measured_runs.len(), 1);
        let run = &report.measured_runs[0];
        assert_eq!(report.aggregate.counters, run.counters);
        assert_eq!(run.counters.inserted_rows, 0);
        assert_eq!(run.counters.deleted_rows, 0);
        assert_eq!(run.counters.rows_returned, 0);
        assert_eq!(run.counters.expected_outcomes.duplicate_key, 0);
        assert_eq!(run.counters.expected_outcomes.write_conflict, 0);
        if mode == "update-all" {
            assert_eq!(run.counters.operations, 1);
            assert_eq!(run.counters.updated_rows, 100);
            assert_eq!((run.counters.found, run.counters.not_found), (0, 0));
            assert_eq!(run.latency.unit, LatencyUnit::UpdateAllTransaction);
            assert_eq!(run.latency.sample_count, 1);
        } else {
            assert_eq!(run.counters.operations, 100);
            assert_eq!(run.counters.found + run.counters.not_found, 100);
            if index == "unique" {
                assert_eq!(run.counters.found, 100);
                assert_eq!(run.counters.updated_rows, 100);
            } else {
                assert!(run.counters.not_found > 0);
                assert!(run.counters.updated_rows > run.counters.found);
            }
            assert_eq!(run.latency.unit, LatencyUnit::UpdatePointBatchTransaction);
            assert_eq!(run.latency.sample_count, 12);
        }
        assert!(stdout.contains(&format!("updated_rows: {}\n", run.counters.updated_rows)));
        assert!(stdout.contains(&format!(
            "updated_rows_per_second: {:.3}\n",
            doradb_bench::measurement::operations_per_second(
                run.counters.updated_rows,
                run.elapsed_nanos
            )
        )));
        assert_eq!(
            toml::from_str::<InvocationReport>(&toml::to_string(&report).unwrap()).unwrap(),
            report
        );
    }

    #[track_caller]
    fn check_delete_template(mode: &str, index: &str) {
        let (stdout, report) = execute_small_template(&format!("delete-{mode}-{index}"));
        let run = &report.measured_runs[0];
        assert_eq!(report.measured_runs.len(), 1);
        assert_eq!(report.aggregate.counters, run.counters);
        assert_eq!(run.counters.inserted_rows, 0);
        assert_eq!(run.counters.updated_rows, 0);
        assert_eq!(run.counters.rows_returned, 0);
        assert_eq!(run.counters.expected_outcomes.duplicate_key, 0);
        assert_eq!(run.counters.expected_outcomes.write_conflict, 0);
        assert!(run.counters.deleted_rows > 0 && run.counters.deleted_rows <= 100);
        if mode == "all" {
            assert_eq!(run.counters.operations, 1);
            assert_eq!(run.counters.deleted_rows, 100);
            assert_eq!((run.counters.found, run.counters.not_found), (0, 0));
            assert_eq!(run.latency.unit, LatencyUnit::DeleteAllTransaction);
            assert_eq!(run.latency.sample_count, 1);
        } else {
            assert_eq!(run.counters.operations, 100);
            assert_eq!(run.counters.found + run.counters.not_found, 100);
            assert!(run.counters.not_found > 0);
            assert!(run.counters.found <= run.counters.deleted_rows);
            if index == "unique" {
                assert_eq!(run.counters.found, run.counters.deleted_rows);
            }
            assert_eq!(run.latency.unit, LatencyUnit::DeleteBatchTransaction);
            assert_eq!(run.latency.sample_count, 12);
        }
        assert!(stdout.contains(&format!("deleted_rows: {}\n", run.counters.deleted_rows)));
        assert!(stdout.contains(&format!(
            "deleted_rows_per_second: {:.3}\n",
            doradb_bench::measurement::operations_per_second(
                run.counters.deleted_rows,
                run.elapsed_nanos
            )
        )));
        assert_eq!(
            toml::from_str::<InvocationReport>(&toml::to_string(&report).unwrap()).unwrap(),
            report
        );
    }

    #[track_caller]
    fn check_dependent_read(
        name: &str,
        index: &str,
        controls: &str,
        counters: WorkloadCounters,
        unit: LatencyUnit,
        samples: u64,
    ) {
        let temp = TempDir::new().unwrap();
        let phases = format!(
            "\n[[phase]]\nworkload = {{ type = \"create-table\", index = \"{index}\" }}\n\
         [[phase]]\nworkload = {{ type = \"insert-seq\", num = 8, batch_size = 4 }}\n\
         [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 2\n\
         workload = {{ type = \"{name}\", {controls} }}\n"
        );
        let (_root, report) = execute_plan(&temp, name, &phases);
        assert_eq!(report.measured_runs.len(), 2, "{name}");
        for (run_index, run) in report.measured_runs.iter().enumerate() {
            assert_eq!(run.counters, counters, "{name} run {run_index}");
            assert_eq!(run.latency.unit, unit, "{name} run {run_index}");
            assert_eq!(run.latency.sample_count, samples, "{name} run {run_index}");
        }
        assert_eq!(report.aggregate.measured_runs, 2, "{name}");
        assert_eq!(
            report.aggregate.counters,
            WorkloadCounters {
                operations: counters.operations * 2,
                found: counters.found * 2,
                rows_returned: counters.rows_returned * 2,
                ..WorkloadCounters::default()
            },
            "{name} aggregate"
        );
        assert_eq!(report.aggregate.latency.unit, unit, "{name} aggregate");
        assert_eq!(
            report.aggregate.latency.sample_count,
            samples * 2,
            "{name} aggregate"
        );
    }

    #[track_caller]
    fn check_explicit_update_replay(name: &str, controls: &str, rows: u64) {
        let temp = TempDir::new().unwrap();
        let (_, report) = execute_plan(
            &temp,
            name,
            &format!(
                "[[phase]]\nworkload = {{ type = 'create-table', index = 'unique' }}\n[[phase]]\nworkload = {{ type = 'insert-seq', num = 3 }}\n[[phase]]\nkind = 'benchmark'\nwarmup_runs = 1\nmeasured_runs = 3\nworkload = {{ {controls}, change_key = true, include_stats = true }}"
            ),
        );
        assert_eq!(report.measured_runs.len(), 3);
        for run in &report.measured_runs {
            assert_eq!(run.counters.operations, 1);
            assert_eq!(run.counters.updated_rows, rows);
            assert_eq!(run.latency.sample_count, 1);
            assert_drained_transaction_diagnostics(name, run);
        }
        assert_eq!(report.aggregate.counters.operations, 3);
        assert_eq!(report.aggregate.counters.updated_rows, rows * 3);
        assert_eq!(report.aggregate.latency.sample_count, 3);
    }

    #[track_caller]
    fn check_specialized_lock(scenario: &str, mode: &str, width: usize, tables: usize) {
        let temp = TempDir::new().unwrap();
        let phases = format!(
            "\n[[phase]]\nworkload = {{ type = \"create-table\", index = \"none\", tables = {tables} }}\n\
             [[phase]]\nkind = \"benchmark\"\n\
             workload = {{ type = \"lock-table\", num = 1, scenario = \"{scenario}\", mode = \"{mode}\", width = {width}, threads = 1, sessions = 1 }}\n"
        );
        let (_root, report) = execute_plan(&temp, &format!("lock-{scenario}"), &phases);
        assert_eq!(report.aggregate.counters.operations, 1, "{scenario}");
        assert_eq!(report.aggregate.latency.sample_count, 1, "{scenario}");
    }

    #[track_caller]
    fn check_create_index_placement(placement: &str, kind: PlacementKind, index: &str) {
        let temp = TempDir::new().unwrap();
        let mut phases = CREATE_INDEX_ENGINE.to_owned();
        phases.push_str("[[phase]]\nworkload = { type = 'create-table', index = 'none' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 4, value_size = '64 B', batch_size = 2 }\n");
        if placement != "hot" {
            phases.push_str("[[phase]]\nworkload = { type = 'freeze-table', all = true }\n[[phase]]\nworkload = { type = 'checkpoint-table' }\n");
        }
        if placement == "mixed" {
            phases.push_str(
                "[[phase]]\nworkload = { type = 'insert-seq', num = 2, value_size = '64 B' }\n",
            );
        }
        let stats = index == "unique";
        phases.push_str(&format!("[[phase]]\nkind = 'benchmark'\nworkload = {{ type = 'create-index', index = '{index}', include_stats = {stats} }}\n"));
        let (_, report) = execute_plan(&temp, &format!("create-{placement}-{index}"), &phases);
        let run = &report.measured_runs[0];
        let Some(WorkloadMetrics::CreateIndex { report: create }) = &run.workload_metrics else {
            panic!("missing CREATE report")
        };
        assert_eq!(create.placement, kind);
        assert_eq!(
            create.index,
            if stats {
                IndexMode::Unique
            } else {
                IndexMode::NonUnique
            }
        );
        assert_eq!(
            create.rows,
            match placement {
                "hot" => RowPlacement {
                    hot_rows: 4,
                    checkpointed_rows: 0
                },
                "checkpointed" => RowPlacement {
                    hot_rows: 0,
                    checkpointed_rows: 4
                },
                _ => RowPlacement {
                    hot_rows: 2,
                    checkpointed_rows: 4
                },
            }
        );
        let total = if placement == "mixed" { 6 } else { 4 };
        assert_eq!(create.total_rows, total);
        let verification = create.verification.as_ref().unwrap();
        assert_eq!(verification.table_rows, total);
        assert_eq!(verification.index_rows, total);
        assert_eq!(verification.fingerprint.len(), 32);
        assert_eq!(create.sampled_process_rss.is_some(), stats);
        assert_eq!(!run.internal_metrics.is_empty(), stats);
        for metric_name in [
            "hot_index_build.completed_builds",
            "create_index.completed_builds",
        ] {
            let metric = run
                .internal_metrics
                .iter()
                .find(|metric| metric.name == metric_name);
            assert_eq!(
                metric.map(|metric| metric.value),
                stats.then_some(1),
                "{metric_name}"
            );
        }
        assert_eq!(run.latency.unit, LatencyUnit::IndexCreation);
        assert_eq!(run.latency.sum_nanos, create.create_elapsed_nanos);
        assert_eq!(run.latency.sample_count, 1);
        assert_eq!(
            run.counters,
            WorkloadCounters {
                operations: 1,
                ..WorkloadCounters::default()
            }
        );
    }

    fn duplicate_index_plan() -> String {
        let random = "[[phase]]\nworkload = { type = 'create-table', index = 'none' }\n[[phase]]\nworkload = { type = 'insert-rand', num = 8, seed = 42, batch_size = 2 }\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'create-index', index = 'non-unique' }\n";
        format!("{CREATE_INDEX_ENGINE}{random}")
    }

    #[track_caller]
    fn check_managed_binding_resolution(full: bool) {
        let temp = TempDir::new().unwrap();
        let phases = format!(
            r#"
[[phase]]
workload = {{ type = "managed-bindings-prepare", tables = 4 }}
[[phase]]
kind = "benchmark"
warmup_runs = 1
measured_runs = 2
workload = {{ type = "resolve-table-binding", num = 17, threads = 2, sessions = 4, include_full_schema = {full}, include_stats = true }}
"#
        );
        let (_, report) = execute_plan(&temp, "bindings", &phases);
        assert_eq!(report.prepare_phases[0].counters.operations, 4);
        assert_eq!(report.measured_runs.len(), 2);
        for run in &report.measured_runs {
            assert_eq!(
                run.counters,
                WorkloadCounters {
                    operations: 17,
                    found: 17,
                    ..WorkloadCounters::default()
                }
            );
            assert_eq!(run.latency.unit, LatencyUnit::TableBindingResolution);
            assert_eq!(run.latency.sample_count, 17);
            assert_ne!(run.internal_metrics, []);
        }
        assert_eq!(report.aggregate.latency.sample_count, 34);
    }

    #[track_caller]
    fn check_freeze_failure(name: &str, preparation: &str, max_rows: u64) {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join(format!("{name}.toml"));
        fs::write(
            &source,
            format!(
                "[engine.transaction]\nlog_sync = 'none'\n\
                 [[phase]]\nworkload = {{ type = 'create-table', index = 'none' }}\n\
                 {preparation}\n\
                 [[phase]]\nkind = 'benchmark'\nwarmup_runs = 0\nmeasured_runs = 1\n\
                 workload = {{ type = 'freeze-table', max_rows = {max_rows} }}\n"
            ),
        )
        .unwrap();
        let root = temp.path().join(format!("{name}-root"));
        let output = run_bench(&root, &["--plan", source.to_str().unwrap()]);
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = assert_failure(output);
        assert!(
            stderr.contains("did not install a nonempty proper prefix"),
            "{name}: {stderr}"
        );
        assert!(!stdout.contains("DoraDB benchmark summary"));
        assert!(root.exists());
        assert!(!root.join("benchmark-result.toml").exists());
    }

    fn checkpointed_freeze_failure_plan() -> String {
        let hot = "[[phase]]\nworkload = { type = 'insert-seq', num = 8, value_size = '128 B', batch_size = 8 }\n";
        format!(
            "[[phase]]\nworkload = {{ type = 'insert-seq', num = 64, value_size = '128 B', batch_size = 8 }}\n\
             [[phase]]\nworkload = {{ type = 'freeze-table', all = true }}\n\
             [[phase]]\nworkload = {{ type = 'checkpoint-table' }}\n{hot}"
        )
    }

    /// Purpose: Resolve managed table bindings repeatedly without full schema loading.
    /// Expected: All sessions retain exact successful operations, statistics, and run/aggregate samples.
    #[test]
    fn managed_binding_resolution_replays_without_schema() {
        check_managed_binding_resolution(false);
    }

    /// Purpose: Resolve managed table bindings repeatedly with full schema loading.
    /// Expected: All sessions retain exact successful operations, statistics, and run/aggregate samples.
    #[test]
    fn managed_binding_resolution_replays_with_schema() {
        check_managed_binding_resolution(true);
    }

    /// Purpose: Reject incomplete and obsolete CLI invocations before touching storage.
    /// Expected: Invalid commands neither create missing roots nor remove existing ones.
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

    /// Purpose: Preserve environment fallback and explicit storage-root precedence.
    /// Expected: Results use the selected root without creating the overridden environment
    /// path.
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

    /// Purpose: Exercise lookup-seq after committed preparation and a warmup.
    /// Expected: Measured runs and aggregates retain exact operation, row, hit, and transaction sample counts.
    #[test]
    fn lookup_seq_plan_preserves_exact_accounting() {
        check_dependent_read(
            "lookup-seq",
            "unique",
            "num = 7, batch_size = 2",
            WorkloadCounters {
                operations: 7,
                found: 7,
                rows_returned: 7,
                ..WorkloadCounters::default()
            },
            LatencyUnit::LookupBatchTransaction,
            4,
        );
    }

    /// Purpose: Exercise lookup-rand after committed preparation and a warmup.
    /// Expected: Measured runs and aggregates retain exact operation, row, hit, and transaction sample counts.
    #[test]
    fn lookup_rand_plan_preserves_exact_accounting() {
        check_dependent_read(
            "lookup-rand",
            "unique",
            "num = 7, seed = 9, batch_size = 2",
            WorkloadCounters {
                operations: 7,
                found: 7,
                rows_returned: 7,
                ..WorkloadCounters::default()
            },
            LatencyUnit::LookupBatchTransaction,
            4,
        );
    }

    /// Purpose: Exercise table-scan after committed preparation and a warmup.
    /// Expected: Measured runs and aggregates retain exact operation, row, hit, and transaction sample counts.
    #[test]
    fn table_scan_plan_preserves_exact_accounting() {
        check_dependent_read(
            "table-scan",
            "none",
            "num = 2, batch_size = 1",
            WorkloadCounters {
                operations: 2,
                found: 0,
                rows_returned: 16,
                ..WorkloadCounters::default()
            },
            LatencyUnit::TableScanBatchTransaction,
            2,
        );
    }

    /// Purpose: Exercise index-scan after committed preparation and a warmup.
    /// Expected: Measured runs and aggregates retain exact operation, row, hit, and transaction sample counts.
    #[test]
    fn index_scan_plan_preserves_exact_accounting() {
        check_dependent_read(
            "index-scan",
            "non-unique",
            "num = 3, range = 2, seed = 9, batch_size = 2",
            WorkloadCounters {
                operations: 3,
                found: 3,
                rows_returned: 6,
                ..WorkloadCounters::default()
            },
            LatencyUnit::IndexScanBatchTransaction,
            2,
        );
    }

    /// Purpose: Exercise index-stream after committed preparation and a warmup.
    /// Expected: Measured runs and aggregates retain exact operation, row, hit, and transaction sample counts.
    #[test]
    fn index_stream_plan_preserves_exact_accounting() {
        check_dependent_read(
            "index-stream",
            "non-unique",
            "num = 3, range = 2, seed = 9",
            WorkloadCounters {
                operations: 3,
                found: 0,
                rows_returned: 6,
                ..WorkloadCounters::default()
            },
            LatencyUnit::IndexStreamTransaction,
            3,
        );
    }

    /// Purpose: Exercise a complete index create/drop cycle through the CLI.
    /// Expected: One cycle reports two operations and one index-DDL latency sample.
    #[test]
    fn index_ddl_plan_preserves_exact_accounting() {
        let temp = TempDir::new().unwrap();
        let phases = "\n[[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
                  [[phase]]\nworkload = { type = \"insert-seq\", num = 8, batch_size = 4 }\n\
                  [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"index-ddl\", num = 1 }\n";
        let (_root, report) = execute_plan(&temp, "index-ddl", phases);
        assert_eq!(report.aggregate.counters.operations, 2);
        assert_eq!(report.aggregate.latency.sample_count, 1);
        assert_eq!(
            report.aggregate.latency.unit,
            LatencyUnit::IndexCreateDropCycle
        );
    }

    /// Purpose: Preserve scan cardinality when changing the requested parallelism.
    /// Expected: Partition targets affect reported planning while returned rows and lifecycle
    /// accounting remain consistent.
    #[test]
    fn parallel_table_scan_matches_target_one_cardinality_and_reports_actual_partitions() {
        let temp = TempDir::new().unwrap();
        let execute = |name: &str, target_partitions: usize| {
            let phases = format!(
                "\n[[phase]]\nworkload = {{ type = \"create-table\", index = \"none\" }}\n\
                 [[phase]]\nworkload = {{ type = \"insert-seq\", num = 8, value_size = \"32 KiB\", batch_size = 8 }}\n\
                 [[phase]]\nkind = \"benchmark\"\nwarmup_runs = 1\nmeasured_runs = 2\n\
                 workload = {{ type = \"parallel-table-scan\", num = 2, target_partitions = {target_partitions} }}\n"
            );
            execute_plan(&temp, name, &phases).1
        };

        let target_one = execute("parallel-scan-one", 1);
        let target_many = execute("parallel-scan-many", 16);
        for (report, target) in [(&target_one, 1), (&target_many, 16)] {
            assert_eq!(report.measured_runs.len(), 2);
            for run in &report.measured_runs {
                assert_eq!(run.counters.operations, 2);
                assert_eq!(run.counters.rows_returned, 16);
                assert_eq!(run.latency.unit, LatencyUnit::ParallelTableScanLifecycle);
                assert_eq!(run.latency.sample_count, 2);
                let Some(WorkloadMetrics::ParallelTableScan {
                    target_partitions,
                    actual_partitions,
                }) = run.workload_metrics
                else {
                    panic!("parallel scan must retain partition metrics")
                };
                assert_eq!(target_partitions, target);
                assert!(actual_partitions > 0);
                if target == 1 {
                    assert_eq!(actual_partitions, 1);
                } else {
                    assert!(actual_partitions < target);
                }
            }
            assert_eq!(report.aggregate.counters.operations, 4);
            assert_eq!(report.aggregate.counters.rows_returned, 32);
            assert_eq!(report.aggregate.latency.sample_count, 4);
        }
        assert_eq!(
            target_one.aggregate.counters.rows_returned,
            target_many.aggregate.counters.rows_returned
        );
    }

    /// Purpose: Replay random range updates that move unique keys.
    /// Expected: Warmups and measured runs retain stable affected-row counts and transaction samples.
    #[test]
    fn random_index_updates_replay_unique_keys() {
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
    }

    /// Purpose: Replay random range updates on duplicate-bearing non-unique keys.
    /// Expected: Payload-only replay retains successful row accounting and per-run and aggregate samples.
    #[test]
    fn random_index_updates_replay_non_unique_payloads() {
        let temp = TempDir::new().unwrap();
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

    /// Purpose: Execute the mixed-occupancy upsert template through the public CLI.
    /// Expected: Both logical actions occur, normalized controls round trip, and request/row rates and batch samples match independently.
    #[test]
    fn upsert_mixed_template_executes() {
        check_upsert_template("upsert-point-rand", false);
    }

    /// Purpose: Execute the overwrite upsert template through the public CLI.
    /// Expected: The omitted domain resolves to preparation and every request updates with zero inserts.
    #[test]
    fn upsert_overwrite_template_executes() {
        check_upsert_template("upsert-point-rand-overwrite", true);
    }

    /// Purpose: Start upsert from a created empty table with repeated targets across batches.
    /// Expected: The explicit singleton range inserts once then updates with one sample per committed batch.
    #[test]
    fn upsert_empty_cli_plan_executes() {
        let temp = TempDir::new().unwrap();
        let (_, report) = execute_plan(
            &temp,
            "empty-upsert",
            "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'upsert-point-rand', num = 3, key_range = { start = 42, len = 1 }, batch_size = 2 }",
        );
        assert_eq!(report.aggregate.counters.operations, 3);
        assert_eq!(report.aggregate.counters.inserted_rows, 1);
        assert_eq!(report.aggregate.counters.updated_rows, 2);
        assert_eq!(report.aggregate.latency.sample_count, 2);
    }

    /// Purpose: Reject invalid upsert plans before acquiring storage-root ownership.
    /// Expected: Invalid controls, shapes, domains, preparation placement, and repetition create neither a root nor success output.
    #[test]
    fn upsert_cli_rejects_invalid_plans_before_root_creation() {
        let prepare = "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n";
        for (fields, error) in [
            ("num = 0", "nonzero"),
            ("num = 1, threads = 0", "nonzero"),
            ("num = 1, sessions = 0", "nonzero"),
            ("num = 1, batch_size = 0", "nonzero"),
            ("num = 1, threads = 2, sessions = 1", "must not exceed"),
            ("num = 1, sessions = 4", "exceed target"),
            ("num = 1, value_size = '0 B'", "positive"),
            ("num = 1, value_size = '1 MiB'", "must not exceed"),
            ("num = 1, change_key = false", "unknown field"),
            ("num = 1, key_range = { start = 0, len = 0 }", "nonempty"),
            (
                "num = 1, key_range = { start = 18446744073709551615, len = 1 }",
                "overflow",
            ),
        ] {
            assert_plan_rejected_before_root_creation(
                &format!(
                    "{prepare}[[phase]]\nkind = 'benchmark'\nworkload = {{ type = 'upsert-point-rand', {fields} }}"
                ),
                error,
            );
        }
        let base = format!(
            "{prepare}[[phase]]\nkind = 'benchmark'\nworkload = {{ type = 'upsert-point-rand', num = 1 }}"
        );
        for repetition in ["warmup_runs = 1", "measured_runs = 2"] {
            assert_plan_rejected_before_root_creation(
                &base.replace(
                    "kind = 'benchmark'",
                    &format!("kind = 'benchmark'\n{repetition}"),
                ),
                "not replay-safe",
            );
        }
        for index in ["none", "non-unique"] {
            assert_plan_rejected_before_root_creation(
                &base.replace("index = 'unique'", &format!("index = '{index}'")),
                "incompatible",
            );
        }
        assert_plan_rejected_before_root_creation(
            "[[phase]]\nkind = 'benchmark'\nworkload = { type = 'upsert-point-rand', num = 1, key_range = { start = 0, len = 1 } }",
            "preceding create-table",
        );
        let empty = base.replace(
            "[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n",
            "",
        );
        assert_plan_rejected_before_root_creation(&empty, "loaded");
        let prepare = base.replace("kind = 'benchmark'", "kind = 'prepare'")
            + "\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'trx-noop', num = 1 }";
        assert_plan_rejected_before_root_creation(&prepare, "final benchmark");
    }

    /// Purpose: Execute the unique full-table update template through the CLI.
    /// Expected: Every prepared row updates in one sampled transaction and output preserves exact counters.
    #[test]
    fn update_all_unique_template_executes() {
        check_update_template("update-all", "unique");
    }

    /// Purpose: Execute full-table updates of the duplicate-bearing random template through the CLI.
    /// Expected: Every duplicate row updates in one sampled transaction and verified output preserves cardinality.
    #[test]
    fn update_all_non_unique_template_executes() {
        check_update_template("update-all", "non-unique");
    }

    /// Purpose: Execute seeded unique point requests through the shipped CLI template.
    /// Expected: Every payload-only request hits, each changes one row, and batch samples and rates remain exact.
    #[test]
    fn update_point_unique_template_executes() {
        check_update_template("update-point-rand", "unique");
    }

    /// Purpose: Execute seeded duplicate-group requests through the shipped CLI template.
    /// Expected: Gaps miss, group updates exceed hit counts, and canonical output retains request/row semantics.
    #[test]
    fn update_point_non_unique_template_executes() {
        check_update_template("update-point-rand", "non-unique");
    }

    /// Purpose: Reject statically invalid new update controls before acquiring a storage root.
    /// Expected: Unsupported fields, empty payloads, excessive sessions, and missing replay capacity create no root or success output.
    #[test]
    fn explicit_update_cli_rejects_invalid_plans_before_root_creation() {
        let prepare = "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n";
        for (controls, error) in [
            ("type = 'update-all', num = 1", "unknown field"),
            ("type = 'update-all', threads = 1", "unknown field"),
            ("type = 'update-all', batch_size = 1", "unknown field"),
            ("type = 'update-all', value_size = '0 B'", "positive"),
            (
                "type = 'update-point-rand', num = 1, sessions = 4",
                "exceed loaded",
            ),
            ("type = 'update-point-rand', num = 0", "nonzero"),
        ] {
            assert_plan_rejected_before_root_creation(
                &format!("{prepare}[[phase]]\nkind = 'benchmark'\nworkload = {{ {controls} }}"),
                error,
            );
        }
        for controls in ["type = 'update-all'", "type = 'update-point-rand', num = 1"] {
            assert_plan_rejected_before_root_creation(
                &format!(
                    "{}[[phase]]\nkind = 'benchmark'\nworkload = {{ {controls}, change_key = true }}",
                    prepare.replace("num = 3", "num = 18446744073709551615")
                ),
                "overflow",
            );
        }
    }

    /// Purpose: Replay full-table key-changing updates after an alternate-domain warmup.
    /// Expected: Runs retain parity, exact row and sample counts, transaction diagnostics, and drained locks.
    #[test]
    fn update_all_cli_replay_preserves_accounting() {
        check_explicit_update_replay("all", "type = 'update-all'", 3);
    }

    /// Purpose: Replay key-changing point updates with idle sessions after a warmup.
    /// Expected: Runs retain parity, exact request/row and sample counts, transaction diagnostics, and drained locks.
    #[test]
    fn update_point_cli_replay_preserves_accounting() {
        check_explicit_update_replay(
            "point",
            "type = 'update-point-rand', num = 1, sessions = 3, batch_size = 2",
            1,
        );
    }

    /// Purpose: Execute the unique full-table delete template with a small fixture.
    /// Expected: Every prepared row is deleted in one sampled transaction with exact output accounting.
    #[test]
    fn delete_all_unique_template_executes() {
        check_delete_template("all", "unique");
    }

    /// Purpose: Execute the non-unique full-table delete template with a small fixture.
    /// Expected: Every prepared row is deleted in one sampled transaction with exact output accounting.
    #[test]
    fn delete_all_non_unique_template_executes() {
        check_delete_template("all", "non-unique");
    }

    /// Purpose: Execute the unique point delete template with a small fixture.
    /// Expected: Requests retain hit/miss, affected-row, and batch-sample accounting in canonical output.
    #[test]
    fn delete_rand_unique_template_executes() {
        check_delete_template("rand", "unique");
    }

    /// Purpose: Execute the non-unique point delete template with a small fixture.
    /// Expected: Requests retain hit/miss, affected-row, and batch-sample accounting in canonical output.
    #[test]
    fn delete_rand_non_unique_template_executes() {
        check_delete_template("rand", "non-unique");
    }

    /// Purpose: Reject destructive replay and unsupported delete controls before filesystem ownership.
    /// Expected: Invalid CLI plans leave no root or success output; sparse request budgets retain exact accounting, diagnostics, and drained worker locks.
    #[test]
    fn delete_cli_rejects_invalid_plans_before_root_creation() {
        for controls in ["type = 'delete-all'", "type = 'delete-rand', num = 1"] {
            let prepare = "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n";
            for extra in ["warmup_runs = 1", "measured_runs = 2"] {
                assert_plan_rejected_before_root_creation(
                    &format!(
                        "{prepare}[[phase]]\nkind = 'benchmark'\n{extra}\nworkload = {{ {controls} }}"
                    ),
                    "replay-safe",
                );
            }
            assert_plan_rejected_before_root_creation(
                &format!(
                    "{prepare}[[phase]]\nkind = 'benchmark'\nworkload = {{ {controls}, value_size = '1 B' }}"
                ),
                "unknown field",
            );
            assert_plan_rejected_before_root_creation(
                &format!(
                    "[[phase]]\nworkload = {{ type = 'create-table', index = 'unique' }}\n[[phase]]\nkind = 'benchmark'\nworkload = {{ {controls} }}"
                ),
                if controls.contains("delete-all") {
                    "preceding nonempty insert phase"
                } else {
                    "requires loaded benchmark data"
                },
            );
        }
        let temp = TempDir::new().unwrap();
        let (_, report) = execute_plan(
            &temp,
            "sparse-delete",
            "[[phase]]\nworkload = { type = 'create-table', index = 'unique' }\n[[phase]]\nworkload = { type = 'insert-seq', num = 3 }\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'delete-rand', num = 1, sessions = 3, include_stats = true }",
        );
        assert_eq!(report.aggregate.counters.operations, 1);
        assert_eq!(report.aggregate.counters.deleted_rows, 1);
        assert_eq!(report.aggregate.latency.sample_count, 1);
        assert_eq!(report.measured_runs.len(), 1);
        assert_drained_transaction_diagnostics("sparse-delete", &report.measured_runs[0]);
    }

    /// Purpose: Keep the shipped random-update template executable through the public CLI.
    /// Expected: Every measured run reports successful updates with consistent transaction and
    /// aggregate samples.
    #[test]
    fn checked_in_update_template_executes_end_to_end() {
        let (stdout, report) = execute_small_template("update-rand");
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

    /// Purpose: Keep the shipped parallel-scan template executable through the public CLI.
    /// Expected: Published results preserve scan cardinality, partition diagnostics, and
    /// aggregate sample accounting.
    #[test]
    fn checked_in_parallel_scan_template_executes_end_to_end() {
        let (stdout, report) = execute_small_template("parallel-table-scan");
        assert!(stdout.contains("workload: parallel-table-scan\n"));
        assert!(stdout.contains("target_partitions: 4\n"));
        assert!(stdout.contains("rows_returned: 600\n"));
        assert!(stdout.contains("rows_per_second: "));
        assert_eq!(report.measured_runs.len(), 3);
        for run in &report.measured_runs {
            assert_eq!(run.counters.operations, 2);
            assert_eq!(run.counters.rows_returned, 200);
            assert_eq!(run.latency.unit, LatencyUnit::ParallelTableScanLifecycle);
            assert_eq!(run.latency.sample_count, 2);
            assert!(matches!(
                run.workload_metrics,
                Some(WorkloadMetrics::ParallelTableScan {
                    target_partitions: 4,
                    actual_partitions: 1..
                })
            ));
        }
        assert_eq!(report.aggregate.counters.operations, 6);
        assert_eq!(report.aggregate.counters.rows_returned, 600);
        assert_eq!(report.aggregate.latency.sample_count, 6);
    }

    /// Purpose: Complete repeated table-pool locking without retaining ownership.
    /// Expected: The CLI drains lock lifecycles and publishes consistent operation and sample
    /// totals.
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

    /// Purpose: Exercise the nested-covered lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_nested_covered_plan_drains_participants() {
        check_specialized_lock("nested-covered", "shared", 3, 3);
    }

    /// Purpose: Exercise the convert lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_convert_plan_drains_participants() {
        check_specialized_lock("convert", "exclusive", 1, 1);
    }

    /// Purpose: Exercise the enqueue lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_enqueue_plan_drains_participants() {
        check_specialized_lock("enqueue", "exclusive", 3, 1);
    }

    /// Purpose: Exercise the cancel-head lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_cancel_head_plan_drains_participants() {
        check_specialized_lock("cancel-head", "exclusive", 3, 1);
    }

    /// Purpose: Exercise the cancel-middle lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_cancel_middle_plan_drains_participants() {
        check_specialized_lock("cancel-middle", "exclusive", 3, 1);
    }

    /// Purpose: Exercise the cancel-tail lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_cancel_tail_plan_drains_participants() {
        check_specialized_lock("cancel-tail", "exclusive", 3, 1);
    }

    /// Purpose: Exercise the promote lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_promote_plan_drains_participants() {
        check_specialized_lock("promote", "exclusive", 3, 1);
    }

    /// Purpose: Exercise the first-touch lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_first_touch_plan_drains_participants() {
        check_specialized_lock("first-touch", "shared", 1, 1);
    }

    /// Purpose: Exercise the scope-close lock scenario through the CLI.
    /// Expected: Coordinated participants drain and publish one operation and one lifecycle sample.
    #[test]
    fn lock_scope_close_plan_drains_participants() {
        check_specialized_lock("scope-close", "shared", 3, 3);
    }

    /// Purpose: Reject dependent reads without committed preparation before acquiring storage.
    /// Expected: The CLI reports the missing load requirement without creating a root or
    /// success output.
    #[test]
    fn invalid_dependent_plan_fails_before_root_creation() {
        assert_plan_rejected_before_root_creation(
            "[[phase]]\nworkload = { type = \"create-table\", index = \"unique\" }\n\
         [[phase]]\nkind = \"benchmark\"\nworkload = { type = \"lookup-seq\", num = 1 }\n",
            "requires loaded benchmark data",
        );
    }

    /// Purpose: Execute both shipped persisted lookup compositions through the CLI.
    /// Expected: Bounded templates publish verified preparation and only lookup operations, rows, and samples in the measured aggregate.
    #[test]
    fn indexed_lookup_templates_compose_through_cli() {
        let temp = TempDir::new().unwrap();
        for (name, template, prepares) in [
            (
                "initial-index",
                include_str!("../templates/lookup-indexed-checkpoint.toml"),
                4,
            ),
            (
                "prepared-index",
                include_str!("../templates/lookup-create-index-prepare.toml"),
                5,
            ),
        ] {
            let input = template
                .lines()
                .filter(|line| {
                    !line.starts_with("name =") && !line.starts_with("engine_defaults =")
                })
                .collect::<Vec<_>>()
                .join("\n")
                .replace("num = 10000", "num = 8")
                .replace("batch_size = 100", "batch_size = 2");
            let (_, report) = execute_plan(&temp, name, &input);
            assert_eq!(report.prepare_phases.len(), prepares);
            assert_eq!(report.aggregate.measured_runs, 3);
            assert_eq!(report.aggregate.latency.sample_count, 12);
            assert_eq!(
                report.aggregate.counters,
                WorkloadCounters {
                    operations: 24,
                    found: 24,
                    rows_returned: 24,
                    ..WorkloadCounters::default()
                }
            );
            if prepares == 5 {
                let create = &report.prepare_phases[4];
                assert_eq!(create.counters.operations, 1);
                let Some(WorkloadMetrics::CreateIndex { report: create }) =
                    &create.workload_metrics
                else {
                    panic!("CREATE")
                };
                assert_eq!(create.placement, PlacementKind::Checkpointed);
                assert_eq!(create.verification.as_ref().unwrap().index_rows, 8);
            }
        }
    }

    /// Purpose: Preserve indexed reads across a prefix checkpoint whose exact row placement is unknown.
    /// Expected: Unique lookups and non-unique scans return all loaded rows across both hot and persisted storage.
    #[test]
    fn indexed_prefix_checkpoints_remain_readable() {
        let temp = TempDir::new().unwrap();
        for index in ["unique", "non-unique"] {
            let workload = if index == "unique" {
                "type = 'lookup-seq', num = 8"
            } else {
                "type = 'index-scan', num = 1, range = 8"
            };
            let input = format!(
                "{CREATE_INDEX_ENGINE}[[phase]]\nworkload = {{ type = 'create-table', index = '{index}' }}\n[[phase]]\nworkload = {{ type = 'insert-seq', num = 8, value_size = '32 KiB', batch_size = 8 }}\n[[phase]]\nworkload = {{ type = 'freeze-table', max_rows = 4 }}\n[[phase]]\nworkload = {{ type = 'checkpoint-table' }}\n[[phase]]\nkind = 'benchmark'\nworkload = {{ {workload} }}\n"
            );
            let (_, report) = execute_plan(&temp, index, &input);
            assert_eq!(report.aggregate.counters.rows_returned, 8);
            assert_eq!(report.aggregate.counters.not_found, 0);
            assert_eq!(
                report.aggregate.counters.operations,
                if index == "unique" { 8 } else { 1 }
            );
            let Some(WorkloadMetrics::FreezeTable {
                approximate_rows, ..
            }) = report.prepare_phases[2].workload_metrics
            else {
                panic!("freeze")
            };
            assert!(approximate_rows > 0 && approximate_rows < 8);
        }
    }

    /// Purpose: Create a unique index over hot rows through the CLI.
    /// Expected: Placement, exact table/index cardinality, diagnostics, and creation latency remain consistent.
    #[test]
    fn create_index_hot_unique_preserves_placement() {
        check_create_index_placement("hot", PlacementKind::Hot, "unique");
    }

    /// Purpose: Create a non-unique index over hot rows through the CLI.
    /// Expected: Placement, exact table/index cardinality, diagnostics, and creation latency remain consistent.
    #[test]
    fn create_index_hot_non_unique_preserves_placement() {
        check_create_index_placement("hot", PlacementKind::Hot, "non-unique");
    }

    /// Purpose: Create a unique index over checkpointed rows through the CLI.
    /// Expected: Placement, exact table/index cardinality, diagnostics, and creation latency remain consistent.
    #[test]
    fn create_index_checkpointed_unique_preserves_placement() {
        check_create_index_placement("checkpointed", PlacementKind::Checkpointed, "unique");
    }

    /// Purpose: Create a non-unique index over checkpointed rows through the CLI.
    /// Expected: Placement, exact table/index cardinality, diagnostics, and creation latency remain consistent.
    #[test]
    fn create_index_checkpointed_non_unique_preserves_placement() {
        check_create_index_placement("checkpointed", PlacementKind::Checkpointed, "non-unique");
    }

    /// Purpose: Create a unique index over mixed rows through the CLI.
    /// Expected: Placement, exact table/index cardinality, diagnostics, and creation latency remain consistent.
    #[test]
    fn create_index_mixed_unique_preserves_placement() {
        check_create_index_placement("mixed", PlacementKind::Mixed, "unique");
    }

    /// Purpose: Create a non-unique index over mixed rows through the CLI.
    /// Expected: Placement, exact table/index cardinality, diagnostics, and creation latency remain consistent.
    #[test]
    fn create_index_mixed_non_unique_preserves_placement() {
        check_create_index_placement("mixed", PlacementKind::Mixed, "non-unique");
    }

    /// Purpose: Create a non-unique index over repeated logical keys.
    /// Expected: Verification counts every duplicate row in the resulting index.
    #[test]
    fn create_non_unique_index_preserves_duplicate_multiplicity() {
        let temp = TempDir::new().unwrap();
        let random = duplicate_index_plan();
        let (_, report) = execute_plan(&temp, "create-duplicates", &random);
        let Some(WorkloadMetrics::CreateIndex { report }) =
            &report.measured_runs[0].workload_metrics
        else {
            panic!("missing CREATE")
        };
        assert_eq!(report.verification.as_ref().unwrap().index_rows, 8);
    }

    /// Purpose: Reject unique-index creation over repeated logical keys.
    /// Expected: The CLI retains the diagnostic root and reports duplicate keys without publishing success.
    #[test]
    fn create_unique_index_rejects_duplicate_keys() {
        let temp = TempDir::new().unwrap();
        let random = duplicate_index_plan();
        let source = temp.path().join("unique-duplicates.toml");
        fs::write(
            &source,
            random.replace("index = 'non-unique'", "index = 'unique'"),
        )
        .unwrap();
        let root = temp.path().join("failed-unique-root");
        let output = run_bench(&root, &["--plan", source.to_str().unwrap()]);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("duplicate key"), "{stderr}");
        assert!(root.exists());
        assert!(!root.join("benchmark-result.toml").exists());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("DoraDB benchmark summary"));
    }

    /// Purpose: Publish coherent checkpoint metrics for a frozen table prefix.
    /// Expected: Reports preserve freeze evidence and consistent attempt, retry, and lifecycle
    /// accounting.
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

    /// Purpose: Reject a hot prefix that rounds up to the whole row page.
    /// Expected: The invalid prefix retains its root and publishes no success artifact.
    #[test]
    fn whole_page_freeze_rejects_rounded_hot_prefix() {
        let hot = "[[phase]]\nworkload = { type = 'insert-seq', num = 8, value_size = '128 B', batch_size = 8 }\n";
        check_freeze_failure("hot-rounded", hot, 4);
    }

    /// Purpose: Reject a prefix larger than the hot suffix after a full checkpoint.
    /// Expected: The invalid prefix retains its root and publishes no success artifact.
    #[test]
    fn whole_page_freeze_rejects_oversized_checkpointed_prefix() {
        check_freeze_failure(
            "checkpointed-oversized",
            &checkpointed_freeze_failure_plan(),
            16,
        );
    }

    /// Purpose: Reject a hot prefix rounded to its whole page after a full checkpoint.
    /// Expected: The invalid prefix retains its root and publishes no success artifact.
    #[test]
    fn whole_page_freeze_rejects_rounded_checkpointed_prefix() {
        check_freeze_failure(
            "checkpointed-rounded",
            &checkpointed_freeze_failure_plan(),
            4,
        );
    }

    /// Purpose: Reject a prefix that consumes the remaining hot rows after a partial checkpoint.
    /// Expected: The invalid prefix retains its root and publishes no success artifact.
    #[test]
    fn whole_page_freeze_rejects_prefix_after_partial_checkpoint() {
        let prefix_checkpointed = "[[phase]]\nworkload = { type = 'insert-seq', num = 8, value_size = '32 KiB', batch_size = 8 }\n\
                                   [[phase]]\nworkload = { type = 'freeze-table', max_rows = 4 }\n\
                                   [[phase]]\nworkload = { type = 'checkpoint-table' }\n";
        check_freeze_failure("prefix-checkpointed", prefix_checkpointed, 7);
    }

    /// Purpose: Preserve an appended hot suffix when freezing a prefix after checkpointing.
    /// Expected: Prefix and remaining suffix stay nonempty and together account for the
    /// appended rows.
    #[test]
    fn prefix_freeze_after_full_checkpoint_preserves_hot_suffix() {
        let temp = TempDir::new().unwrap();
        let phases = "[[phase]]\nworkload = { type = 'create-table', index = 'none' }\n\
                      [[phase]]\nworkload = { type = 'insert-seq', num = 64, value_size = '128 B', batch_size = 8 }\n\
                      [[phase]]\nworkload = { type = 'freeze-table', all = true }\n\
                      [[phase]]\nworkload = { type = 'checkpoint-table' }\n\
                      [[phase]]\nworkload = { type = 'insert-seq', num = 8, value_size = '32 KiB', batch_size = 8 }\n\
                      [[phase]]\nworkload = { type = 'freeze-table', max_rows = 4 }\n\
                      [[phase]]\nworkload = { type = 'checkpoint-table' }\n\
                      [[phase]]\nkind = 'benchmark'\nworkload = { type = 'freeze-table', all = true }\n";
        let (_, report) = execute_plan(&temp, "checkpointed-prefix", phases);
        let Some(WorkloadMetrics::FreezeTable {
            approximate_rows: prefix_rows,
            page_count: prefix_pages,
            ..
        }) = report.prepare_phases[5].workload_metrics
        else {
            panic!("missing prefix freeze metrics")
        };
        let Some(WorkloadMetrics::FreezeTable {
            approximate_rows: suffix_rows,
            page_count: suffix_pages,
            ..
        }) = report.measured_runs[0].workload_metrics
        else {
            panic!("missing hot suffix freeze metrics")
        };
        assert!(prefix_rows > 0 && suffix_rows > 0);
        assert_eq!(prefix_rows + suffix_rows, 8);
        assert!(prefix_pages > 0 && suffix_pages > 0);
    }

    /// Purpose: Validate recovery of a database without user tables.
    /// Expected: Recovery reports an empty fixture and retains the storage root.
    #[test]
    fn recovery_verifies_empty_database() {
        assert_recovery_fixture("empty", None, None, false, false);
    }

    /// Purpose: Preserve an empty heap table across recovery with statistics enabled.
    /// Expected: Recovery verifies the table identity and empty contents and reports cumulative statistics.
    #[test]
    fn recovery_verifies_empty_heap_table() {
        assert_recovery_fixture("empty-table", Some("none"), None, false, true);
    }

    /// Purpose: Preserve an empty unique index across recovery.
    /// Expected: Recovery verifies the table and index contents without collecting optional statistics.
    #[test]
    fn recovery_verifies_empty_unique_index() {
        assert_recovery_fixture("empty-index", Some("unique"), None, false, false);
    }

    /// Purpose: Recover a populated heap table from durable log records.
    /// Expected: Every inserted row is replayed and the recovered contents match the original fixture.
    #[test]
    fn recovery_verifies_loaded_heap() {
        assert_recovery_fixture("heap", Some("none"), Some("insert-seq"), false, true);
    }

    /// Purpose: Rebuild a unique index over sequentially inserted rows during recovery.
    /// Expected: Recovery replays every row and reconstructs index entries matching the table contents.
    #[test]
    fn recovery_verifies_sequential_unique_index() {
        assert_recovery_fixture("unique", Some("unique"), Some("insert-seq"), false, true);
    }

    /// Purpose: Recover a non-unique index populated with random keys and duplicates.
    /// Expected: Replayed rows and rebuilt index entries preserve the fixture contents and multiplicity.
    #[test]
    fn recovery_verifies_random_non_unique_index() {
        assert_recovery_fixture(
            "duplicates",
            Some("non-unique"),
            Some("insert-rand"),
            false,
            true,
        );
    }

    /// Purpose: Rebuild a unique index populated in random key order during recovery.
    /// Expected: Recovery preserves every row and matching index contents without optional statistics.
    #[test]
    fn recovery_verifies_random_unique_index() {
        assert_recovery_fixture(
            "random-unique",
            Some("unique"),
            Some("insert-rand"),
            false,
            false,
        );
    }

    /// Purpose: Recover a table containing a checkpointed prefix and a hot suffix.
    /// Expected: Recovery skips checkpointed rows, replays the hot suffix, and preserves all contents.
    #[test]
    fn recovery_verifies_checkpointed_prefix_and_hot_suffix() {
        assert_recovery_fixture("checkpoint", Some("none"), Some("insert-seq"), true, true);
    }

    /// Purpose: Require durable logging before admitting a recovery benchmark.
    /// Expected: Nondurable plans fail with a durability diagnostic before root creation or
    /// success publication.
    #[test]
    fn recovery_rejects_nondurable_plans_before_creating_root() {
        assert_plan_rejected_before_root_creation(
            "[engine.transaction]\nlog_sync = 'none'\n[[phase]]\nkind = 'benchmark'\nworkload = { type = 'recovery' }",
            "fsync or fdatasync",
        );
    }

    /// Purpose: Release the old engine before pausing for recovery profiling.
    /// Expected: The paused process holds no storage descriptors and resumed recovery verifies
    /// the retained data.
    #[test]
    fn recovery_profiler_pause_follows_old_engine_teardown() {
        recovery_profiler_case(false);
    }

    /// Purpose: Preserve diagnostic storage when recovery fails after the profiling pause.
    /// Expected: Reopen failure retains the root and publishes no success report or artifact.
    #[test]
    fn recovery_reopen_failure_retains_root_without_success_output() {
        recovery_profiler_case(true);
    }

    /// Purpose: Pause after preparation without starting benchmark execution or publishing
    /// success.
    /// Expected: Resuming completes the configured runs and publishes results with the pause
    /// setting preserved.
    #[test]
    fn profiler_pause_stops_before_benchmark_and_resumes_to_success() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("profiler-pause.toml");
        fs::write(
            &source,
            "name = \"profiler pause\"\n[engine.transaction]\nlog_sync = \"none\"\n\
             [[phase]]\nworkload = { type = \"create-table\", index = \"none\" }\n\
             [[phase]]\nkind = \"benchmark\"\npause = true\nwarmup_runs = 1\nmeasured_runs = 2\n\
             workload = { type = \"trx-noop\", num = 2 }\n",
        )
        .unwrap();
        let root = temp.path().join("profiler-pause-root");
        let mut child = Command::new(env!("CARGO_BIN_EXE_doradb-bench"))
            .args([
                "--root",
                root.to_str().unwrap(),
                "--plan",
                source.to_str().unwrap(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        let (stdout_lines, stdout_handle) = capture_lines(child.stdout.take().unwrap());
        let (stderr_lines, stderr_handle) = capture_lines(child.stderr.take().unwrap());
        let mut child = ChildGuard::new(child);
        let deadline = Instant::now() + SUBPROCESS_TIMEOUT;

        let pausing = stderr_lines
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        assert_eq!(
            pausing,
            format!("DORADB_BENCH_PAUSING pid={pid} phase=2 workload=trx-noop resume=SIGCONT")
        );
        wait_for_stopped(pid, deadline);
        assert!(child.child.try_wait().unwrap().is_none());
        assert!(stdout_lines.try_recv().is_err());
        assert!(!root.join("benchmark-result.toml").exists());

        child.resume();
        let status = child.wait_until(deadline);
        assert!(status.success(), "benchmark child failed with {status}");
        let stdout = stdout_handle.join().unwrap();
        let stderr = stderr_handle.join().unwrap();
        assert!(stdout.contains("DoraDB benchmark summary\n"));
        assert!(stdout.contains("measured_runs: 2\n"));
        assert!(stderr.contains(&format!(
            "DORADB_BENCH_RESUMED pid={pid} phase=2 workload=trx-noop\n"
        )));

        let encoded = fs::read_to_string(root.join("benchmark-result.toml")).unwrap();
        assert!(encoded.contains("pause = true"));
        let report: InvocationReport = toml::from_str(&encoded).unwrap();
        let Phase::Benchmark { measurement, .. } = &report.plan.phases[1] else {
            panic!("final phase must be a benchmark")
        };
        assert!(measurement.pause);
        assert_eq!(measurement.warmup_runs, 1);
        assert_eq!(measurement.measured_runs.get(), 2);
        assert_eq!(report.prepare_phases.len(), 1);
        assert_eq!(report.measured_runs.len(), 2);
        assert_eq!(report.aggregate.measured_runs, 2);
        assert_eq!(report.aggregate.counters.operations, 4);
    }
}
