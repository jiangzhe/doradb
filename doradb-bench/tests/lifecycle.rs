#[cfg(test)]
mod tests {
    use doradb_storage::{Engine, EngineConfig, TableLockMode};
    use std::fs;
    use std::path::Path;
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

    fn run_bench_with_env(root: &Path, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_doradb-bench"))
            .env("DORADB_BENCH_ROOT", root)
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

    fn internal_metric(root: &Path, name: &str) -> u128 {
        fs::read_to_string(root.join("benchmark-internal-stats.csv"))
            .unwrap()
            .lines()
            .skip(1)
            .find_map(|line| {
                let (metric, value) = line.split_once(',')?;
                (metric == name).then(|| value.parse().unwrap())
            })
            .unwrap_or_else(|| panic!("missing internal metric {name}"))
    }

    fn loaded_table_count(root: &Path) -> usize {
        smol::block_on(async {
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            let table_count = session.list_table_ids().unwrap().len();
            session.close().await.unwrap();
            engine.shutdown();
            table_count
        })
    }

    fn assert_all_tables_exclusively_lockable(root: &Path) {
        smol::block_on(async {
            let engine = Engine::bootstrap(EngineConfig::default().storage_root(root))
                .await
                .unwrap();
            let mut session = engine.new_session().unwrap();
            for table_id in session.list_table_ids().unwrap() {
                session
                    .lock_table(table_id, TableLockMode::Exclusive)
                    .await
                    .unwrap();
                session.unlock_table(table_id).unwrap();
            }
            session.close().await.unwrap();
            engine.shutdown();
        });
    }

    #[test]
    fn root_can_come_from_environment_and_cli_overrides_it() {
        let temp = TempDir::new().unwrap();
        let env_root = temp.path().join("env-root");
        let cli_root = temp.path().join("cli-root");

        assert_success(run_bench_with_env(&env_root, &["prepare"]));
        assert!(env_root.exists());
        assert_success(run_bench_with_env(&env_root, &["cleanup"]));

        assert_success(run_bench_with_env(
            &env_root,
            &["--root", cli_root.to_str().unwrap(), "prepare"],
        ));
        assert!(cli_root.exists());
        assert!(!env_root.exists());
        assert_success(run_bench(&cli_root, &["cleanup"]));
    }

    #[test]
    fn exactly_one_execution_mode_is_required() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        let plan = temp.path().join("missing-plan.toml");

        let stderr = assert_failure(run_bench(&root, &[]));
        assert!(stderr.contains("exactly one of --plan or a lifecycle command is required"));

        let stderr = assert_failure(run_bench(
            &root,
            &["--plan", plan.to_str().unwrap(), "prepare"],
        ));
        assert!(stderr.contains("exactly one of --plan or a lifecycle command is required"));
        assert!(!root.exists());
    }

    #[test]
    fn lifecycle_prepare_run_insert_and_cleanup() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        let prepare_stdout = assert_success(run_bench(
            &root,
            &["prepare", "--value-size", "64", "--batch-size", "5"],
        ));
        assert!(prepare_stdout.contains("prepared storage_root="));
        assert!(prepare_stdout.contains("index=none"));
        assert!(prepare_stdout.contains("tables=1"));
        assert!(prepare_stdout.contains("log_sync=fsync"));
        assert!(prepare_stdout.contains("value_size=64"));
        assert!(prepare_stdout.contains("batch_size=5"));
        assert!(root.join("benchmark-manifest.toml").exists());

        let run_stdout = assert_success(run_bench(
            &root,
            &[
                "run",
                "insert-seq",
                "--num",
                "3",
                "--batch-size",
                "2",
                "--threads",
                "1",
                "--sessions",
                "1",
            ],
        ));
        assert!(run_stdout.contains("Configuration"));
        assert!(!run_stdout.contains("Internal Stats"));
        assert!(run_stdout.contains("Final Result"));
        assert!(run_stdout.contains("include_stats: false"));
        assert!(run_stdout.contains("operations: 3"));
        assert!(run_stdout.contains("failures: 0"));

        let manifest = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();
        assert!(manifest.contains("value_size = 64"));
        assert!(manifest.contains("batch_size = 5"));
        assert!(manifest.contains("next_key = 3"));
        assert!(manifest.contains("rows_inserted = 3"));

        let result_md = fs::read_to_string(root.join("benchmark-result.md")).unwrap();
        assert!(result_md.contains("# DoraDB Benchmark Result"));
        assert!(!result_md.contains("## Internal Stats"));
        assert!(result_md.contains("## Final Result"));

        assert!(!root.join("benchmark-internal-stats.csv").exists());

        let result_csv = fs::read_to_string(root.join("benchmark-result.csv")).unwrap();
        let mut result_lines = result_csv.lines();
        let header = result_lines.next().unwrap();
        let row = result_lines.next().unwrap();
        assert!(result_lines.next().is_none());
        assert!(header.starts_with("workload,rand,include_stats,storage_root,num"));
        let columns: Vec<_> = row.split(',').collect();
        assert_eq!(columns[0], "insert-seq");
        assert_eq!(columns[1], "false");
        assert_eq!(columns[2], "false");
        assert_eq!(columns[4], "3");
        assert_eq!(columns[5], "");
        assert_eq!(columns[6], "64");
        assert_eq!(columns[7], "2");
        assert_eq!(columns[9], "none");
        assert_eq!(columns[11], "3");
        assert_eq!(columns[14], "fsync");
        assert_eq!(&columns[16..19], &["", "", ""]);
        assert_eq!(columns[19], "3");
        assert_eq!(columns[20], "3");
        assert_eq!(columns[27], "0");

        let cleanup_stdout = assert_success(run_bench(&root, &["cleanup"]));
        assert!(cleanup_stdout.contains("removed storage_root="));
        assert!(!root.exists());
    }

    #[test]
    fn prepare_reports_existing_root_error_through_binary() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        fs::create_dir(&root).unwrap();

        let stderr = assert_failure(run_bench(&root, &["prepare", "--index", "none"]));
        assert!(stderr.contains("must not exist for prepare"));
        assert!(root.exists());
    }

    #[test]
    fn failed_result_write_does_not_advance_manifest_next_key() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(
            &root,
            &["prepare", "--index", "none", "--log-sync", "none"],
        ));
        fs::create_dir(root.join("benchmark-result.csv")).unwrap();

        let stderr = assert_failure(run_bench(
            &root,
            &[
                "run",
                "insert-seq",
                "--num",
                "1",
                "--batch-size",
                "1",
                "--threads",
                "1",
                "--sessions",
                "1",
            ],
        ));
        assert!(stderr.contains("failed to install benchmark output"));

        let manifest = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();
        assert!(manifest.contains("next_key = 0"));
        assert!(manifest.contains("rows_inserted = 0"));
    }

    #[test]
    fn lifecycle_unique_lookup_read_workloads() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(&root, &["prepare", "--index", "unique"]));
        assert_success(run_bench(
            &root,
            &[
                "run",
                "insert-rand",
                "--num",
                "5",
                "--value-size",
                "16",
                "--seed",
                "1",
            ],
        ));

        let seq_stdout = assert_success(run_bench(
            &root,
            &[
                "run",
                "lookup-seq",
                "--num",
                "7",
                "--batch-size",
                "2",
                "--include-stats",
            ],
        ));
        assert!(seq_stdout.contains("workload: lookup-seq"));
        assert!(seq_stdout.contains("include_stats: true"));
        assert!(seq_stdout.contains("batch_size: 2"));
        assert!(seq_stdout.contains("Internal Stats"));
        assert!(seq_stdout.contains("operations: 7"));
        assert!(seq_stdout.contains("found: 7"));
        assert!(seq_stdout.contains("not_found: 0"));
        let internal_stats = fs::read_to_string(root.join("benchmark-internal-stats.csv")).unwrap();
        assert!(internal_stats.starts_with("metric-name,metric-value\n"));
        assert!(internal_stats.contains("transaction.commit_count,"));
        assert!(internal_stats.contains("buffer.mem.cache_hits,"));

        let rand_stdout = assert_success(run_bench(
            &root,
            &["run", "lookup-rand", "--num", "7", "--seed", "2"],
        ));
        assert!(rand_stdout.contains("workload: lookup-rand"));
        assert!(rand_stdout.contains("include_stats: false"));
        assert!(!rand_stdout.contains("Internal Stats"));
        assert!(rand_stdout.contains("operations: 7"));
        assert!(rand_stdout.contains("found: 7"));
        assert!(rand_stdout.contains("not_found: 0"));
        assert!(!root.join("benchmark-internal-stats.csv").exists());

        assert_success(run_bench(&root, &["cleanup"]));
    }

    #[test]
    fn lifecycle_table_scan_without_secondary_index() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(
            &root,
            &["prepare", "--index", "none", "--log-sync", "none"],
        ));
        assert_success(run_bench(
            &root,
            &["run", "insert-seq", "--num", "4", "--value-size", "16"],
        ));

        let scan_stdout = assert_success(run_bench(&root, &["run", "table-scan"]));
        assert!(scan_stdout.contains("workload: table-scan"));
        assert!(scan_stdout.contains("operations: 1"));
        assert!(scan_stdout.contains("rows_returned: 4"));

        assert_success(run_bench(&root, &["cleanup"]));
    }

    #[test]
    fn lifecycle_index_range_workloads_support_both_index_modes() {
        let temp = TempDir::new().unwrap();
        for index in ["unique", "non-unique"] {
            let root = temp.path().join(index);
            assert_success(run_bench(&root, &["prepare", "--index", index]));
            assert_success(run_bench(
                &root,
                &["run", "insert-seq", "--num", "8", "--value-size", "16"],
            ));
            let manifest_before = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();

            let scan_stdout = assert_success(run_bench(
                &root,
                &[
                    "run",
                    "index-scan",
                    "--num",
                    "4",
                    "--range",
                    "3",
                    "--seed",
                    "3",
                    "--batch-size",
                    "2",
                    "--threads",
                    "2",
                    "--sessions",
                    "2",
                ],
            ));
            assert!(scan_stdout.contains("workload: index-scan"));
            assert!(scan_stdout.contains("range: 3"));
            assert!(scan_stdout.contains("operations: 4"));
            assert!(scan_stdout.contains("found: 4"));
            assert!(scan_stdout.contains("not_found: 0"));
            assert!(scan_stdout.contains("rows_returned: 12"));
            assert!(scan_stdout.contains("failures: 0"));

            let stream_stdout = assert_success(run_bench(
                &root,
                &[
                    "run",
                    "index-stream",
                    "--num",
                    "4",
                    "--range",
                    "3",
                    "--seed",
                    "5",
                    "--threads",
                    "2",
                    "--sessions",
                    "2",
                ],
            ));
            assert!(stream_stdout.contains("workload: index-stream"));
            assert!(stream_stdout.contains("rand: true"));
            assert!(stream_stdout.contains("range: 3"));
            assert!(stream_stdout.contains("seed: 5"));
            assert!(stream_stdout.contains("operations: 4"));
            assert!(stream_stdout.contains("rows_returned: 12"));
            assert_eq!(
                fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap(),
                manifest_before
            );

            if index == "unique" {
                let full_stdout =
                    assert_success(run_bench(&root, &["run", "index-scan", "--num", "1"]));
                assert!(full_stdout.contains("range: 8"));
                assert!(full_stdout.contains("rows_returned: 8"));

                let stderr =
                    assert_failure(run_bench(&root, &["run", "index-stream", "--range", "9"]));
                assert!(stderr.contains("--range (9) must not exceed loaded key range length (8)"));
            } else {
                assert_success(run_bench(
                    &root,
                    &["run", "insert-rand", "--num", "8", "--seed", "9"],
                ));
                for workload in ["index-scan", "index-stream"] {
                    let stdout = assert_success(run_bench(
                        &root,
                        &[
                            "run", workload, "--num", "2", "--range", "3", "--seed", "11",
                        ],
                    ));
                    assert!(stdout.contains("operations: 2"));
                    assert!(stdout.contains("failures: 0"));
                }
            }

            assert_success(run_bench(&root, &["cleanup"]));
        }
    }

    #[test]
    fn lifecycle_stmt_and_trx_noop() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(
            &root,
            &["prepare", "--index", "none", "--log-sync", "none"],
        ));
        let manifest_before = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();

        let stmt_stdout = assert_success(run_bench(
            &root,
            &[
                "run",
                "stmt-noop",
                "--num",
                "5",
                "--threads",
                "2",
                "--sessions",
                "3",
            ],
        ));
        assert!(stmt_stdout.contains("workload: stmt-noop"));
        assert!(stmt_stdout.contains("operations: 5"));
        assert!(stmt_stdout.contains("inserted_rows: 0"));
        assert!(stmt_stdout.contains("rows_returned: 0"));
        assert!(stmt_stdout.contains("loaded_key_range: [0, 0)"));
        assert!(stmt_stdout.contains("log_sync: none"));
        assert_eq!(
            fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap(),
            manifest_before
        );

        let trx_stdout = assert_success(run_bench(
            &root,
            &[
                "run",
                "trx-noop",
                "--num",
                "4",
                "--threads",
                "2",
                "--sessions",
                "3",
                "--include-stats",
            ],
        ));
        assert!(trx_stdout.contains("workload: trx-noop"));
        assert!(trx_stdout.contains("operations: 4"));
        assert_eq!(internal_metric(&root, "transaction.commit_count"), 0);
        assert_eq!(
            fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap(),
            manifest_before
        );

        assert_success(run_bench(&root, &["cleanup"]));
    }

    #[test]
    fn lifecycle_table_and_index_ddl_cycles() {
        let temp = TempDir::new().unwrap();
        let table_root = temp.path().join("table-ddl");

        assert_success(run_bench(&table_root, &["prepare", "--index", "unique"]));
        let table_manifest_before =
            fs::read_to_string(table_root.join("benchmark-manifest.toml")).unwrap();
        let table_stdout = assert_success(run_bench(
            &table_root,
            &["run", "table-ddl", "--threads", "1", "--sessions", "2"],
        ));
        assert!(table_stdout.contains("workload: table-ddl"));
        assert!(table_stdout.contains("num: 1"));
        assert!(table_stdout.contains("operations: 2"));
        assert_eq!(loaded_table_count(&table_root), 1);
        assert_eq!(
            fs::read_to_string(table_root.join("benchmark-manifest.toml")).unwrap(),
            table_manifest_before
        );
        assert_success(run_bench(
            &table_root,
            &["run", "insert-seq", "--num", "2", "--value-size", "16"],
        ));
        let scan_stdout = assert_success(run_bench(&table_root, &["run", "table-scan"]));
        assert!(scan_stdout.contains("rows_returned: 2"));
        assert_success(run_bench(&table_root, &["cleanup"]));

        let index_root = temp.path().join("index-ddl");
        assert_success(run_bench(&index_root, &["prepare", "--index", "none"]));
        let empty_index_stdout = assert_success(run_bench(&index_root, &["run", "index-ddl"]));
        assert!(empty_index_stdout.contains("operations: 2"));
        assert_success(run_bench(
            &index_root,
            &["run", "insert-seq", "--num", "3", "--value-size", "16"],
        ));
        let index_manifest_before =
            fs::read_to_string(index_root.join("benchmark-manifest.toml")).unwrap();
        let index_stdout = assert_success(run_bench(
            &index_root,
            &[
                "run",
                "index-ddl",
                "--num",
                "2",
                "--threads",
                "2",
                "--sessions",
                "2",
            ],
        ));
        assert!(index_stdout.contains("workload: index-ddl"));
        assert!(index_stdout.contains("num: 2"));
        assert!(index_stdout.contains("operations: 4"));
        assert!(index_stdout.contains("loaded_key_range: [0, 3)"));
        assert_eq!(
            fs::read_to_string(index_root.join("benchmark-manifest.toml")).unwrap(),
            index_manifest_before
        );

        let second_stdout = assert_success(run_bench(&index_root, &["run", "index-ddl"]));
        assert!(second_stdout.contains("operations: 2"));
        let scan_stdout = assert_success(run_bench(&index_root, &["run", "table-scan"]));
        assert!(scan_stdout.contains("rows_returned: 3"));
        assert_success(run_bench(&index_root, &["cleanup"]));
    }

    #[test]
    fn lifecycle_lock_table_modes_use_prepared_pool_and_cleanup_claims() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("lock-table");

        let prepare_stdout = assert_success(run_bench(
            &root,
            &["prepare", "--tables", "3", "--log-sync", "none"],
        ));
        assert!(prepare_stdout.contains("index=none"));
        assert!(prepare_stdout.contains("tables=3"));
        assert!(prepare_stdout.contains("log_sync=none"));
        assert_eq!(loaded_table_count(&root), 3);
        let manifest_before = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();

        let cases = [
            (
                &[
                    "run",
                    "lock-table",
                    "--num",
                    "7",
                    "--threads",
                    "2",
                    "--sessions",
                    "5",
                ][..],
                "session",
                false,
                false,
                0,
            ),
            (
                &[
                    "run",
                    "lock-table",
                    "--num",
                    "6",
                    "--unlock",
                    "--threads",
                    "2",
                    "--sessions",
                    "5",
                ][..],
                "session",
                true,
                false,
                0,
            ),
            (
                &[
                    "run",
                    "lock-table",
                    "--num",
                    "6",
                    "--unlock",
                    "--rand",
                    "--seed",
                    "9",
                    "--threads",
                    "2",
                    "--sessions",
                    "5",
                ][..],
                "session",
                true,
                true,
                9,
            ),
            (
                &[
                    "run",
                    "lock-table",
                    "--num",
                    "2",
                    "--scope",
                    "transaction",
                    "--threads",
                    "2",
                    "--sessions",
                    "5",
                ][..],
                "transaction",
                false,
                false,
                0,
            ),
            (
                &[
                    "run",
                    "lock-table",
                    "--num",
                    "6",
                    "--scope",
                    "transaction",
                    "--unlock",
                    "--threads",
                    "2",
                    "--sessions",
                    "5",
                ][..],
                "transaction",
                true,
                false,
                0,
            ),
            (
                &[
                    "run",
                    "lock-table",
                    "--num",
                    "6",
                    "--scope",
                    "transaction",
                    "--unlock",
                    "--rand",
                    "--seed",
                    "11",
                    "--threads",
                    "2",
                    "--sessions",
                    "5",
                ][..],
                "transaction",
                true,
                true,
                11,
            ),
        ];

        for (args, scope, unlock, random, seed) in cases {
            let stdout = assert_success(run_bench(&root, args));
            assert!(stdout.contains("workload: lock-table"));
            assert!(stdout.contains(&format!("scope: {scope}")));
            assert!(stdout.contains(&format!("unlock: {unlock}")));
            assert!(stdout.contains(&format!("rand: {random}")));
            assert!(stdout.contains(&format!("seed: {seed}")));
            assert!(stdout.contains("tables: 3"));
            assert!(stdout.contains("log_sync: none"));
            let num = args
                .windows(2)
                .find_map(|pair| (pair[0] == "--num").then_some(pair[1]))
                .unwrap();
            assert!(stdout.contains(&format!("operations: {num}")));
            assert!(stdout.contains("inserted_rows: 0"));
            assert!(stdout.contains("rows_returned: 0"));
            assert_eq!(
                fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap(),
                manifest_before
            );
            assert_all_tables_exclusively_lockable(&root);
        }

        let result_csv = fs::read_to_string(root.join("benchmark-result.csv")).unwrap();
        let mut lines = result_csv.lines();
        let header = lines.next().unwrap().split(',').collect::<Vec<_>>();
        let row = lines.next().unwrap().split(',').collect::<Vec<_>>();
        let value = |name: &str| row[header.iter().position(|column| *column == name).unwrap()];
        assert_eq!(value("scope"), "transaction");
        assert_eq!(value("unlock"), "true");
        assert_eq!(value("tables"), "3");
        assert_eq!(value("rand"), "true");
        assert_eq!(value("seed"), "11");
        assert_eq!(value("operations"), "6");

        assert_success(run_bench(
            &root,
            &["run", "insert-seq", "--num", "2", "--value-size", "16"],
        ));
        let scan_stdout = assert_success(run_bench(&root, &["run", "table-scan"]));
        assert!(scan_stdout.contains("rows_returned: 2"));
        assert_success(run_bench(&root, &["cleanup"]));
    }

    #[test]
    fn read_workloads_fail_before_measurement_when_incompatible() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(&root, &["prepare", "--index", "none"]));
        assert_success(run_bench(
            &root,
            &["run", "insert-seq", "--num", "2", "--value-size", "16"],
        ));

        let stderr = assert_failure(run_bench(&root, &["run", "lookup-seq", "--num", "1"]));
        assert!(stderr.contains("lookup-seq workload requires prepared index mode unique"));

        let stderr = assert_failure(run_bench(&root, &["run", "index-scan", "--num", "1"]));
        assert!(
            stderr
                .contains("index-scan workload requires prepared index mode unique or non-unique")
        );

        let stderr = assert_failure(run_bench(&root, &["run", "index-stream"]));
        assert!(
            stderr.contains(
                "index-stream workload requires prepared index mode unique or non-unique"
            )
        );

        assert_success(run_bench(&root, &["cleanup"]));
    }

    #[test]
    fn read_workloads_fail_before_measurement_without_loaded_data() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(&root, &["prepare", "--index", "unique"]));

        let stderr = assert_failure(run_bench(&root, &["run", "lookup-seq", "--num", "1"]));
        assert!(stderr.contains("read workload requires loaded benchmark data"));

        let stderr = assert_failure(run_bench(&root, &["run", "index-stream"]));
        assert!(stderr.contains("read workload requires loaded benchmark data"));

        let stderr = assert_failure(run_bench(&root, &["run", "index-ddl"]));
        assert!(stderr.contains("index-ddl workload requires prepared index mode none"));

        assert_success(run_bench(&root, &["cleanup"]));
    }

    #[test]
    fn plan_trx_noop_writes_canonical_results_and_plan_marker() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench-plan");
        let plan_path = temp.path().join("plan.toml");
        fs::write(
            &plan_path,
            r#"
name = "trx-noop-smoke"
meta = "not accepted here"
"#,
        )
        .unwrap();
        let stderr = assert_failure(run_bench(&root, &["-p", plan_path.to_str().unwrap()]));
        assert!(stderr.contains("unknown field"));
        assert!(!root.exists());

        fs::write(
            &plan_path,
            r#"
name = "trx-noop-smoke"

[engine]
meta_buffer_bytes = 16777216

[engine.transaction]
log_sync = "none"

[engine.index_buffer]
max_file_size_bytes = 67108864
max_mem_size_bytes = 16777216

[engine.data_buffer]
max_file_size_bytes = 67108864
max_mem_size_bytes = 16777216

[engine.file]
readonly_buffer_size_bytes = 33554432

[workload_defaults]
threads = 2
sessions = 2
include_stats = true

[[phase]]
workload = { type = "trx-noop", num = 1 }

[[phase]]
kind = "benchmark"
warmup_runs = 1
measured_runs = 2
workload = { type = "trx-noop", num = 4 }
"#,
        )
        .unwrap();

        let stdout = assert_success(run_bench(&root, &["-p", plan_path.to_str().unwrap()]));
        assert!(stdout.contains("completed benchmark plan="));
        let marker = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();
        assert!(marker.contains("mode = \"plan\""));
        assert!(marker.contains("plan_source"));

        let result = fs::read_to_string(root.join("benchmark-result.toml")).unwrap();
        assert!(result.contains("status = \"success\""));
        assert!(result.contains("measured_runs = 2"));
        assert!(result.contains("unit = \"transaction-lifecycle\""));
        assert!(result.contains("kind = \"counter-delta\""));
        assert!(result.contains("elapsed_nanos = \""));
        assert_eq!(result.matches("run_index = ").count(), 2);

        let markdown = fs::read_to_string(root.join("benchmark-result.md")).unwrap();
        assert!(markdown.contains("# DoraDB Benchmark Plan Result"));
        assert!(markdown.contains(&result));

        let cleanup_stdout = assert_success(run_bench(&root, &["cleanup"]));
        assert!(cleanup_stdout.contains("removed storage_root="));
        assert!(!root.exists());
    }
}
