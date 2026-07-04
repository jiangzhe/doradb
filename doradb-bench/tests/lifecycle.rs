#[cfg(test)]
mod tests {
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

    #[test]
    fn lifecycle_prepare_run_insert_and_cleanup() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        let prepare_stdout = assert_success(run_bench(&root, &["prepare"]));
        assert!(prepare_stdout.contains("prepared storage_root="));
        assert!(root.join("benchmark-manifest.toml").exists());

        let run_stdout = assert_success(run_bench(
            &root,
            &[
                "run",
                "insert",
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
        assert!(run_stdout.contains("Internal Stats"));
        assert!(run_stdout.contains("Final Result"));
        assert!(run_stdout.contains("operations: 3"));
        assert!(run_stdout.contains("failures: 0"));

        let manifest = fs::read_to_string(root.join("benchmark-manifest.toml")).unwrap();
        assert!(manifest.contains("next_key = 3"));

        let result_md = fs::read_to_string(root.join("benchmark-result.md")).unwrap();
        assert!(result_md.contains("# DoraDB Benchmark Result"));
        assert!(result_md.contains("## Final Result"));

        let internal_stats = fs::read_to_string(root.join("benchmark-internal-stats.csv")).unwrap();
        assert!(internal_stats.starts_with("metric-name,metric-value\n"));
        assert!(internal_stats.contains("transaction.commit_count,"));
        assert!(internal_stats.contains("buffer.mem.cache_hits,"));

        let result_csv = fs::read_to_string(root.join("benchmark-result.csv")).unwrap();
        let mut result_lines = result_csv.lines();
        let header = result_lines.next().unwrap();
        let row = result_lines.next().unwrap();
        assert!(result_lines.next().is_none());
        assert!(header.starts_with("workload,rand,storage_root,num"));
        let columns: Vec<_> = row.split(',').collect();
        assert_eq!(columns[0], "insert");
        assert_eq!(columns[1], "false");
        assert_eq!(columns[3], "3");
        assert_eq!(columns[5], "2");
        assert_eq!(columns[11], "3");
        assert_eq!(columns[15], "0");

        let cleanup_stdout = assert_success(run_bench(&root, &["cleanup"]));
        assert!(cleanup_stdout.contains("removed storage_root="));
        assert!(!root.exists());
    }

    #[test]
    fn prepare_reports_existing_root_error_through_binary() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");
        fs::create_dir(&root).unwrap();

        let stderr = assert_failure(run_bench(&root, &["prepare"]));
        assert!(stderr.contains("must not exist for prepare"));
        assert!(root.exists());
    }

    #[test]
    fn failed_result_write_does_not_advance_manifest_next_key() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("bench");

        assert_success(run_bench(&root, &["prepare"]));
        fs::create_dir(root.join("benchmark-result.csv")).unwrap();

        let stderr = assert_failure(run_bench(
            &root,
            &[
                "run",
                "insert",
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
    }
}
