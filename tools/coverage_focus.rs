#!/usr/bin/env -S cargo +nightly -q -Zscript
---
[package]
edition = "2024"
---

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

const COVERAGE_WORK_DIR: &str = "target/coverage-focus";
const TARGET_DIR: &str = "target/coverage-focus/cargo";
const LLVM_COV_TARGET_DIR: &str = "target/coverage-focus/llvm-cov-target";
const LCOV_OUT: &str = "target/coverage-focus/lcov.info";
const COVERAGE_IGNORE_FILENAME_REGEX: &str =
    r"(^|/)(target|legacy)(/|$)|/\.cargo/(registry|git)/|/rustlib/src/rust/";

#[derive(Debug)]
struct Args {
    targets: Vec<FocusTarget>,
    write_path: Option<PathBuf>,
    top_uncovered: usize,
    show_output: bool,
}

#[derive(Debug, Clone)]
struct FocusTarget {
    raw: String,
    abs_key: String,
    display: String,
    is_dir: bool,
}

#[derive(Debug, Clone, Default)]
struct FileCoverage {
    abs_key: String,
    display_path: String,
    line_hits: BTreeMap<u32, u64>,
}

#[derive(Debug, Clone)]
struct FileReport {
    abs_key: String,
    path: String,
    lines_found: usize,
    lines_hit: usize,
    uncovered_lines: Vec<u32>,
}

#[derive(Debug, Clone)]
struct CoverageTotals {
    lines_found: usize,
    lines_hit: usize,
}

#[derive(Debug, Clone)]
struct MatchResult {
    files: Vec<FileReport>,
    totals: CoverageTotals,
}

#[derive(Debug, Clone)]
struct TargetReport {
    target: FocusTarget,
    result: MatchResult,
}

#[derive(Debug, Clone)]
struct FocusReport {
    targets: Vec<TargetReport>,
    totals: CoverageTotals,
    unique_file_count: usize,
}

fn usage() -> &'static str {
    "Usage: tools/coverage_focus.rs --path <repo-path> [--path <repo-path> ...] [--write <markdown-path>] [--top-uncovered <n>] [--verbose]\n\
\n\
Prerequisites:\n\
- `cargo-nextest` available in PATH (`cargo install --locked cargo-nextest`)\n\
- `cargo-llvm-cov` available in PATH (`cargo install --locked cargo-llvm-cov`)\n\
- LLVM tools with `llvm-cov` and `llvm-profdata` (for example `rustup component add llvm-tools`)\n\
- Optional `COVERAGE_FOCUS_LLVM_PATH` can point to the LLVM tools directory containing both tools\n\
- Default coverage runs use the repository-default `io_uring` backend; install `libaio1` and `libaio-dev` separately when you also need to validate the alternate `libaio` backend on Linux\n\
\n\
Output:\n\
- By default, command stdout/stderr is hidden and only step descriptions are printed\n\
- Repeat `--path` to compute several focused reports from one coverage run\n\
- Use `--verbose` to stream full build/test/report output"
}

fn main() {
    if let Err(err) = run() {
        if err.is_empty() {
            return;
        }
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let repo_root =
        env::current_dir().map_err(|e| format!("failed to resolve current directory: {e}"))?;
    let args = parse_args(&repo_root)?;

    precheck_repo_layout(&repo_root)?;
    precheck_cargo_nextest()?;
    precheck_cargo_llvm_cov()?;
    let llvm_tools_dir = precheck_llvm_tools(&repo_root)?;

    prepare_coverage_dirs()?;
    run_coverage_phases(&repo_root, &llvm_tools_dir, args.show_output)?;
    validate_lcov_output()?;

    let lcov_rows = parse_lcov(Path::new(LCOV_OUT), &repo_root)?;
    let matched = focus_targets(&lcov_rows, &args)?;

    let report = render_console_report(&args, &matched);
    print!("{report}");

    if let Some(path) = &args.write_path {
        let markdown = render_markdown_report(&args, &matched);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
        fs::write(path, markdown)
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
        println!("markdown report written: {}", path.display());
    }

    Ok(())
}

fn parse_args(repo_root: &Path) -> Result<Args, String> {
    let mut args = env::args().skip(1);
    let mut target_raws: Vec<String> = Vec::new();
    let mut write_path: Option<PathBuf> = None;
    let mut top_uncovered: usize = 10;
    let mut show_output = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --path\n{}", usage()));
                };
                target_raws.push(v);
            }
            "--write" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --write\n{}", usage()));
                };
                write_path = Some(PathBuf::from(v));
            }
            "--top-uncovered" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --top-uncovered\n{}", usage()));
                };
                top_uncovered = v
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --top-uncovered value: {v}"))?;
            }
            "--verbose" | "--show-output" => {
                show_output = true;
            }
            "--help" | "-h" => {
                println!("{}", usage());
                return Err(String::new());
            }
            _ => return Err(format!("unknown arg: {arg}\n{}", usage())),
        }
    }

    if target_raws.is_empty() {
        return Err(format!("missing required --path\n{}", usage()));
    }
    let targets = parse_focus_targets(target_raws, repo_root)?;

    Ok(Args {
        targets,
        write_path,
        top_uncovered,
        show_output,
    })
}

fn parse_focus_targets(raws: Vec<String>, repo_root: &Path) -> Result<Vec<FocusTarget>, String> {
    let mut targets = Vec::with_capacity(raws.len());
    let mut seen: BTreeMap<String, String> = BTreeMap::new();

    for raw in raws {
        let target_path = PathBuf::from(&raw);
        let target_abs = if target_path.is_absolute() {
            target_path
        } else {
            repo_root.join(target_path)
        };

        if !target_abs.exists() {
            return Err(format!(
                "target path does not exist: {}",
                target_abs.display()
            ));
        }

        let target_abs = target_abs.canonicalize().map_err(|e| {
            format!(
                "failed to canonicalize target path {}: {e}",
                target_abs.display()
            )
        })?;

        let abs_key = normalize_abs_key(&target_abs, repo_root);
        if let Some(prev_raw) = seen.insert(abs_key.clone(), raw.clone()) {
            return Err(format!(
                "duplicate target path after canonicalization: `{raw}` duplicates `{prev_raw}`"
            ));
        }

        targets.push(FocusTarget {
            raw,
            abs_key,
            display: rel_display(&target_abs, repo_root),
            is_dir: target_abs.is_dir(),
        });
    }

    Ok(targets)
}

fn precheck_repo_layout(repo_root: &Path) -> Result<(), String> {
    let expected = [
        repo_root.join(".github/workflows/build.yml"),
        repo_root.join("Cargo.toml"),
    ];
    for path in expected {
        if !path.exists() {
            return Err(format!(
                "repository preflight failed, missing: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn precheck_cargo_nextest() -> Result<(), String> {
    let status = Command::new("cargo")
        .args(["nextest", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| {
            "`cargo-nextest` not found in PATH. Install with: cargo install --locked cargo-nextest"
                .to_string()
        })?;
    if !status.success() {
        return Err(
            "`cargo nextest --version` failed. Ensure cargo-nextest is correctly installed."
                .to_string(),
        );
    }
    Ok(())
}

fn precheck_cargo_llvm_cov() -> Result<(), String> {
    let status = Command::new("cargo")
        .args(["llvm-cov", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| {
            "`cargo-llvm-cov` not found in PATH. Install with: cargo install --locked cargo-llvm-cov"
                .to_string()
        })?;
    if !status.success() {
        return Err(
            "`cargo llvm-cov --version` failed. Ensure cargo-llvm-cov is correctly installed."
                .to_string(),
        );
    }
    Ok(())
}

fn precheck_llvm_tools(repo_root: &Path) -> Result<PathBuf, String> {
    let llvm_dir = detect_llvm_tools_dir(repo_root).ok_or_else(|| {
        "llvm tools not found. Install with `rustup component add llvm-tools` or set `COVERAGE_FOCUS_LLVM_PATH` to a directory containing both `llvm-cov` and `llvm-profdata`.".to_string()
    })?;

    for tool in ["llvm-cov", "llvm-profdata"] {
        let tool_path = llvm_dir.join(tool);
        let status = Command::new(&tool_path)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| format!("failed to execute `{}`: {e}", tool_path.display()))?;
        if !status.success() {
            return Err(format!(
                "`{} --version` failed. Ensure LLVM tools are correctly installed.",
                tool_path.display()
            ));
        }
    }
    Ok(llvm_dir)
}

fn prepare_coverage_dirs() -> Result<(), String> {
    fs::create_dir_all(COVERAGE_WORK_DIR)
        .map_err(|e| format!("failed to create {COVERAGE_WORK_DIR}: {e}"))?;
    fs::create_dir_all(TARGET_DIR).map_err(|e| format!("failed to create {TARGET_DIR}: {e}"))?;
    remove_path_if_exists(Path::new(LLVM_COV_TARGET_DIR))?;
    remove_path_if_exists(Path::new(LCOV_OUT))?;
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)
            .map_err(|e| format!("failed to remove {}: {e}", path.display()))?;
    } else {
        fs::remove_file(path).map_err(|e| format!("failed to remove {}: {e}", path.display()))?;
    }
    Ok(())
}

fn run_coverage_phases(
    repo_root: &Path,
    llvm_tools_dir: &Path,
    show_output: bool,
) -> Result<(), String> {
    println!("== coverage phase: default io_uring backend via cargo llvm-cov nextest ==");
    run_checked(
        repo_root,
        "cargo",
        coverage_nextest_args(),
        &coverage_env(repo_root, llvm_tools_dir),
        show_output,
    )
    .map_err(format_coverage_phase_error)?;

    Ok(())
}

fn coverage_nextest_args() -> &'static [&'static str] {
    &[
        "llvm-cov",
        "nextest",
        "--no-clean",
        "--lcov",
        "--output-path",
        LCOV_OUT,
        "--no-default-ignore-filename-regex",
        "--ignore-filename-regex",
        COVERAGE_IGNORE_FILENAME_REGEX,
        "-p",
        "doradb-storage",
        "--profile",
        "ci",
    ]
}

fn coverage_env(repo_root: &Path, llvm_tools_dir: &Path) -> Vec<(&'static str, String)> {
    vec![
        (
            "CARGO_TARGET_DIR",
            repo_root.join(TARGET_DIR).to_string_lossy().to_string(),
        ),
        (
            "CARGO_LLVM_COV_TARGET_DIR",
            repo_root
                .join(LLVM_COV_TARGET_DIR)
                .to_string_lossy()
                .to_string(),
        ),
        (
            "LLVM_COV",
            llvm_tools_dir
                .join("llvm-cov")
                .to_string_lossy()
                .to_string(),
        ),
        (
            "LLVM_PROFDATA",
            llvm_tools_dir
                .join("llvm-profdata")
                .to_string_lossy()
                .to_string(),
        ),
        ("RUST_BACKTRACE", "full".to_string()),
    ]
}

fn run_checked(
    repo_root: &Path,
    program: &str,
    args: &[&str],
    envs: &[(&str, String)],
    show_output: bool,
) -> Result<(), String> {
    if show_output {
        println!("$ {} {}", program, args.join(" "));
    }
    let mut cmd = Command::new(program);
    cmd.args(args).current_dir(repo_root);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    if !show_output {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to start `{program}`: {e}"))?;
    ensure_success(program, args, status, show_output)
}

fn ensure_success(
    program: &str,
    args: &[&str],
    status: ExitStatus,
    show_output: bool,
) -> Result<(), String> {
    if status.success() {
        return Ok(());
    }
    let code = status
        .code()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "terminated by signal".to_string());
    let mut msg = format!("command failed (exit {code}): {program} {}", args.join(" "));
    if !show_output {
        msg.push_str("\nrerun with `--verbose` to see command logs.");
    }
    Err(msg)
}

fn format_coverage_phase_error(base: String) -> String {
    let mut out = String::new();
    out.push_str(&base);
    out.push_str("\n");
    out.push_str("default coverage phase failed. Confirm this machine supports io_uring for the repository-default backend. If you also need alternate libaio validation, install `libaio1` and `libaio-dev` (Ubuntu: `sudo apt-get install -y libaio1 libaio-dev`) and run that path separately.");
    out
}

fn validate_lcov_output() -> Result<(), String> {
    if !Path::new(LCOV_OUT).exists() {
        return Err(format!(
            "coverage run finished but output not found: {LCOV_OUT}"
        ));
    }
    let lcov = fs::read_to_string(LCOV_OUT)
        .map_err(|e| format!("failed to read merged lcov output {LCOV_OUT}: {e}"))?;
    if !lcov.lines().any(|line| line.starts_with("SF:")) {
        return Err("generated LCOV is empty. Check `cargo llvm-cov nextest` logs, ensure llvm tools are available, and confirm ignore filters are not excluding the requested Rust sources.".to_string());
    }
    Ok(())
}

fn parse_lcov(path: &Path, repo_root: &Path) -> Result<Vec<FileCoverage>, String> {
    let text =
        fs::read_to_string(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let mut files: BTreeMap<String, FileCoverage> = BTreeMap::new();

    let mut current_abs: Option<String> = None;
    let mut current_sf = String::new();

    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("SF:") {
            current_sf = normalize_path(rest);
            let abs_key = normalize_abs_key(Path::new(&current_sf), repo_root);
            current_abs = Some(abs_key.clone());
            files
                .entry(abs_key.clone())
                .or_insert_with(|| FileCoverage {
                    abs_key,
                    display_path: rel_display_from_abs_key(&current_sf, repo_root),
                    line_hits: BTreeMap::new(),
                });
            continue;
        }

        if let Some(rest) = line.strip_prefix("DA:") {
            let Some(abs_key) = &current_abs else {
                continue;
            };
            let Some((line_no_str, hits_str)) = rest.split_once(',') else {
                continue;
            };
            let Ok(line_no) = line_no_str.parse::<u32>() else {
                continue;
            };
            let hit_token = hits_str.split(',').next().unwrap_or(hits_str);
            let Ok(hits) = hit_token.parse::<u64>() else {
                continue;
            };
            let row = files.get_mut(abs_key).expect("entry must exist");
            let prev = row.line_hits.entry(line_no).or_insert(0);
            *prev = (*prev).max(hits);
            continue;
        }

        if line == "end_of_record" {
            current_abs = None;
            current_sf.clear();
        }
    }

    Ok(files.into_values().collect())
}

fn focus_targets(rows: &[FileCoverage], args: &Args) -> Result<FocusReport, String> {
    let mut reports = Vec::with_capacity(args.targets.len());
    let mut unmatched = Vec::new();
    let mut unique_files: BTreeMap<String, FileReport> = BTreeMap::new();

    for target in &args.targets {
        let result = focus_target(rows, target);
        if result.files.is_empty() {
            unmatched.push(target.display.clone());
        } else {
            for file in &result.files {
                unique_files
                    .entry(file.abs_key.clone())
                    .or_insert_with(|| file.clone());
            }
            reports.push(TargetReport {
                target: target.clone(),
                result,
            });
        }
    }

    if !unmatched.is_empty() {
        return Err(format!(
            "no LCOV entries matched target(s): {}. Confirm each path is part of instrumented Rust sources.",
            unmatched
                .iter()
                .map(|target| format!("`{target}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let unique_file_count = unique_files.len();
    let totals = coverage_totals(unique_files.values());

    Ok(FocusReport {
        targets: reports,
        totals,
        unique_file_count,
    })
}

fn focus_target(rows: &[FileCoverage], target: &FocusTarget) -> MatchResult {
    let mut out: Vec<FileReport> = Vec::new();

    for row in rows {
        let is_match = if target.is_dir {
            is_prefix_path(&target.abs_key, &row.abs_key)
        } else {
            row.abs_key == target.abs_key
        };
        if is_match {
            out.push(file_report(row));
        }
    }

    sort_file_reports(&mut out);
    let totals = coverage_totals(out.iter());

    MatchResult { files: out, totals }
}

fn file_report(row: &FileCoverage) -> FileReport {
    let mut uncovered = Vec::new();
    let mut lines_hit = 0usize;
    for (line_no, hits) in &row.line_hits {
        if *hits > 0 {
            lines_hit += 1;
        } else {
            uncovered.push(*line_no);
        }
    }

    FileReport {
        abs_key: row.abs_key.clone(),
        path: row.display_path.clone(),
        lines_found: row.line_hits.len(),
        lines_hit,
        uncovered_lines: uncovered,
    }
}

fn sort_file_reports(files: &mut [FileReport]) {
    files.sort_by(|a, b| {
        let ac = coverage_pct(a.lines_hit, a.lines_found);
        let bc = coverage_pct(b.lines_hit, b.lines_found);
        ac.partial_cmp(&bc)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.lines_found.cmp(&a.lines_found))
            .then_with(|| a.path.cmp(&b.path))
    });
}

fn coverage_totals<'a>(files: impl IntoIterator<Item = &'a FileReport>) -> CoverageTotals {
    let mut totals = CoverageTotals {
        lines_found: 0,
        lines_hit: 0,
    };
    for file in files {
        totals.lines_found += file.lines_found;
        totals.lines_hit += file.lines_hit;
    }
    totals
}

fn render_console_report(args: &Args, report: &FocusReport) -> String {
    if report.targets.len() == 1 {
        return render_single_target_report(args, &report.targets[0]);
    }

    let mut out = String::new();
    let summary_pct = coverage_pct(report.totals.lines_hit, report.totals.lines_found);

    out.push_str("# Coverage Focus Report\n\n");
    out.push_str(&format!("- Requested path args: {}\n", args.targets.len()));
    out.push_str(&format!(
        "- Unique matched files: {}\n",
        report.unique_file_count
    ));
    out.push_str(&format!(
        "- Deduplicated total line coverage: {}/{} ({summary_pct:.2}%)\n\n",
        report.totals.lines_hit, report.totals.lines_found
    ));

    out.push_str("## Requested targets\n\n");
    out.push_str("| target | kind | matched files | line coverage |\n");
    out.push_str("|---|---|---:|---:|\n");
    for target_report in &report.targets {
        let result = &target_report.result;
        let pct = coverage_pct(result.totals.lines_hit, result.totals.lines_found);
        out.push_str(&format!(
            "| `{}` | {} | {} | {}/{} ({:.2}%) |\n",
            target_report.target.display,
            target_kind(&target_report.target),
            result.files.len(),
            result.totals.lines_hit,
            result.totals.lines_found,
            pct
        ));
    }

    for target_report in &report.targets {
        out.push_str(&format!(
            "\n## Target: `{}`\n\n",
            target_report.target.display
        ));
        write_target_summary(&mut out, target_report);
        write_file_table(
            &mut out,
            "### Per-file line coverage",
            &target_report.result.files,
        );
        write_hotspots(
            &mut out,
            "### Uncovered-line hotspots",
            &target_report.result.files,
            args.top_uncovered,
        );
    }

    out
}

fn render_single_target_report(args: &Args, target_report: &TargetReport) -> String {
    let mut out = String::new();
    let result = &target_report.result;
    let summary_pct = coverage_pct(result.totals.lines_hit, result.totals.lines_found);

    out.push_str("# Coverage Focus Report\n\n");
    out.push_str(&format!(
        "- Requested path arg: `{}`\n",
        target_report.target.raw
    ));
    out.push_str(&format!("- Target: `{}`\n", target_report.target.display));
    out.push_str(&format!(
        "- Target kind: {}\n",
        target_kind(&target_report.target)
    ));
    out.push_str(&format!("- Matched files: {}\n", result.files.len()));
    out.push_str(&format!(
        "- Total line coverage: {}/{} ({summary_pct:.2}%)\n\n",
        result.totals.lines_hit, result.totals.lines_found
    ));

    write_file_table(&mut out, "## Per-file line coverage", &result.files);
    write_hotspots(
        &mut out,
        "## Uncovered-line hotspots",
        &result.files,
        args.top_uncovered,
    );

    out
}

fn write_target_summary(out: &mut String, target_report: &TargetReport) {
    let result = &target_report.result;
    let pct = coverage_pct(result.totals.lines_hit, result.totals.lines_found);
    out.push_str(&format!(
        "- Requested path arg: `{}`\n",
        target_report.target.raw
    ));
    out.push_str(&format!(
        "- Target kind: {}\n",
        target_kind(&target_report.target)
    ));
    out.push_str(&format!("- Matched files: {}\n", result.files.len()));
    out.push_str(&format!(
        "- Target line coverage: {}/{} ({pct:.2}%)\n\n",
        result.totals.lines_hit, result.totals.lines_found
    ));
}

fn write_file_table(out: &mut String, heading: &str, files: &[FileReport]) {
    out.push_str(heading);
    out.push_str("\n\n");
    out.push_str("| file | lines hit | lines found | coverage | uncovered |\n");
    out.push_str("|---|---:|---:|---:|---:|\n");
    for row in files {
        let pct = coverage_pct(row.lines_hit, row.lines_found);
        out.push_str(&format!(
            "| `{}` | {} | {} | {:.2}% | {} |\n",
            row.path,
            row.lines_hit,
            row.lines_found,
            pct,
            row.uncovered_lines.len()
        ));
    }
}

fn write_hotspots(out: &mut String, heading: &str, files: &[FileReport], top_uncovered: usize) {
    out.push_str("\n");
    out.push_str(heading);
    out.push_str("\n\n");
    let mut hotspots: Vec<&FileReport> = files
        .iter()
        .filter(|r| !r.uncovered_lines.is_empty())
        .collect();
    hotspots.sort_by(|a, b| {
        b.uncovered_lines
            .len()
            .cmp(&a.uncovered_lines.len())
            .then_with(|| a.path.cmp(&b.path))
    });

    if hotspots.is_empty() {
        out.push_str("All matched executable lines are covered.\n");
        return;
    }

    for row in hotspots.into_iter().take(top_uncovered) {
        let preview = row
            .uncovered_lines
            .iter()
            .take(24)
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = if row.uncovered_lines.len() > 24 {
            ", ..."
        } else {
            ""
        };
        out.push_str(&format!(
            "- `{}`: {} uncovered lines (first: {}{})\n",
            row.path,
            row.uncovered_lines.len(),
            preview,
            suffix
        ));
    }
}

fn target_kind(target: &FocusTarget) -> &'static str {
    if target.is_dir { "directory" } else { "file" }
}

fn render_markdown_report(args: &Args, result: &FocusReport) -> String {
    render_console_report(args, result)
}

fn coverage_pct(lines_hit: usize, lines_found: usize) -> f64 {
    if lines_found == 0 {
        100.0
    } else {
        (lines_hit as f64) * 100.0 / (lines_found as f64)
    }
}

fn normalize_path(raw: &str) -> String {
    raw.replace('\\', "/")
}

fn normalize_abs_key(path: &Path, repo_root: &Path) -> String {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        repo_root.join(path)
    };
    normalize_lexical(&joined)
}

fn normalize_lexical(path: &Path) -> String {
    let mut stack: Vec<String> = Vec::new();
    let mut has_root = false;

    for comp in path.components() {
        match comp {
            Component::Prefix(prefix) => {
                stack.push(prefix.as_os_str().to_string_lossy().to_string())
            }
            Component::RootDir => has_root = true,
            Component::CurDir => {}
            Component::ParentDir => {
                if !stack.is_empty() {
                    stack.pop();
                }
            }
            Component::Normal(seg) => stack.push(seg.to_string_lossy().to_string()),
        }
    }

    let mut out = String::new();
    if has_root {
        out.push('/');
    }
    out.push_str(&stack.join("/"));
    out
}

fn is_prefix_path(prefix: &str, full: &str) -> bool {
    if prefix == full {
        return true;
    }
    if !full.starts_with(prefix) {
        return false;
    }
    full.as_bytes().get(prefix.len()) == Some(&b'/')
}

fn rel_display(path: &Path, repo_root: &Path) -> String {
    if let Ok(rel) = path.strip_prefix(repo_root) {
        return normalize_path(&rel.to_string_lossy());
    }
    normalize_path(&path.to_string_lossy())
}

fn rel_display_from_abs_key(source_path: &str, repo_root: &Path) -> String {
    let source = Path::new(source_path);
    if source.is_relative() {
        return normalize_path(source_path);
    }

    if let Ok(rel) = source.strip_prefix(repo_root) {
        return normalize_path(&rel.to_string_lossy());
    }

    normalize_path(source_path)
}

fn has_required_llvm_tools(dir: &Path) -> bool {
    dir.join("llvm-cov").is_file() && dir.join("llvm-profdata").is_file()
}

fn detect_llvm_tools_dir(repo_root: &Path) -> Option<PathBuf> {
    if let Ok(value) = env::var("COVERAGE_FOCUS_LLVM_PATH") {
        let candidate = PathBuf::from(value);
        if has_required_llvm_tools(&candidate) {
            return Some(candidate);
        }
    }

    let from_rustc = Command::new("rustc")
        .arg("--print")
        .arg("target-libdir")
        .current_dir(repo_root)
        .output()
        .ok()
        .and_then(|out| {
            if !out.status.success() {
                return None;
            }
            let lib_dir = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim().to_string());
            let rustlib_dir = lib_dir.parent()?;
            let bin_dir = rustlib_dir.join("bin");
            if has_required_llvm_tools(&bin_dir) {
                Some(bin_dir)
            } else {
                None
            }
        });
    if from_rustc.is_some() {
        return from_rustc;
    }

    let from_rustup_stable = Command::new("rustup")
        .args(["run", "stable", "rustc", "--print", "target-libdir"])
        .current_dir(repo_root)
        .output()
        .ok()
        .and_then(|out| {
            if !out.status.success() {
                return None;
            }
            let lib_dir = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim().to_string());
            let rustlib_dir = lib_dir.parent()?;
            let bin_dir = rustlib_dir.join("bin");
            if has_required_llvm_tools(&bin_dir) {
                Some(bin_dir)
            } else {
                None
            }
        });
    if from_rustup_stable.is_some() {
        return from_rustup_stable;
    }

    let system_bin = PathBuf::from("/usr/bin");
    if has_required_llvm_tools(&system_bin) {
        return Some(system_bin);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lcov_merges_line_hits_for_same_file() {
        let repo_root = Path::new("/repo");
        let lcov = "SF:src/lib.rs\nDA:10,0\nDA:11,1\nDA:10,3\nend_of_record\n";
        let fixture = std::env::temp_dir().join(format!(
            "coverage-focus-fixture-{}.info",
            std::process::id()
        ));
        fs::write(&fixture, lcov).unwrap();

        let rows = parse_lcov(&fixture, repo_root).unwrap();
        fs::remove_file(fixture).ok();

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.line_hits.get(&10), Some(&3));
        assert_eq!(row.line_hits.get(&11), Some(&1));
    }

    #[test]
    fn normalize_and_prefix_match_handle_directory_boundaries() {
        assert!(is_prefix_path("/repo/src", "/repo/src/lib.rs"));
        assert!(is_prefix_path("/repo/src", "/repo/src"));
        assert!(!is_prefix_path("/repo/src", "/repo/src2/lib.rs"));
    }

    #[test]
    fn focus_target_file_and_directory() {
        let rows = vec![
            FileCoverage {
                abs_key: "/repo/src/a.rs".to_string(),
                display_path: "src/a.rs".to_string(),
                line_hits: BTreeMap::from([(1, 0), (2, 1)]),
            },
            FileCoverage {
                abs_key: "/repo/src/b.rs".to_string(),
                display_path: "src/b.rs".to_string(),
                line_hits: BTreeMap::from([(10, 1)]),
            },
        ];

        let file_target = FocusTarget {
            raw: "src/a.rs".to_string(),
            abs_key: "/repo/src/a.rs".to_string(),
            display: "src/a.rs".to_string(),
            is_dir: false,
        };
        let file_report = focus_target(&rows, &file_target);
        assert_eq!(file_report.files.len(), 1);
        assert_eq!(file_report.totals.lines_found, 2);
        assert_eq!(file_report.totals.lines_hit, 1);

        let dir_target = FocusTarget {
            raw: "src".to_string(),
            abs_key: "/repo/src".to_string(),
            display: "src".to_string(),
            is_dir: true,
        };
        let dir_report = focus_target(&rows, &dir_target);
        assert_eq!(dir_report.files.len(), 2);
        assert_eq!(dir_report.totals.lines_found, 3);
        assert_eq!(dir_report.totals.lines_hit, 2);
    }

    #[test]
    fn parse_focus_targets_allows_multiple_paths_and_rejects_duplicates() {
        let fixture =
            std::env::temp_dir().join(format!("coverage-focus-targets-{}", std::process::id()));
        remove_path_if_exists(&fixture).unwrap();
        fs::create_dir_all(fixture.join("src")).unwrap();
        fs::write(fixture.join("src/a.rs"), "").unwrap();

        let targets =
            parse_focus_targets(vec!["src".to_string(), "src/a.rs".to_string()], &fixture).unwrap();
        assert_eq!(targets.len(), 2);
        assert!(targets[0].is_dir);
        assert!(!targets[1].is_dir);

        let duplicate = parse_focus_targets(
            vec!["src/a.rs".to_string(), "./src/a.rs".to_string()],
            &fixture,
        )
        .unwrap_err();
        assert!(duplicate.contains("duplicate target path"));

        fs::remove_dir_all(fixture).ok();
    }

    #[test]
    fn focus_targets_deduplicates_overlapping_rollup() {
        let rows = vec![
            FileCoverage {
                abs_key: "/repo/src/a.rs".to_string(),
                display_path: "src/a.rs".to_string(),
                line_hits: BTreeMap::from([(1, 0), (2, 1)]),
            },
            FileCoverage {
                abs_key: "/repo/src/b.rs".to_string(),
                display_path: "src/b.rs".to_string(),
                line_hits: BTreeMap::from([(10, 1)]),
            },
        ];
        let args = Args {
            targets: vec![
                FocusTarget {
                    raw: "src".to_string(),
                    abs_key: "/repo/src".to_string(),
                    display: "src".to_string(),
                    is_dir: true,
                },
                FocusTarget {
                    raw: "src/a.rs".to_string(),
                    abs_key: "/repo/src/a.rs".to_string(),
                    display: "src/a.rs".to_string(),
                    is_dir: false,
                },
            ],
            write_path: None,
            top_uncovered: 10,
            show_output: false,
        };

        let report = focus_targets(&rows, &args).unwrap();
        assert_eq!(report.targets.len(), 2);
        assert_eq!(report.unique_file_count, 2);
        assert_eq!(report.totals.lines_found, 3);
        assert_eq!(report.totals.lines_hit, 2);

        let rendered = render_console_report(&args, &report);
        assert!(rendered.contains("- Requested path args: 2"));
        assert!(rendered.contains("- Deduplicated total line coverage: 2/3 (66.67%)"));
        assert!(rendered.contains("## Target: `src`"));
        assert!(rendered.contains("## Target: `src/a.rs`"));
    }

    #[test]
    fn focus_targets_reports_all_unmatched_targets() {
        let rows = vec![FileCoverage {
            abs_key: "/repo/src/a.rs".to_string(),
            display_path: "src/a.rs".to_string(),
            line_hits: BTreeMap::from([(1, 1)]),
        }];
        let args = Args {
            targets: vec![
                FocusTarget {
                    raw: "src/missing.rs".to_string(),
                    abs_key: "/repo/src/missing.rs".to_string(),
                    display: "src/missing.rs".to_string(),
                    is_dir: false,
                },
                FocusTarget {
                    raw: "tests".to_string(),
                    abs_key: "/repo/tests".to_string(),
                    display: "tests".to_string(),
                    is_dir: true,
                },
            ],
            write_path: None,
            top_uncovered: 10,
            show_output: false,
        };

        let err = focus_targets(&rows, &args).unwrap_err();
        assert!(err.contains("`src/missing.rs`"));
        assert!(err.contains("`tests`"));
    }

    #[test]
    fn single_target_report_keeps_existing_summary_shape() {
        let rows = vec![FileCoverage {
            abs_key: "/repo/src/a.rs".to_string(),
            display_path: "src/a.rs".to_string(),
            line_hits: BTreeMap::from([(1, 0), (2, 1)]),
        }];
        let args = Args {
            targets: vec![FocusTarget {
                raw: "src/a.rs".to_string(),
                abs_key: "/repo/src/a.rs".to_string(),
                display: "src/a.rs".to_string(),
                is_dir: false,
            }],
            write_path: None,
            top_uncovered: 10,
            show_output: false,
        };

        let report = focus_targets(&rows, &args).unwrap();
        let rendered = render_console_report(&args, &report);
        assert!(rendered.contains("- Requested path arg: `src/a.rs`"));
        assert!(rendered.contains("- Target: `src/a.rs`"));
        assert!(rendered.contains("- Total line coverage: 1/2 (50.00%)"));
        assert!(!rendered.contains("Deduplicated total line coverage"));
    }

    #[test]
    fn coverage_nextest_args_disable_default_filename_exclusions() {
        let args = coverage_nextest_args();
        assert!(args.contains(&"--no-default-ignore-filename-regex"));
        assert!(args.contains(&"--ignore-filename-regex"));
        assert!(args.contains(&COVERAGE_IGNORE_FILENAME_REGEX));
    }

    #[test]
    fn coverage_nextest_args_preserve_warm_build_cache() {
        let args = coverage_nextest_args();
        assert!(args.contains(&"--no-clean"));
        assert!(args.contains(&"--lcov"));
        assert!(args.contains(&LCOV_OUT));
        assert!(args.contains(&"--profile"));
        assert!(args.contains(&"ci"));
    }

    #[test]
    fn has_required_llvm_tools_requires_both_binaries() {
        let fixture =
            std::env::temp_dir().join(format!("coverage-focus-llvm-tools-{}", std::process::id()));
        remove_path_if_exists(&fixture).unwrap();
        fs::create_dir_all(&fixture).unwrap();

        fs::write(fixture.join("llvm-cov"), "").unwrap();
        assert!(!has_required_llvm_tools(&fixture));

        fs::write(fixture.join("llvm-profdata"), "").unwrap();
        assert!(has_required_llvm_tools(&fixture));

        fs::remove_dir_all(fixture).ok();
    }
}
