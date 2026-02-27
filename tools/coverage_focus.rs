#!/usr/bin/env -S cargo +nightly -Zscript
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
const PROFILE_ROOT: &str = "target/coverage-focus/profiles";
const PROFILE_DEFAULT_DIR: &str = "target/coverage-focus/profiles/default";
const PROFILE_NO_DEFAULT_DIR: &str = "target/coverage-focus/profiles/no-default";
const TARGET_DEFAULT_DIR: &str = "target/coverage-focus/cargo-default";
const TARGET_NO_DEFAULT_DIR: &str = "target/coverage-focus/cargo-no-default";
const LCOV_OUT: &str = "target/coverage-focus/lcov.merged.info";

#[derive(Debug)]
struct Args {
    target_raw: String,
    target_abs: PathBuf,
    target_display: String,
    target_is_dir: bool,
    write_path: Option<PathBuf>,
    top_uncovered: usize,
    show_output: bool,
}

#[derive(Debug, Clone, Default)]
struct FileCoverage {
    abs_key: String,
    display_path: String,
    line_hits: BTreeMap<u32, u64>,
}

#[derive(Debug, Clone)]
struct FileReport {
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

fn usage() -> &'static str {
    "Usage: cargo +nightly -Zscript tools/coverage_focus.rs --path <repo-path> [--write <markdown-path>] [--top-uncovered <n>] [--verbose]\n\
\n\
Prerequisites:\n\
- `grcov` available in PATH (`cargo install grcov`)\n\
- LLVM tools with `llvm-profdata` (for example `rustup component add llvm-tools`)\n\
- Optional `COVERAGE_FOCUS_LLVM_PATH` can point to the LLVM tools directory\n\
- Default-feature test phase may require `libaio1` and `libaio-dev` in Linux environments\n\
\n\
Output:\n\
- By default, command stdout/stderr is hidden and only step descriptions are printed\n\
- Use `--verbose` to stream full build/test/grcov output"
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
    precheck_grcov()?;
    let llvm_tools_dir = precheck_llvm_tools(&repo_root)?;

    prepare_coverage_dirs()?;
    run_coverage_phases(&repo_root, args.show_output)?;
    generate_merged_lcov(&repo_root, &llvm_tools_dir, args.show_output)?;

    let lcov_rows = parse_lcov(Path::new(LCOV_OUT), &repo_root)?;
    let matched = focus_target(&lcov_rows, &args, &repo_root)?;

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
    let mut target_raw: Option<String> = None;
    let mut write_path: Option<PathBuf> = None;
    let mut top_uncovered: usize = 10;
    let mut show_output = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                let Some(v) = args.next() else {
                    return Err(format!("missing value for --path\n{}", usage()));
                };
                target_raw = Some(v);
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

    let target_raw = target_raw.ok_or_else(|| format!("missing required --path\n{}", usage()))?;
    let target_path = PathBuf::from(&target_raw);
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

    let target_display = rel_display(&target_abs, repo_root);
    let target_is_dir = target_abs.is_dir();

    Ok(Args {
        target_raw,
        target_abs,
        target_display,
        target_is_dir,
        write_path,
        top_uncovered,
        show_output,
    })
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

fn precheck_grcov() -> Result<(), String> {
    let status = Command::new("grcov")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| "`grcov` not found in PATH. Install with: cargo install grcov".to_string())?;
    if !status.success() {
        return Err("`grcov --version` failed. Ensure grcov is correctly installed.".to_string());
    }
    Ok(())
}

fn precheck_llvm_tools(repo_root: &Path) -> Result<PathBuf, String> {
    let llvm_dir = detect_llvm_tools_dir(repo_root).ok_or_else(|| {
        "llvm tools not found. Install with `rustup component add llvm-tools` or set `COVERAGE_FOCUS_LLVM_PATH` to a directory containing `llvm-profdata`.".to_string()
    })?;
    let status = Command::new(llvm_dir.join("llvm-profdata"))
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| {
            format!(
                "failed to execute `{}`: {e}",
                llvm_dir.join("llvm-profdata").display()
            )
        })?;
    if !status.success() {
        return Err(format!(
            "`{} --version` failed. Ensure LLVM tools are correctly installed.",
            llvm_dir.join("llvm-profdata").display()
        ));
    }
    Ok(llvm_dir)
}

fn prepare_coverage_dirs() -> Result<(), String> {
    remove_path_if_exists(Path::new(COVERAGE_WORK_DIR))?;
    fs::create_dir_all(PROFILE_DEFAULT_DIR)
        .map_err(|e| format!("failed to create {PROFILE_DEFAULT_DIR}: {e}"))?;
    fs::create_dir_all(PROFILE_NO_DEFAULT_DIR)
        .map_err(|e| format!("failed to create {PROFILE_NO_DEFAULT_DIR}: {e}"))?;
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

fn run_coverage_phases(repo_root: &Path, show_output: bool) -> Result<(), String> {
    println!("== coverage phase: default features ==");
    let default_profile = repo_root
        .join(PROFILE_DEFAULT_DIR)
        .join("coverage-%p-%m.profraw");
    let default_target_dir = repo_root.join(TARGET_DEFAULT_DIR);
    let default_env = vec![
        (
            "RUSTFLAGS",
            "-Cinstrument-coverage -Ctarget-cpu=native".to_string(),
        ),
        (
            "LLVM_PROFILE_FILE",
            default_profile.to_string_lossy().to_string(),
        ),
        (
            "CARGO_TARGET_DIR",
            default_target_dir.to_string_lossy().to_string(),
        ),
        ("RUST_BACKTRACE", "full".to_string()),
    ];
    run_checked(
        repo_root,
        "cargo",
        &["test", "--all"],
        &default_env,
        show_output,
    )
    .map_err(|msg| format_default_phase_error(msg))?;

    println!("== coverage phase: no default features ==");
    let no_default_profile = repo_root
        .join(PROFILE_NO_DEFAULT_DIR)
        .join("coverage-%p-%m.profraw");
    let no_default_target_dir = repo_root.join(TARGET_NO_DEFAULT_DIR);
    let no_default_env = vec![
        (
            "RUSTFLAGS",
            "-Cinstrument-coverage -Ctarget-cpu=native".to_string(),
        ),
        (
            "LLVM_PROFILE_FILE",
            no_default_profile.to_string_lossy().to_string(),
        ),
        (
            "CARGO_TARGET_DIR",
            no_default_target_dir.to_string_lossy().to_string(),
        ),
        ("RUST_BACKTRACE", "full".to_string()),
    ];
    run_checked(
        repo_root,
        "cargo",
        &["test", "--all", "--no-default-features"],
        &no_default_env,
        show_output,
    )?;

    Ok(())
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

fn run_checked_owned(
    repo_root: &Path,
    program: &str,
    args: &[String],
    envs: &[(&str, &str)],
    show_output: bool,
) -> Result<(), String> {
    if show_output {
        println!("$ {} {}", program, args.join(" "));
    }
    let mut cmd = Command::new(program);
    cmd.args(args)
        .current_dir(repo_root)
        .envs(envs.iter().copied());
    if !show_output {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to start `{program}`: {e}"))?;
    let arg_slices = args.iter().map(String::as_str).collect::<Vec<_>>();
    ensure_success(program, &arg_slices, status, show_output)
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

fn format_default_phase_error(base: String) -> String {
    let mut out = String::new();
    out.push_str(&base);
    out.push_str("\n");
    out.push_str("default-feature coverage phase failed. If this environment is missing libaio, install `libaio1` and `libaio-dev` (Ubuntu: `sudo apt-get install -y libaio1 libaio-dev`) and rerun.");
    out
}

fn generate_merged_lcov(
    repo_root: &Path,
    llvm_tools_dir: &Path,
    show_output: bool,
) -> Result<(), String> {
    println!("== merge coverage with grcov ==");
    let mut args = vec![
        PROFILE_ROOT.to_string(),
        "--binary-path".to_string(),
        COVERAGE_WORK_DIR.to_string(),
        "-s".to_string(),
        ".".to_string(),
        "-t".to_string(),
        "lcov".to_string(),
        "--branch".to_string(),
        "--ignore-not-existing".to_string(),
        "--ignore".to_string(),
        "target/".to_string(),
        "--ignore".to_string(),
        "legacy/**".to_string(),
        "--ignore".to_string(),
        "**/legacy/**".to_string(),
    ];
    args.push("--llvm-path".to_string());
    args.push(llvm_tools_dir.to_string_lossy().to_string());
    args.push("-o".to_string());
    args.push(LCOV_OUT.to_string());
    run_checked_owned(repo_root, "grcov", &args, &[], show_output)?;

    if !Path::new(LCOV_OUT).exists() {
        return Err(format!("grcov finished but output not found: {LCOV_OUT}"));
    }
    let lcov = fs::read_to_string(LCOV_OUT)
        .map_err(|e| format!("failed to read merged lcov output {LCOV_OUT}: {e}"))?;
    if !lcov.lines().any(|line| line.starts_with("SF:")) {
        return Err("merged LCOV is empty. Check `grcov` logs for `llvm-profdata`/version mismatch and ensure llvm tools are available.".to_string());
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

fn focus_target(
    rows: &[FileCoverage],
    args: &Args,
    repo_root: &Path,
) -> Result<MatchResult, String> {
    let target_key = normalize_abs_key(&args.target_abs, repo_root);
    let mut out: Vec<FileReport> = Vec::new();

    for row in rows {
        let is_match = if args.target_is_dir {
            is_prefix_path(&target_key, &row.abs_key)
        } else {
            row.abs_key == target_key
        };
        if !is_match {
            continue;
        }

        let mut uncovered = Vec::new();
        let mut lines_hit = 0usize;
        for (line_no, hits) in &row.line_hits {
            if *hits > 0 {
                lines_hit += 1;
            } else {
                uncovered.push(*line_no);
            }
        }

        out.push(FileReport {
            path: row.display_path.clone(),
            lines_found: row.line_hits.len(),
            lines_hit,
            uncovered_lines: uncovered,
        });
    }

    if out.is_empty() {
        return Err(format!(
            "no LCOV entries matched target `{}`. Confirm the path is part of instrumented Rust sources.",
            args.target_display
        ));
    }

    out.sort_by(|a, b| {
        let ac = coverage_pct(a.lines_hit, a.lines_found);
        let bc = coverage_pct(b.lines_hit, b.lines_found);
        ac.partial_cmp(&bc)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.lines_found.cmp(&a.lines_found))
            .then_with(|| a.path.cmp(&b.path))
    });

    let totals = CoverageTotals {
        lines_found: out.iter().map(|r| r.lines_found).sum(),
        lines_hit: out.iter().map(|r| r.lines_hit).sum(),
    };

    Ok(MatchResult { files: out, totals })
}

fn render_console_report(args: &Args, result: &MatchResult) -> String {
    let mut out = String::new();
    let summary_pct = coverage_pct(result.totals.lines_hit, result.totals.lines_found);

    out.push_str("# Coverage Focus Report\n\n");
    out.push_str(&format!("- Requested path arg: `{}`\n", args.target_raw));
    out.push_str(&format!("- Target: `{}`\n", args.target_display));
    out.push_str(&format!(
        "- Target kind: {}\n",
        if args.target_is_dir {
            "directory"
        } else {
            "file"
        }
    ));
    out.push_str(&format!("- Matched files: {}\n", result.files.len()));
    out.push_str(&format!(
        "- Total line coverage: {}/{} ({summary_pct:.2}%)\n\n",
        result.totals.lines_hit, result.totals.lines_found
    ));

    out.push_str("## Per-file line coverage\n\n");
    out.push_str("| file | lines hit | lines found | coverage | uncovered |\n");
    out.push_str("|---|---:|---:|---:|---:|\n");
    for row in &result.files {
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

    out.push_str("\n## Uncovered-line hotspots\n\n");
    let mut hotspots: Vec<&FileReport> = result
        .files
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
        return out;
    }

    for row in hotspots.into_iter().take(args.top_uncovered) {
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

    out
}

fn render_markdown_report(args: &Args, result: &MatchResult) -> String {
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

fn detect_llvm_tools_dir(repo_root: &Path) -> Option<PathBuf> {
    if let Ok(value) = env::var("COVERAGE_FOCUS_LLVM_PATH") {
        let candidate = PathBuf::from(value);
        if candidate.join("llvm-profdata").is_file() {
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
            if bin_dir.join("llvm-profdata").is_file() {
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
            if bin_dir.join("llvm-profdata").is_file() {
                Some(bin_dir)
            } else {
                None
            }
        });
    if from_rustup_stable.is_some() {
        return from_rustup_stable;
    }

    let system_bin = PathBuf::from("/usr/bin");
    if system_bin.join("llvm-profdata").is_file() {
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
        let repo = Path::new("/repo");
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

        let file_args = Args {
            target_raw: "src/a.rs".to_string(),
            target_abs: PathBuf::from("/repo/src/a.rs"),
            target_display: "src/a.rs".to_string(),
            target_is_dir: false,
            write_path: None,
            top_uncovered: 10,
        };
        let file_report = focus_target(&rows, &file_args, repo).unwrap();
        assert_eq!(file_report.files.len(), 1);
        assert_eq!(file_report.totals.lines_found, 2);
        assert_eq!(file_report.totals.lines_hit, 1);

        let dir_args = Args {
            target_raw: "src".to_string(),
            target_abs: PathBuf::from("/repo/src"),
            target_display: "src".to_string(),
            target_is_dir: true,
            write_path: None,
            top_uncovered: 10,
        };
        let dir_report = focus_target(&rows, &dir_args, repo).unwrap();
        assert_eq!(dir_report.files.len(), 2);
        assert_eq!(dir_report.totals.lines_found, 3);
        assert_eq!(dir_report.totals.lines_hit, 2);
    }
}
