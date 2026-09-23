mod lcov;
mod model;
mod report;
mod runner;
mod source;

use model::{CoverageReport, Manifest, Result, SCHEMA, digest};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const HELP: &str = "Usage: tools/coverage.rs <run|report> [options]

run     Collect fresh stable default-feature iouring workspace coverage.
report  Validate and render completed artifacts without running Cargo or tests.

  --output-dir <dir>    Run artifacts (default: target/coverage)
  --input <dir>         Report input (default: target/coverage)
  --path <file|dir>     Limit presentation; repeat for multiple targets
  --write <path>        Write the same report as Markdown
  --top-uncovered <n>   Number of uncovered-file hotspots (default: 10)
  --verbose            Show command output (--show-output is an alias)
  --help               Show this help

Prerequisites: nightly-2026-05-22 script interpreter, stable Rust and matching
llvm-tools, cargo-llvm-cov, cargo-nextest. Run from the repository root.
Canonical artifacts: lcov.info and coverage.json. raw.lcov is diagnostic only.
Output directories must be outside snapshotted source/config trees or inside
an excluded subtree such as target/.";

#[derive(Debug)]
struct Args {
    run: bool,
    directory: PathBuf,
    paths: Vec<String>,
    write: Option<PathBuf>,
    top: usize,
    verbose: bool,
}

fn parse(args: impl Iterator<Item = String>) -> Result<Option<Args>> {
    let mut values = args.peekable();
    let command = values.next().ok_or(HELP)?;
    if matches!(command.as_str(), "--help" | "-h") {
        return Ok(None);
    }
    let run = match command.as_str() {
        "run" => true,
        "report" => false,
        _ => return Err(format!("unknown command {command}\n{HELP}")),
    };
    let mut args = Args {
        run,
        directory: "target/coverage".into(),
        paths: Vec::new(),
        write: None,
        top: 10,
        verbose: false,
    };
    while let Some(option) = values.next() {
        if matches!(option.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        if matches!(option.as_str(), "--verbose" | "--show-output") {
            args.verbose = true;
            continue;
        }
        if !matches!(
            option.as_str(),
            "--path" | "--write" | "--top-uncovered" | "--output-dir" | "--input"
        ) {
            return Err(format!("unknown option {option}"));
        }
        let value = values
            .next()
            .filter(|v| !v.starts_with("--"))
            .ok_or_else(|| format!("missing value for {option}"))?;
        match option.as_str() {
            "--path" => args.paths.push(value),
            "--write" => args.write = Some(value.into()),
            "--top-uncovered" => {
                args.top = value
                    .parse()
                    .map_err(|_| "--top-uncovered requires a nonnegative integer")?
            }
            "--output-dir" if run => args.directory = value.into(),
            "--input" if !run => args.directory = value.into(),
            _ => return Err(format!("{option} is not supported for {command}")),
        }
    }
    Ok(Some(args))
}

pub(super) fn main(values: impl Iterator<Item = String>) -> Result<()> {
    let Some(args) = parse(values)? else {
        println!("{HELP}");
        return Ok(());
    };
    let root = env::current_dir()
        .map_err(|e| e.to_string())?
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let output = if args.run {
        runner::output_directory(&root, &args.directory)?
    } else {
        root.join(&args.directory)
    };
    let _lock = runner::OutputLock::acquire(&output, args.run)?;
    let output = output.canonicalize().map_err(|e| e.to_string())?;
    let (manifest, report) = if args.run {
        match run(&root, &output, &args) {
            Ok(result) => result,
            Err(error) => {
                // Keep raw/log diagnostics, but never expose a failed run as upload input.
                for name in ["coverage.json", "lcov.info"] {
                    let _ = fs::remove_file(output.join(name));
                }
                return Err(error);
            }
        }
    } else {
        load(&root, &output)?
    };
    let targets = report::selections(&root, &args.paths, &manifest.sources)?;
    let rendered = report::render(&report, &targets, args.top);
    print!("{rendered}");
    if let Some(path) = args.write {
        let absolute = model::normalize(&root, &path)?;
        let destination = if absolute.exists() {
            absolute.canonicalize().map_err(|e| e.to_string())?
        } else {
            absolute
        };
        if ["coverage.json", "lcov.info", "raw.lcov", ".coverage.lock"]
            .iter()
            .any(|name| destination == output.join(name))
            || destination.strip_prefix(&root).ok().is_some_and(|p| {
                manifest
                    .context
                    .inputs
                    .contains_key(&p.to_string_lossy().replace('\\', "/"))
            })
        {
            return Err(
                "--write must not overwrite coverage artifacts or snapshotted build inputs".into(),
            );
        }
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        fs::write(&path, rendered).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(())
}

fn run(root: &Path, output: &Path, args: &Args) -> Result<(Manifest, CoverageReport)> {
    // Invalidate first: even a preflight failure must not leave stale upload input.
    runner::invalidate(output)?;
    let initial = runner::fingerprints(&runner::snapshot(root)?);
    let mut preflight = runner::preflight(root)?;
    let before = runner::snapshot(root)?;
    let mut after_preflight = runner::fingerprints(&before);
    // Cargo metadata may create the ignored lockfile on a fresh checkout.
    if !initial.contains_key("Cargo.lock") {
        after_preflight.remove("Cargo.lock");
    }
    if initial != after_preflight {
        return Err(
            "source/build inputs changed during preflight; regenerate with tools/coverage.rs run"
                .into(),
        );
    }
    preflight.context.inputs = runner::fingerprints(&before);
    let sources = source::index(root, &before, preflight.roots)?;
    report::selections(root, &args.paths, &sources)?;
    runner::check_inputs(root, &preflight.context.inputs)?;
    let raw = runner::collect_coverage(root, output, &preflight.llvm, args.verbose)?;
    let parsed = lcov::parse(&raw, root, &sources)?;
    let (report, files, counts) = lcov::filter(&parsed, &sources)?;
    runner::check_inputs(root, &preflight.context.inputs)?;
    let canonical = lcov::serialize(&report);
    let manifest = Manifest {
        schema: SCHEMA,
        complete: true,
        collection_root: root.to_string_lossy().into(),
        context: preflight.context,
        raw_digest: digest(&raw),
        report_digest: digest(&canonical),
        sources,
        files,
        counts,
    };
    runner::atomic_write(output, "raw.lcov", raw.as_bytes())?;
    runner::atomic_write(output, "lcov.info", canonical.as_bytes())?;
    runner::atomic_write(
        output,
        "coverage.json",
        &serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?,
    )?;
    eprintln!(
        "coverage: raw {:?}; removed {:?}; production {:?}",
        manifest.counts.raw, manifest.counts.removed, manifest.counts.retained
    );
    Ok((manifest, report))
}

fn load(root: &Path, output: &Path) -> Result<(Manifest, CoverageReport)> {
    let result = || -> Result<(Manifest, CoverageReport)> {
        let manifest: Manifest = serde_json::from_slice(
            &fs::read(output.join("coverage.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if manifest.schema != SCHEMA || !manifest.complete {
            return Err("incompatible or incomplete manifest".into());
        }
        runner::check_inputs(root, &manifest.context.inputs)?;
        for (path, policy) in &manifest.sources {
            if manifest.context.inputs.get(path) != Some(&policy.digest) {
                return Err(format!("source policy digest mismatch: {path}"));
            }
        }
        let raw = fs::read_to_string(output.join("raw.lcov")).map_err(|e| e.to_string())?;
        let canonical = fs::read_to_string(output.join("lcov.info")).map_err(|e| e.to_string())?;
        if digest(&raw) != manifest.raw_digest || digest(&canonical) != manifest.report_digest {
            return Err("report digest mismatch".into());
        }
        let parsed = lcov::parse(
            &raw,
            Path::new(&manifest.collection_root),
            &manifest.sources,
        )?;
        let (report, files, counts) = lcov::filter(&parsed, &manifest.sources)?;
        if files != manifest.files
            || counts != manifest.counts
            || lcov::serialize(&report) != canonical
        {
            return Err("manifest/canonical derived totals mismatch".into());
        }
        Ok((manifest, report))
    };
    result().map_err(|e| {
        format!("invalid coverage artifact: {e}; regenerate with tools/coverage.rs run")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_fixture(root: &Path) -> PathBuf {
        use super::model::RunContext;
        use std::collections::BTreeMap;
        let source_path = "doradb-storage/src/lib.rs";
        fs::create_dir_all(root.join("doradb-storage/src")).unwrap();
        let text = "pub fn unused() {}\n#[test]\nfn test() {}\n";
        fs::write(root.join(source_path), text).unwrap();
        let roots = vec![source::Root {
            path: root.join(source_path),
            owner: "fixture:lib".into(),
            scope: None,
            configuration: source::Configuration::default(),
        }];
        let inputs = runner::snapshot(root).unwrap();
        let sources = source::index(root, &inputs, roots).unwrap();
        let raw = format!(
            "SF:{}\nDA:1,0\nDA:3,2\nLF:2\nLH:1\nend_of_record\n",
            root.join(source_path).display()
        );
        let (report, files, counts) =
            lcov::filter(&lcov::parse(&raw, root, &sources).unwrap(), &sources).unwrap();
        let canonical = lcov::serialize(&report);
        let context = RunContext {
            revision: "fixture".into(),
            versions: BTreeMap::new(),
            target: "fixture".into(),
            target_cfg: Default::default(),
            features: BTreeMap::new(),
            build_profile: "test".into(),
            nextest_profile: "ci".into(),
            inputs: runner::fingerprints(&inputs),
        };
        let manifest = Manifest {
            schema: SCHEMA,
            complete: true,
            collection_root: root.to_string_lossy().into(),
            context,
            raw_digest: digest(&raw),
            report_digest: digest(&canonical),
            sources,
            files,
            counts,
        };
        let output = root.join("target/coverage");
        fs::create_dir_all(&output).unwrap();
        runner::atomic_write(&output, "raw.lcov", raw.as_bytes()).unwrap();
        runner::atomic_write(&output, "lcov.info", canonical.as_bytes()).unwrap();
        runner::atomic_write(
            &output,
            "coverage.json",
            &serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        output
    }

    /// Purpose: Preserve the unified CLI and prevent report-only requests from selecting run options.
    /// Expected: Repeated paths, output aliases and defaults parse correctly; invalid command options fail.
    #[test]
    fn command_contract() {
        let parse_args = |args: &[&str]| parse(args.iter().map(|v| v.to_string()));
        let args = parse_args(&[
            "report",
            "--path",
            "src",
            "--path",
            "src/a.rs",
            "--show-output",
        ])
        .unwrap()
        .unwrap();
        assert!(!args.run);
        assert_eq!(args.paths, ["src", "src/a.rs"]);
        assert!(args.verbose);
        assert_eq!(args.top, 10);
        assert!(parse_args(&["report", "--output-dir", "x"]).is_err());
        assert!(parse_args(&["run", "--input", "x"]).is_err());
        assert!(parse_args(&["run", "--path"]).is_err());
        assert!(parse_args(&["run", "--help"]).unwrap().is_none());
    }

    /// Purpose: Validate completed reports before reusing coverage across checkout locations.
    /// Expected: Relocation preserves exact counts and corrupted reports, schema, totals or source fail.
    #[test]
    fn artifact_validation_and_portability() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let output = artifact_fixture(first.path());
        let moved = artifact_fixture(second.path());
        for name in ["raw.lcov", "lcov.info", "coverage.json"] {
            fs::copy(output.join(name), moved.join(name)).unwrap();
        }
        let (manifest, report) = load(second.path(), &moved).unwrap();
        assert_eq!(
            manifest.counts.retained,
            model::Totals {
                covered: 0,
                uncovered: 1
            }
        );
        assert_eq!(report["doradb-storage/src/lib.rs"].lines[&1], 0);
        let original = fs::read(moved.join("coverage.json")).unwrap();
        for (field, value) in [
            ("schema", serde_json::json!(99)),
            ("complete", serde_json::json!(false)),
            (
                "counts",
                serde_json::json!({"raw": {"covered": 0, "uncovered": 0}, "removed": {"covered": 0, "uncovered": 0}, "retained": {"covered": 0, "uncovered": 0}}),
            ),
        ] {
            let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
            changed[field] = value;
            fs::write(
                moved.join("coverage.json"),
                serde_json::to_vec(&changed).unwrap(),
            )
            .unwrap();
            assert!(
                load(second.path(), &moved)
                    .unwrap_err()
                    .contains("regenerate"),
                "{field}"
            );
        }
        fs::write(moved.join("coverage.json"), original).unwrap();
        fs::write(moved.join("lcov.info"), "tampered").unwrap();
        assert!(
            load(second.path(), &moved)
                .unwrap_err()
                .contains("digest mismatch")
        );
        fs::copy(output.join("lcov.info"), moved.join("lcov.info")).unwrap();
        fs::write(second.path().join("doradb-storage/src/lib.rs"), "changed").unwrap();
        assert!(
            load(second.path(), &moved)
                .unwrap_err()
                .contains("inputs changed")
        );
    }
}
