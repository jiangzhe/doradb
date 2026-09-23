use super::model::{Result, RunContext, digest, repo_path};
use super::source::{Configuration, Root};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

const SNAPSHOT_ROOTS: [&str; 4] = ["doradb-storage", "doradb-bench", "tools/coverage", ".cargo"];
const SNAPSHOT_SKIPPED_DIRECTORIES: [&str; 2] = ["target", ".git"];

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    workspace_members: Vec<String>,
    resolve: Resolve,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    manifest_path: PathBuf,
    features: BTreeMap<String, serde_json::Value>,
    targets: Vec<Target>,
}

#[derive(Deserialize)]
struct Target {
    name: String,
    kind: Vec<String>,
    src_path: PathBuf,
    #[serde(default, rename = "required-features")]
    required_features: Vec<String>,
}

#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<Resolved>,
}

#[derive(Deserialize)]
struct Resolved {
    id: String,
    features: BTreeSet<String>,
}

pub(super) struct Preflight {
    pub(super) context: RunContext,
    pub(super) roots: Vec<Root>,
    pub(super) llvm: PathBuf,
}

pub(super) struct OutputLock {
    file: File,
}

impl OutputLock {
    pub(super) fn acquire(output: &Path, writer: bool) -> Result<Self> {
        fs::create_dir_all(output).map_err(|e| e.to_string())?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(output.join(".coverage.lock"))
            .map_err(|e| e.to_string())?;
        let lock = if writer {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        lock.map_err(|e| {
            format!(
                "coverage directory already in use: {} ({e})",
                output.display()
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for OutputLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(super) fn stable_cargo(root: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .arg("+stable")
        .current_dir(root)
        .env("RUSTUP_TOOLCHAIN", "stable");
    command
}

fn stable_rustc(root: &Path) -> Command {
    let mut command = Command::new("rustc");
    command
        .arg("+stable")
        .current_dir(root)
        .env("RUSTUP_TOOLCHAIN", "stable");
    command
}

fn capture(command: &mut Command) -> Result<String> {
    let output = command
        .output()
        .map_err(|e| format!("cannot start {command:?}: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "{command:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| e.to_string())
}

pub(super) fn preflight(root: &Path) -> Result<Preflight> {
    reject_overrides(root)?;
    let compiler = capture(stable_rustc(root).arg("-vV"))?;
    let target = compiler
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or("rustc did not report host target")?
        .to_string();
    let sysroot = capture(stable_rustc(root).args(["--print", "sysroot"]))?;
    let llvm = Path::new(sysroot.trim())
        .join("lib/rustlib")
        .join(&target)
        .join("bin");
    let mut versions = BTreeMap::from([("rustc".into(), compiler)]);
    for tool in ["llvm-cov", "llvm-profdata"] {
        let version = capture(Command::new(llvm.join(tool)).arg("--version")).map_err(|e| {
            format!(
                "{e}\nInstall matching tools: rustup component add llvm-tools --toolchain stable"
            )
        })?;
        versions.insert(tool.into(), version.trim().into());
    }
    for tool in ["llvm-cov", "nextest"] {
        versions.insert(
            format!("cargo-{tool}"),
            capture(stable_cargo(root).args([tool, "--version"]))?
                .trim()
                .into(),
        );
    }
    versions.insert(
        "cargo".into(),
        capture(stable_cargo(root).arg("--version"))?.trim().into(),
    );
    let mut predicates: BTreeSet<_> = capture(stable_rustc(root).args(["--print", "cfg"]))?
        .lines()
        .map(str::to_string)
        .collect();
    predicates.insert("coverage".into());
    let metadata: Metadata = serde_json::from_str(&capture(stable_cargo(root).args([
        "metadata",
        "--format-version",
        "1",
        "--filter-platform",
        &target,
    ]))?)
    .map_err(|e| e.to_string())?;
    let resolved: BTreeMap<_, _> = metadata
        .resolve
        .nodes
        .into_iter()
        .map(|n| (n.id, n.features))
        .collect();
    let mut roots = Vec::new();
    let mut features = BTreeMap::new();
    for package in metadata
        .packages
        .into_iter()
        .filter(|p| metadata.workspace_members.contains(&p.id))
    {
        let selected = resolved
            .get(&package.id)
            .ok_or("workspace package missing from metadata resolve")?
            .clone();
        if package.name == "doradb-storage"
            && (!selected.contains("iouring") || selected.contains("libaio"))
        {
            return Err("coverage requires the default iouring backend without libaio".into());
        }
        let manifest: toml::Value =
            toml::from_str(&fs::read_to_string(&package.manifest_path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        if manifest
            .get("profile")
            .is_some_and(|p| p.get("dev").is_some() || p.get("test").is_some())
        {
            return Err("unmodeled dev/test profile overrides in workspace manifest".into());
        }
        features.insert(package.name.clone(), selected.clone());
        for target in package.targets {
            if target.kind.iter().any(|k| k == "custom-build") {
                return Err("workspace build scripts require explicit cfg/input modeling".into());
            }
            if !target
                .required_features
                .iter()
                .all(|f| selected.contains(f))
            {
                continue;
            }
            let scope = if target.kind.iter().any(|k| {
                matches!(
                    k.as_str(),
                    "lib" | "bin" | "rlib" | "staticlib" | "cdylib" | "dylib"
                )
            }) {
                None
            } else if target.kind.iter().any(|k| k == "test") {
                Some("integration-test target".into())
            } else {
                Some(format!("out of scope: {} target", target.kind.join("/")))
            };
            roots.push(Root {
                path: target.src_path,
                owner: format!("{}:{}:{}", package.name, target.kind.join("/"), target.name),
                scope,
                configuration: Configuration {
                    predicates: predicates.clone(),
                    features: selected.clone(),
                    declared_features: package.features.keys().cloned().collect(),
                },
            });
        }
    }
    if !features.contains_key("doradb-storage") || !features.contains_key("doradb-bench") {
        return Err("run coverage from the Doradb repository root".into());
    }
    let revision = capture(
        Command::new("git")
            .current_dir(root)
            .args(["rev-parse", "HEAD"]),
    )?
    .trim()
    .into();
    Ok(Preflight {
        roots,
        llvm,
        context: RunContext {
            revision,
            versions,
            target,
            target_cfg: predicates,
            features,
            build_profile: "test (unoptimized, debug assertions)".into(),
            nextest_profile: "ci".into(),
            inputs: BTreeMap::new(),
        },
    })
}

fn reject_overrides(root: &Path) -> Result<()> {
    for (key, value) in env::vars() {
        if value.is_empty() {
            continue;
        }
        if matches!(
            key.as_str(),
            "RUSTFLAGS"
                | "CARGO_ENCODED_RUSTFLAGS"
                | "RUSTDOCFLAGS"
                | "CARGO_ENCODED_RUSTDOCFLAGS"
                | "CARGO_BUILD_TARGET"
                | "CARGO_BUILD_RUSTFLAGS"
                | "CARGO_BUILD_RUSTC"
                | "CARGO_BUILD_RUSTC_WRAPPER"
                | "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"
                | "RUSTC_BOOTSTRAP"
                | "RUSTC"
                | "RUSTC_WRAPPER"
                | "RUSTC_WORKSPACE_WRAPPER"
                | "RUSTDOC"
                | "LLVM_COV"
                | "LLVM_PROFDATA"
                | "LLVM_PROFILE_FILE"
        ) || key.starts_with("CARGO_PROFILE_")
            || key.starts_with("CARGO_LLVM_COV_")
            || (key.starts_with("CARGO_TARGET_") && key.ends_with("_RUSTFLAGS"))
        {
            return Err(format!(
                "unmodeled environment override {key}; unset it before coverage collection"
            ));
        }
    }
    let manifest: toml::Value =
        toml::from_str(&fs::read_to_string(root.join("Cargo.toml")).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    if manifest
        .get("profile")
        .is_some_and(|p| p.get("dev").is_some() || p.get("test").is_some())
    {
        return Err("unmodeled dev/test build profile override".into());
    }
    for path in cargo_configs(root) {
        let config: toml::Value =
            toml::from_str(&fs::read_to_string(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        for key in ["build", "target", "profile", "env", "unstable"] {
            if config.get(key).is_some() {
                return Err(format!(
                    "unmodeled Cargo configuration [{key}] in {}; remove the override or extend configuration modeling",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

fn cargo_configs(root: &Path) -> Vec<PathBuf> {
    let mut directories: Vec<_> = root.ancestors().map(|p| p.join(".cargo")).collect();
    if let Some(home) = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|h| PathBuf::from(h).join(".cargo")))
    {
        directories.push(home);
    }
    let mut paths: Vec<_> = directories
        .into_iter()
        .flat_map(|p| [p.join("config"), p.join("config.toml")])
        .filter(|p| p.is_file())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

/// Resolve a run destination without creating it or hiding snapshotted inputs.
pub(super) fn output_directory(root: &Path, requested: &Path) -> Result<PathBuf> {
    let output = resolve_directory(&root.join(requested))?;
    for directory in SNAPSHOT_ROOTS {
        let scanned = resolve_directory(&root.join(directory))?;
        if let Ok(relative) = output.strip_prefix(&scanned)
            && !relative.components().any(
                |part| matches!(part, Component::Normal(name) if skip_snapshot_directory(name)),
            )
        {
            return Err(format!(
                "--output-dir {} is inside snapshotted tree `{directory}`; use target/coverage or another location outside the scanned trees",
                output.display()
            ));
        }
    }
    Ok(output)
}

fn resolve_directory(path: &Path) -> Result<PathBuf> {
    let mut resolved = PathBuf::new();
    for part in path.components() {
        match part {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(name) => {
                resolved.push(name);
                match fs::symlink_metadata(&resolved) {
                    Ok(_) => {
                        // Resolve existing ancestors before applying '..'; a symlink can
                        // change which directory that component leaves. Missing suffixes
                        // stay lexical until an existing ancestor is reached again.
                        resolved = resolved.canonicalize().map_err(|e| {
                            format!("cannot resolve directory {}: {e}", resolved.display())
                        })?;
                        if !resolved.is_dir() {
                            return Err(format!("not a directory: {}", resolved.display()));
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(format!(
                            "cannot resolve directory {}: {error}",
                            resolved.display()
                        ));
                    }
                }
            }
            part => resolved.push(part.as_os_str()),
        }
    }
    Ok(resolved)
}

fn skip_snapshot_directory(name: &OsStr) -> bool {
    SNAPSHOT_SKIPPED_DIRECTORIES
        .iter()
        .any(|skipped| name == *skipped)
}

/// Capture contents, not mtimes; include absent optional inputs so additions invalidate reuse.
pub(super) fn snapshot(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut inputs = BTreeMap::new();
    for directory in SNAPSHOT_ROOTS {
        collect(root, &root.join(directory), &mut inputs)?;
    }
    for name in [
        "Cargo.toml",
        "Cargo.lock",
        "tools/coverage.rs",
        ".config/nextest.toml",
        ".codecov.yml",
        ".github/workflows/coverage.yml",
        "rust-toolchain",
        "rust-toolchain.toml",
        "build.rs",
    ] {
        if root.join(name).is_file() {
            inputs.insert(
                name.into(),
                fs::read_to_string(root.join(name)).map_err(|e| e.to_string())?,
            );
        }
    }
    Ok(inputs)
}

fn collect(root: &Path, directory: &Path, inputs: &mut BTreeMap<String, String>) -> Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if entry.file_type().map_err(|e| e.to_string())?.is_symlink() {
            return Err(format!(
                "source snapshot does not support symlinks: {}",
                path.display()
            ));
        }
        if path.is_dir() {
            if !path.file_name().is_some_and(skip_snapshot_directory) {
                collect(root, &path, inputs)?;
            }
        } else if path.extension().is_some_and(|e| e == "rs" || e == "toml")
            || path.file_name().is_some_and(|n| n == "config")
        {
            inputs.insert(
                repo_path(root, &path)?,
                fs::read_to_string(&path).map_err(|e| e.to_string())?,
            );
        }
    }
    Ok(())
}

pub(super) fn fingerprints(snapshot: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    snapshot
        .iter()
        .map(|(name, source)| (name.clone(), digest(source)))
        .collect()
}

pub(super) fn check_inputs(root: &Path, expected: &BTreeMap<String, String>) -> Result<()> {
    let actual = fingerprints(&snapshot(root)?);
    if &actual != expected {
        let changed: BTreeSet<_> = actual
            .keys()
            .chain(expected.keys())
            .filter(|k| actual.get(*k) != expected.get(*k))
            .collect();
        return Err(format!(
            "source/build inputs changed: {changed:?}; regenerate with tools/coverage.rs run"
        ));
    }
    Ok(())
}

pub(super) fn invalidate(output: &Path) -> Result<()> {
    for name in ["coverage.json", "lcov.info", "raw.lcov", "raw.pending.lcov"] {
        match fs::remove_file(output.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

pub(super) fn collect_coverage(
    root: &Path,
    output: &Path,
    llvm: &Path,
    verbose: bool,
) -> Result<String> {
    let work = output.join(".coverage-work");
    if work.is_symlink()
        || ["cargo", "llvm"]
            .iter()
            .any(|name| work.join(name).is_symlink())
    {
        return Err("coverage build directories must not be symlinks".into());
    }
    if work.exists()
        && fs::read_to_string(work.join("owner")).ok().as_deref()
            != Some("doradb coverage schema 1\n")
    {
        return Err(format!(
            "refusing to clean unowned coverage directory {}",
            work.display()
        ));
    }
    fs::create_dir_all(&work).map_err(|e| e.to_string())?;
    fs::write(work.join("owner"), "doradb coverage schema 1\n").map_err(|e| e.to_string())?;
    let environment = [
        ("CARGO_TARGET_DIR", work.join("cargo")),
        ("CARGO_LLVM_COV_TARGET_DIR", work.join("llvm")),
        ("LLVM_COV", llvm.join("llvm-cov")),
        ("LLVM_PROFDATA", llvm.join("llvm-profdata")),
    ];
    let raw = output.join("raw.pending.lcov");
    let commands = [
        ("clean", vec!["llvm-cov", "clean", "--workspace"]),
        (
            "nextest",
            vec![
                "llvm-cov",
                "nextest",
                "--no-report",
                "--workspace",
                "--profile",
                "ci",
                "--locked",
            ],
        ),
        (
            "export",
            vec![
                "llvm-cov",
                "report",
                "--lcov",
                "--output-path",
                raw.to_str().ok_or("non-UTF8 output path")?,
                "--no-default-ignore-filename-regex",
            ],
        ),
    ];
    for (name, args) in commands {
        eprintln!("coverage: {name} (stable, default iouring workspace)");
        let mut command = stable_cargo(root);
        command
            .args(args)
            .envs(environment.iter().map(|(k, p)| (*k, p)))
            .env("RUST_BACKTRACE", "full");
        logged(&mut command, &output.join(format!("{name}.log")), verbose)?;
    }
    fs::read_to_string(raw).map_err(|e| format!("missing raw LLVM export: {e}"))
}

fn logged(command: &mut Command, log: &Path, verbose: bool) -> Result<()> {
    let file = File::create(log).map_err(|e| e.to_string())?;
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{command:?}: {e}"))?;
    let stdout = child.stdout.take().ok_or("missing child stdout")?;
    let stderr = child.stderr.take().ok_or("missing child stderr")?;
    let log = log.to_path_buf();
    let destination = Arc::new(Mutex::new(file));
    let stream = |mut input: Box<dyn Read + Send>, destination: Arc<Mutex<File>>| {
        thread::spawn(move || -> io::Result<()> {
            let mut buffer = [0; 8192];
            loop {
                let count = input.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                destination
                    .lock()
                    .map_err(|_| io::Error::other("log mutex poisoned"))?
                    .write_all(&buffer[..count])?;
                if verbose {
                    io::stderr().write_all(&buffer[..count])?;
                }
            }
            Ok(())
        })
    };
    let stdout = stream(Box::new(stdout), destination.clone());
    let stderr = stream(Box::new(stderr), destination);
    let status = child.wait().map_err(|e| e.to_string())?;
    for handle in [stdout, stderr] {
        handle
            .join()
            .map_err(|_| "log capture thread failed")?
            .map_err(|e| e.to_string())?;
    }
    if verbose {
        eprintln!("coverage: command log {}", log.display());
    }
    if !status.success() {
        let text = fs::read_to_string(&log).unwrap_or_default();
        let tail = text
            .lines()
            .rev()
            .take(30)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "{command:?} failed ({status}); log: {}\n{tail}",
            log.display()
        ));
    }
    Ok(())
}

pub(super) fn atomic_write(output: &Path, name: &str, data: &[u8]) -> Result<()> {
    let temporary = output.join(format!(".{name}.pending"));
    let mut file = File::create(&temporary).map_err(|e| e.to_string())?;
    file.write_all(data)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())?;
    fs::rename(temporary, output.join(name)).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Purpose: Keep coverage output out of scanned trees without modifying rejected destinations.
    /// Expected: Scanned roots and descendants fail; ignored subtrees and unrelated locations remain usable.
    #[test]
    fn output_locations_follow_snapshot_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        // A target component above the repository must not exempt scanned inputs.
        let root = dir.path().join("target/repository");
        fs::create_dir_all(&root).unwrap();
        for tree in ["doradb-storage", "doradb-bench", "tools/coverage", ".cargo"] {
            for suffix in ["", "out", "target/../out", "target-extra/report"] {
                let path = Path::new(tree).join(suffix);
                for requested in [&path, &root.join(&path)] {
                    let error = output_directory(&root, requested).unwrap_err();
                    assert!(
                        error.contains(&format!("snapshotted tree `{tree}`")),
                        "{error}"
                    );
                    assert!(error.contains("target/coverage"), "{error}");
                }
            }
        }
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);

        for path in [
            "target/coverage",
            "reports",
            "doradb-storage-extra/out",
            "tools/coverage-extra/out",
            "doradb-storage/nested/target/out",
            "doradb-bench/target/out",
            "tools/coverage/target/out",
            ".cargo/target/out",
            "doradb-storage/.git/out",
        ] {
            assert_eq!(
                output_directory(&root, Path::new(path)).unwrap(),
                root.join(path)
            );
        }
        let outside = dir.path().join("outside");
        assert_eq!(output_directory(&root, &outside).unwrap(), outside);
        assert_eq!(
            output_directory(&root, Path::new("../../outside")).unwrap(),
            outside
        );
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);

        let existing = root.join("doradb-storage/out");
        fs::create_dir_all(&existing).unwrap();
        fs::write(existing.join("coverage.json"), "previous manifest").unwrap();
        fs::write(existing.join("lcov.info"), "previous report").unwrap();
        assert!(output_directory(&root, &existing).is_err());
        assert_eq!(
            fs::read_to_string(existing.join("coverage.json")).unwrap(),
            "previous manifest"
        );
        assert_eq!(
            fs::read_to_string(existing.join("lcov.info")).unwrap(),
            "previous report"
        );
        assert!(!existing.join(".coverage.lock").exists());
    }

    /// Purpose: Resolve physical output ownership through aliases and missing path suffixes.
    /// Expected: Symlinks and parent components cannot bypass scanned trees or falsely reject ignored destinations.
    #[cfg(unix)]
    #[test]
    fn output_aliases_preserve_physical_ownership() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("repo");
        fs::create_dir_all(root.join("doradb-storage/src")).unwrap();
        fs::create_dir_all(root.join("doradb-storage/target")).unwrap();
        let outside = dir.path().join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(root.join("doradb-storage"), root.join("storage-alias")).unwrap();
        symlink(root.join("doradb-storage"), root.join("target")).unwrap();
        symlink(
            root.join("doradb-storage/target"),
            root.join("target-alias"),
        )
        .unwrap();
        symlink(&outside, root.join("outside-alias")).unwrap();
        for path in [
            "storage-alias/out",
            "storage-alias/src/../out",
            "missing/../storage-alias/out",
            "target/out",
        ] {
            let error = output_directory(&root, Path::new(path)).unwrap_err();
            assert!(
                error.contains("snapshotted tree `doradb-storage`"),
                "{path}: {error}"
            );
        }
        assert_eq!(
            output_directory(&root, Path::new("target-alias/out")).unwrap(),
            root.join("doradb-storage/target/out")
        );
        assert_eq!(
            output_directory(&root, Path::new("outside-alias/out")).unwrap(),
            outside.join("out")
        );
        // Resolve the symlink before '..', which leaves its physical parent.
        assert_eq!(
            output_directory(&root, Path::new("outside-alias/../out")).unwrap(),
            dir.path().join("out")
        );

        // Snapshot traversal also follows an aliased root itself.
        let cargo_config = dir.path().join("cargo-config");
        fs::create_dir(&cargo_config).unwrap();
        symlink(&cargo_config, root.join(".cargo")).unwrap();
        let error = output_directory(&root, &cargo_config.join("out")).unwrap_err();
        assert!(error.contains("snapshotted tree `.cargo`"), "{error}");
        assert!(!cargo_config.join("out").exists());
        assert!(!root.join("missing").exists());
    }

    /// Purpose: Keep generated coverage files out of fingerprints while retaining real source-change detection.
    /// Expected: Accepted ignored outputs leave snapshots stable and an adjacent production edit invalidates them.
    #[test]
    fn accepted_outputs_do_not_change_source_fingerprints() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for path in [
            "doradb-storage/src/lib.rs",
            "doradb-bench/src/main.rs",
            "tools/coverage/source.rs",
            ".cargo/config.toml",
        ] {
            let path = root.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "original\n").unwrap();
        }
        let before = snapshot(root).unwrap();
        assert_eq!(
            before.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                ".cargo/config.toml",
                "doradb-bench/src/main.rs",
                "doradb-storage/src/lib.rs",
                "tools/coverage/source.rs",
            ]
        );
        let expected = fingerprints(&before);
        for tree in ["doradb-storage", "doradb-bench", "tools/coverage", ".cargo"] {
            for skipped in ["target", ".git"] {
                let output =
                    output_directory(root, &Path::new(tree).join(skipped).join("coverage"))
                        .unwrap();
                let generated = output.join(".coverage-work/llvm/build");
                fs::create_dir_all(&generated).unwrap();
                fs::write(generated.join("generated.rs"), "fn generated() {}\n").unwrap();
                fs::write(generated.join("generated.toml"), "generated = true\n").unwrap();
            }
        }
        check_inputs(root, &expected).unwrap();
        fs::write(root.join("doradb-storage/src/lib.rs"), "changed\n").unwrap();
        let error = check_inputs(root, &expected).unwrap_err();
        assert!(error.contains("doradb-storage/src/lib.rs"), "{error}");
    }

    /// Purpose: Prevent concurrent writers and stale success after a failed replacement run.
    /// Expected: A second writer is rejected and invalidation removes completion and upload artifacts.
    #[test]
    fn locking_and_invalidation() {
        let dir = tempfile::tempdir().unwrap();
        let lock = OutputLock::acquire(dir.path(), true).unwrap();
        assert!(OutputLock::acquire(dir.path(), true).is_err());
        atomic_write(dir.path(), "coverage.json", b"old manifest").unwrap();
        atomic_write(dir.path(), "lcov.info", b"old report").unwrap();
        invalidate(dir.path()).unwrap();
        assert!(!dir.path().join("coverage.json").exists());
        assert!(!dir.path().join("lcov.info").exists());
        drop(lock);
        let reader = OutputLock::acquire(dir.path(), false).unwrap();
        assert!(OutputLock::acquire(dir.path(), false).is_ok());
        assert!(OutputLock::acquire(dir.path(), true).is_err());
        drop(reader);
        assert!(OutputLock::acquire(dir.path(), true).is_ok());
    }

    /// Purpose: Make provenance independent of checkout location and sensitive to input changes.
    /// Expected: Identical content is reusable elsewhere while additions and edits require regeneration.
    #[test]
    fn content_fingerprints_and_relocation() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        for dir in [first.path(), second.path()] {
            fs::write(dir.join("Cargo.toml"), "[workspace]\n").unwrap();
        }
        let expected = fingerprints(&snapshot(first.path()).unwrap());
        check_inputs(second.path(), &expected).unwrap();
        fs::write(second.path().join("Cargo.lock"), "new lock").unwrap();
        assert!(
            check_inputs(second.path(), &expected)
                .unwrap_err()
                .contains("Cargo.lock")
        );
    }

    /// Purpose: Keep workspace subprocesses on stable under a nightly script interpreter.
    /// Expected: Commands select stable explicitly and carry a stable toolchain environment.
    #[test]
    fn stable_command_contract() {
        let command = stable_cargo(Path::new("."));
        assert_eq!(command.get_args().collect::<Vec<_>>(), ["+stable"]);
        assert!(
            command
                .get_envs()
                .any(|(k, v)| k == "RUSTUP_TOOLCHAIN" && v == Some(std::ffi::OsStr::new("stable")))
        );
    }

    /// Purpose: Verify source filtering against stable LLVM mappings from a compiled test harness.
    /// Expected: Called production lines stay covered, uncalled production stays uncovered, and test hooks vanish.
    #[test]
    fn compiled_fixture_preserves_production_denominator() {
        use super::super::{lcov, source};
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("src")).unwrap();
        fs::create_dir(dir.path().join(".config")).unwrap();
        fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"coverage-fixture\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[workspace]\n").unwrap();
        fs::write(
            dir.path().join(".config/nextest.toml"),
            "[profile.ci]\nfail-fast = false\n[profile.ci.junit]\npath = \"junit.xml\"\n",
        )
        .unwrap();
        let text = "pub fn called() -> u32 {\n    #[cfg(test)]\n    let hook = 1;\n    #[cfg(test)]\n    assert_eq!(hook, 1);\n    7\n}\npub fn uncalled() -> u32 {\n    99\n}\n#[cfg(test)]\nfn helper() { assert_eq!(called(), 7); }\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn calls() { super::helper(); }\n}\n";
        fs::write(dir.path().join("src/lib.rs"), text).unwrap();
        capture(stable_cargo(dir.path()).args(["generate-lockfile", "--offline"])).unwrap();
        let version = capture(stable_rustc(dir.path()).arg("-vV")).unwrap();
        let host = version
            .lines()
            .find_map(|l| l.strip_prefix("host: "))
            .unwrap();
        let sysroot = capture(stable_rustc(dir.path()).args(["--print", "sysroot"])).unwrap();
        let llvm = Path::new(sysroot.trim())
            .join("lib/rustlib")
            .join(host)
            .join("bin");
        let output = dir.path().join("target/coverage");
        fs::create_dir_all(&output).unwrap();
        let raw = collect_coverage(dir.path(), &output, &llvm, false).unwrap();
        let roots = vec![Root {
            path: dir.path().join("src/lib.rs"),
            owner: "fixture:lib".into(),
            scope: None,
            configuration: Configuration::default(),
        }];
        let policies = source::index(
            dir.path(),
            &BTreeMap::from([("src/lib.rs".into(), text.into())]),
            roots,
        )
        .unwrap();
        let raw = lcov::parse(&raw, dir.path(), &policies).unwrap();
        let (report, _, totals) = lcov::filter(&raw, &policies).unwrap();
        let lines = &report["src/lib.rs"].lines;
        assert_eq!(
            lines.keys().copied().collect::<Vec<_>>(),
            [1, 6, 7, 8, 9, 10]
        );
        for line in [1, 6, 7] {
            assert!(lines[&line] > 0);
        }
        for line in [8, 9, 10] {
            assert_eq!(lines[&line], 0);
        }
        assert!(totals.removed.covered > 0);
        assert!(dir.path().join("target/nextest/ci/junit.xml").is_file());
    }

    /// Purpose: Preserve actionable failures and refuse cleaning directories not owned by coverage.
    /// Expected: Failed commands retain stderr and exit diagnostics; foreign build contents remain untouched.
    #[test]
    fn failure_logs_and_owned_cleaning() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("failure.log");
        let error = logged(
            Command::new("sh").args(["-c", "echo intentional-fixture-failure >&2; exit 7"]),
            &log,
            false,
        )
        .unwrap_err();
        assert!(error.contains("intentional-fixture-failure"));
        assert!(error.contains("exit status: 7"));
        assert!(
            fs::read_to_string(&log)
                .unwrap()
                .contains("intentional-fixture-failure")
        );
        let work = dir.path().join(".coverage-work");
        fs::create_dir(&work).unwrap();
        fs::write(work.join("keep"), "foreign cache").unwrap();
        let error =
            collect_coverage(dir.path(), dir.path(), Path::new("/unused"), false).unwrap_err();
        assert!(error.contains("unowned"));
        assert_eq!(
            fs::read_to_string(work.join("keep")).unwrap(),
            "foreign cache"
        );
    }
}
