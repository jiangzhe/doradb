use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use std::result;

pub(super) const SCHEMA: u32 = 1;

pub(super) type Result<T> = result::Result<T, String>;
pub(super) type CoverageReport = BTreeMap<String, FileCoverage>;
pub(super) type SourceIndex = BTreeMap<String, FilePolicy>;

/// Half-open positions use one-based lines and zero-based Unicode scalar columns.
/// proc-macro2 uses this convention, including CRLF as a single line ending.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(super) struct Position {
    pub(super) line: usize,
    pub(super) column: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(super) struct ExcludedRange {
    pub(super) start: Position,
    pub(super) end: Position,
    pub(super) reason: String,
    pub(super) owner: String,
}

impl ExcludedRange {
    pub(super) fn contains(&self, start: Position, end: Position) -> bool {
        self.start <= start && end <= self.end
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct FilePolicy {
    pub(super) digest: String,
    pub(super) line_count: u32,
    pub(super) owners: BTreeSet<String>,
    pub(super) whole_file: Option<String>,
    pub(super) exclusions: Vec<ExcludedRange>,
    /// Only excluded or ambiguous lines are stored. Missing lines are eligible.
    pub(super) lines: BTreeMap<u32, LinePolicy>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum LinePolicy {
    Excluded,
    Mixed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct FileCoverage {
    pub(super) lines: BTreeMap<u32, u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Totals {
    pub(super) covered: usize,
    pub(super) uncovered: usize,
}

impl Totals {
    pub(super) fn lines(self) -> usize {
        self.covered + self.uncovered
    }

    pub(super) fn add(&mut self, other: Self) {
        self.covered += other.covered;
        self.uncovered += other.uncovered;
    }

    pub(super) fn from_file(file: &FileCoverage) -> Self {
        let covered = file.lines.values().filter(|&&count| count > 0).count();
        Self {
            covered,
            uncovered: file.lines.len() - covered,
        }
    }

    pub(super) fn from_report(report: &CoverageReport) -> Self {
        let mut total = Self::default();
        for file in report.values() {
            total.add(Self::from_file(file));
        }
        total
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Counts {
    pub(super) raw: Totals,
    pub(super) removed: Totals,
    pub(super) retained: Totals,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct RemovedLine {
    pub(super) count: u64,
    pub(super) reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct FileAudit {
    pub(super) counts: Counts,
    pub(super) removed: BTreeMap<u32, RemovedLine>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RunContext {
    pub(super) revision: String,
    pub(super) versions: BTreeMap<String, String>,
    pub(super) target: String,
    pub(super) target_cfg: BTreeSet<String>,
    pub(super) features: BTreeMap<String, BTreeSet<String>>,
    pub(super) build_profile: String,
    pub(super) nextest_profile: String,
    pub(super) inputs: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub(super) schema: u32,
    pub(super) complete: bool,
    pub(super) collection_root: String,
    pub(super) context: RunContext,
    pub(super) raw_digest: String,
    pub(super) report_digest: String,
    pub(super) sources: SourceIndex,
    pub(super) files: BTreeMap<String, FileAudit>,
    pub(super) counts: Counts,
}

pub(super) fn digest(bytes: impl AsRef<[u8]>) -> String {
    blake3::hash(bytes.as_ref()).to_hex().to_string()
}

/// Resolve lexical aliases without permitting relative paths to leave the root.
pub(super) fn normalize(root: &Path, path: &Path) -> Result<PathBuf> {
    let mut result = if path.is_absolute() {
        PathBuf::new()
    } else {
        root.to_path_buf()
    };
    for part in path.components() {
        match part {
            Component::ParentDir => {
                if (!path.is_absolute() && result == root) || !result.pop() {
                    return Err(format!("path escapes collection root: {}", path.display()));
                }
            }
            Component::CurDir => {}
            part => result.push(part.as_os_str()),
        }
    }
    Ok(result)
}

pub(super) fn repo_path(root: &Path, path: &Path) -> Result<String> {
    path.strip_prefix(root)
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .map_err(|_| format!("source is outside repository: {}", path.display()))
}
