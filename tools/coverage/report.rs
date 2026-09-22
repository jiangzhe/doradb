use super::model::{CoverageReport, Result, SourceIndex, Totals, repo_path};
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug)]
pub(super) struct Selection {
    pub(super) path: String,
    pub(super) directory: bool,
}

impl Selection {
    fn matches(&self, file: &str) -> bool {
        file == self.path
            || (self.directory
                && (self.path.is_empty()
                    || file
                        .strip_prefix(&self.path)
                        .is_some_and(|suffix| suffix.starts_with('/'))))
    }
}

pub(super) fn selections(
    root: &Path,
    paths: &[String],
    sources: &SourceIndex,
) -> Result<Vec<Selection>> {
    let mut seen = BTreeSet::new();
    let mut selections = Vec::new();
    for path in paths {
        let full = root
            .join(path)
            .canonicalize()
            .map_err(|e| format!("target does not exist: {path}: {e}"))?;
        let relative = repo_path(root, &full)?;
        if !seen.insert(relative.clone()) {
            return Err(format!("duplicate canonical target: {path}"));
        }
        let selection = Selection {
            path: relative,
            directory: full.is_dir(),
        };
        if !sources.keys().any(|p| selection.matches(p)) {
            return Err(format!(
                "unrelated or uninstrumented target: {path}; no known workspace source ownership"
            ));
        }
        selections.push(selection);
    }
    Ok(selections)
}

fn percentage(totals: Totals) -> String {
    if totals.lines() == 0 {
        "N/A: no production executable lines".into()
    } else {
        format!(
            "{:.2}%",
            100.0 * totals.covered as f64 / totals.lines() as f64
        )
    }
}

fn line_ranges(lines: &[u32]) -> String {
    let mut result = Vec::new();
    let mut at = 0;
    while at < lines.len() {
        let first = lines[at];
        let mut last = first;
        at += 1;
        while at < lines.len() && lines[at] == last + 1 {
            last = lines[at];
            at += 1;
        }
        result.push(if last == first {
            first.to_string()
        } else {
            format!("{first}-{last}")
        });
    }
    result.join(", ")
}

pub(super) fn render(report: &CoverageReport, selections: &[Selection], top: usize) -> String {
    let included = |path: &str| selections.is_empty() || selections.iter().any(|s| s.matches(path));
    let files: CoverageReport = report
        .iter()
        .filter(|(path, _)| included(path))
        .map(|(p, f)| (p.clone(), f.clone()))
        .collect();
    let totals = Totals::from_report(&files);
    let mut result = format!(
        "# Production line coverage\n\nBuild: stable, default-feature iouring workspace; test build, nextest ci.\nMetric: distinct retained LLVM executable lines; covered iff count > 0.\n\nCombined: {} — {} covered, {} uncovered, {} lines across {} files.\n",
        percentage(totals),
        totals.covered,
        totals.uncovered,
        totals.lines(),
        files.len()
    );
    if !selections.is_empty() {
        result.push_str("\n| Target | Covered | Uncovered | Lines | Coverage |\n| --- | ---: | ---: | ---: | ---: |\n");
        for selection in selections {
            let subset = files
                .iter()
                .filter(|(path, _)| selection.matches(path))
                .map(|(p, f)| (p.clone(), f.clone()))
                .collect();
            let total = Totals::from_report(&subset);
            result.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                if selection.path.is_empty() {
                    "."
                } else {
                    &selection.path
                },
                total.covered,
                total.uncovered,
                total.lines(),
                percentage(total)
            ));
        }
    }
    let mut rows: Vec<_> = files
        .iter()
        .map(|(path, file)| (path, Totals::from_file(file), file))
        .collect();
    rows.sort_by(|a, b| {
        (a.1.covered as u128 * b.1.lines() as u128)
            .cmp(&(b.1.covered as u128 * a.1.lines() as u128))
            .then_with(|| b.1.lines().cmp(&a.1.lines()))
            .then_with(|| a.0.cmp(b.0))
    });
    result.push_str("\n| File | Covered | Uncovered | Lines | Coverage |\n| --- | ---: | ---: | ---: | ---: |\n");
    for (path, total, _) in &rows {
        result.push_str(&format!(
            "| {path} | {} | {} | {} | {} |\n",
            total.covered,
            total.uncovered,
            total.lines(),
            percentage(*total)
        ));
    }
    rows.sort_by(|a, b| b.1.uncovered.cmp(&a.1.uncovered).then_with(|| a.0.cmp(b.0)));
    result.push_str("\nUncovered-line hotspots:\n\n");
    for (path, total, file) in rows
        .into_iter()
        .filter(|(_, total, _)| total.uncovered > 0)
        .take(top)
    {
        let lines: Vec<_> = file
            .lines
            .iter()
            .filter_map(|(&line, &count)| (count == 0).then_some(line))
            .collect();
        result.push_str(&format!(
            "- {path}: {} uncovered — {}\n",
            total.uncovered,
            line_ranges(&lines)
        ));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::model::{FileCoverage, FilePolicy};
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;

    /// Purpose: Deduplicate overlapping targets while using path-component boundaries.
    /// Expected: Combined counts include each selected file once and exclude similarly prefixed siblings.
    #[test]
    fn overlapping_targets_and_hotspots() {
        let files = BTreeMap::from([
            (
                "src/io/a.rs".into(),
                FileCoverage {
                    lines: BTreeMap::from([(1, 1), (4, 0), (5, 0)]),
                },
            ),
            (
                "src/io_other/b.rs".into(),
                FileCoverage {
                    lines: BTreeMap::from([(1, 0)]),
                },
            ),
        ]);
        let selected = [
            Selection {
                path: "src/io".into(),
                directory: true,
            },
            Selection {
                path: "src/io/a.rs".into(),
                directory: false,
            },
        ];
        let rendered = render(&files, &selected, 10);
        assert!(rendered.contains("1 covered, 2 uncovered, 3 lines across 1 files"));
        assert!(rendered.contains("src/io/a.rs: 2 uncovered — 4-5"));
        assert!(!rendered.contains("io_other"));
    }

    /// Purpose: Distinguish known zero-denominator targets from erroneous selections.
    /// Expected: Known empty targets show N/A; duplicate aliases and unrelated targets fail.
    #[test]
    fn empty_and_invalid_selections() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("tests.rs"), "").unwrap();
        fs::write(dir.path().join("other.rs"), "").unwrap();
        let sources = BTreeMap::from([("tests.rs".into(), FilePolicy::default())]);
        let selected = selections(dir.path(), &["tests.rs".into()], &sources).unwrap();
        assert!(
            render(&BTreeMap::new(), &selected, 10).contains("N/A: no production executable lines")
        );
        assert!(
            selections(
                dir.path(),
                &["tests.rs".into(), "./tests.rs".into()],
                &sources
            )
            .unwrap_err()
            .contains("duplicate")
        );
        assert!(
            selections(dir.path(), &["other.rs".into()], &sources)
                .unwrap_err()
                .contains("uninstrumented")
        );
    }
}
