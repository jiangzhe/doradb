use super::model::{
    Counts, CoverageReport, FileAudit, FileCoverage, LinePolicy, RemovedLine, Result, SourceIndex,
    Totals, normalize,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

#[derive(Default)]
struct Record {
    path: String,
    lines: BTreeMap<u32, u64>,
    found: Option<usize>,
    hit: Option<usize>,
}

pub(super) fn parse(text: &str, root: &Path, sources: &SourceIndex) -> Result<CoverageReport> {
    let mut report = CoverageReport::new();
    let mut record: Option<Record> = None;
    for (offset, line) in text.lines().enumerate() {
        let result = parse_line(line, root, sources, &mut record, &mut report);
        result.map_err(|e| format!("LCOV line {}: {e}", offset + 1))?;
    }
    if record.is_some() {
        return Err("LCOV missing end_of_record".into());
    }
    if report.is_empty() {
        return Err("LCOV has no source records".into());
    }
    Ok(report)
}

fn parse_line(
    line: &str,
    root: &Path,
    sources: &SourceIndex,
    current: &mut Option<Record>,
    report: &mut CoverageReport,
) -> Result<()> {
    if line == "end_of_record" {
        let record = current.take().ok_or("end_of_record without SF")?;
        // LLVM summaries include expansion regions that need not appear as DA lines.
        // Validate their shape, but derive our metric exclusively from distinct DA records.
        if !matches!((record.found, record.hit), (Some(found), Some(hit)) if hit <= found) {
            return Err(format!("{}: missing or inconsistent LF/LH", record.path));
        }
        let file = report.entry(record.path).or_default();
        for (line, count) in record.lines {
            file.lines
                .entry(line)
                .and_modify(|old| *old = (*old).max(count))
                .or_insert(count);
        }
        return Ok(());
    }
    let (kind, value) = line
        .split_once(':')
        .ok_or_else(|| format!("unsupported record `{line}`"))?;
    if kind == "TN" && current.is_none() {
        return Ok(());
    }
    if kind == "SF" {
        if current.is_some() || value.is_empty() {
            return Err("nested or empty SF record".into());
        }
        let path = normalize(root, Path::new(value))?;
        let path = match path.strip_prefix(root) {
            Ok(relative) => relative.to_string_lossy().replace('\\', "/"),
            Err(_) => format!("@external:{}", path.display()),
        };
        *current = Some(Record {
            path,
            ..Record::default()
        });
        return Ok(());
    }
    let record = current
        .as_mut()
        .ok_or_else(|| format!("{kind} before SF"))?;
    match kind {
        "DA" => {
            let parts: Vec<_> = value.split(',').collect();
            if !(2..=3).contains(&parts.len()) || parts.last() == Some(&"") {
                return Err("malformed DA record".into());
            }
            let line = integer::<u32>(parts[0])?;
            let count = integer::<u64>(parts[1])?;
            if line == 0
                || sources
                    .get(&record.path)
                    .is_some_and(|p| line > p.line_count)
            {
                return Err(format!(
                    "{}: executable line {line} outside source bounds",
                    record.path
                ));
            }
            record
                .lines
                .entry(line)
                .and_modify(|old| *old = (*old).max(count))
                .or_insert(count);
        }
        "LF" | "LH" => {
            let field = if kind == "LF" {
                &mut record.found
            } else {
                &mut record.hit
            };
            if field.replace(integer(value)?).is_some() {
                return Err(format!("duplicate {kind}"));
            }
        }
        // LLVM function/branch records are intentionally absent from the canonical metric.
        "FN" | "FNDA" => {
            let (number, name) = value.split_once(',').ok_or("malformed function record")?;
            integer::<u64>(number)?;
            if name.is_empty() {
                return Err("empty function name".into());
            }
        }
        "FNF" | "FNH" | "BRF" | "BRH" => {
            integer::<u64>(value)?;
        }
        "BRDA" => {
            let fields: Vec<_> = value.split(',').collect();
            if fields.len() != 4 {
                return Err("malformed BRDA".into());
            }
            for field in &fields[..3] {
                integer::<u64>(field)?;
            }
            if fields[3] != "-" {
                integer::<u64>(fields[3])?;
            }
        }
        _ => return Err(format!("unsupported LCOV field `{kind}`")),
    }
    Ok(())
}

fn integer<T: FromStr>(value: &str) -> Result<T> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("invalid unsigned integer `{value}`"));
    }
    value
        .parse()
        .map_err(|_| format!("integer overflow `{value}`"))
}

pub(super) fn filter(
    raw: &CoverageReport,
    sources: &SourceIndex,
) -> Result<(CoverageReport, BTreeMap<String, FileAudit>, Counts)> {
    let mut filtered = CoverageReport::new();
    let mut audit = BTreeMap::new();
    let mut counts = Counts::default();
    for (path, file) in raw {
        let policy = sources.get(path);
        let scope_reason = if path.starts_with("@external:") {
            Some("out of scope: external source")
        } else if path.starts_with("target/")
            || path.split('/').any(|part| part == ".coverage-work")
        {
            Some("out of scope: generated artifact")
        } else {
            None
        };
        if policy.is_none() && scope_reason.is_none() {
            return Err(format!(
                "unknown in-scope source ownership: {path}; extend the source index"
            ));
        }
        let mut retained = FileCoverage::default();
        let mut file_audit = FileAudit::default();
        file_audit.counts.raw = Totals::from_file(file);
        for (&line, &count) in &file.lines {
            let mut reason = scope_reason.map(str::to_string);
            if let Some(policy) = policy {
                if let Some(whole) = &policy.whole_file {
                    reason = Some(whole.clone());
                } else {
                    match policy.lines.get(&line) {
                        Some(LinePolicy::Mixed) => {
                            let implicated: Vec<_> = policy
                                .exclusions
                                .iter()
                                .filter(|r| {
                                    r.start.line <= line as usize && r.end.line >= line as usize
                                })
                                .collect();
                            return Err(format!(
                                "{path}:{line}: mixed production/test token ownership cannot be separated by LCOV; place the test-only node on separate lines; ranges: {implicated:?}"
                            ));
                        }
                        Some(LinePolicy::Excluded) => {
                            reason = Some("test-only/inactive source; see exclusion ranges".into())
                        }
                        None => {}
                    }
                }
            }
            if let Some(reason) = reason {
                file_audit
                    .removed
                    .insert(line, RemovedLine { count, reason });
            } else {
                retained.lines.insert(line, count);
            }
        }
        file_audit.counts.retained = Totals::from_file(&retained);
        file_audit.counts.removed = Totals {
            covered: file_audit.counts.raw.covered - file_audit.counts.retained.covered,
            uncovered: file_audit.counts.raw.uncovered - file_audit.counts.retained.uncovered,
        };
        counts.raw.add(file_audit.counts.raw);
        counts.removed.add(file_audit.counts.removed);
        counts.retained.add(file_audit.counts.retained);
        audit.insert(path.clone(), file_audit);
        if !retained.lines.is_empty() {
            filtered.insert(path.clone(), retained);
        }
    }
    if filtered.is_empty() {
        return Err("empty production coverage; no executable lines remain".into());
    }
    Ok((filtered, audit, counts))
}

pub(super) fn serialize(report: &CoverageReport) -> String {
    let mut result = String::new();
    for (path, file) in report {
        if file.lines.is_empty() {
            continue;
        }
        result.push_str(&format!("SF:{path}\n"));
        for (line, count) in &file.lines {
            result.push_str(&format!("DA:{line},{count}\n"));
        }
        let totals = Totals::from_file(file);
        result.push_str(&format!(
            "LF:{}\nLH:{}\nend_of_record\n",
            totals.lines(),
            totals.covered
        ));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::model::FilePolicy;
    use super::*;

    fn sources() -> SourceIndex {
        BTreeMap::from([(
            "src/lib.rs".into(),
            FilePolicy {
                line_count: 5,
                lines: BTreeMap::from([(4, LinePolicy::Excluded), (5, LinePolicy::Excluded)]),
                ..FilePolicy::default()
            },
        )])
    }

    /// Purpose: Correct duplicate denominators while keeping uncovered production lines.
    /// Expected: Canonical records retain exact production hits and zeroes with recomputed integer totals.
    #[test]
    fn deduplication_filtering_and_serialization() {
        let text = "SF:/repo/src/lib.rs\nFN:1,f\nFNDA:1,f\nFNF:1\nFNH:1\nDA:1,0,checksum\nDA:2,0\nDA:4,8\nDA:5,0\nLF:7\nLH:2\nBRDA:1,0,0,-\nBRF:1\nBRH:0\nend_of_record\nSF:src/./lib.rs\nDA:1,3\nLF:1\nLH:1\nend_of_record\n";
        let raw = parse(text, Path::new("/repo"), &sources()).unwrap();
        let (report, audit, counts) = filter(&raw, &sources()).unwrap();
        assert_eq!(report["src/lib.rs"].lines, BTreeMap::from([(1, 3), (2, 0)]));
        assert_eq!(
            counts.raw,
            Totals {
                covered: 2,
                uncovered: 2
            }
        );
        assert_eq!(
            counts.removed,
            Totals {
                covered: 1,
                uncovered: 1
            }
        );
        assert_eq!(audit["src/lib.rs"].removed.len(), 2);
        assert_eq!(
            serialize(&report),
            "SF:src/lib.rs\nDA:1,3\nDA:2,0\nLF:2\nLH:1\nend_of_record\n"
        );
    }

    /// Purpose: Prevent malformed exports from silently improving coverage.
    /// Expected: Missing fields, invalid counts, truncated records, escapes and bad bounds fail.
    #[test]
    fn strict_lcov_validation() {
        for text in [
            "DA:1,1\n",
            "SF:src/lib.rs\nDA:1,1\nLF:1\nLH:1\n",
            "SF:src/lib.rs\nDA:0,1\nLF:1\nLH:1\nend_of_record\n",
            "SF:src/lib.rs\nDA:6,1\nLF:1\nLH:1\nend_of_record\n",
            "SF:src/lib.rs\nDA:1,-1\nLF:1\nLH:0\nend_of_record\n",
            "SF:src/lib.rs\nDA:1,1\nLF:1\nLH:2\nend_of_record\n",
            "SF:src/lib.rs\nDA:1,1\nend_of_record\n",
            "SF:../../lib.rs\n",
            "SF:src/lib.rs\nNEW:1\n",
        ] {
            assert!(
                parse(text, Path::new("/repo"), &sources()).is_err(),
                "accepted {text}"
            );
        }
    }

    /// Purpose: Distinguish external coverage and ambiguous source from valid production ownership.
    /// Expected: External lines are audited, unknown local files and mixed lines fail closed.
    #[test]
    fn scope_and_ambiguous_lines() {
        let mut raw = BTreeMap::from([
            (
                "src/lib.rs".into(),
                FileCoverage {
                    lines: BTreeMap::from([(1, 0)]),
                },
            ),
            (
                "@external:/dependency/lib.rs".into(),
                FileCoverage {
                    lines: BTreeMap::from([(1, 5)]),
                },
            ),
        ]);
        let (report, _, counts) = filter(&raw, &sources()).unwrap();
        assert_eq!(report.len(), 1);
        assert_eq!(counts.removed.covered, 1);
        raw.insert("unknown.rs".into(), FileCoverage::default());
        assert!(
            filter(&raw, &sources())
                .unwrap_err()
                .contains("unknown in-scope")
        );
        raw.remove("unknown.rs");
        let mut policy = sources();
        policy
            .get_mut("src/lib.rs")
            .unwrap()
            .lines
            .insert(1, LinePolicy::Mixed);
        assert!(
            filter(&raw, &policy)
                .unwrap_err()
                .contains("mixed production/test")
        );
    }
}
